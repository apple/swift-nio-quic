//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2026 Apple Inc. and the SwiftNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Logging
import NIOCore
import X509

@available(anyAppleOS 26, *)
private enum MultiplexerContinuation {
    case connectionMultiplexerContinuation(any ConnectionMultiplexerContinuation)
    case closure(
        connectionInitializer: @Sendable (any Channel, QUICStreamCreator) -> EventLoopFuture<Void>,
        inboundStreamInitializer: @Sendable (any Channel) -> EventLoopFuture<Void>,
        finish: @Sendable () -> Void,
        role: Role
    )
}

/// A handler for QUIC connections.
/// Add this to a UDP channel.
/// It can multiplex multiple QUIC connections.
@available(anyAppleOS 26, *)
public final class QUICHandler {
    private enum State {
        case accepting
        case shuttingDown(EventLoopPromise<Void>)
        case shutdown(EventLoopFuture<Void>)
    }

    /// The channel this handler resides in.
    public let udpChannel: any Channel

    /// The QUIC configuration.
    private let quicConfiguration: QUICConfiguration

    /// A registry of connections.
    private var connectionRegistry: ConnectionRegistry<QUICConnectionChannel.TransportView>

    /// How new connections are surfaced to the user: either a multiplexer continuation or a pair
    /// of initializer closures.
    private var multiplexerContinuation: MultiplexerContinuation?
    /// The event loop of the channel we are added to.
    private let eventLoop: any EventLoop
    /// The logger used everywhere.
    private let logger: Logger
    /// The generator used for creating source connection IDs for new connections.
    private var quicConnectionIDGenerator: any QUICConnectionIDGenerator
    /// Our current state.
    private var state: State = .accepting
    /// Boolean to indicate if we wrote something.
    private var didWrite = false
    /// Whether we're expecting a channelReadComplete. This is used to delay flushing the channel until the a read complete is received.
    private var expectingChannelReadComplete: Bool = false
    /// The context of the channel handler.
    private var context: ChannelHandlerContext?
    /// The next connection handle to use. Don't use directly, use `nextConnectionHandle()` instead.
    private var connectionHandle: ConnectionHandle
    /// Verifies the server identitfy.
    private var asyncVerifierRunner: AsyncVerifierRunner?
    /// Provide certificates and signature to authenticate.
    private var authenticator: Authenticator?

    private func nextConnectionHandle() -> ConnectionHandle {
        let handle = self.connectionHandle
        self.connectionHandle.formNext()
        return handle
    }

    /// Creates a new ``QUICHandler`` and a ``QUICHandler/ConnectionMultiplexer``.
    ///
    /// - Parameters:
    ///   - channel: The channel this handler resides in.
    ///   - QUICConfiguration: The quic configuration to use for this handler.
    ///   - logger: The logger.
    ///   - inboundStreamChannelInitializer: A closure called for any new inbound stream.
    /// - Returns: The handler and the connection multiplexer.
    public static func makeHandlerAndConnectionMultiplexer<Output: Sendable>(
        channel: any Channel,
        quicConfiguration: QUICConfiguration,
        logger: Logger,
        inboundStreamChannelInitializer: @Sendable @escaping (any Channel) -> EventLoopFuture<Output>
    ) throws -> (QUICHandler, QUICHandler.ConnectionMultiplexer<Output>) {
        try self.makeHandlerAndConnectionMultiplexer(
            channel: channel,
            quicConfiguration: quicConfiguration,
            logger: logger,
            inboundStreamChannelInitializer: inboundStreamChannelInitializer,
            quicConnectionIDGenerator: RandomQUICConnectionIDGenerator()
        )
    }

    /// Creates a new ``QUICHandler`` and a ``QUICHandler/ConnectionMultiplexer``.
    ///
    /// - Parameters:
    ///   - channel: The channel this handler resides in.
    ///   - QUICConfiguration: The quic configuration to use for this handler.
    ///   - logger: The logger.
    ///   - inboundStreamChannelInitializer: A closure called for any new inbound stream.
    ///   - quicConnectionIDGenerator: The generator used for creating source connection IDs.
    /// - Returns: The handler and the connection multiplexer.
    public static func makeHandlerAndConnectionMultiplexer<Output: Sendable>(
        channel: any Channel,
        quicConfiguration: QUICConfiguration,
        logger: Logger,
        inboundStreamChannelInitializer: @Sendable @escaping (any Channel) -> EventLoopFuture<Output>,
        quicConnectionIDGenerator: any QUICConnectionIDGenerator
    ) throws -> (QUICHandler, QUICHandler.ConnectionMultiplexer<Output>) {

        // If this is a client using certificates (the raw public key configuration paths are not set), we need to
        // create a verifier for the TLS handshake. At the moment there is no mTLS support, so only
        // clients need to do this.
        //
        // See also: https://github.com/apple/swift-nio-quic/issues/5
        let asyncVerifier: AsyncVerifier?
        if quicConfiguration.role == .client,
            let verifierConfiguration = quicConfiguration.verificationConfiguration,
            case .x509Certificates(trustRootsFilePath: let trustRootsPath) = verifierConfiguration
        {
            if let trustRootsPath {
                asyncVerifier = try AsyncVerifier(
                    trustRootsPath: trustRootsPath,
                    certificateVerification: quicConfiguration.peerCertificateVerification,
                    eventLoop: channel.eventLoop
                )
            } else {
                asyncVerifier = AsyncVerifier(
                    certificateVerification: quicConfiguration.peerCertificateVerification,
                    eventLoop: channel.eventLoop
                )
            }
        } else {
            asyncVerifier = nil
        }

        let authenticator: Authenticator?
        if quicConfiguration.role == .server,
            let authenticatorConfiguration = quicConfiguration.authenticationConfiguration,
            case .x509Certificates(
                certificateChainFilePath: let certificateChainFilePath,
                privateKeyFilePath: let privateKeyFilePath
            ) = authenticatorConfiguration
        {
            authenticator = try Authenticator(
                certificateFilePath: certificateChainFilePath,
                privateKeyFilePath: privateKeyFilePath
            )
        } else {
            authenticator = nil
        }

        let handler = QUICHandler(
            channel: channel,
            quicConfiguration: quicConfiguration,
            asyncVerifier: asyncVerifier,
            authenticator: authenticator,
            logger: logger,
            quicConnectionIDGenerator: quicConnectionIDGenerator
        )

        let multiplexer = ConnectionMultiplexer(
            eventLoop: channel.eventLoop,
            role: quicConfiguration.role,
            inboundStreamInitializer: inboundStreamChannelInitializer,
            createNewConnection: NIOLoopBound(handler.createNewConnection, eventLoop: channel.eventLoop)
        )

        handler.multiplexerContinuation = .connectionMultiplexerContinuation(multiplexer)

        return (handler, multiplexer)
    }

    /// Initialises a new QUIC handler.
    ///
    /// - Parameters:
    ///   - channel: The channel this handler resides in.
    ///   - quicConfiguration: The quic configuration to use for this handler.
    ///   - asyncVerifier: Callback provider for SwiftTLS certificate verification.
    ///   - logger: The logger.
    init(
        channel: any Channel,
        quicConfiguration: QUICConfiguration,
        asyncVerifier: AsyncVerifier?,
        authenticator: Authenticator?,
        logger: Logger,
        quicConnectionIDGenerator: any QUICConnectionIDGenerator = RandomQUICConnectionIDGenerator()
    ) {
        self.udpChannel = channel
        self.eventLoop = channel.eventLoop
        self.quicConfiguration = quicConfiguration
        self.logger = logger
        self.quicConnectionIDGenerator = quicConnectionIDGenerator
        if let asyncVerifier {
            self.asyncVerifierRunner = .init(asyncVerifier: asyncVerifier)
        }
        self.authenticator = authenticator
        self.connectionHandle = .initial
        self.connectionRegistry = ConnectionRegistry()
    }

    /// Initialises a new ``QUICHandler``.
    ///
    /// - Parameters:
    ///   - channel: The channel this handler resides in.
    ///   - quicConfiguration: The quic configuration to use for this handler.
    ///   - asyncVerifier: Callback provider for SwiftTLS certificate verification.
    ///   - authenticator: Authenticator for SwiftTLS certificate verification.
    ///   - logger: The logger.
    ///   - inboundConnectionInitializer: Called for every incoming connection and allows you to add handlers.
    ///   - inboundStreamInitializer: Called for every incoming stream on inbound connections and allows you to add handlers. This isn't called for inbound streams on outbound connections.
    ///   - noMoreConnections: Called when this handler becomes inactive. After this, `inboundConnectionInitializer` won't be called again.
    public init(
        channel: any Channel,
        quicConfiguration: QUICConfiguration,
        asyncVerifier: AsyncVerifier?,
        authenticator: Authenticator?,
        logger: Logger,
        inboundConnectionInitializer: @escaping @Sendable (any Channel, QUICStreamCreator) -> EventLoopFuture<Void>,
        inboundStreamInitializer: @escaping @Sendable (any Channel) -> EventLoopFuture<Void>,
        noMoreConnections: @escaping @Sendable () -> Void,
        quicConnectionIDGenerator: any QUICConnectionIDGenerator = RandomQUICConnectionIDGenerator()
    ) {
        self.udpChannel = channel
        self.eventLoop = channel.eventLoop
        self.quicConfiguration = quicConfiguration
        self.logger = logger
        self.quicConnectionIDGenerator = quicConnectionIDGenerator
        self.multiplexerContinuation = .closure(
            connectionInitializer: inboundConnectionInitializer,
            inboundStreamInitializer: inboundStreamInitializer,
            finish: noMoreConnections,
            role: quicConfiguration.role
        )
        if let asyncVerifier {
            self.asyncVerifierRunner = .init(asyncVerifier: asyncVerifier)
        }
        self.authenticator = authenticator
        self.connectionHandle = .initial
        self.connectionRegistry = ConnectionRegistry()
    }

    /// Create a new outbound QUIC connection.
    /// - Parameters:
    ///   - serverName: The server to connect to.
    ///   - remoteAddress: The address to connect to.
    ///   - connectionInitializer: How to initialize the connection. This closure will be called with a channel and a stream creator.
    ///   - inboundStreamInitializer: How to initialize any inbound streams on the new connection. This closure is
    ///     called with each new inbound stream channel; it is the only place inbound streams are surfaced. Add your per-stream handlers here.
    /// - Returns: The initialized connection channel and a stream creator to open outbound streams
    ///     on it with.
    public func createOutboundConnection(
        serverName: String,
        remoteAddress: SocketAddress,
        connectionInitializer: @escaping @Sendable (any Channel, QUICStreamCreator) -> EventLoopFuture<Void>,
        inboundStreamInitializer: @escaping @Sendable (any Channel) -> EventLoopFuture<Void>
    ) -> EventLoopFuture<(any Channel, QUICStreamCreator)> {
        let promise = self.eventLoop.makePromise(of: (QUICConnectionChannel, QUICStreamCreator).self)

        do {
            try self.createNewConnection(
                promise: promise,
                serverName: serverName,
                remoteAddress: remoteAddress
            ) { channel, streamCreator in
                channel.setInboundStreamInitializer(.closure(inboundStreamInitializer))
                return connectionInitializer(channel, streamCreator)
            }
        } catch {
            promise.fail(error)
        }

        return promise.futureResult.map { channel, streamCreator in (channel, streamCreator) }
    }

    /// Shuts the server down gracefully.
    ///
    /// - Parameters:
    ///     - deadline: Deadline until connections are closed gracefully. Afterwards they will be forcibly closed.
    /// - Returns: A future that is notified once the server is closed.
    public func shutdownGracefully(deadline: NIODeadline) -> EventLoopFuture<Void> {
        let promise = self.eventLoop.makePromise(of: Void.self)
        self.shutdownGracefully(deadline: deadline, promise: promise)
        return promise.futureResult
    }

    private func shutdownGracefully(deadline: NIODeadline, promise: EventLoopPromise<Void>) {
        self.eventLoop.assertInEventLoop()

        switch self.state {
        case .shutdown(let shutdownFuture):
            self.logger.trace("QUICHandler is already shutdown")
            shutdownFuture.cascade(to: promise)
        case .shuttingDown(let shutdownPromise):
            self.logger.trace("QUICHandler is already trying to shutdown gracefully")
            shutdownPromise.futureResult.cascade(to: promise)
        case .accepting:
            self.logger.trace("QUICHandler is trying to shut down gracefully")
            let internalPromise = self.eventLoop.makePromise(of: Void.self).assumeIsolated()
            internalPromise.futureResult.nonisolated().cascade(to: promise)
            self.state = .shuttingDown(internalPromise.nonisolated())

            internalPromise.futureResult.whenComplete { result in
                self.didShutDown(result)
            }

            self.connectionRegistry.shutdownConnections(
                deadline: deadline,
                promise: internalPromise
            )
        }
    }

    private func didShutDown(_ result: Result<Void, any Error>) {
        self.eventLoop.assertInEventLoop()
        switch self.state {
        case .accepting, .shutdown:
            preconditionFailure("We should be shutting down right now.")
        case .shuttingDown(let promise):
            self.logger.trace("QUICHandler shutdown")
            self.state = .shutdown(promise.futureResult)
            promise.completeWith(result)
        }
    }

    private func createNewConnection(
        promise: EventLoopPromise<(QUICConnectionChannel, QUICStreamCreator)>,
        serverName: String,
        remoteAddress: SocketAddress,
        connectionInitializer:
            @Sendable @escaping (
                _ channel: QUICConnectionChannel,
                _ streamCreator: QUICStreamCreator
            ) -> EventLoopFuture<Void>
    ) throws {
        switch self.state {
        case .accepting:
            guard let localAddress = self.context?.localAddress else {
                return promise.fail(QUICError.noLocalAddress)
            }
            let sourceConnectionID = self.quicConnectionIDGenerator.next()

            let connectionLogger = {
                var logger = logger
                logger[metadataKey: LoggingKeys.connectionOriginalSCID] = "\("none")"
                logger[metadataKey: LoggingKeys.connectionSCID] = "\(sourceConnectionID)"
                logger[metadataKey: LoggingKeys.connectionDCID] = "\("none")"
                logger[metadataKey: LoggingKeys.addressLocal] = "\(localAddress)"
                logger[metadataKey: LoggingKeys.addressRemote] = "\(remoteAddress)"
                return logger
            }()
            // The context is set when the channel becomes active so force unwrapping is okay here
            let quicConnection = try SwiftNetworkQUICConnection.client(
                configuration: self.quicConfiguration,
                sourceConnectionID: sourceConnectionID,
                serverName: serverName,
                asyncVerifier: asyncVerifierRunner?.asyncVerifier,
                localAddress: localAddress,
                remoteAddress: remoteAddress,
                eventLoop: self.context!.eventLoop,
                logger: connectionLogger
            )

            let handle = self.nextConnectionHandle()
            let channel = self.makeQUICConnectionChannel(
                quicConnection: quicConnection,
                handle: handle
            )
            let view = channel.transportView
            self.connectionRegistry.insert(view, forHandle: handle, connectionID: sourceConnectionID)

            let streamCreator = channel.makeStreamCreator(role: self.quicConfiguration.role)
            let activePromise = self.eventLoop.makePromise(of: Void.self)
            view.initialize(promise: activePromise) { channel in
                connectionInitializer(channel, streamCreator)
            }

            activePromise.futureResult
                .assumeIsolated()
                .whenComplete { result in
                    switch result {
                    case .success:
                        promise.succeed((channel, streamCreator))
                    case .failure(let error):
                        promise.fail(error)
                    }
                }

            channel.closeFuture.assumeIsolated().whenComplete { _ in
                self.connectionDidClose(handle)
            }

        case .shuttingDown:
            promise.fail(QUICError.quicHandlerShuttingDown)

        case .shutdown:
            promise.fail(QUICError.quicHandlerShutdown)
        }
    }

    /// Hands the packet which created a connection to its channel once the connection has been
    /// initialized.
    ///
    /// The connection only queues the packet: it consumes its queue when the parent channel's read
    /// loop ends. If the connection initializer completed asynchronously then that read loop has
    /// already ended and no 'channelReadComplete' is coming for this packet, so the end of the read
    /// loop is signalled here instead. Without it the packet — an INITIAL, so the whole handshake —
    /// waits for the peer to retransmit.
    private func deliverFirstPacket(
        _ packet: ByteBuffer,
        to view: QUICConnectionChannel.TransportView
    ) {
        self.eventLoop.assertInEventLoop()
        view.parentChannelRead(packet)

        if !self.expectingChannelReadComplete {
            view.parentChannelReadComplete()
        }
    }

    private func connectionDidClose(_ handle: ConnectionHandle) {
        self.eventLoop.assertInEventLoop()
        self.connectionRegistry.remove(handle)
    }
}

@available(*, unavailable)
extension QUICHandler: Sendable {}

@available(anyAppleOS 26, *)
extension QUICHandler: ChannelInboundHandler {
    public typealias InboundIn = AddressedEnvelope<ByteBuffer>
    public typealias OutboundOut = AddressedEnvelope<ByteBuffer>

    public func handlerAdded(context: ChannelHandlerContext) {
        self.logger.trace("QUICHandler added to channel pipeline")
        self.context = context
    }

    public func handlerRemoved(context: ChannelHandlerContext) {
        self.logger.trace("QUICHandler removed from channel pipeline")
        self.context = nil
        self.connectionRegistry.shutdownConnections(deadline: .now(), promise: nil)
    }

    public func channelInactive(context: ChannelHandlerContext) {
        self.logger.trace("QUICHandlers' parent channel became inactive")
        asyncVerifierRunner?.terminate()

        self.connectionRegistry.notifyParentChannelInactive()

        switch self.multiplexerContinuation {
        case .none:
            break
        case .closure(_, _, let onFinish, _):
            onFinish()
        case .connectionMultiplexerContinuation(let cont):
            cont.finish()
        }

        if let asyncVerifierRunner = self.asyncVerifierRunner {
            context.eventLoop.makeFutureWithTask {
                await asyncVerifierRunner.join()
            }.assumeIsolated().whenComplete { _ in
                context.fireChannelInactive()
            }
        } else {
            context.fireChannelInactive()
        }
    }

    public func channelWritabilityChanged(context: ChannelHandlerContext) {
        self.logger.trace(
            "QUICHandlers' parent channel writability changed",
            metadata: [
                LoggingKeys.channelWritability: "\(context.channel.isWritable)"
            ]
        )

        let isWritable = context.channel.isWritable
        self.connectionRegistry.notifyParentChannelWritabilityChanged(isWritable)
    }

    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.expectingChannelReadComplete = true
        let addressedEnvelope = self.unwrapInboundIn(data)
        var header: QUICPacketHeader
        self.logger.trace(
            "QUICHandler read packet",
            metadata: [
                LoggingKeys.addressRemote: "\(addressedEnvelope.remoteAddress)"
            ]
        )

        do {
            guard
                let quicPacketHeader = try addressedEnvelope.data.parseQUICPacketHeader(
                    destinationIDLength: self.quicConnectionIDGenerator.connectionIDLength
                )
            else {
                throw QUICError.quicPacketHeaderDecodingFailed
            }
            header = quicPacketHeader
            self.logger.trace(
                "QUICHandler read packet routing to \(self.quicConfiguration.role)",
                metadata: [
                    LoggingKeys.addressRemote: "\(addressedEnvelope.remoteAddress)",
                    LoggingKeys.packetType: "\(header.type)",
                    LoggingKeys.packetVersion: "\(String(describing: header.version))",
                    LoggingKeys.connectionSCID: "\(header.sourceConnectionID?.description ?? "none")",
                    LoggingKeys.connectionDCID: "\(header.destinationConnectionID.description)",
                ]
            )

            switch self.state {
            case .accepting:
                if let view = self.connectionRegistry[header.destinationConnectionID] {
                    view.parentChannelRead(addressedEnvelope.data)
                } else if self.quicConfiguration.role == .server {
                    // Only INITIAL packets can create new connections. However, we do need to
                    // pass packets with unknown versions to Swift QUIC to initiate version
                    // negotation.
                    if header.type == .initial || header.type == .versionNegotiation {
                        try self.acceptNewConnection(
                            for: addressedEnvelope,
                            sourceConnectionID: header.sourceConnectionID,
                            destinationConnectionID: header.destinationConnectionID,
                            // This force unwrap is fine. We really need to have a local address at this point
                            localAddress: context.localAddress!
                        )
                    } else {
                        self.logger.trace(
                            "QUICHandler dropping non-INITIAL packet without a connection",
                            metadata: {
                                [
                                    LoggingKeys.addressRemote: "\(addressedEnvelope.remoteAddress)",
                                    LoggingKeys.connectionSCID: "\(header.sourceConnectionID?.description ?? "none")",
                                    LoggingKeys.connectionDCID:
                                        "\(header.destinationConnectionID.description)",
                                    LoggingKeys.packetType: "\(header.type)",
                                ]
                            }()
                        )
                    }
                } else {
                    self.logger.warning(
                        "QUICHandler dropping packet",
                        metadata: [
                            LoggingKeys.addressRemote: "\(addressedEnvelope.remoteAddress)",
                            LoggingKeys.connectionSCID: "\(header.sourceConnectionID?.description ?? "none")",
                            LoggingKeys.connectionDCID: "\(header.destinationConnectionID.description)",
                        ]
                    )
                }
            case .shuttingDown:
                // We still need to forward packets to open connections but not accept new ones.
                guard let view = self.connectionRegistry[header.destinationConnectionID] else {
                    self.logger.warning(
                        "QUICHandler dropping packet since the server is shutting down and not accepting new connections",
                        metadata: [
                            LoggingKeys.addressRemote: "\(addressedEnvelope.remoteAddress)",
                            LoggingKeys.connectionSCID: "\(header.sourceConnectionID?.description ?? "none")",
                            LoggingKeys.connectionDCID: "\(header.destinationConnectionID.description)",
                        ]
                    )
                    break
                }

                self.logger.trace(
                    "QUICHandler forwarding read to multiplexer",
                    metadata: [
                        LoggingKeys.addressRemote: "\(addressedEnvelope.remoteAddress)",
                        LoggingKeys.connectionSCID: "\(header.sourceConnectionID?.description ?? "none")",
                        LoggingKeys.connectionDCID: "\(header.destinationConnectionID)",
                    ]
                )

                view.parentChannelRead(addressedEnvelope.data)

            case .shutdown:
                self.logger.warning(
                    "QUICHandler dropping packet since the server is shutdown",
                    metadata: [
                        LoggingKeys.addressRemote: "\(addressedEnvelope.remoteAddress)",
                        LoggingKeys.connectionSCID: "\(header.sourceConnectionID?.description ?? "none")",
                        LoggingKeys.connectionDCID: "\(header.destinationConnectionID.description)",
                    ]
                )
            }
        } catch {
            // We may also fire an error here even if we are already shutdown.
            context.fireErrorCaught(error)
            return
        }
    }

    public func channelReadComplete(context: ChannelHandlerContext) {
        self.logger.trace("QUICHandler read complete")

        self.connectionRegistry.notifyParentChannelReadComplete()
        self.expectingChannelReadComplete = false

        if self.didWrite {
            self.didWrite = false
            self.logger.trace("QUICHandler flushing")
            context.flush()
        }
        context.fireChannelReadComplete()
    }

    public func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        switch event {
        case is ChannelShouldQuiesceEvent:
            let promise = context.eventLoop.makePromise(of: Void.self).assumeIsolated()
            promise.futureResult.whenComplete { _ in
                context.close(promise: nil)
            }

            self.connectionRegistry.shutdownConnections(
                deadline: .now() + .minutes(1),
                promise: promise
            )

        default:
            self.connectionRegistry.notifyParentChannelUserInboundEventTriggered(event)
        }
    }

    private func acceptNewConnection(
        for addressedEnvelope: AddressedEnvelope<ByteBuffer>,
        sourceConnectionID: QUICConnectionID?,
        destinationConnectionID: QUICConnectionID,
        localAddress: SocketAddress
    ) throws {
        // The original DCID was generated by the client. We'll generate a new one to use as our SCID.
        let newSourceConnectionID: QUICConnectionID
        if let sourceConnectionID {
            newSourceConnectionID = self.quicConnectionIDGenerator.next(
                sourceConnectionID: sourceConnectionID,
                destinationConnectionID: destinationConnectionID
            )
        } else {
            newSourceConnectionID = self.quicConnectionIDGenerator.next()
        }

        // The destination connection ID chosen by the client is the "original" source ID.
        let connectionLogger = {
            var logger = logger
            logger[metadataKey: LoggingKeys.connectionOriginalSCID] =
                "\(destinationConnectionID.description)"
            logger[metadataKey: LoggingKeys.connectionSCID] = "\(newSourceConnectionID)"
            logger[metadataKey: LoggingKeys.connectionDCID] = "\(sourceConnectionID?.description ?? "none")"
            logger[metadataKey: LoggingKeys.addressLocal] = "\(localAddress)"
            logger[metadataKey: LoggingKeys.addressRemote] = "\(addressedEnvelope.remoteAddress)"
            return logger
        }()

        connectionLogger.trace("QUICHandler accepting new connection")
        // The context is set when the channel becomes active so force unwrapping is okay here
        let quicConnection = try SwiftNetworkQUICConnection.server(
            configuration: self.quicConfiguration,
            sourceConnectionID: newSourceConnectionID,
            authenticator: self.authenticator,
            localAddress: localAddress,
            remoteAddress: addressedEnvelope.remoteAddress,
            logger: connectionLogger,
            eventLoop: self.context!.eventLoop
        )

        let handle = self.nextConnectionHandle()

        switch self.multiplexerContinuation! {
        case .closure(let connectionInitializer, let inboundStreamInitializer, _, let role):
            let channel = self.makeQUICConnectionChannel(
                quicConnection: quicConnection,
                handle: handle
            )
            channel.setInboundStreamInitializer(.closure(inboundStreamInitializer))

            let view = channel.transportView
            self.connectionRegistry.insert(view, forHandle: handle, connectionID: newSourceConnectionID)
            // Keep track of the original DCID the peer used in case they retransmit or send
            // additional INITIAL packets.
            // TODO: Remove the DCID alias from multiplexing.
            if newSourceConnectionID != destinationConnectionID {
                self.connectionRegistry.associate(destinationConnectionID, with: handle)
            }

            let streamCreator = channel.makeStreamCreator(role: role)
            let initPromise = self.eventLoop.makePromise(of: Void.self)
            view.initialize(promise: initPromise) { ch in
                connectionInitializer(ch, streamCreator)
            }

            initPromise.futureResult.assumeIsolated().whenComplete { result in
                switch result {
                case .success:
                    self.deliverFirstPacket(addressedEnvelope.data, to: view)
                case .failure:
                    // Nothing to unwind: failing initialization closes the channel, so
                    // 'connectionDidClose' does the teardown.
                    ()
                }
            }

            channel.closeFuture.assumeIsolated().whenComplete { _ in
                self.connectionDidClose(handle)
            }

        case .connectionMultiplexerContinuation(let multiplexerContinuation):
            let channel = self.makeQUICConnectionChannel(
                quicConnection: quicConnection,
                handle: handle
            )
            let view = channel.transportView
            self.connectionRegistry.insert(view, forHandle: handle, connectionID: newSourceConnectionID)
            // Keep track of the original DCID the peer used in case they retransmit or send
            // additional INITIAL packets.
            // TODO: Remove the DCID alias from multiplexing.
            if newSourceConnectionID != destinationConnectionID {
                self.connectionRegistry.associate(destinationConnectionID, with: handle)
            }

            let outputPromise = self.eventLoop.makePromise(of: (any Sendable).self)
            let initPromise = self.eventLoop.makePromise(of: Void.self)
            initPromise.futureResult.cascadeFailure(to: outputPromise)

            view.initialize(promise: initPromise) { _ in
                multiplexerContinuation.initialize(
                    channel: channel,
                    logger: connectionLogger
                ).map { output in
                    outputPromise.succeed(output)
                    return ()
                }
            }

            initPromise.futureResult
                .and(outputPromise.futureResult)
                .assumeIsolated()
                .whenComplete { result in
                    switch result {
                    case .success(let (_, output)):
                        connectionLogger.trace("QUICHandler yielding output to multiplexer")
                        multiplexerContinuation.yield(connection: output)
                        self.deliverFirstPacket(addressedEnvelope.data, to: view)
                    case .failure:
                        // Nothing to unwind: failing initialization closes the channel, so
                        // 'connectionDidClose' does the teardown.
                        ()
                    }
                }

            channel.closeFuture.assumeIsolated().whenComplete { _ in
                self.connectionDidClose(handle)
            }
        }
    }
}

@available(anyAppleOS 26, *)
public struct QUICHandlerHandle: Sendable {
    private let _wrapped: NIOLoopBound<QUICHandler>

    internal init(wrapping handler: QUICHandler, eventLoop: any EventLoop) {
        self._wrapped = .init(handler, eventLoop: eventLoop)
    }

    /// Shuts the server down gracefully.
    ///
    /// - Parameters:
    ///     - deadline: Deadline until connections are closed gracefully. Afterwards they will be forcibly closed.
    /// - Returns: A future that is notified once the server is closed.
    public func shutdownGracefully(deadline: NIODeadline) async throws {
        try await self._wrapped.flatSubmit {
            $0.shutdownGracefully(deadline: deadline)
        }.get()
    }
}

@available(anyAppleOS 26, *)
extension QUICHandler {
    public func makeHandle() -> QUICHandlerHandle {
        QUICHandlerHandle(wrapping: self, eventLoop: self.eventLoop)
    }
}

@available(anyAppleOS 26, *)
extension QUICHandler {
    internal func writeDatagram(
        _ envelope: AddressedEnvelope<ByteBuffer>,
        promise: EventLoopPromise<Void>?
    ) {
        guard let context = self.context else {
            promise?.fail(ChannelError.ioOnClosedChannel)
            return
        }

        self.logger.trace(
            "QUICHandler writing outbound data",
            metadata: [
                LoggingKeys.addressRemote: "\(envelope.remoteAddress)",
                LoggingKeys.channelOutboundBytes: "\(envelope.data.readableBytes)",
            ]
        )
        self.didWrite = true
        context.write(QUICHandler.wrapOutboundOut(envelope), promise: promise)
    }

    internal func flush() {
        if self.didWrite && !self.expectingChannelReadComplete {
            self.logger.trace("QUICHandler flushing")
            self.didWrite = false
            self.context?.flush()
        }
    }

    internal func read() {
        self.logger.trace("QUICHandler read from child channel")
        self.context?.read()
    }
}

@available(anyAppleOS 26, *)
extension QUICHandler {
    struct ChildView: QUICTransport {
        private let handler: QUICHandler

        init(_ handler: QUICHandler) {
            handler.eventLoop.assertInEventLoop()
            self.handler = handler
        }

        func writeDatagram(
            _ envelope: AddressedEnvelope<ByteBuffer>,
            promise: EventLoopPromise<Void>?
        ) {
            self.handler.writeDatagram(envelope, promise: promise)
        }

        func flush() {
            self.handler.flush()
        }

        func read() {
            self.handler.read()
        }
    }
}

@available(anyAppleOS 26, *)
@available(*, unavailable)
extension QUICHandler.ChildView: Sendable {}

@available(anyAppleOS 26, *)
extension QUICHandler {
    struct RegistrarView: QUICConnectionIDRegistrar {
        private let handler: QUICHandler
        private let handle: ConnectionHandle

        init(_ handler: QUICHandler, handle: ConnectionHandle) {
            handler.eventLoop.assertInEventLoop()
            self.handler = handler
            self.handle = handle
        }

        func associate(_ newID: QUICConnectionID) -> Bool {
            self.handler.connectionRegistry.associate(newID, with: self.handle)
        }

        func retire(_ connectionID: QUICConnectionID) -> Bool {
            self.handler.connectionRegistry.retire(connectionID, from: self.handle)
        }

        func generateID() -> QUICConnectionID {
            self.handler.quicConnectionIDGenerator.next()
        }
    }
}

@available(anyAppleOS 26, *)
@available(*, unavailable)
extension QUICHandler.RegistrarView: Sendable {}

@available(anyAppleOS 26, *)
extension QUICHandler {
    private func makeQUICConnectionChannel(
        quicConnection: SwiftNetworkQUICConnection,
        handle: ConnectionHandle
    ) -> QUICConnectionChannel {
        QUICConnectionChannel(
            udpChannel: self.udpChannel,
            connection: .live(quicConnection),
            registrar: .live(QUICHandler.RegistrarView(self, handle: handle)),
            transport: .live(QUICHandler.ChildView(self)),
            isServer: quicConnection.role == .server
        )
    }
}

/// An opaque, stable identity for a QUIC connection.
struct ConnectionHandle: Hashable, Sendable {
    private var rawValue: UInt64

    static var initial: ConnectionHandle {
        Self(rawValue: 0)
    }

    private init(rawValue: UInt64) {
        self.rawValue = rawValue
    }

    fileprivate mutating func formNext() {
        self.rawValue &+= 1
    }

    func next() -> Self {
        var next = self
        next.formNext()
        return next
    }
}
