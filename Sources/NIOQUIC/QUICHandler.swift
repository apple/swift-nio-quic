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

import ChildChannelMultiplexer
import Logging
import NIOCore
import X509

@available(macOS 11, iOS 14.0, tvOS 14.0, watchOS 7.0, macCatalyst 14.0, *)
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
@available(macOS 11, iOS 14.0, tvOS 14.0, watchOS 7.0, macCatalyst 14.0, *)
public final class QUICHandler {
    private enum State {
        case accepting
        case shuttingDown(EventLoopPromise<Void>)
        case shutdown(EventLoopFuture<Void>)
    }

    /// The channel this handler resides in.
    public let udpChannel: any Channel

    /// The QUIC configuration used to accept connections.
    private let quicConfiguration: QUICConfiguration
    /// The multiplexer used for multiplexing the connections.
    private let connectionMultiplexer:
        ChildChannelMultiplexer<
            QUICConnectionID,
            Never,
            QUICConnectionChildChannelStateMachine,
            WatermarkedChildChannelWritablityStrategy<QUICConnectionChannelOutboundMessage>,
            AddressedEnvelope<ByteBuffer>,
            AddressedEnvelope<ByteBuffer>,
            QUICConnectionChannelInboundMessage,
            QUICConnectionChannelOutboundMessage,
            QUICConnectionChildChannelStateMachine.Task,
            QUICHandler
        >
    /// The type erased connection multiplexer.
    private var multiplexerContinuation: MultiplexerContinuation?
    /// The event loop of the channel we are added to.
    private let eventLoop: any EventLoop
    /// The logger used everywhere.
    private let logger: Logger
    /// The generator used for creating source connection IDs for new connections.
    private var quicConnectionIDGenerator: any QUICConnectionIDGenerator
    /// Our current state.
    private var state: State = .accepting
    /// The buffer used for outbound data.
    private var outboundBuffer: ByteBuffer
    /// Boolean to indicate if we wrote something.
    private var didWrite = false
    /// Whether we're expecting a channelReadComplete. This is used to delay flushing the channel until the a read complete is received.
    private var expectingChannelReadComplete: Bool = false
    /// The context of the channel handler.
    private var context: ChannelHandlerContext?
    /// The optional metrics to be recorded.
    private var metrics: QUICMetrics?
    /// A dictionary of QUIC connection IDs to QUIC connection metrics providers.
    private var quicConnectionMetrics: [QUICConnectionID: () -> SwiftNetworkQUICConnection.Metrics]
    /// Verifies the server identitfy.
    private var asyncVerifierRunner: AsyncVerifierRunner?
    /// Provide certificates and signature to authenticate.
    private var authenticator: Authenticator?

    /// Creates a new ``QUICHandler`` and a ``QUICHandler/ConnectionMultiplexer``.
    ///
    /// - Parameters:
    ///   - channel: The channel this handler resides in.
    ///   - QUICConfiguration: The quic configuration to use for this handler.
    ///   - logger: The logger.
    ///   - metrics: The optional metrics to be recorded.
    ///   - inboundStreamChannelInitializer: A closure called for any new inbound stream.
    /// - Returns: The handler and the connection multiplexer.
    public static func makeHandlerAndConnectionMultiplexer<Output: Sendable>(
        channel: any Channel,
        quicConfiguration: QUICConfiguration,
        logger: Logger,
        metrics: QUICMetrics? = nil,
        inboundStreamChannelInitializer: @Sendable @escaping (any Channel) -> EventLoopFuture<Output>
    ) throws -> (QUICHandler, QUICHandler.ConnectionMultiplexer<Output>) {
        try self.makeHandlerAndConnectionMultiplexer(
            channel: channel,
            quicConfiguration: quicConfiguration,
            logger: logger,
            metrics: metrics,
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
    ///   - metrics: The optional metrics to be recorded.
    ///   - inboundStreamChannelInitializer: A closure called for any new inbound stream.
    ///   - quicConnectionIDGenerator: The generator used for creating source connection IDs.
    /// - Returns: The handler and the connection multiplexer.
    public static func makeHandlerAndConnectionMultiplexer<Output: Sendable>(
        channel: any Channel,
        quicConfiguration: QUICConfiguration,
        logger: Logger,
        metrics: QUICMetrics? = nil,
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
            metrics: metrics,
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
    ///   - metrics: The optional metrics to be recorded.
    init(
        channel: any Channel,
        quicConfiguration: QUICConfiguration,
        asyncVerifier: AsyncVerifier?,
        authenticator: Authenticator?,
        logger: Logger,
        metrics: QUICMetrics? = nil,
        quicConnectionIDGenerator: any QUICConnectionIDGenerator = RandomQUICConnectionIDGenerator()
    ) {
        self.udpChannel = channel
        self.eventLoop = channel.eventLoop
        self.connectionMultiplexer = .init(eventLoop: self.eventLoop, logger: logger)
        self.quicConfiguration = quicConfiguration
        self.outboundBuffer = channel.allocator.buffer(capacity: ByteBuffer.maxDatagramSize)
        self.logger = logger
        self.metrics = metrics
        self.quicConnectionMetrics = [:]
        self.quicConnectionIDGenerator = quicConnectionIDGenerator
        if let asyncVerifier {
            self.asyncVerifierRunner = .init(asyncVerifier: asyncVerifier)
        }
        self.authenticator = authenticator
    }

    /// Initialises a new ``QUICHandler``.
    ///
    /// - Parameters:
    ///   - channel: The channel this handler resides in.
    ///   - quicConfiguration: The quic configuration to use for this handler.
    ///   - asyncVerifier: Callback provider for SwiftTLS certificate verification.
    ///   - authenticator: Authenticator for SwiftTLS certificate verification.
    ///   - logger: The logger.
    ///   - metrics: The optional metrics to be recorded.
    ///   - inboundConnectionInitializer: Called for every incoming connection and allows you to add handlers.
    ///   - inboundStreamInitializer: Called for every incoming stream on inbound connections and allows you to add handlers. This isn't called for inbound streams on outbound connections.
    ///   - noMoreConnections: Called when this handler becomes inactive. After this, `inboundConnectionInitializer` won't be called again.
    public init(
        channel: any Channel,
        quicConfiguration: QUICConfiguration,
        asyncVerifier: AsyncVerifier?,
        authenticator: Authenticator?,
        logger: Logger,
        metrics: QUICMetrics? = nil,
        inboundConnectionInitializer: @escaping @Sendable (any Channel, QUICStreamCreator) -> EventLoopFuture<Void>,
        inboundStreamInitializer: @escaping @Sendable (any Channel) -> EventLoopFuture<Void>,
        noMoreConnections: @escaping @Sendable () -> Void,
        quicConnectionIDGenerator: any QUICConnectionIDGenerator = RandomQUICConnectionIDGenerator()
    ) {
        self.udpChannel = channel
        self.eventLoop = channel.eventLoop
        self.connectionMultiplexer = .init(eventLoop: self.eventLoop, logger: logger)
        self.quicConfiguration = quicConfiguration
        self.outboundBuffer = channel.allocator.buffer(capacity: ByteBuffer.maxDatagramSize)
        self.logger = logger
        self.metrics = metrics
        self.quicConnectionMetrics = [:]
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
    }

    /// Create a new outbound QUIC connection.
    /// - Parameters:
    ///   - serverName: The server to connect to.
    ///   - remoteAddress: The address to connect to.
    ///   - connectionInitializer: How to initialize the connection. This closure will be called with a channel and a stream creator.
    ///   - inboundStreamInitializer: How to initialize any inbound streams on the new connection. This closure will be called with the stream channel.
    ///     The returned channel is a QUIC connection channel. You can create streams on that channel by using the provided stream creator.
    ///     You will receive inbound stream channels as inbound reads on the connection channel.
    ///     You will likely want to use this closure to add a handler with an InboundIn = any Channel. Then in channelRead, you can initialize the stream channels.
    /// - Returns: The initialized connection channel.
    public func createOutboundConnection(
        serverName: String,
        remoteAddress: SocketAddress,
        connectionInitializer: @escaping @Sendable (any Channel, QUICStreamCreator) -> EventLoopFuture<Void>,
        inboundStreamInitializer: @escaping @Sendable (any Channel) -> EventLoopFuture<Void>
    ) -> EventLoopFuture<any Channel> {
        let channelPromise = self.eventLoop.makePromise(of: (any Channel).self)

        do {
            let role = self.quicConfiguration.role
            try self.createNewConnection(
                promise: channelPromise,
                serverName: serverName,
                remoteAddress: remoteAddress
            ) { channel, connectionID, quicConnection, metrics, logger in
                channel.eventLoop.makeCompletedFuture {
                    let connectionChannelHandler =
                        QUICConnectionChannelHandler(
                            quicConnection: quicConnection,
                            eventLoop: channel.eventLoop,
                            role: role,
                            logger: logger,
                            metrics: metrics,
                            inboundStreamInitializer: inboundStreamInitializer
                        )

                    try channel.pipeline.syncOperations.addHandler(connectionChannelHandler)

                    if let connectionDurationTimer = metrics?.connectionCloseMetrics?.connectionDuration {
                        let connectionDurationHandler = ChannelDurationHandler(
                            durationTimer: connectionDurationTimer
                        )
                        try channel.pipeline.syncOperations.addHandler(connectionDurationHandler)
                    }

                    let errorCatchingHandler = ErrorCatchingHandler(logger: logger)
                    try channel.pipeline.syncOperations.addHandler(errorCatchingHandler)
                    metrics?.quicConnectionHandlerMetrics?.openConnections?.increment()

                    return (channel, connectionChannelHandler.makeStreamCreator(role: .client))
                }.flatMap { channel, streamCreator in
                    connectionInitializer(channel, streamCreator)
                }
            }
        } catch {
            channelPromise.fail(error)
        }

        return channelPromise.futureResult
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
            let internalPromise = self.eventLoop.makePromise(of: Void.self)
            internalPromise.futureResult.cascade(to: promise)
            self.state = .shuttingDown(internalPromise)

            internalPromise.futureResult.assumeIsolated().whenComplete { result in
                self.didShutDown(result)
            }

            self.connectionMultiplexer.shutdownGracefully(deadline: deadline, promise: internalPromise)
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
        promise: EventLoopPromise<any Channel>,
        serverName: String,
        remoteAddress: SocketAddress,
        streamChannelInitializer:
            @Sendable @escaping (
                any Channel, QUICConnectionID, SwiftNetworkQUICConnection, QUICMetrics?, Logger
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
            let quicConnection = try SwiftNetworkQUICConnection(
                configuration: self.quicConfiguration,
                sourceConnectionID: sourceConnectionID,
                serverName: serverName,
                asyncVerifier: asyncVerifierRunner?.asyncVerifier,
                localAddress: localAddress,
                remoteAddress: remoteAddress,
                logger: connectionLogger,
                eventLoop: self.context!.eventLoop,
                udpChannel: self.udpChannel
            )

            // Register callback for handling new inbound connection IDs
            quicConnection.associateConnectionID = self.makeAssociateConnectionIDCallback(
                logger: connectionLogger
            )
            quicConnection.retireConnectionID = self.makeRetireConnectionIDCallback(
                logger: connectionLogger
            )
            quicConnection.generateConnectionID = self.makeGenerateConnectionIDCallback()

            let stateMachine = QUICConnectionChildChannelStateMachine(
                quicConnection: quicConnection,
                localAddress: localAddress,
                remoteAddress: remoteAddress,
                allocator: self.udpChannel.allocator,
                logger: connectionLogger
            )

            self.connectionMultiplexer.createChildChannel(
                promise: promise,
                newChannelID: .channelID(sourceConnectionID),
                stateMachine: stateMachine,
                // We default to an 32kB outbound buffer size: this is a good trade off for avoiding excessive buffering while ensuring that decent
                // throughput can be maintained. We use 4kB as the low water mark.
                writabilityStrategy: .init(highWatermark: 32768, lowWatermark: 4096),
                localAddress: localAddress,
                remoteAddress: remoteAddress
            ) { channel, _ in
                quicConnection.setConnectionChannel(channel)
                return streamChannelInitializer(
                    channel,
                    sourceConnectionID,
                    quicConnection,
                    self.metrics,
                    connectionLogger
                )
            }

        case .shuttingDown:
            promise.fail(QUICError.quicHandlerShuttingDown)

        case .shutdown:
            promise.fail(QUICError.quicHandlerShutdown)
        }
    }

    private func updateMetricsForCreatedConnection() {
        self.metrics?.quicConnectionHandlerMetrics?.openConnections?.increment()
    }

    /// Creates a closure for registering new inbound connection IDs. This method returns a closure that can be safely stored and called.
    private func makeAssociateConnectionIDCallback(
        logger: Logger
    )
        -> @Sendable (_ existingConnectionID: QUICConnectionID, _ extraConnectionID: QUICConnectionID) ->
        EventLoopFuture<Void>
    {
        // Use NIOLoopBound to safely capture the multiplexer for use on the event loop
        let loopBoundMultiplexer = NIOLoopBound(self, eventLoop: self.eventLoop)

        return { existingConnectionID, extraConnectionID in
            // Ensure we're running on the correct event loop.
            let promise = loopBoundMultiplexer.eventLoop.makePromise(of: Void.self)
            loopBoundMultiplexer.eventLoop.execute {
                do {
                    try loopBoundMultiplexer.value.connectionMultiplexer.addExtraChannelID(
                        existingChannelID: existingConnectionID,
                        extraChannelID: extraConnectionID
                    )

                    logger.trace(
                        "Registered new inbound connection ID",
                        metadata: [
                            LoggingKeys.connectionSCID: "\(existingConnectionID)",
                            "extraSCID": "\(extraConnectionID)",
                        ]
                    )

                    promise.succeed()
                } catch {
                    logger.error(
                        "Failed to register new inbound connection ID",
                        metadata: [
                            LoggingKeys.connectionSCID: "\(existingConnectionID)",
                            "extraSCID": "\(extraConnectionID)",
                            "error": "\(error)",
                        ]
                    )
                    promise.fail(QUICError.failedToAssociateConnectionID)
                }
            }
            return promise.futureResult
        }
    }

    /// Creates a closure for retiring inbound connection IDs. This method returns a closure that can be safely stored and called.
    private func makeRetireConnectionIDCallback(
        logger: Logger
    ) -> @Sendable (QUICConnectionID) -> EventLoopFuture<Void> {
        // Use NIOLoopBound to safely capture the multiplexer for use on the event loop
        let loopBoundMultiplexer = NIOLoopBound(self, eventLoop: self.eventLoop)

        return { retiredConnectionID in
            // Ensure we're running on the correct event loop.
            let promise = loopBoundMultiplexer.eventLoop.makePromise(of: Void.self)
            loopBoundMultiplexer.eventLoop.execute {
                do {
                    try loopBoundMultiplexer.value.connectionMultiplexer.removeChannelID(
                        retiredConnectionID
                    )

                    logger.trace(
                        "Retired inbound connection ID",
                        metadata: [
                            "retiredSCID": "\(retiredConnectionID)"
                        ]
                    )

                    promise.succeed()
                } catch {
                    logger.error(
                        "Failed to retire inbound connection ID",
                        metadata: [
                            "retiredSCID": "\(retiredConnectionID)",
                            "error": "\(error)",
                        ]
                    )
                    promise.fail(QUICError.failedToRetireConnectionID)
                }
            }
            return promise.futureResult
        }
    }

    /// Creates a closure for generating new connection IDs. This method returns a closure that can be safely stored and called.
    private func makeGenerateConnectionIDCallback() -> @Sendable () -> QUICConnectionID {
        let loopBoundHandler = NIOLoopBound(self, eventLoop: self.eventLoop)

        return {
            loopBoundHandler.value.quicConnectionIDGenerator.next()
        }
    }
}

@available(*, unavailable)
extension QUICHandler: Sendable {}

@available(macOS 11, iOS 14.0, tvOS 14.0, watchOS 7.0, macCatalyst 14.0, *)
extension QUICHandler: ChannelInboundHandler {
    public typealias InboundIn = AddressedEnvelope<ByteBuffer>
    public typealias OutboundOut = AddressedEnvelope<ByteBuffer>

    public func handlerAdded(context: ChannelHandlerContext) {
        self.logger.trace("QUICHandler added to channel pipeline")
        self.context = context
        self.connectionMultiplexer.start(with: self)
    }

    public func handlerRemoved(context: ChannelHandlerContext) {
        self.logger.trace("QUICHandler removed from channel pipeline")
        self.context = nil
        self.connectionMultiplexer.shutdownGracefully(deadline: .now(), promise: nil)
    }

    public func channelInactive(context: ChannelHandlerContext) {
        self.logger.trace("QUICHandlers' parent channel became inactive")
        asyncVerifierRunner?.terminate()
        self.connectionMultiplexer.parentChannelInactive()
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
        self.connectionMultiplexer.parentChannelWritabilityChanged(newValue: context.channel.isWritable)
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
                if self.connectionMultiplexer.hasChannel(with: header.destinationConnectionID) {
                    self.logger.trace(
                        "QUICHandler forwarding read to multiplexer",
                        metadata: {
                            [
                                LoggingKeys.addressRemote: "\(addressedEnvelope.remoteAddress)",
                                LoggingKeys.connectionSCID: "\(header.sourceConnectionID?.description ?? "none")",
                                LoggingKeys.connectionDCID: "\(header.destinationConnectionID)",
                            ]
                        }()
                    )
                    try self.connectionMultiplexer.parentChannelRead(
                        addressedEnvelope,
                        for: header.destinationConnectionID
                    )
                } else if self.quicConfiguration.role == .server {
                    guard header.type == .initial || header.type == .versionNegotiation else {
                        // Only INITIAL packets can create new connections. However, we do need to pass packets with
                        // unknown versions to Swift QUIC to initiate version negotation.
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
                        return
                    }
                    try self.acceptNewConnection(
                        for: addressedEnvelope,
                        sourceConnectionID: header.sourceConnectionID,
                        destinationConnectionID: header.destinationConnectionID,
                        // This force unwrap is fine. We really need to have a local address at this point
                        localAddress: context.localAddress!
                    )
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
                guard self.connectionMultiplexer.hasChannel(with: header.destinationConnectionID) else {
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
                try self.connectionMultiplexer.parentChannelRead(addressedEnvelope, for: header.destinationConnectionID)
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
        self.connectionMultiplexer.parentChannelReadComplete()
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
            let promise = context.eventLoop.makePromise(of: Void.self)
            promise.futureResult.assumeIsolated().whenComplete { _ in
                context.close(promise: nil)
            }
            self.connectionMultiplexer.shutdownGracefully(deadline: .now() + .minutes(1), promise: promise)

        default:
            self.connectionMultiplexer.parentChannelUserInboundEventTriggered(event)
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
        let quicConnection = try SwiftNetworkQUICConnection(
            configuration: self.quicConfiguration,
            sourceConnectionID: newSourceConnectionID,
            // We are passing nil here since we are not supporting retry/tokens.
            originalDestinationConnectionID: nil,
            authenticator: self.authenticator,
            localAddress: localAddress,
            remoteAddress: addressedEnvelope.remoteAddress,
            logger: connectionLogger,
            eventLoop: self.context!.eventLoop,
            udpChannel: self.udpChannel
        )

        // Register callback for handling new inbound connection IDs
        quicConnection.associateConnectionID = self.makeAssociateConnectionIDCallback(
            logger: connectionLogger
        )
        // Register callback for handling retired connection IDs
        quicConnection.retireConnectionID = self.makeRetireConnectionIDCallback(
            logger: connectionLogger
        )
        quicConnection.generateConnectionID = self.makeGenerateConnectionIDCallback()

        self.quicConnectionMetrics[newSourceConnectionID] = { quicConnection.currentMetrics() }

        let stateMachine = QUICConnectionChildChannelStateMachine(
            quicConnection: quicConnection,
            localAddress: localAddress,
            remoteAddress: addressedEnvelope.remoteAddress,
            allocator: self.udpChannel.allocator,
            logger: connectionLogger
        )

        switch self.multiplexerContinuation! {
        case .closure(let connectionInitializer, let inboundStreamInitializer, _, let role):
            self.connectionMultiplexer.createChildChannel(
                promise: nil,
                newChannelID: .channelID(newSourceConnectionID),
                stateMachine: stateMachine,
                // We default to an 32kB outbound buffer size: this is a good trade off for avoiding excessive buffering while ensuring that decent
                // throughput can be maintained. We use 4kB as the low water mark.
                writabilityStrategy: .init(highWatermark: 32768, lowWatermark: 4096),
                localAddress: localAddress,
                remoteAddress: addressedEnvelope.remoteAddress
            ) { channel, _ in
                quicConnection.setConnectionChannel(channel)
                let connectionChannelHandler = QUICConnectionChannelHandler(
                    quicConnection: quicConnection,
                    eventLoop: channel.eventLoop,
                    role: role,
                    logger: self.logger,
                    metrics: self.metrics,
                    inboundStreamInitializer: inboundStreamInitializer
                )
                let streamCreator = connectionChannelHandler.makeStreamCreator(role: role)

                return channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(connectionChannelHandler)

                    if let connectionDurationTimer = self.metrics?.connectionCloseMetrics?.connectionDuration {
                        let connectionDurationHandler = ChannelDurationHandler(durationTimer: connectionDurationTimer)
                        try channel.pipeline.syncOperations.addHandler(connectionDurationHandler)
                    }

                    let errorCatchingHandler = ErrorCatchingHandler(logger: self.logger)
                    try channel.pipeline.syncOperations.addHandler(errorCatchingHandler)
                    self.metrics?.quicConnectionHandlerMetrics?.openConnections?.increment()

                    return channel
                }.flatMap { (channel: any Channel) -> EventLoopFuture<Void> in
                    connectionInitializer(channel, streamCreator)
                }
            }

            // We keep track of the original DCID the peer used in case they retransmit or send additional INITIAL packets.
            // TODO: Remmove dcid from multiplexing.
            // Do we need to retire the destination connection ID chosen by the client at some point? We need to keep it
            // for a while because the Initial packet might be fragemented, but once we are past the handshake it should
            // no longer be used.
            if newSourceConnectionID != destinationConnectionID {
                try self.connectionMultiplexer.addExtraChannelID(
                    existingChannelID: newSourceConnectionID,
                    extraChannelID: destinationConnectionID
                )
            }

            connectionLogger.trace("QUICHandler forwarding read to multiplexer")
            try self.connectionMultiplexer.parentChannelRead(addressedEnvelope, for: newSourceConnectionID)
        case .connectionMultiplexerContinuation(let multiplexerContinuation):
            let channelPromise = self.eventLoop.makePromise(of: (any Channel).self)
            let outputPromise = self.eventLoop.makePromise(of: (any Sendable).self)
            channelPromise.futureResult.cascadeFailure(to: outputPromise)

            self.connectionMultiplexer.createChildChannel(
                promise: channelPromise,
                newChannelID: .channelID(newSourceConnectionID),
                stateMachine: stateMachine,
                // We default to an 32kB outbound buffer size: this is a good trade off for avoiding excessive buffering while ensuring that decent
                // throughput can be maintained. We use 4kB as the low water mark.
                writabilityStrategy: .init(highWatermark: 32768, lowWatermark: 4096),
                localAddress: localAddress,
                remoteAddress: addressedEnvelope.remoteAddress
            ) { channel, _ in
                quicConnection.setConnectionChannel(channel)
                return multiplexerContinuation.initialize(
                    channel: channel,
                    connectionID: newSourceConnectionID,
                    quicConnection: quicConnection,
                    metrics: self.metrics,
                    logger: connectionLogger
                )
                .flatMapErrorThrowing { error in
                    outputPromise.fail(error)
                    throw error
                }
                .map {
                    outputPromise.succeed($0)
                    return ()
                }
            }

            // We have to await both futures here because of two reasons:
            // 1. The channelPromise future is indicating if creating the channel actually succeeded.
            //    We have to await this to know if we actually created a new child channel.
            // 2. While the outputPromise might succeed the channelPromise can still fail for unrelated
            //    reasons.
            channelPromise.futureResult
                .flatMap { channel -> EventLoopFuture<(any Channel, any Sendable)> in
                    outputPromise.futureResult.map { (channel, $0) }
                }
                .whenSuccess { [multiplexerContinuation] channel, output in
                    connectionLogger.trace("QUICHandler yielding output to multiplexer")
                    multiplexerContinuation.yield(connection: output, channel: channel)
                }

            // We keep track of the original DCID the peer used in case they retransmit or send additional INITIAL packets.
            if newSourceConnectionID != destinationConnectionID {
                try self.connectionMultiplexer.addExtraChannelID(
                    existingChannelID: newSourceConnectionID,
                    extraChannelID: destinationConnectionID
                )
            }

            connectionLogger.trace("QUICHandler forwarding read to multiplexer")
            try self.connectionMultiplexer.parentChannelRead(addressedEnvelope, for: newSourceConnectionID)
        }
    }
}

@available(macOS 11, iOS 14.0, tvOS 14.0, watchOS 7.0, macCatalyst 14.0, *)
extension QUICHandler: ChildChannelMultiplexerDelegate {
    public typealias ChildChannelID = QUICConnectionID
    public typealias ChildChannelIDProperties = Never
    public typealias ParentChannelMessage = AddressedEnvelope<ByteBuffer>

    /// The parent channel of accepted connections.
    public var parent: any Channel {
        self.udpChannel
    }

    public func writeFromChildChannel(
        childChannelID: ChildChannelID,
        message: AddressedEnvelope<ByteBuffer>,
        promise: EventLoopPromise<Void>?
    ) {
        guard let context = self.context else {
            promise?.fail(ChannelError.ioOnClosedChannel)
            return
        }

        self.logger.trace(
            "QUICHandler writing outbound data",
            metadata: [
                LoggingKeys.addressRemote: "\(message.remoteAddress)",
                LoggingKeys.channelOutboundBytes: "\(message.data.readableBytes)",
            ]
        )
        self.didWrite = true
        context.write(self.wrapOutboundOut(message), promise: promise)
    }

    public func flushFromChildChannel(childChannelID: ChildChannelID) {
        // If a child channel flushes and we aren't in a channelReadComplete loop, we need to flush. Otherwise
        // we can just wait.
        if self.didWrite && !self.expectingChannelReadComplete {
            self.logger.trace("QUICHandler flushing")
            self.didWrite = false
            self.context?.flush()
        }
    }

    public func readFromChildChannel(childChannelID: ChildChannelID) {
        // The child channel is still able to issue new reads when the parent channel
        // already became inactive. We tolerate these reads because nothing will happen.
        self.logger.trace("QUICHandler read from child channel")
        self.context?.read()
    }

    public func childChannelClosed(childChannelIDOrProperties: ChildChannelIDOrProperties<QUICConnectionID, Never>) {
        if case ChildChannelIDOrProperties.childChannelID(let connectionID) = childChannelIDOrProperties {
            if let closedConnectionMetricsProvider = self.quicConnectionMetrics.removeValue(forKey: connectionID) {
                let connectionMetrics = closedConnectionMetricsProvider()
                if let connectionCloseMetrics = self.metrics?.connectionCloseMetrics {
                    connectionCloseMetrics.receivedPackets?.record(connectionMetrics.receivedPackets)
                    connectionCloseMetrics.sentPackets?.record(connectionMetrics.sentPackets)
                    connectionCloseMetrics.lostPackets?.record(connectionMetrics.lostPackets)
                    connectionCloseMetrics.roundTripTimeInNanoseconds?.record(
                        .nanoseconds(connectionMetrics.roundTripTimeInNanoseconds)
                    )
                    connectionCloseMetrics.congestionWindowInBytes?.record(connectionMetrics.congestionWindowInBytes)
                    connectionCloseMetrics.deliveryRateInBytesPerSecond?.record(
                        connectionMetrics.deliveryRateInBytesPerSecond
                    )
                }
                if let quicConnectionHandlerMetrics = self.metrics?.quicConnectionHandlerMetrics {
                    quicConnectionHandlerMetrics.openConnections?.decrement()
                }
            }
        }
    }
}

extension ByteBuffer {
    fileprivate static let maxDatagramSize = 1350
}

@available(macOS 11, iOS 14.0, tvOS 14.0, watchOS 7.0, macCatalyst 14.0, *)
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

@available(macOS 11, iOS 14.0, tvOS 14.0, watchOS 7.0, macCatalyst 14.0, *)
extension QUICHandler {
    public func makeHandle() -> QUICHandlerHandle {
        QUICHandlerHandle(wrapping: self, eventLoop: self.eventLoop)
    }
}
