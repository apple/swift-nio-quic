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

import Crypto
import DequeModule
import Logging
@_spi(CustomByteBufferAllocator) import NIOCore
import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
@_spi(SwiftTLSOptions) @_spi(SwiftTLSProtocol) import SwiftTLS
import Synchronization
import X509

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

extension NIOCore.ByteBuffer {
    @inlinable
    internal mutating func withUnsafeMutableReadableBytesWithStorageManagement2<T>(
        _ body: (UnsafeMutableRawBufferPointer, AnyObject) throws -> T
    ) rethrows -> T {
        try self.withUnsafeReadableBytesWithStorageManagement { ptr, owner in
            let _ = owner.retain()
            let unwrappedOwner = owner.takeRetainedValue()
            return try body(UnsafeMutableRawBufferPointer(mutating: ptr), unwrappedOwner)
        }
    }
}

/// A wrapper around the objects and state we need to keep track of and access a QUIC connection in SwiftNetwork.
/// Holds the references required to access QUIC connections and streams
@available(anyAppleOS 26, *)
final class SwiftNetworkQUICConnection {
    struct Metrics {
        /// The number of QUIC packets received on the connection.
        var receivedPackets: Int

        /// The number of QUIC packets sent on the connection.
        var sentPackets: Int

        /// The number of QUIC packets lost on the connection.
        var lostPackets: Int

        /// The estimated round-trip time of the connection.
        var roundTripTimeInNanoseconds: Int

        /// The size of the connection’s congestion window in bytes.
        var congestionWindowInBytes: Int

        /// The most recent data delivery rate estimate in bytes/s.
        var deliveryRateInBytesPerSecond: Int
    }

    private var swiftNetworkQUICConnection: SwiftNetwork.QUICConnection
    private let localAddress: SocketAddress
    private let remoteAddress: SocketAddress
    private let outputHandler: QUICChannelOutputHandler
    private let logger: Logger
    private let role: Role
    private let swiftNetworkParameters: SwiftNetwork.Parameters
    private let eventLoop: any EventLoop

    // Callback to associate extra inbound connection IDs in the multiplexer, i.e., connection IDs used as DCIDs by our peers.
    internal var associateConnectionID:
        ((_ existingConnectionID: QUICConnectionID, _ extraConnectionID: QUICConnectionID) -> EventLoopFuture<Void>)?
    // Callback to retire inbound connection IDs in the multiplexer.
    internal var retireConnectionID: ((_ retiredConnectionID: QUICConnectionID) -> EventLoopFuture<Void>)?
    /// Closure to generate a new connection ID. Captured from QUICHandler's generator.
    internal var generateConnectionID: (@Sendable () -> QUICConnectionID)?
    // All active source connection IDs.
    private var activeSCIDs: [QUICConnectionID]
    // All retired connection IDs.
    private var retiredSCIDs = [QUICConnectionID]()
    // The order of adding and retiring new connection IDs might leave us with an empty list.
    // To prevent that we buffer removal of the last ID until we receive a new one.
    private var scidPendingDeletion: QUICConnectionID?

    private var connectionNewFlowHandler: QUICChannelNewFlowHandler?
    private var streamInputHandlers: [QUICStreamID: QUICChannelStreamHandler] = [:]
    private var pendingInitialClientStream: QUICChannelStreamHandler?

    private var connectionStateMachine = QUICConnectionStateMachine()

    private var finalizedOutput: Deque<ByteBuffer> = []
    private var inputPacketQueue: FrameArray = FrameArray(capacity: 10)
    private var newlyConnectedStreams: Set<QUICStreamID> = []
    private var networkContext: NetworkContext

    private var streamOptions: QUICStreamProtocol.QUICStreamOptions
    private var streamChannelCreationHandler:
        (
            (
                QUICStreamID, EventLoopPromise<any Channel>,
                @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
            ) -> Void
        )?

    /// Tracks whether the connection child channel state machine is actively
    /// driving outbound writes. When zero, any output frames finalized by
    /// SwiftNetwork (e.g. from timers) must trigger an explicit drain via the channel event.
    private var outboundDrainsScheduled: Int = 0

    /// The connection child channel. Used to fire `QUICDrainOutputEvent` when
    /// output is produced outside the state-machine write path.
    internal var channel: (any Channel)?

    /// Sets the connection channel and propagates it as the parent channel for inbound streams
    /// (via the new flow handler) and any pre-created outbound stream (the initial client stream).
    ///
    /// Must be called once the connection child channel has been created and before any
    /// inbound packet is fed into the connection.
    internal func setConnectionChannel(_ channel: any Channel) {
        self.channel = channel
        self.connectionNewFlowHandler?.setConnectionChannel(channel)
        self.pendingInitialClientStream?.setConnectionChannel(channel)
    }

    /// Generator for temporary IDs to track streams before Swift QUIC assignes the stream ID.
    /// QUIC limits streams per connection to 2^62-1, as such the ID space should never be exhaused.
    private var temporaryIDGenerator = OpaqueIDGenerator<UInt64>()

    /// qlog prefix IDs are incremented per stream.
    private let _qlogPrefixIDCounter = Atomic<Int>(0)
    private func nextQLogPrefixID() -> Int {
        _qlogPrefixIDCounter.wrappingAdd(1, ordering: .relaxed).newValue
    }

    /// Track IDs for qlog files to ensure connections write individual logs.
    private static let _serverConnectionQLogIDCounter = Atomic<Int>(1)
    private static func nextServerConnectionQLogID() -> Int {
        Self._serverConnectionQLogIDCounter.wrappingAdd(1, ordering: .relaxed).oldValue
    }

    private static let _clientConnectionQLogIDCounter = Atomic<Int>(1)
    private static func nextClientConnectionQLogID() -> Int {
        Self._clientConnectionQLogIDCounter.wrappingAdd(1, ordering: .relaxed).oldValue
    }

    // The RFC gives a minimum number of connection IDs that implementations should support.
    // Exception: Connections with zero-length connection IDs should not advertise additional ones.
    private static let QUIC_MIN_CONNECTION_IDS: Int = 2
    // Even if our peer supports more connection IDs, we will only advertise up to this limit.
    private static let SWIFT_NIO_QUIC_MAX_ANNOUNCE_CIDS: Int = 8

    private let connectionQLogID: Int

    /// Outbound stream creation is sometimes blocked on stream allowances. While waiting for Swift QUIC to create
    /// the stream the initialization data must be stored.
    private struct PendingStreamData {
        let streamHandler: QUICChannelStreamHandler
        let channelActivationPromise: EventLoopPromise<any Channel>
        let streamChannelInitializer: (any Channel, QUICStreamID) -> EventLoopFuture<Void>
    }

    /// Maps a temporary ID to the data required for its initialization.
    private var pendingOutboundStreams: [OpaqueIDGenerator<UInt64>.ID: PendingStreamData] = [:]

    private var observedStreamIDs = Set<QUICStreamID>()

    private func checkAndAddStreamID(_ streamID: QUICStreamID) {
        if self.observedStreamIDs.contains(streamID) {
            fatalError("adding already observed stream: \(streamID)")
        }
        self.observedStreamIDs.insert(streamID)
    }

    /// Returns `true` if the connection is in any termination state.
    var isTerminating: Bool {
        self.connectionStateMachine.isTerminating
    }

    /// Determines the action a child channel should take after outbound data has been processed.
    ///
    /// - Parameter isChannelInitializing: `true` if the channel is still initializing.
    /// - Returns: The action the caller should take.
    func outboundDataProcessed(isChannelInitializing: Bool) -> QUICConnectionStateMachine.OutboundDataProcessedAction {
        self.connectionStateMachine.outboundDataProcessed(isChannelInitializing: isChannelInitializing)
    }

    /// Creates a new client-side connection.
    ///
    /// - Parameters:
    ///     - configuration: The configuration to use when creating the connection.
    ///     - sourceConnectionID: The client's source connection ID.
    ///     - serverName: The server name of the peer used to verify the peer's certificate.
    ///     - asyncVerifier: Verifies the server identity when using X509-based auth.
    ///     - localAddress: The socket address we are sending from.
    ///     - remoteAddress: The socket address of the peer.
    ///     - eventLoop:  EventLoop to schedule events on inside of SwiftQUIC
    ///     - logger: Logger to log events
    static func client(
        configuration: QUICConfiguration,
        sourceConnectionID: QUICConnectionID,
        serverName: String?,
        asyncVerifier: AsyncVerifier?,
        localAddress: SocketAddress,
        remoteAddress: SocketAddress,
        eventLoop: any EventLoop,
        logger: Logger
    ) throws -> SwiftNetworkQUICConnection {
        // TODO: Verify that the serverName is always required and reflect that in the type system.
        // See also: https://github.com/apple/swift-nio-quic/issues/6
        guard let serverName = serverName else {
            throw QUICError.tlsConfigurationIncomplete
        }

        return try SwiftNetworkQUICConnection(
            configuration: configuration,
            sourceConnectionID: sourceConnectionID,
            serverName: serverName,
            localAddress: localAddress,
            remoteAddress: remoteAddress,
            eventLoop: eventLoop,
            mode: .client(asyncVerifier),
            logger: logger
        )
    }

    /// Accepts a new server-side connection.
    ///
    /// - Parameters:
    ///     - configuration: The configuration to use when creating the connection.
    ///     - sourceConnectionID: The server's source connection ID.
    ///     - authenticator: Authenticates the server when using X509 certificates.
    ///     - localAddress: The remote socket address of the peer
    ///     - remoteAddress: The socket address of the peer.
    ///     - logger: Logger to log events
    ///     - eventLoop:  EventLoop to schedule events on inside of SwiftQUIC
    static func server(
        configuration: QUICConfiguration,
        sourceConnectionID: QUICConnectionID,
        authenticator: Authenticator?,
        localAddress: SocketAddress,
        remoteAddress: SocketAddress,
        logger: Logger,
        eventLoop: any EventLoop
    ) throws -> SwiftNetworkQUICConnection {
        guard let serverName = configuration.serverName else {
            throw QUICError.tlsConfigurationIncomplete
        }

        return try SwiftNetworkQUICConnection(
            configuration: configuration,
            sourceConnectionID: sourceConnectionID,
            serverName: serverName,
            localAddress: localAddress,
            remoteAddress: remoteAddress,
            eventLoop: eventLoop,
            mode: .server(authenticator),
            logger: logger
        )
    }

    private enum Mode {
        case client(AsyncVerifier?)
        case server(Authenticator?)
    }

    private init(
        configuration: QUICConfiguration,
        sourceConnectionID: QUICConnectionID,
        serverName: String,
        localAddress: SocketAddress,
        remoteAddress: SocketAddress,
        eventLoop: any EventLoop,
        mode: Mode,
        logger: Logger
    ) throws {
        switch mode {
        case .client:
            self.role = .client
        case .server:
            self.role = .server
        }

        self.logger = logger
        self.localAddress = localAddress
        self.remoteAddress = remoteAddress
        self.finalizedOutput.reserveCapacity(100)
        self.newlyConnectedStreams.reserveCapacity(100)

        self.activeSCIDs = [sourceConnectionID]

        var swiftNetworkParameters = SwiftNetwork.Parameters()
        self.eventLoop = eventLoop
        let networkContext = NetworkContext(
            identifier: "swift-nio-quic-context-\(self.role.description)",
            externalScheduler: QUICChannelEventLoop(eventLoop: eventLoop)
        )
        swiftNetworkParameters.context = networkContext
        self.networkContext = networkContext
        swiftNetworkParameters.isServer = self.role == .server

        let quicOptions = try QUICStreamProtocol.options(from: configuration)
        switch mode {
        case .client(let asyncVerifier):
            // 'forceVersionNegotiation' is client-only.
            quicOptions.connectionOptions.forceVersionNegotiation = configuration.forceVersionNegotiation
            quicOptions.tlsOptions = try .clientOptions(
                from: configuration,
                asyncVerifier: asyncVerifier,
                serverName: serverName
            )

        case .server(let authenticator):
            guard let authConfig = configuration.authenticationConfiguration else {
                // Either keys for rawPublicKeyAuthenticaiton or certificates are required.
                throw QUICError.tlsConfigurationIncomplete
            }
            // 'retry' is a server-only option.
            quicOptions.connectionOptions.retry = configuration.sendRetry
            quicOptions.tlsOptions = try .serverOptions(
                from: configuration,
                authConfig: authConfig,
                authenticator: authenticator,
                serverName: serverName
            )
        }

        // '!' is okay: the `options(...)` call above throws if this isn't set.
        let perProtocolOptions = quicOptions.perProtocolOptions!
        perProtocolOptions.quicConnectionOptions.disableAutomaticNewConnectionIDs = true
        sourceConnectionID.withUnsafeBufferPointer { bufferPointer in
            perProtocolOptions.quicConnectionOptions.sourceConnectionID = Array(bufferPointer)
        }
        self.streamOptions = perProtocolOptions

        let swiftNetworkQUICConnection = SwiftNetwork.QUICConnection(context: swiftNetworkParameters.context)
        self.swiftNetworkParameters = swiftNetworkParameters
        self.swiftNetworkQUICConnection = swiftNetworkQUICConnection

        self.connectionQLogID = Self.nextClientConnectionQLogID()
        let prefix = role == .server ? "L" : "C"
        quicOptions.setLogID(prefix: prefix, parent: "1", protocolLogIDNumber: self.connectionQLogID)
        quicOptions.setProtocolInstance(swiftNetworkQUICConnection.reference)

        swiftNetworkParameters.defaultStack.prepend(applicationProtocol: quicOptions)
        let swiftNetworkPath = SwiftNetwork.PathProperties(parameters: swiftNetworkParameters)

        let localEndpoint = localAddress.toEndpoint()
        let remoteEndpoint = remoteAddress.toEndpoint()
        let listenerLinkage = StreamListenerLinkage(reference: self.swiftNetworkQUICConnection.reference)

        let newFlowHandler = QUICChannelNewFlowHandler(
            local: localEndpoint,
            remote: remoteEndpoint,
            parameters: swiftNetworkParameters,
            path: swiftNetworkPath,
            logger: logger,
            remoteAddress: remoteAddress,
            localAddress: localAddress,
            role: self.role,
            streamListenerProtocol: listenerLinkage,
            // Keep-alive is driven by the connection flow handler on the server, but by the
            // initial client stream on the client (set up below).
            keepAliveInterval: self.role == .server ? configuration.keepAliveInterval : nil
        )
        self.connectionNewFlowHandler = newFlowHandler

        // Clients set up an initial bidirectional stream (stream ID 0).
        switch mode {
        case .client:
            let streamID = QUICStreamID(rawValue: 0)
            let streamListenerLinkage = StreamListenerLinkage(reference: self.swiftNetworkQUICConnection.reference)
            let streamHandler = QUICChannelStreamHandler(
                role: .client,
                local: localEndpoint,
                remote: remoteEndpoint,
                parameters: swiftNetworkParameters,
                path: swiftNetworkPath,
                streamID: streamID,
                logger: logger,
                remoteAddress: remoteAddress,
                localAddress: localAddress,
                listenerProtocol: streamListenerLinkage,
                connectionChannel: nil,
                eventLoop: self.eventLoop,
                keepAliveInterval: configuration.keepAliveInterval
            )

            if let streamHandler {
                self.pendingInitialClientStream = streamHandler
            } else {
                fatalError("Could not create a new stream handler")
            }

        case .server:
            ()
        }

        self.outputHandler = QUICChannelOutputHandler(
            role: self.role,
            logger: logger,
            context: swiftNetworkParameters.context
        )

        self.start(
            localEndpoint: localEndpoint,
            remoteEndpoint: remoteEndpoint,
            path: swiftNetworkPath,
            keyLogPath: configuration.keyLogPath
        )
    }

    private func start(
        localEndpoint: Endpoint,
        remoteEndpoint: Endpoint,
        path: SwiftNetwork.PathProperties,
        keyLogPath: String?
    ) {
        self.outputHandler.setInputFramesHandler {
            self.outputHandlerGetInputFrames(maximumDatagramCount: $0)
        }

        self.outputHandler.setFinalizeOutputFramesHandler {
            self.outputHandlerFinalizeOutputFrames(frames: $0)
        }

        do {
            try self.swiftNetworkQUICConnection.attachLowerDatagramProtocolForNewPath(
                self.outputHandler.reference,
                remote: remoteEndpoint,
                local: localEndpoint,
                parameters: self.swiftNetworkParameters,
                path: path
            )
        } catch {
            fatalError("Could not attach output handler to SwiftNetwork QUIC connection instance")
        }

        // Start the initial client stream (if any) before the connection flow handler.
        if let streamHandler = self.pendingInitialClientStream, let streamID = streamHandler.streamID {
            streamHandler.setDisconnectedEventHandler { error in
                self.streamHandlerHandleDisconnected(streamID: streamID, error: error)
            }
            streamHandler.start()
        }

        guard let newFlowHandler = self.connectionNewFlowHandler else {
            self.logger.error("Failed to unwrap new flow handler, returning")
            return
        }

        // This disconnected handler is called at the connection level (when flow: .allFlows),
        // not for individual streams. Connection-level events include: connection close, draining state, and connection errors.
        newFlowHandler.start(NewFlowView(self))
        switch self.role {
        case .client:
            self.log("Finished starting a new client side connection with existing client stream: 0")
        case .server:
            self.log("Finished starting a new server side connection")
        }

        if let keyLogPath {
            self.setKeylogPath(keyLogPath)
        }
    }

    /// Sets up a Swift QUIC stream for an outbound (locally-initiated) stream.
    ///
    /// Creates and starts a ``QUICChannelStreamHandler`` keyed by `temporaryID`. If the
    /// stream ID is available, it immediately calls ``finishOutboundStreamSetup``; otherwise
    /// it waits for the connected event and resolves the ID asynchronously. In both cases
    /// `streamChannelCreationHandler` is invoked to create the stream channel in the multiplexer
    /// once the stream is ready.
    ///
    /// The `channelActivationPromise` and `streamChannelInitializer` need to be passed to
    /// the handler when creating the stream channel.
    ///
    /// - Parameters:
    ///   - streamType: The directionality and initiator of the stream.
    ///   - channelActivationPromise: Fulfilled with the new ``Channel`` once the stream is fully set up, or failed on error.
    ///   - streamChannelInitializer: Called with the new channel and its confirmed stream ID to configure the channel pipeline.
    internal func addNewOutboundStreamInputHandler(
        streamType: QUICStreamType,
        channelActivationPromise: EventLoopPromise<any Channel>,
        connectionChannel: any Channel,
        streamChannelInitializer: @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
    ) throws {
        // Generate a new temporary ID for the stream.
        let temporaryID = self.temporaryIDGenerator.generate()

        // Check if a pre-created initial client stream is available and the stream to open
        // matches the the stream type (client-initiated bidirectional stream).
        if streamType == .clientInitiatedBidirectional,
            let pendingInitialClientStream = self.pendingInitialClientStream,
            let streamID = pendingInitialClientStream.streamID
        {
            assert(streamID.rawValue == 0, "The stream ID does not match the expected initial stream ID.")
            self.checkAndAddStreamID(streamID)
            if pendingInitialClientStream.streamStateMachine.isConnected {
                let streamHandler = pendingInitialClientStream
                self.pendingInitialClientStream = nil
                self.finishOutboundStreamSetup(
                    temporaryID: temporaryID,
                    streamID: streamID,
                    streamHandler: streamHandler,
                    channelActivationPromise: channelActivationPromise,
                    streamChannelInitializer: streamChannelInitializer
                )
                return
            } else {
                pendingInitialClientStream.clearHandlers()
                self.pendingInitialClientStream = nil
                log("Initial stream is not connected. Setting up a new stream instead.")
            }
        }

        log("Creating a new outbound stream with temporary ID: \(temporaryID)")

        var swiftNetworkParameters = SwiftNetwork.Parameters()
        swiftNetworkParameters.context = self.swiftNetworkParameters.context

        let swiftNetworkPath = SwiftNetwork.PathProperties(parameters: swiftNetworkParameters)
        let quicOptions = QUICStreamProtocol.options()
        switch streamType {
        case .clientInitiatedBidirectional:
            if self.role != .client {
                throw QUICError.invalidStreamTypeForRole
            }
            quicOptions.isUnidirectional = false
        case .serverInitiatedBidirectional:
            if self.role != .server {
                throw QUICError.invalidStreamTypeForRole
            }
            quicOptions.isUnidirectional = false
        case .clientInitiatedUnidirectional:
            if self.role != .client {
                throw QUICError.invalidStreamTypeForRole
            }
            quicOptions.isUnidirectional = true
        case .serverInitiatedUnidirectional:
            if self.role != .server {
                throw QUICError.invalidStreamTypeForRole
            }
            quicOptions.isUnidirectional = true
        }
        quicOptions.setProtocolInstance(self.swiftNetworkQUICConnection.reference)

        quicOptions.setLogID(
            prefix: "C\(self.nextQLogPrefixID())",
            parent: "1",
            protocolLogIDNumber: self.connectionQLogID
        )

        swiftNetworkParameters.defaultStack.prepend(applicationProtocol: quicOptions)

        let localEndpoint = localAddress.toEndpoint()
        let remoteEndpoint = remoteAddress.toEndpoint()
        let listenerLinkage = StreamListenerLinkage(reference: self.swiftNetworkQUICConnection.reference)

        guard
            let streamHandler = QUICChannelStreamHandler(
                role: self.role,
                local: localEndpoint,
                remote: remoteEndpoint,
                parameters: swiftNetworkParameters,
                path: swiftNetworkPath,
                streamID: nil,
                logger: logger,
                remoteAddress: remoteAddress,
                localAddress: localAddress,
                listenerProtocol: listenerLinkage,
                connectionChannel: connectionChannel,
                eventLoop: connectionChannel.eventLoop
            )
        else {
            fatalError("Could not create a new outbound stream handler")
        }
        streamHandler.start()

        // Fast path: Check if the metadata is available immediately.
        guard let metadata: ProtocolMetadata<QUICProtocol> = streamHandler.getStreamMetadata(),
            let rawStreamID = metadata.streamID, streamHandler.streamStateMachine.isConnected
        else {
            // Metadata not yet available or stream not connected. Wait for the connected callback.
            self.pendingOutboundStreams[temporaryID] = .init(
                streamHandler: streamHandler,
                channelActivationPromise: channelActivationPromise,
                streamChannelInitializer: streamChannelInitializer
            )
            // And wait for the connected event from the stream handler.
            streamHandler.setConnectedEventHandler { streamID in
                self.outboundStreamConnectedCallback(
                    temporaryID: temporaryID,
                    streamID: streamID
                )
            }
            return
        }

        let streamID = QUICStreamID(rawValue: rawStreamID)
        self.checkAndAddStreamID(streamID)
        self.finishOutboundStreamSetup(
            temporaryID: temporaryID,
            streamID: streamID,
            streamHandler: streamHandler,
            channelActivationPromise: channelActivationPromise,
            streamChannelInitializer: streamChannelInitializer
        )
    }

    /// Dispatches ``finishOutboundStreamSetup`` onto the event loop after a pending stream fires its connected event.
    ///
    /// - Parameters:
    ///   - temporaryID: The local ID to track the stream during its creation in `pendingStreams`.
    ///   - streamID: The real stream ID assigned by Swift QUIC, passed through to `finishOutboundStreamSetup`.
    private func outboundStreamConnectedCallback(
        temporaryID: OpaqueIDGenerator<UInt64>.ID,
        streamID: QUICStreamID?
    ) {
        self.eventLoop.assumeIsolated().execute {
            guard let streamID else {
                // Stream ID not available during stream creation.
                self.log("stream with temporary ID \(temporaryID) connected but has no stream ID available")
                // Drop stream.
                let pendingStreamData = self.pendingOutboundStreams.removeValue(forKey: temporaryID)
                if let pendingStreamData {
                    pendingStreamData.streamHandler.clearHandlers()
                    pendingStreamData.channelActivationPromise.fail(QUICError.invalidStreamState)
                }
                return
            }

            guard let outboundStreamData = self.pendingOutboundStreams[temporaryID] else {
                // Fast path already ran
                return
            }
            self.checkAndAddStreamID(streamID)
            self.finishOutboundStreamSetup(
                temporaryID: temporaryID,
                streamID: streamID,
                streamHandler: outboundStreamData.streamHandler,
                channelActivationPromise: outboundStreamData.channelActivationPromise,
                streamChannelInitializer: outboundStreamData.streamChannelInitializer
            )
            self.pendingOutboundStreams.removeValue(forKey: temporaryID)
        }
    }

    /// Registers the confirmed stream handler and notifies `outboundStreamConnectedHandler`.
    ///
    /// Must be called on the event loop. Records the handler under `streamID`, wires up the
    /// critical-error hook, disconnected event handler, and fires `outboundStreamConnectedHandler`
    /// with both IDs so the caller can reconcile the temporary ID with the stream ID.
    ///
    /// - Parameters:
    ///   - temporaryID: The local ID for tracking the pending stream.
    ///   - streamID: The stream ID assigned by Swift QUIC; used as the storage key.
    ///   - streamHandler: The connected stream handler to register.
    private func finishOutboundStreamSetup(
        temporaryID: OpaqueIDGenerator<UInt64>.ID,
        streamID: QUICStreamID,
        streamHandler: QUICChannelStreamHandler,
        channelActivationPromise: EventLoopPromise<any Channel>,
        streamChannelInitializer: @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
    ) {
        // This should be true. Either we arrive here throught he connected callback,
        // which schedules this on the event loop or through the fast path, which
        // checks the connected status.
        assert(
            streamHandler.streamStateMachine.isConnected,
            "Outbound stream handler for temporary ID \(temporaryID) is not connected"
        )

        // Ensure the stream handler has the stream ID assigned.
        streamHandler.streamID = streamID
        log("stream with temporary ID \(temporaryID) connected as stream \(streamID)")
        self.streamInputHandlers[streamID] = streamHandler

        streamHandler.setDisconnectedEventHandler { error in
            self.streamHandlerHandleDisconnected(streamID: streamID, error: error)
        }

        // Do NOT append to newlyConnectedStreams. QUICConnectionChannelHandler creates the
        // child channel via outboundStreamConnectedHandler.
        self.streamChannelCreationHandler?(streamID, channelActivationPromise, streamChannelInitializer)
    }

    /// Registers a stub stream handler that has been transitioned to the connected state.
    /// This is only intended for use in tests where the network stack isn't running.
    func registerConnectedStubStreamHandler(
        for streamID: QUICStreamID,
        direction: QUICStreamDirection
    ) {
        guard let connectionChannel = self.channel else {
            fatalError("Connection channel unavailable")
        }

        let handler = QUICChannelStreamHandler(
            role: self.role,
            parameters: self.swiftNetworkParameters,
            streamID: streamID,
            logger: self.logger,
            remoteAddress: self.remoteAddress,
            localAddress: self.localAddress,
            connectionChannel: connectionChannel
        )
        switch handler.streamStateMachine.streamConnected(direction: direction) {
        case .activateStream: break
        case .ignoreAlreadyConnected:
            assertionFailure("freshly created handler should not already be connected")
        case .ignoreAlreadyClosed:
            assertionFailure("freshly created handler should not already be closed")
        }
        self.streamInputHandlers[streamID] = handler
    }

    /// Sets the handler called when an outbound stream is confirmed by Swift QUIC to create a stream channel in the multiplexer.
    ///
    /// The handler receives `(QUICStreamID, EventLoopPromise<any Channel>, @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>)` and is invoked from the event loop.
    ///
    /// - Parameter handler: The callback to invoke, or `nil` to clear the existing handler.
    internal func setStreamChannelCreationHandler(
        _ handler: (
            (
                QUICStreamID, EventLoopPromise<any Channel>,
                @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
            ) -> Void
        )?
    ) {
        self.streamChannelCreationHandler = handler
    }

    deinit {
        self.inputPacketQueue.finalizeAllFramesAsFailed()
    }

    /// Local logging function to debug the datapath
    ///
    /// This layer adds the context and fetches the message only if the debug flags are enabled.
    ///
    /// - Parameters:
    ///     - logMessage: The logMessage that is fetched by an autoclosure.  For performance reasons we could gate this behind a flag.
    private func log(_ logMessage: @autoclosure () -> String) {
        #if DEBUG
        let message = logMessage()
        let stateDescription = self.connectionStateMachine.stateDescription
        self.logger.trace("[\(self.role.description)][\(stateDescription)]  \(message)")
        #endif
    }

    /// Sets keylog output to the designated file.
    ///
    /// This needs to be called as soon as the connection is created, to avoid
    /// missing some early logs.
    ///
    /// - Parameters:
    ///     - filePath: The path to the file where the keylog output will be written to.
    func setKeylogPath(_ filePath: String) {
        // TODO: https://github.com/apple/swift-nio-quic/issues/7
    }

    // We may need to propagate an error through here in the future
    private func tearDownConnectionState() {
        self.inputPacketQueue.finalizeAllFramesAsFailed()
        for (_, streamHandler) in self.streamInputHandlers {
            streamHandler.stop(detachFromLowerProtocol: true)
        }
        for (_, pendingStreamData) in self.pendingOutboundStreams {
            pendingStreamData.streamHandler.stop(detachFromLowerProtocol: true)
            pendingStreamData.channelActivationPromise.fail(ChannelError.ioOnClosedChannel)
        }
        self.pendingOutboundStreams.removeAll()
        self.streamChannelCreationHandler = nil
        if let connectionNewFlowHandler = self.connectionNewFlowHandler {
            connectionNewFlowHandler.stop()
            connectionNewFlowHandler.teardown()
        }
        self.connectionNewFlowHandler = nil
        // Break cycle with outputHandler, which holds closures that capture self, i.e., the connection.
        self.outputHandler.clearHandlers()
    }

    /// Action returned by `close()` indicating what happened.
    enum CloseAction {
        /// Close was initiated, caller should proceed with close handling.
        case closeInitiated
        /// Connection was already closing or closed.
        case alreadyClosed
    }

    /// Closes the connection.
    ///
    /// - Parameters:
    ///     - sendApplicationClose: The parameter specifies whether an application close should be sent to the peer. Otherwise a normal connection close is sent.
    ///     - errorCode: The application error code.
    ///     - reason: The reason for closing.
    /// - Returns: Action indicating what happened.
    func close(sendApplicationClose: Bool, errorCode: Int64, reason: String) -> CloseAction {
        guard !self.isTerminating, let newFlowHandler = self.connectionNewFlowHandler else {
            log("close() returning early - already terminating")
            return .alreadyClosed
        }

        // Capture before initiateClose() below mutates the state machine. A `.connecting`
        // connection transitions straight to `.closed`, and `hasEstablishedConnection`
        // reports `true` for `.closed` unconditionally - so reading it afterwards would
        // always say "established" even for a connection that never left `.connecting`.
        let hasEstablishedConnection = self.connectionStateMachine.hasEstablishedConnection

        // Transition state machine to closing state FIRST, before SwiftNetwork cleanup
        // This is crucial because newFlowHandler.stop() synchronously fires disconnected event
        let action = self.connectionStateMachine.initiateClose(
            sendApplicationClose: sendApplicationClose,
            errorCode: errorCode,
            reason: reason
        )

        switch action {
        case .sendCloseFrame:
            // State machine successfully transitioned to closing
            break
        case .alreadyClosing:
            // State machine already in closing/draining/closed state
            log("close() - state machine already closing")
            return .alreadyClosed
        }

        // Now tell SwiftNetwork to send CONNECTION_CLOSE and clean up streams
        // newFlowHandler.stop() will synchronously trigger handleConnectionDisconnected()
        // which will see we're in .closing state and transition to .closed
        if sendApplicationClose {
            newFlowHandler.stop(error: NetworkError(quicApplicationError: UInt64(errorCode), reason: reason))
        } else {
            if let transportError = QUICTransportError(UInt64(errorCode), reason) {
                newFlowHandler.stop(error: NetworkError(quicTransportError: transportError))
            }
        }
        // Clean up callbacks before teardown to break retain cycles
        self.cleanupCallbacks()
        newFlowHandler.teardown()
        log("close sentApplicationClose: \(sendApplicationClose), errorCode: \(errorCode), reason: \(reason)")

        // For connections that never established (still in idle or early handshake states),
        // clean up synchronously.
        if !hasEstablishedConnection {
            // Never established - clean up synchronously
            self.tearDownConnectionState()
            assert(
                self.connectionStateMachine.stateDescription == "disconnected",
                "State should be closed after teardown"
            )
        }
        // For established connections, teardown happens via handleConnectionDisconnected (called by newFlowHandler.stop above)
        return .closeInitiated
    }

    /// Cleans up callbacks to break retain cycles before connection teardown
    private func cleanupCallbacks() {
        self.log("Cleaning up connection callbacks")
        // This ensures the closure that captures QUICHandler is released
        self.associateConnectionID = nil
        self.retireConnectionID = nil
        self.generateConnectionID = nil
    }

    func removeStreamHandler(streamID: QUICStreamID) -> Bool {
        if let _ = self.streamInputHandlers.removeValue(forKey: streamID) {
            return true
        }
        return false
    }

    func streamInputHandler(streamID: QUICStreamID) -> QUICChannelStreamHandler? {
        self.streamInputHandlers[streamID]
    }

    func closeAllStreamHandlers() -> [EventLoopFuture<Void>] {
        if streamInputHandlers.isEmpty {
            return []
        }
        let futures = streamInputHandlers.values.map { $0.closeFuture }
        for stream in streamInputHandlers.values {
            stream.stop(detachFromLowerProtocol: true)
        }
        return futures
    }

    func fireUserInboundEventOnAllStreams(_ event: any Sendable) {
        for (_, streamHandler) in self.streamInputHandlers {
            streamHandler.pipeline.fireUserInboundEventTriggered(event)
        }
    }

    /// Processes  QUIC packets received from the peer.
    ///
    /// On success the number of bytes processed from the input buffer is
    /// returned. On error the connection will be closed.
    ///
    /// Coalesced packets will be processed as necessary.
    ///
    /// Note that the contents of the input buffer `packet` might be modified by
    /// this function due to, for example, in-place decryption.
    ///
    /// - Parameters:
    ///     - packet: The input buffer containing the QUIC packets.
    /// - Returns: The number of bytes processed.
    @discardableResult
    @inlinable
    func receivePacket(_ packet: NIOCore.ByteBuffer) -> Int {
        var packet = packet
        log("receivePacket called with \(packet.readableBytes) bytes")
        packet.withUnsafeMutableReadableBytesWithStorageManagement2 { buffer, owner in
            self.inputPacketQueue.add(frame: Frame(customBuffer: buffer, owner: owner))
        }
        return packet.readableBytes
    }

    /// Singals to the QUIC stack that the input queue is ready to be consumed
    func flushInputQueue() {
        if self.inputPacketQueue.isEmpty {
            return
        }
        self.outputHandler.invokeInputAvailable()
    }

    /// Writes a single QUIC packet to be sent to the peer.
    ///
    /// The application should call ``nextOutboundPacket()`` multiple times until there are no more packets to send.
    ///
    ///  * When the application receives QUIC packets from the peer (that is,
    ///    any time ``receivePacket``  is also called).
    ///
    ///  * When the connection timer expires (that is, any time ``timeout()``
    ///    is also called).
    ///
    ///  * When the application sends data to the peer (for examples, any time ``writeDataForStream``is called).
    ///
    @discardableResult
    @inlinable
    func nextOutboundPacket() -> ByteBuffer? {
        self.finalizedOutput.popFirst()
    }

    /// Returns stream IDs for newly connected streams.
    /// This list is returned so that the state machine can setup child channels for these streams if they are not already available.
    /// These new streams may not be in the readable state yet.
    ///
    /// - Returns: An array of stream IDs (empty if no streams are newly connected).
    func newlyConnectedStreamIDs() -> [QUICStreamID] {
        // Return all newly connected streams that still have handlers and are
        // still in the connected state. A stream that had `stop()` called
        // (transitioning to `.closed`) should not be returned as newly connected.
        let streamIDs: [QUICStreamID] = self.newlyConnectedStreams.compactMap { streamID in
            guard let streamHandler = self.streamInputHandlers[streamID] else {
                return nil
            }
            guard streamHandler.streamStateMachine.isConnected else {
                return nil
            }
            return streamID
        }
        self.newlyConnectedStreams.removeAll()
        return streamIDs
    }

    func currentMetrics() -> Metrics {
        // TODO: https://github.com/apple/swift-nio-quic/issues/1
        .init(
            receivedPackets: 0,
            sentPackets: 0,
            lostPackets: 0,
            roundTripTimeInNanoseconds: 0,
            congestionWindowInBytes: 0,
            deliveryRateInBytesPerSecond: 0
        )
    }

}

@available(anyAppleOS 26, *)
extension SwiftNetworkQUICConnection {

    /// Schedule a connection close due to a given error. This can be called from callbacks
    /// and avoids tearing down the state of the caller directly.
    ///
    /// Note: This must be called from the event loop.
    private func scheduleConnectionClose(error: QUICTransportError.QUICTransportErrorCode, reason: String) {
        self.eventLoop.assumeIsolated().execute {
            let action = self.close(
                sendApplicationClose: false,
                errorCode: error.rawValue,
                reason: reason
            )

            switch action {
            case .closeInitiated, .alreadyClosed:
                // No follow-up decisions to make. We just need to close the connection.
                return
            }
        }
    }

    /// Handles registration of new inbound connection IDs advertised by the peer.
    /// This method is called when a `NEW_CONNECTION_ID` frame is received from the peer.
    /// It forwards the registration request to the QUICHandler which owns the multiplexer.
    ///
    /// - Parameters:
    ///   - extraConnectionID: The new connection ID to register
    private func handleAssociateConnectionID(_ extraConnectionID: QUICConnectionID) {
        assert(self.activeSCIDs.count >= 1, "Cannot associate a new connection ID without an existing one")

        if self.activeSCIDs.contains(extraConnectionID) {
            self.log("Connection ID \(extraConnectionID) is already associated with this connection")
            // We can get repeated frames for the same connection ID (provided they have the same sequence number).
            // We cannot check this here, but Swift QUIC should have caught such a violation.
            return
        }

        if self.retiredSCIDs.contains(extraConnectionID) {
            self.logger.error("Retired connection ID \(extraConnectionID) as issued again.")
            // RFC: "As a trivial example, this means the same connection ID MUST NOT be issued more than once on the same connection."
            self.scheduleConnectionClose(
                error: QUICTransportError.QUICTransportErrorCode.protocolViolation,
                reason: "Protocol violation: The same connection ID must not be issued more than once"
            )
            return
        }

        guard let existingSCID = self.activeSCIDs.first else {
            self.logger.error(
                "Cannot associate new Connection ID (\(extraConnectionID)) because we don't have an existing connection ID"
            )
            self.scheduleConnectionClose(
                error: QUICTransportError.QUICTransportErrorCode.internalError,
                reason: "Internal server error: Failed to add extra connection ID"
            )
            return
        }

        guard let associateConnectionID = self.associateConnectionID else {
            self.logger.error(
                "Cannot associate new Connection ID (\(extraConnectionID)) because the callback is missing"
            )
            // The callback is missing. Close the connection with an internal server error.
            self.scheduleConnectionClose(
                error: QUICTransportError.QUICTransportErrorCode.internalError,
                reason: "Internal server error: Failed to add extra connection ID"
            )
            return
        }

        self.log(
            "Associating extra inbound connection ID: \(extraConnectionID) for connection with existing ID: \(existingSCID)"
        )

        // Add this to our list to avoid repeatedly going to the QUIC handler. Our peers are allowed to send this frame repeatedly.
        self.activeSCIDs.append(extraConnectionID)

        // Propagate the association to the QUIC handler.
        let promise: EventLoopFuture<Void> = associateConnectionID(
            existingSCID,
            extraConnectionID
        )

        // Make sure we handle the result on our event loop.
        promise.hop(to: self.eventLoop).assumeIsolated().whenComplete { result in
            switch result {
            case .success:
                // Sometimes connection IDs are retired before new ones are added. Each connection requires at least one ID to refer
                // to its channel. Remove the ID pending deletion now thata new ID is available.
                if let obsoleteSCID = self.scidPendingDeletion {
                    self.handleRetireConnectionID(obsoleteSCID)
                    // Reset the ID. If retirement failes, this connection will be closed.
                    self.scidPendingDeletion = nil
                }

            case .failure(let error):
                self.logger.error("Failed to associate extra Connection ID: \(extraConnectionID): \(error)")
                // Remove the ID again.
                if let idx = self.activeSCIDs.firstIndex(of: extraConnectionID) {
                    self.activeSCIDs.remove(at: idx)
                }
                // Failed to make the new association. Close the connection with an internal server error.
                self.scheduleConnectionClose(
                    error: QUICTransportError.QUICTransportErrorCode.internalError,
                    reason: "Internal server error: Failed to add extra connection ID"
                )
            }
        }
    }

    /// Generates a new connection ID via the generator closure and announces it to libnetcore.
    /// Skips if the generator closure is not set or if connection IDs are zero-length.
    private func announceNewConnectionID() {
        guard let generateConnectionID = self.generateConnectionID else {
            return
        }
        let newCID = generateConnectionID()
        // Connections with a 0-length connection ID cannot announce new connection IDs.
        guard newCID.length > 0 else {
            return
        }
        self.connectionNewFlowHandler?.requestAssociationOfConnectionID(newCID)
    }

    /// Handles removal of retired inbound connection IDs propagated by the peer.
    /// This method is called when a `RETIRE_CONNECTION_ID` frame is received from the peer.
    /// It forwards the removal request to the QUICHandler which owns the multiplexer.
    ///
    /// - Parameters:
    ///   - retiredConnectionID: The retired connection ID to remove
    private func handleRetireConnectionID(_ retiredConnectionID: QUICConnectionID) {
        guard let index = self.activeSCIDs.firstIndex(of: retiredConnectionID) else {
            self.log("Connection ID \(retiredConnectionID) is not associated with this connection")
            return
        }

        // Removing the last ID will make the channel inaccessible. Buffer deletion until a new ID is available.
        guard self.activeSCIDs.count > 1 else {
            self.log(
                "Buffering removal of retired inbound connection ID \(retiredConnectionID) since it is our only available ID"
            )
            self.scidPendingDeletion = retiredConnectionID
            return
        }

        guard let retireConnectionID = self.retireConnectionID else {
            self.logger.error("Cannot retire Connection ID (\(retiredConnectionID)) because the callback is missing")
            // The callback is missing. Close the connection with an internal server error.
            self.scheduleConnectionClose(
                error: QUICTransportError.QUICTransportErrorCode.internalError,
                reason: "Internal server error: Failed to retire connection ID"
            )
            return
        }

        // Remove it first, so repeated calls will exit early.
        self.activeSCIDs.remove(at: index)

        let promise: EventLoopFuture<Void> = retireConnectionID(retiredConnectionID)

        // Make sure to handle the result on our event loop.
        promise.hop(to: self.eventLoop).assumeIsolated().whenComplete { result in
            switch result {
            case .success:
                // It's gone. Save it to check ID reuse. This might not be worth it, but we can save them for now.
                self.retiredSCIDs.append(retiredConnectionID)
                // Generate a replacement CID.
                self.announceNewConnectionID()

            case .failure(let error):
                self.logger.error("Failed to retire extra Connection ID: \(retiredConnectionID): \(error)")
                // Add the ID again. Just in case.
                self.activeSCIDs.append(retiredConnectionID)
                // Failed to retire the connection ID. Close the connection with an internal server error.
                self.scheduleConnectionClose(
                    error: QUICTransportError.QUICTransportErrorCode.internalError,
                    reason: "Internal server error: Failed to retire connection ID"
                )
            }
        }
    }
}

// Callbacks coming from QUICChannelStreamHandler and QUICChannelNewFlowHandler
@available(anyAppleOS 26, *)
extension SwiftNetworkQUICConnection {

    /// Handle disconnected events from SwiftNetwork for individual stream handlers.
    ///
    /// Connection-level errors are handled separately via `handleConnectionDisconnected`.
    func streamHandlerHandleDisconnected(streamID: QUICStreamID, error: NetworkError?) {
        if !self.removeStreamHandler(streamID: streamID) {
            log("[S\(streamID)] not found, ignoring")
        }
    }

    enum CIDAnnouncementDecision: Equatable {
        case announce(count: Int)
        case closeTransportParameterError(reason: String)
    }

    static func decideCIDAnnouncementCount(
        peerAnnouncedLimit: Int?,
        localAnnouncementCap: Int
    ) -> CIDAnnouncementDecision {
        // In the absence of an advertised limit use the RFC default of 2.
        let peerLimit = peerAnnouncedLimit ?? Self.QUIC_MIN_CONNECTION_IDS
        if peerLimit < Self.QUIC_MIN_CONNECTION_IDS {
            return .closeTransportParameterError(
                reason: "advertised active_connection_id_limit must be at least 2"
            )
        }
        // One CID is already in use.
        return .announce(count: min(localAnnouncementCap, peerLimit - 1))
    }

    /// Handle connected events from SwiftNetwork for connection-level handlers (e.g., newFlowHandler).
    private func handleConnectionConnected() {
        let action = self.connectionStateMachine.receiveConnectedEvent()
        switch action {
        case .logConnectionEstablished:
            self.logger.trace("Connection established")
        case .invalidTransition:
            self.logger.warning(
                "Received duplicate connected event",
                metadata: ["state": "\(self.connectionStateMachine.stateDescription)"]
            )
            // Do not announce connection IDs again.
            return
        }

        // Generate additional CIDs for the peer after handshake completes.
        if self.generateConnectionID != nil {
            // Query the value from Swift QUIC. If the peer did explicitly share a limit,
            // use the RFC minimum.
            let announcedPeerLimit =
                self.connectionNewFlowHandler?
                .getConnectionMetadata()?
                .connectionMetadata?
                .activeConnectionIDLimit

            let action = Self.decideCIDAnnouncementCount(
                peerAnnouncedLimit: announcedPeerLimit,
                localAnnouncementCap: Self.SWIFT_NIO_QUIC_MAX_ANNOUNCE_CIDS
            )
            switch action {
            case .closeTransportParameterError(let reason):
                let action = self.close(
                    sendApplicationClose: false,
                    errorCode: QUICTransportErrorCode.transportParameterError.rawValue,
                    reason: reason
                )

                switch action {
                case .closeInitiated, .alreadyClosed:
                    // No follow-up decisions to make. We just need to close the connection.
                    return
                }
            case .announce(let count):
                for _ in 0..<count {
                    // This will only announce IDs longer than 0 and skip this otherwise.
                    self.announceNewConnectionID()
                }
            }
        } else {
            log("Will not announce new connection IDs because no generator was configured")
        }
    }

    /// Handle disconnected events from SwiftNetwork for connection-level handlers (e.g., newFlowHandler).
    private func handleConnectionDisconnected(error: NetworkError?) {
        // State machine handles all error inspection and conversion
        let (stateAction, errorAction) = self.connectionStateMachine.receiveDisconnectedEvent(error: error)

        // Execute error action if present
        if let errorAction {
            switch errorAction {
            case .abruptClose:
                let _ = self.connectionStateMachine.abruptClose()
            }
        }

        switch stateAction {
        case .beginDraining(let error):
            if let error {
                self.logger.trace("Beginning connection draining with error", metadata: ["error": "\(error)"])
            } else {
                self.logger.trace("Beginning connection draining")
            }
            // Complete draining and tear down.
            // SwiftNetwork manages the draining timeout per RFC 9000 §10.2.2 internally.
            // By the time this handler is called, the draining period is complete and
            // we can safely finalize the connection closure.
            let action = self.connectionStateMachine.completeDraining()
            switch action {
            case .finalizeClosure:
                self.logger.trace("Finalizing connection closure after draining")
                self.tearDownConnectionState()
            case .alreadyClosed:
                // Connection went directly to closed (e.g., failed during handshake)
                self.logger.trace("Connection already closed, tearing down state")
                self.tearDownConnectionState()
            case .notDraining:
                self.logger.warning("Unexpected state when completing draining")
            }

        case .completeClosing:
            self.logger.trace("Completing connection closing")
            // We initiated the close, now tear down the connection state
            self.tearDownConnectionState()

        case .alreadyClosing:
            self.logger.trace("Already closing, ignoring disconnected event")

        case .invalidTransition:
            self.logger.warning("Invalid transition on disconnected event")
        }
    }
}

// Callbacks coming from QUICChannelNewFlowHandler
@available(anyAppleOS 26, *)
extension SwiftNetworkQUICConnection {

    /// Marks a new server stream as newly connected and adds the new stream to streamInputHandlers
    /// This is done to make sure the state machine sets up a new server side stream for this stream handler.
    /// The sequence of events goes:
    ///  * handleNewFlow is called with a new stream
    ///  * The new stream is marked in newlyConnectedStreams
    ///  * QUICConnectionChildChannelStateMachine checks newlyAvailableStreams and creates a new server stream channel
    ///  * When the new server stream channel is created markServerStreamInputReady is called and marks the stream as readable.
    ///
    private func newFlowHandlerAddNewStream(streamHandler: QUICChannelStreamHandler) {
        // Set stream-specific disconnected event handler (similar to client-side in addNewStreamInputHandler)
        if let streamID = streamHandler.streamID {
            self.newlyConnectedStreams.insert(streamID)
            self.streamInputHandlers[streamID] = streamHandler
            streamHandler.setDisconnectedEventHandler { error in
                self.streamHandlerHandleDisconnected(streamID: streamID, error: error)
            }
        }
    }
}

@available(anyAppleOS 26, *)
extension SwiftNetworkQUICConnection {
    /// Request retiring a connection ID that we are using to address our peer.
    func requestRetirementOfConnectionID(_ connectionID: QUICConnectionID) throws {
        guard let flowHandler = self.connectionNewFlowHandler else {
            self.logger.error("Failed to retire connection ID (\(connectionID)): flow handler is not set")
            throw QUICError.failedToRetireConnectionID
        }

        flowHandler.requestsRetirementOfConnectionID(connectionID)
    }

    /// Request associating a new connection ID that our peer can use to contact us.
    /// Note: Endpoints limit connection IDs. The peer might not accept new connection IDs at this time.
    func requestAssociationOfConnectionID(_ connectionID: QUICConnectionID) throws {
        guard let flowHandler = self.connectionNewFlowHandler else {
            self.logger.error("failed to associate connection ID (\(connectionID)): flow handler is not set")
            throw QUICError.failedToAssociateConnectionID
        }

        flowHandler.requestAssociationOfConnectionID(connectionID)
    }
}

@available(anyAppleOS 26, *)
extension SwiftNetworkQUICConnection {
    #if DEBUG  // For testing purposes only
    /// Injects a connection ID into the retired set. Useful for testing because it allows
    /// triggering the protocol violation path when the peer reissues this ID.
    func _forTesting_addRetiredSCID(_ connectionID: QUICConnectionID) {
        self.retiredSCIDs.append(connectionID)
    }

    /// Returns the current list of active source connection IDs.
    /// Useful for tests that need to discover the initial SCID (which doesn't generate an event).
    func _forTesting_getActiveSCIDs() -> [QUICConnectionID] {
        self.activeSCIDs
    }

    /// Removes a connection ID from `activeSCIDs` without calling the `retireConnectionID`
    /// callback. This preserves routing in the parent multiplexer while allowing tests to
    /// manipulate the active SCID set. Mirrors the buffering logic of `handleRetireConnectionID`:
    /// if this is the last active SCID, it is stored in `scidPendingDeletion` instead of removed.
    func _forTesting_removeFromActiveSCIDs(_ connectionID: QUICConnectionID) {
        guard let index = self.activeSCIDs.firstIndex(of: connectionID) else {
            return
        }

        guard self.activeSCIDs.count > 1 else {
            self.scidPendingDeletion = connectionID
            return
        }

        self.activeSCIDs.remove(at: index)
        self.retiredSCIDs.append(connectionID)
    }
    #endif
}

// Callbacks coming from QUICChannelOutputHandler
@available(anyAppleOS 26, *)
extension SwiftNetworkQUICConnection {

    internal func flushOutputFrames() {
        self.finalizedOutput.removeAll()
    }

    /// Consumes the inputPacketQueue and transforms the ByteBuffers from receivePacket to frames.
    ///
    /// These frames are to be consumed by the QUIC stack when invokeInputAvailable is called.
    /// - Parameter maximumDatagramCount: The maximum number of datagrams to consume
    /// - Returns: A converted frame array to the protocol stack.
    internal func outputHandlerGetInputFrames(maximumDatagramCount: Int) -> FrameArray? {
        guard self.inputPacketQueue.count > 0 else {
            self.log("No input packets")
            return nil
        }
        return inputPacketQueue.drainArray(maximumFrameCount: maximumDatagramCount)
    }

    /// Builds the finalized output frames so they can be written in writeOutboundData.
    ///
    /// These are QUIC output frames that have already been built by the protocol stack.
    internal func outputHandlerFinalizeOutputFrames(frames: consuming FrameArray) {
        log("finalizeOutputFrames: \(frames.count)")
        var didFinalizeFrames = false
        frames.iterateMutableFrames { frame in
            if frame.unclaimedLength == 0 {
                frame.finalize(success: true)
                return true
            }

            if let bufferConfig = frame.takeOwnershipOfCustomFinalizerBuffer() {
                frame.finalize(success: true)
                let outputBuffer = ByteBuffer(
                    takingOwnershipOf: bufferConfig.bufferPointer,
                    allocator: FrameMemory.allocator,
                    readerIndex: bufferConfig.readerOffset,
                    writerIndex: bufferConfig.writerOffset
                )
                self.finalizedOutput.append(outputBuffer)
                didFinalizeFrames = true
                return true
            }

            assertionFailure("Encountered frame with unexpected buffer type.")
            frame.finalize(success: false)
            return true
        }

        if didFinalizeFrames && !self.hasOutboundDrainScheduled {
            self.triggerOutOfBandWriteEvent()
        }
    }

    /// Check if an outbound drain is scheduled.
    var hasOutboundDrainScheduled: Bool {
        self.outboundDrainsScheduled > 0
    }

    /// Note that an outbound write is scheduled to happen. When new frames arrive via
    /// `outputHandlerFinalizeOutputFrames` an outbound drain should not bt triggered.
    func outboundDrainScheduled() {
        self.outboundDrainsScheduled += 1
    }

    /// Note that the outbound drain has happend.
    func outboundDrainFinished() {
        assert(
            self.outboundDrainsScheduled > 0,
            "Called outboundDrainFinished without previously calling outboundDrainScheduled"
        )
        self.outboundDrainsScheduled -= 1
    }

    /// Trigger outbound writes that drain `finalizedOutput`. This should only be called when no outbound drain is scheduled.
    func triggerOutOfBandWriteEvent() {
        assert(self.outboundDrainsScheduled == 0, "This should only be called outside the state machine write path")
        switch self.connectionStateMachine.receiveOutOfBandWriteRequest(connectionChannel: self.channel) {
        case .ignoreRequest:
            log("Ignoring request to trigger write")
            return
        case .unexpectedRequest:
            self.logger.error("[\(self.role)] Dropping unexpected request to trigger write")
        case .triggerEvent(let connectionChannel):
            log("Triggering out-of-band outbound write event")
            connectionChannel.eventLoop.assumeIsolated().execute {
                connectionChannel.triggerUserOutboundEvent(QUICDrainOutputEvent(), promise: nil)
            }
        }
    }
}

@available(anyAppleOS 26, *)
extension Frame {
    /// The frames returned by Swift QUIC were created in `QUICChannelOutputHandler.getDatagramsToSend`. They all hold
    /// the same buffer type and a pointer to allocated memory. Any other buffer type is unexpected and a logic error. To take ownership
    /// of the buffer type, ByteBuffer requires the pointer, the offest to start reading and the length of the readable section.
    ///
    /// Replacing the buffer with .empty disables automatic cleanup of the underlying memory in `frame.finalize(_:)`.
    mutating func takeOwnershipOfCustomFinalizerBuffer()
        -> (bufferPointer: UnsafeMutableRawBufferPointer, readerOffset: Int, writerOffset: Int)?
    {
        guard
            let readerAddress = self.span?.withUnsafeBufferPointer({ ptr -> UnsafeRawPointer? in
                guard let baseAddress = ptr.baseAddress else {
                    return nil
                }
                return UnsafeRawPointer(baseAddress)
            })
        else {
            return nil
        }

        var result: (bufferPointer: UnsafeMutableRawBufferPointer, readerOffset: Int, writerOffset: Int)? = nil
        switch self.buffer {
        case .empty:
            return result
        case .bytes:
            return result
        case .customOwner:
            return result
        case .customFinalizer(let bufferPointer, _):
            guard let baseAddress = bufferPointer.baseAddress else {
                return result
            }
            // Reader offset is the distance between buffer start and span start.
            let readerOffset = readerAddress - UnsafeRawPointer(baseAddress)
            let writerOffset = readerOffset + self.unclaimedLength
            result = (bufferPointer, readerOffset, writerOffset)
        }

        // The frame no longer owns the buffer. Take away its reference.
        self.buffer = .empty
        return result
    }
}

@available(anyAppleOS 26, *)
extension SwiftNetworkQUICConnection {
    /// A view over the connection for the `QUICChannelNewFlowHandler`.
    struct NewFlowView {
        private let connection: SwiftNetworkQUICConnection

        fileprivate init(_ connection: SwiftNetworkQUICConnection) {
            self.connection = connection
        }

        func connected() {
            self.connection.handleConnectionConnected()
        }

        func disconnected(error: NetworkError?) {
            self.connection.handleConnectionDisconnected(error: error)
        }

        func newInboundStream(_ handler: QUICChannelStreamHandler) {
            self.connection.newFlowHandlerAddNewStream(streamHandler: handler)
        }

        func associateConnectionID(_ cid: QUICConnectionID) {
            self.connection.handleAssociateConnectionID(cid)
        }

        func retireConnectionID(_ cid: QUICConnectionID) {
            self.connection.handleRetireConnectionID(cid)
        }
    }
}
