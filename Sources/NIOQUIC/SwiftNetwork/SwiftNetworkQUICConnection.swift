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
    private var swiftNetworkQUICConnection: SwiftNetwork.QUICConnection
    let localAddress: SocketAddress
    let remoteAddress: SocketAddress
    private let outputHandler: QUICChannelOutputHandler
    private let logger: Logger
    let role: Role
    private let swiftNetworkParameters: SwiftNetwork.Parameters
    private let eventLoop: any EventLoop

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

    /// The connection's datagram (RFC 9221) flow, attached once the handshake completes.
    private var datagramTransport: DatagramTransport?

    private var connectionStateMachine = QUICConnectionStateMachine()

    private var finalizedOutput: Deque<ByteBuffer> = []
    private var inputPacketQueue: FrameArray = FrameArray(capacity: 10)
    private var networkContext: NetworkContext

    private var streamOptions: QUICStreamProtocol.QUICStreamOptions

    /// The connection channel. Used to drive out-of-band output drains when SwiftNetwork
    /// finalizes frames outside any drain bracket initiated by the channel.
    internal var channelView: QUICConnectionChannel.ConnectionView?

    /// Sets the connection channel and propagates it as the parent channel for inbound streams
    /// (via the new flow handler) and any pre-created outbound stream (the initial client stream).
    ///
    /// Must be called once the connection child channel has been created and before any
    /// inbound packet is fed into the connection.
    internal func setDriver(_ channel: QUICConnectionChannel) {
        self.channelView = channel.connectionView
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
    /// the stream the completion must be stored so it can be invoked once the stream ID is assigned.
    private struct PendingStreamData {
        let streamHandler: QUICChannelStreamHandler
        let onStreamReady: (Result<(streamID: QUICStreamID, handler: QUICChannelStreamHandler), any Error>) -> Void
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
        let streamListenerLinkage = StreamListenerLinkage(reference: self.swiftNetworkQUICConnection.reference)
        let datagramListenerLinkage = DatagramListenerLinkage(reference: self.swiftNetworkQUICConnection.reference)

        let newFlowHandler = QUICChannelNewFlowHandler(
            local: localEndpoint,
            remote: remoteEndpoint,
            parameters: swiftNetworkParameters,
            path: swiftNetworkPath,
            logger: logger,
            remoteAddress: remoteAddress,
            localAddress: localAddress,
            role: self.role,
            streamListenerProtocol: streamListenerLinkage,
            datagramListenerProtocol: datagramListenerLinkage,
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
            streamHandler.setDisconnectedEventHandler { _ in
                self.streamHandlerHandleDisconnected(streamID: streamID)
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
    /// `onStreamReady` is invoked with the confirmed stream ID and handler once the stream is
    /// ready, or with a failure if the stream could not be set up.
    ///
    /// - Parameters:
    ///   - streamType: The directionality and initiator of the stream.
    ///   - connectionChannel: The parent connection channel for the new stream.
    ///   - onStreamReady: Called with the confirmed stream ID and handler once the stream is fully set up, or with an error on failure.
    internal func addNewOutboundStreamInputHandler(
        streamType: QUICStreamType,
        connectionChannel: any Channel,
        onStreamReady:
            @escaping (Result<(streamID: QUICStreamID, handler: QUICChannelStreamHandler), any Error>) -> Void
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
                    onStreamReady: onStreamReady
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
                onStreamReady: onStreamReady
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
            onStreamReady: onStreamReady
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
                    pendingStreamData.onStreamReady(.failure(QUICError.invalidStreamState))
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
                onStreamReady: outboundStreamData.onStreamReady
            )
            self.pendingOutboundStreams.removeValue(forKey: temporaryID)
        }
    }

    /// Registers the confirmed stream handler and notifies `onStreamReady`.
    ///
    /// Must be called on the event loop. Records the handler under `streamID`, wires up the
    /// disconnected event handler, and invokes `onStreamReady` with the confirmed stream ID and
    /// handler so the caller can create the stream channel.
    ///
    /// - Parameters:
    ///   - temporaryID: The local ID for tracking the pending stream.
    ///   - streamID: The stream ID assigned by Swift QUIC; used as the storage key.
    ///   - streamHandler: The connected stream handler to register.
    ///   - onStreamReady: Called with the confirmed stream ID and handler once registration completes.
    private func finishOutboundStreamSetup(
        temporaryID: OpaqueIDGenerator<UInt64>.ID,
        streamID: QUICStreamID,
        streamHandler: QUICChannelStreamHandler,
        onStreamReady: (Result<(streamID: QUICStreamID, handler: QUICChannelStreamHandler), any Error>) -> Void
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

        streamHandler.setDisconnectedEventHandler { _ in
            self.streamHandlerHandleDisconnected(streamID: streamID)
        }

        // Do NOT append to newlyConnectedStreams. The connection channel creates the
        // child channel from the completion below.
        onStreamReady(.success((streamID: streamID, handler: streamHandler)))
    }

    /// Registers a stub stream handler that has been transitioned to the connected state.
    /// This is only intended for use in tests where the network stack isn't running.
    func registerConnectedStubStreamHandler(
        for streamID: QUICStreamID,
        direction: QUICStreamDirection
    ) {
        guard let connectionChannel = self.channelView?.channel else {
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
            pendingStreamData.onStreamReady(.failure(ChannelError.ioOnClosedChannel))
        }
        self.pendingOutboundStreams.removeAll()
        if let connectionNewFlowHandler = self.connectionNewFlowHandler {
            connectionNewFlowHandler.stop()
            connectionNewFlowHandler.teardown()
        }
        self.connectionNewFlowHandler = nil
        // Break cycle with the datagram transport, which holds this connection as its reader.
        self.datagramTransport?.close()
        self.datagramTransport = nil
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
        // The channel reference is dropped in 'dropChannelReferences()' once the channel has gone
        // inactive: it's still needed here to deliver 'connectionClosed' back to the channel.
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

    /// Drops every strong reference the connection holds back to the connection channel.
    ///
    /// The channel owns the connection; in turn the connection's `channelView`, flow handler,
    /// pending initial client stream and per-stream handlers each hold the channel back. That is
    /// a retain cycle that keeps both alive forever. The channel calls this once it has gone
    /// inactive so both can deinit.
    func dropChannelReferences() {
        self.channelView = nil
        self.connectionNewFlowHandler?.clearConnectionChannel()

        self.pendingInitialClientStream?.connectionChannel = nil
        self.pendingInitialClientStream = nil

        for (_, streamHandler) in self.streamInputHandlers {
            streamHandler.connectionChannel = nil
        }
        self.streamInputHandlers.removeAll()
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

    func closeAllStreams() -> [EventLoopFuture<Void>] {
        if streamInputHandlers.isEmpty {
            return []
        }
        let futures = streamInputHandlers.values.map { $0.closeFuture }
        for stream in streamInputHandlers.values {
            stream.stop(detachFromLowerProtocol: true)
            stream.closeIfNeeded()
        }
        return futures
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
    func receivePacketsComplete() {
        if self.inputPacketQueue.isEmpty {
            return
        }
        self.outputHandler.invokeInputAvailable()
    }

    /// Writes a single QUIC packet to be sent to the peer.
    ///
    /// The application should call ``nextPacketToSend()`` multiple times until there are no more packets to send.
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
    func nextPacketToSend() -> ByteBuffer? {
        self.finalizedOutput.popFirst()
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

        guard let channelView = self.channelView else {
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
        let associated = channelView.associate(extraConnectionID)

        if associated {
            // Sometimes connection IDs are retired before new ones are added. Each connection requires at least one ID to refer
            // to its channel. Remove the ID pending deletion now that a new ID is available.
            if let obsoleteSCID = self.scidPendingDeletion {
                // Only clear the pending deletion if it was actually retired. If it couldn't be
                // (e.g. the ID being associated *is* the pending one, so it's still our only
                // active ID), keep it buffered so a later distinct association can retire it.
                if self.handleRetireConnectionID(obsoleteSCID) {
                    self.scidPendingDeletion = nil
                }
            }
        } else {
            self.logger.error("Failed to associate extra Connection ID: \(extraConnectionID)")

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

    /// Generates a new connection ID via the generator closure and announces it to libnetcore.
    /// Skips if the generator closure is not set or if connection IDs are zero-length.
    private func announceNewConnectionID() {
        guard let channelView = self.channelView else { return }

        let newCID = channelView.generateID()

        // Connections with a 0-length connection ID cannot announce new connection IDs.
        if newCID.length > 0 {
            self.connectionNewFlowHandler?.requestAssociationOfConnectionID(newCID)
        }
    }

    /// Handles removal of retired inbound connection IDs propagated by the peer.
    /// This method is called when a `RETIRE_CONNECTION_ID` frame is received from the peer.
    /// It forwards the removal request to the QUICHandler which owns the multiplexer.
    ///
    /// - Parameters:
    ///   - retiredConnectionID: The retired connection ID to remove
    /// - Returns: `true` if the ID was actually retired. `false` if the retirement was
    ///   buffered (it was our last active ID) or skipped (unknown ID / missing callback).
    ///   Callers processing a deferred retirement use this to decide whether to clear
    ///   `scidPendingDeletion`: a buffered retirement must stay pending so a later
    ///   association of a *different* ID can retire it.
    @discardableResult
    private func handleRetireConnectionID(_ retiredConnectionID: QUICConnectionID) -> Bool {
        guard let index = self.activeSCIDs.firstIndex(of: retiredConnectionID) else {
            self.log("Connection ID \(retiredConnectionID) is not associated with this connection")
            return false
        }

        // Removing the last ID will make the channel inaccessible. Buffer deletion until a new ID is available.
        guard self.activeSCIDs.count > 1 else {
            self.log(
                "Buffering removal of retired inbound connection ID \(retiredConnectionID) since it is our only available ID"
            )
            self.scidPendingDeletion = retiredConnectionID
            return false
        }

        guard let channelView = self.channelView else {
            self.logger.error("Cannot retire Connection ID (\(retiredConnectionID)) because the callback is missing")
            // The callback is missing. Close the connection with an internal server error.
            self.scheduleConnectionClose(
                error: QUICTransportError.QUICTransportErrorCode.internalError,
                reason: "Internal server error: Failed to retire connection ID"
            )
            return false
        }

        // Remove it first, so repeated calls will exit early.
        self.activeSCIDs.remove(at: index)

        let retired = channelView.retire(retiredConnectionID)

        if retired {
            // It's gone. Save it to check ID reuse. This might not be worth it, but we can save them for now.
            self.retiredSCIDs.append(retiredConnectionID)
            // Generate a replacement CID.
            self.announceNewConnectionID()
            return true
        } else {
            self.logger.error("Failed to retire extra Connection ID: \(retiredConnectionID)")
            // Add the ID again. Just in case.
            self.activeSCIDs.append(retiredConnectionID)
            // Failed to retire the connection ID. Close the connection with an internal server error.
            self.scheduleConnectionClose(
                error: QUICTransportError.QUICTransportErrorCode.internalError,
                reason: "Internal server error: Failed to retire connection ID"
            )
            return false
        }
    }
}

// Callbacks coming from QUICChannelStreamHandler and QUICChannelNewFlowHandler
@available(anyAppleOS 26, *)
extension SwiftNetworkQUICConnection {

    /// Handle disconnected events from SwiftNetwork for individual stream handlers.
    ///
    /// Connection-level errors are handled separately via `handleConnectionDisconnected`.
    func streamHandlerHandleDisconnected(streamID: QUICStreamID) {
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
            // Do not attach the datagram flow or announce connection IDs again.
            return
        }

        // Generate additional CIDs for the peer after handshake completes.
        if let channelView = self.channelView {
            // Attach the datagram flow before telling the channel: the channel may write datagrams
            // as soon as it learns the peer's advertised size.
            let peerMaxDatagramFrameSize = self.attachDatagramFlow()
            channelView.handshakeCompleted(peerMaxDatagramFrameSize: peerMaxDatagramFrameSize)

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

    /// Attaches the connection's datagram (RFC 9221) flow.
    ///
    /// - Returns: The peer's advertised `max_datagram_frame_size`, or `0` if the peer does not
    ///   accept datagrams (or the flow could not be attached, so none can be sent).
    private func attachDatagramFlow() -> Int {
        guard let transport = self.connectionNewFlowHandler?.attachDatagramFlow() else {
            return 0
        }

        self.setDatagramTransport(.live(transport))

        // The peer's announced max frame size. Assume 0 (the peer does not accept datagrams) if the
        // value is not available on the metadata.
        //
        // Better still, use SwiftNetwork's *usable* datagram size, which already subtracts framing
        // overhead and path MTU (`QUICDatagramFlow.updateUsableDatagramFrameSize`) — passing that
        // would make the channel's size check exact instead of the current payload-only estimate.
        let remoteFrameSize =
            self.connectionNewFlowHandler?.getConnectionMetadata()?.connectionMetadata?.remoteMaxDatagramFrameSize ?? 0

        return Int(remoteFrameSize)
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

            self.channelView?.connectionClosed(error: error)

        case .completeClosing:
            self.logger.trace("Completing connection closing")
            // We initiated the close, now tear down the connection state
            self.tearDownConnectionState()

            // Notify the channel so it can fire channelInactive and complete its close future.
            // A connection-initiated close (e.g. a protocol violation detected locally) is not
            // otherwise visible to the channel; the `.beginDraining` (peer close) path notifies
            // the channel for the same reason. Idempotent when the channel initiated the close.
            self.channelView?.connectionClosed(error: nil)

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
    ///  * The connection channel harvests newlyConnectedStreamIDs() and creates a new server stream channel
    ///  * When the new server stream channel is created markServerStreamInputReady is called and marks the stream as readable.
    ///
    private func newFlowHandlerAddNewStream(streamHandler: QUICChannelStreamHandler) {
        // Set stream-specific disconnected event handler (similar to client-side in addNewStreamInputHandler)
        if let streamID = streamHandler.streamID {
            streamHandler.setDisconnectedEventHandler { _ in
                self.streamHandlerHandleDisconnected(streamID: streamID)
            }

            self.streamInputHandlers[streamID] = streamHandler
            self.channelView?.newInboundStream(id: streamID, channel: streamHandler)
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

        if didFinalizeFrames {
            self.triggerOutOfBandWriteEvent()
        }
    }

    /// Trigger outbound writes that drain `finalizedOutput`. This should only be called when no outbound drain is in progress.
    func triggerOutOfBandWriteEvent() {
        switch self.connectionStateMachine.receiveOutOfBandWriteRequest(connectionChannel: self.channelView) {
        case .ignoreRequest:
            log("Ignoring request to trigger write")
        case .unexpectedRequest:
            // Not worth more than a trace: this is called for every batch of frames finalized, so
            // it's expected once the connection is closed or has dropped its channel.
            log("Dropping unexpected request to trigger write")
        case .triggerEvent(let channelView):
            log("Triggering out-of-band outbound write event")
            channelView.drainOutbound()
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

    /// Installs the connection's datagram (RFC 9221) transport, registering the connection as its
    /// reader.
    ///
    /// The transport holds this connection until it is closed, which happens as part of the
    /// connection's teardown.
    func setDatagramTransport(_ transport: DatagramTransport) {
        transport.setReader(connection: self)
        self.datagramTransport = transport
    }
}

// MARK: - Inbound datagrams

@available(anyAppleOS 26, *)
extension SwiftNetworkQUICConnection {
    /// A datagram arrived on the connection's datagram flow: hand it to the channel, which fires it
    /// as a `channelRead`.
    ///
    /// - Precondition: The datagram must adhere to the frame size limits advertised by this peer.
    func read(datagram: ByteBuffer) {
        self.channelView?.datagramRead(datagram)
    }

    /// The datagram flow failed: hand the error to the channel, which fires it as an `errorCaught`.
    func error(_ error: any Error) {
        self.channelView?.datagramError(error)
    }
}

@available(anyAppleOS 26, *)
extension SwiftNetworkQUICConnection: QUICConnectionProtocol {
    func close(isApplicationClose: Bool, errorCode: Int64, reason: String) -> Bool {
        let action = self.close(
            sendApplicationClose: isApplicationClose,
            errorCode: errorCode,
            reason: reason
        )

        switch action {
        case .closeInitiated:
            return true
        case .alreadyClosed:
            return false
        }
    }

    func quiesceStreams() {
        for stream in self.streamInputHandlers.values {
            stream.pipeline.fireUserInboundEventTriggered(ChannelShouldQuiesceEvent())
        }
    }

    func writeDatagram(_ datagram: ByteBuffer) -> Bool {
        if let datagramTransport = self.datagramTransport {
            return datagramTransport.write(datagram: datagram)
        } else {
            // No datagram flow: either the handshake hasn't completed or attaching it failed.
            return false
        }
    }

    func flushDatagrams() {
        self.datagramTransport?.flush()
    }
}
