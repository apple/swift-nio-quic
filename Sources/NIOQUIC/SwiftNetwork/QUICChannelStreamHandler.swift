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
import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
import Synchronization

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

enum StreamClosureState {
    case closeAndDisconnect  // Calls close and stop to detach everything
    case closeOnly  // Calls close only
    case disconnectOnly  // Calls stop and detach only, could result in disconnect event as well
    case detachAndClose  // Called on disconnect event callback
}

/// `QUICChannelStreamHandler` is the bridge between SwiftNetwork and our code on the application-side;
/// one `QUICChannelStreamHandler` exists for each QUIC stream.
@available(anyAppleOS 26, *)
final class QUICChannelStreamHandler: ProtocolInstanceContainer, InboundStreamHandler, @unchecked Sendable {

    // MARK: Channel and ChannelCore conformanace

    /// The channel of the QUIC Connection.
    @usableFromInline
    var connectionChannel: (any Channel)?
    /// The event loop of the connection channel.
    @usableFromInline
    let eventLoop: any EventLoop
    /// The allocator of the parent.
    @usableFromInline
    let allocator: ByteBufferAllocator
    /// Atomic that stores if this channel is currently active.
    @usableFromInline
    let _isActive: Atomic<Bool>
    /// Atomic that stores if this channel is currently writable.
    @usableFromInline
    let _isWritable: Atomic<Bool>
    /// The actual channel pipeline.
    /// This needs to be an implicitly unwrapped optional because the ChannelPipeline holds a ref to this Channel
    /// and that the ChannelPipeline is responsible for breaking the retain cycle.
    @usableFromInline
    var _pipeline: ChannelPipeline!
    /// The local ``SocketAddress``.
    @usableFromInline
    let _localAddress: SocketAddress?
    /// The remote peer’s ``SocketAddress``.
    @usableFromInline
    let _remoteAddress: SocketAddress?
    let _closePromise: EventLoopPromise<Void>

    // MARK: Private Constant state

    internal typealias LowerProtocol = OutboundStreamLinkage

    private let role: Role
    private let keepAliveInterval: Duration?
    private let logger: Logger
    internal var reference: ProtocolInstanceReference = ProtocolInstanceReference()

    internal var eventManager = ProtocolEventManager()
    internal var streamStateMachine: QUICStreamStateMachine
    internal var pipelineStateMachine: QUICStreamPipelineStateMachine
    internal var swiftNetworkStreamHandle: SwiftNetworkStreamHandle

    // Stream state machine (RFC 9000 Section 3 compliant)
    internal var context: SwiftNetwork.NetworkContext
    internal var streamID: QUICStreamID?

    // MARK: Private mutable state

    /// Whether the application has opted in to half-closure semantics for received STOP_SENDING.
    private var understandsStopSending = false
    private var logPrefix: String = ""
    private var bufferedReadData: ByteBuffer = ByteBuffer()
    private var bufferedWriteData: TinyArray<ByteBuffer> = []

    /// A flag that is `true` when the downstream consumer has requested a read that has not yet been satisfied.
    ///
    /// This flag is set by the `read0()` method and cleared by the `streamRead()` method once inbound data has actually
    /// been delivered down the pipeline. When `false`, `handleInboundDataAvailableEvent()` leaves inbound data residing
    /// in SwiftNetwork's stream buffer so that its flow-control mechanisms have more accurate signals about the rate at
    /// which this endpoint is consuming inbound data.
    private var pendingRead: Bool = false

    /// Whether the `autoRead` channel option is enabled. This value is inherited from the parent connection channel.
    /// When `true`, `self.pipeline.read()` is called after every `channelReadComplete` to keep data flowing.
    ///
    /// Defaults to `true`. This value can be updated by calling `setOption`.
    private var autoRead: Bool = true

    private var originalMetadata: ProtocolMetadata<QUICProtocol>?
    private var connectedEventHandler: ((QUICStreamID?) -> Void)?
    private var disconnectedEventHandler: ((NetworkError?) -> Void)?

    internal init(
        role: Role,
        parameters: Parameters,
        streamID: QUICStreamID?,
        logger: Logger,
        remoteAddress: SocketAddress,
        localAddress: SocketAddress?,
        connectionChannel: any Channel,
        keepAliveInterval: Duration? = nil
    ) {
        self.role = role
        self.streamID = streamID
        self.logger = logger
        self._remoteAddress = remoteAddress
        self._localAddress = localAddress
        self.context = parameters.context
        self.keepAliveInterval = keepAliveInterval
        #if DEBUG
        self.logPrefix = "[\(self.role.description)][S\(streamID == nil ? "?" : String(streamID!.rawValue))]"
        #endif
        self.streamStateMachine = QUICStreamStateMachine()
        self.pipelineStateMachine = QUICStreamPipelineStateMachine()
        self.swiftNetworkStreamHandle = SwiftNetworkStreamHandle()

        self.connectionChannel = connectionChannel
        self.eventLoop = connectionChannel.eventLoop
        self.allocator = connectionChannel.allocator
        self._isActive = Atomic(false)
        self._isWritable = Atomic(true)
        self._closePromise = connectionChannel.eventLoop.makePromise(of: Void.self)
        self._pipeline = ChannelPipeline(channel: self)

        // This creates a retain cycle, it's broken in close '_close(error:promise:)'
        self.reference = ProtocolInstanceReference(custom: self)
    }

    internal init?(
        role: Role,
        local: Endpoint,
        remote: Endpoint,
        parameters: Parameters,
        path: PathProperties,
        streamID: QUICStreamID?,
        logger: Logger,
        remoteAddress: SocketAddress,
        localAddress: SocketAddress?,
        listenerProtocol: StreamListenerLinkage,
        connectionChannel: (any Channel)?,
        eventLoop: any EventLoop,
        keepAliveInterval: Duration? = nil,
    ) {
        self.role = role
        self.streamID = streamID
        self.logger = logger
        self._remoteAddress = remoteAddress
        self._localAddress = localAddress
        self.context = parameters.context
        self.keepAliveInterval = keepAliveInterval
        #if DEBUG
        self.logPrefix = "[\(self.role.description)][S\(streamID == nil ? "?" : String(streamID!.rawValue))]"
        #endif
        self.streamStateMachine = QUICStreamStateMachine()
        self.pipelineStateMachine = QUICStreamPipelineStateMachine()
        self.swiftNetworkStreamHandle = SwiftNetworkStreamHandle()

        self.eventLoop = eventLoop
        self.allocator = connectionChannel?.allocator ?? ByteBufferAllocator()
        self.connectionChannel = connectionChannel
        self._isActive = Atomic(false)
        self._isWritable = Atomic(true)
        self._closePromise = eventLoop.makePromise(of: Void.self)

        // This creates a retain cycle, it's broken in close '_close(error:promise:)'
        self.reference = ProtocolInstanceReference(custom: self)

        let linkage: OutboundStreamLinkage
        do throws(NetworkError) {
            linkage = try listenerProtocol.invokeAttachUpperStreamProtocolToNewFlow(
                reference,
                remote: remote,
                local: local,
                parameters: parameters,
                path: path
            )
        } catch {
            self.log("Error attaching lower protocol: \(error)")
            return nil
        }
        self.swiftNetworkStreamHandle = SwiftNetworkStreamHandle(linkage: linkage)
        self._pipeline = ChannelPipeline(channel: self)
    }

    /// The stream direction based on the stream ID and the local peer's role.
    private var streamDirection: QUICStreamDirection {
        guard let streamID = self.streamID else {
            // Direction is unknown until the stream ID is assigned (pendingID state).
            // The caller (handleConnectedEvent) assigns the ID before reading this.
            preconditionFailure("streamDirection accessed before stream ID is assigned")
        }
        switch streamID.type {
        case .clientInitiatedBidirectional, .serverInitiatedBidirectional:
            return .bidirectional
        case .clientInitiatedUnidirectional:
            return self.role == .client ? .sendOnly : .receiveOnly
        case .serverInitiatedUnidirectional:
            return self.role == .server ? .sendOnly : .receiveOnly
        }
    }

    func setConnectionChannel(_ channel: any Channel) {
        self.connectionChannel = channel
    }

    /// Sets a callback invoked when the stream transitions to the connected state and its stream ID is known.
    ///
    /// Only set this for outbound streams where the confirmed stream ID may not be available immediately.
    ///
    /// - Parameter connectedEventHandler: Called with the confirmed ``QUICStreamID`` once the stream connects.
    func setConnectedEventHandler(_ connectedEventHandler: @escaping (QUICStreamID?) -> Void) {
        self.connectedEventHandler = connectedEventHandler
    }

    // Set a disconnected event callback for the stream handler to notify the connection of disconnected events.
    // Disconnected events are not necessarily errors - they can signal clean connection closure or draining completion.
    func setDisconnectedEventHandler(_ disconnectedEventHandler: @escaping (NetworkError?) -> Void) {
        self.disconnectedEventHandler = disconnectedEventHandler
    }

    // Set all handlers to nil
    func clearHandlers() {
        self.connectedEventHandler = nil
        self.disconnectedEventHandler = nil
    }

    /// Local logging function to debug the datapath
    ///
    /// This layer adds the context and fetches the message only if the debug flags are enabled.
    ///
    /// - Parameters:
    ///     - logMessage: The logMessage that is fetched by an autoclosure.  For performance reasons we could gate this behind a flag.
    func log(_ logMessage: @autoclosure () -> String) {
        #if DEBUG
        let message = logMessage()
        self.logger.trace("\(self.logPrefix) \(message)")
        #endif
    }

    // Event that signals that RESET_STREAM was received
    internal func handleInboundAbortedEvent(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        error: SwiftNetwork.NetworkError?
    ) {
        if let error, let applicationErrorCode = error.quicApplicationError, applicationErrorCode >= 0 {
            self.log("Received reset stream error: \(applicationErrorCode)")
            // applicationErrorCode comes from the wire as a QUIC variable-length
            // integer (< 2^62), so the force-unwrap is safe.
            let errorCode = NIOQUICHelpers.QUICApplicationErrorCode(
                UInt64(applicationErrorCode)
            )!
            do {
                try self.handleReceivedResetStream(applicationErrorCode: errorCode)
            } catch {
                self.logger.warning("\(self.logPrefix) handleInboundAbortedEvent errored: \(error)")
                switch error {
                case .wrongDirection:
                    self.closeStream(mode: .disconnectOnly, error: error, promise: nil)
                case .notConnected:
                    break
                }
            }
        } else {
            self.log("handleInboundAbortedEvent called with error: \(String(describing: error))")
        }
    }

    // Extracted to avoid compiler crash in `-O` builds on Swift 6.3
    @inline(never)
    private func handleReceivedResetStream(
        applicationErrorCode errorCode: NIOQUICHelpers.QUICApplicationErrorCode
    ) throws(QUICStreamStateMachine.InvalidTransition) {
        switch try self.streamStateMachine.receiveResetStream(
            applicationErrorCode: errorCode,
            finalSize: 0
        ) {
        case .closeStream(_):
            self.closeStream(
                mode: .closeAndDisconnect,
                error: NIOQUICHelpers.QUICStreamResetError(code: errorCode),
                promise: nil
            )
        case .doNotCloseStream(_):
            self.closeStream(
                mode: .closeAndDisconnect,
                error: NIOQUICHelpers.QUICStreamResetError(code: errorCode),
                promise: nil
            )

        case .ignore(.alreadyFullyReceived):
            self.log("receiveResetStream: all data already received, ignoring")

        case .ignore(.alreadyReset):
            self.log("receiveResetStream: stream already reset, ignoring")
        }
    }

    // Event that signals that STOP_SENDING was received
    internal func handleOutboundAbortedEvent(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        error: SwiftNetwork.NetworkError?
    ) {
        if let error, let applicationErrorCode = error.quicApplicationError, applicationErrorCode >= 0 {
            self.log("Received stop sending error: \(applicationErrorCode)")
            // applicationErrorCode comes from the wire as a QUIC variable-length
            // integer (< 2^62), so the force-unwrap is safe.
            let errorCode = NIOQUICHelpers.QUICApplicationErrorCode(
                UInt64(applicationErrorCode)
            )!
            do {
                switch try self.streamStateMachine.receiveStopSending(
                    applicationErrorCode: errorCode
                ) {
                case .sendReset(let error):
                    if self.understandsStopSending {
                        self.pipeline.fireUserInboundEventTriggered(
                            NIOQUICHelpers.QUICStopSendingEvent(code: errorCode)
                        )
                        if self.streamStateMachine.hasReceivedFin {
                            self.closeStream(
                                mode: .closeOnly,
                                error: NIOQUICHelpers.QUICStopSendingError(code: error.errorCode),
                                promise: nil
                            )
                        }
                    } else {
                        self.closeStream(
                            mode: .closeAndDisconnect,
                            error: NIOQUICHelpers.QUICStopSendingError(code: error.errorCode),
                            promise: nil
                        )
                    }
                case .sendResetAndCloseStream(let error):
                    self.closeStream(
                        mode: .closeAndDisconnect,
                        error: NIOQUICHelpers.QUICStopSendingError(code: error.errorCode),
                        promise: nil
                    )
                case .ignore(.alreadyFinished):
                    self.log("receiveStopSending: stream already finished, ignoring")

                case .ignore(.alreadyReset):
                    self.log("receiveStopSending: stream already reset, ignoring")
                }
            } catch {
                self.logger.warning("\(self.logPrefix) handleOutboundAbortedEvent errored: \(error)")
                switch error {
                case .wrongDirection:
                    self.closeStream(mode: .disconnectOnly, error: error, promise: nil)
                case .notConnected:
                    break
                }
            }
        } else {
            self.log("handleOutboundAbortedEvent called with error: \(String(describing: error))")
        }
    }

    // Send a STOP_SENDING frame to close the read (Note, this stream will receive a RESET_STREAM in return)
    internal func abortInbound(error: NetworkError?) {
        self.fromExternal {
            do throws(NetworkError) {
                self.log("abortInbound")
                switch self.swiftNetworkStreamHandle.invokeAbortInbound() {
                case .proceed(let linkage):
                    try linkage.invokeAbortInbound(self.reference, error: error)
                case .ignore:
                    break
                }
            } catch {
                self.log("Failed to abort inbound: \(error)")
                self.closeStream(mode: .disconnectOnly, error: error, promise: nil)
            }
        }
    }

    // Send a RESET_STREAM frame to close the write
    internal func abortOutbound(error: NetworkError?) {
        self.fromExternal {
            do throws(NetworkError) {
                switch self.swiftNetworkStreamHandle.invokeAbortOutbound() {
                case .proceed(let linkage):
                    try linkage.invokeAbortOutbound(self.reference, error: error)
                case .ignore:
                    break
                }
            } catch {
                self.log("Failed to abort outbound: \(error)")
                self.closeStream(mode: .disconnectOnly, error: error, promise: nil)
            }
        }
    }

    // MARK: - Inbound initializer dispatch
    //
    // Called by `QUICConnectionChannelHandler.channelRead` after the per-stream
    // pipeline state machine returns `.runInitializer`. Each overload runs one
    // of the application-supplied initializer flavours, then on success calls
    // `markInitializerComplete()` and surfaces the now-ready stream.

    /// Run the multiplexer-style initializer; on success yield to the
    /// continuation and trigger the first read.
    internal func initialize(
        multiplexerContinuation continuation: any StreamMultiplexerContinuation,
        streamID: QUICStreamID
    ) {
        continuation.initialize(channel: self, streamID: streamID)
            .whenComplete { result in
                switch result {
                case .success(let output):
                    switch self.pipelineStateMachine.markInitializerComplete() {
                    case .surfaceInitializedStream:
                        continuation.yield(output: output, channel: self)
                        self.tryToAutoRead()
                    case .ignoreAlreadyComplete:
                        break
                    }
                case .failure(let error):
                    self.logger.error("Inbound stream failed to initialize", metadata: ["error": "\(error)"])
                    self.close(promise: nil)
                }
            }
    }

    /// Run the closure-style initializer; on success deliver the ready stream
    /// up `context` and trigger the first read.
    internal func initialize(
        _ context: ChannelHandlerContext,
        _ initializer: @escaping @Sendable (any Channel) -> EventLoopFuture<Void>
    ) {
        initializer(self).assumeIsolated().whenComplete { result in
            switch result {
            case .success:
                switch self.pipelineStateMachine.markInitializerComplete() {
                case .surfaceInitializedStream:
                    context.fireChannelRead(QUICConnectionChannelHandler.wrapInboundOut(self))
                    self.tryToAutoRead()
                case .ignoreAlreadyComplete:
                    break
                }
            case .failure(let error):
                self.logger.error("Inbound stream failed to initialize", metadata: ["error": "\(error)"])
                self.close(promise: nil)
            }
        }
    }

    /// No initializer to run; synchronously deliver the stream up `context`
    /// and trigger the first read.
    internal func initialize(_ context: ChannelHandlerContext) {
        switch self.pipelineStateMachine.markInitializerComplete() {
        case .surfaceInitializedStream:
            context.fireChannelRead(QUICConnectionChannelHandler.wrapInboundOut(self))
            self.tryToAutoRead()
        case .ignoreAlreadyComplete:
            break
        }
    }

    // Start the stream handler
    func start(fromNewFlowHandler: Bool = false) {
        log("start")
        self.eventLoop.preconditionInEventLoop()
        switch self.swiftNetworkStreamHandle.invokeConnect() {
        case .proceed(let linkage):
            linkage.invokeConnect(self.reference)
        case .handleViolation(let reason):
            preconditionFailure("invokeConnect on SwiftNetworkStreamHandle: \(reason)")
        }
        // If started from NewFlowHandler then presumably there is application data waiting in the read queue
        // Do an optimistic read here when the flow is started and then all future data that comes in will get the handleInboundDataAvailableEvent event.
        // The handleInboundDataAvailableEvent signals that its time to read data on the stream.
        if fromNewFlowHandler {
            let bytesRead = self.read()
            if bytesRead > 0 {
                log("start new flow optimistic read returning: \(bytesRead) bytes")
            }
        }
    }

    // Stop and disconnect the stream handler
    func stop(detachFromLowerProtocol: Bool = false) {
        log("stop")
        self.eventLoop.preconditionInEventLoop()
        // Close the stream state machine; the same cleanup follows whether the stream was
        // previously open (.close) or already closed (.ignoreAlreadyClosed).
        switch self.streamStateMachine.close(reason: .clean) {
        case .close:
            break
        case .ignoreAlreadyClosed:
            break
        }

        switch self.swiftNetworkStreamHandle.invokeDisconnect() {
        case .proceed(let linkage):
            linkage.invokeDisconnect(self.reference, error: nil)
        case .ignore:
            break
        }
        if detachFromLowerProtocol {
            switch self.swiftNetworkStreamHandle.invokeDetach() {
            case .proceed(let linkage):
                do throws(NetworkError) {
                    try linkage.invokeDetach(self.reference)
                } catch {
                    self.log("Failed to detach lower protocol: \(error)")
                }
            case .skipAlreadyDetached:
                break
            }
        }
    }

    /// Called when the stream handler receives a disconnected event.
    ///
    /// Runs the close cascade synchronously
    internal func handleDisconnectedEvent(_ from: ProtocolInstanceReference, error: NetworkError?) {
        self.log("handleDisconnectedEvent error: \(String(describing: error))")
        switch self.swiftNetworkStreamHandle.invokeDetach() {
        case .proceed(let linkage):
            do throws(NetworkError) {
                try linkage.invokeDetach(self.reference)
            } catch {
                self.log("Failed to detach lower protocol: \(error)")
            }
        case .skipAlreadyDetached:
            break
        }
        if self.isActive {
            self._close(error: error, promise: nil)
        }
    }

    @inline(__always)
    private func closeStream(mode: StreamClosureState, error: (any Error)?, promise: EventLoopPromise<Void>?) {
        log("closeStream mode: \(mode)")
        switch mode {
        case .closeAndDisconnect:  // Calls close and stop to detach everything
            _close(error: error, promise: promise)
            stop(detachFromLowerProtocol: true)
        case .closeOnly:  // Calls close only
            _close(error: error, promise: promise)
        case .disconnectOnly:  // Calls stop and detach only, could result in disconnect event as well
            stop(detachFromLowerProtocol: true)
        case .detachAndClose:  // Called on disconnect event callback
            switch self.swiftNetworkStreamHandle.invokeDetach() {
            case .proceed(let linkage):
                do throws(NetworkError) {
                    try linkage.invokeDetach(self.reference)
                } catch {
                    self.log("Failed to detach lower protocol: \(error)")
                }
            case .skipAlreadyDetached:
                break
            }
            if self.isActive {
                self._close(error: error, promise: promise)
            }
        }
    }

    /// Drains SwiftNetwork's stream buffer and delivers bytes downstream, but only when there is an outstanding read
    /// request from the downstream consumer, i.e. `self.pendingRead == true`.
    ///
    /// When there is no demand, we leave bytes inside SwiftNetwork's buffer as SwiftNetwork uses its buffer occupancy
    /// to drive flow control. If we always drained it, SwiftNetwork would think the data has been consumed and keep
    /// advancing its flow control window, defeating backpressure and letting unbounded bytes accumulate here.
    private func deliverInboundDataDownstreamIfDemand() {
        // Only continue if there is a pending read.
        guard self.pendingRead else {
            return
        }

        // Pull the latest bytes out of SwiftNetwork into `self.bufferedReadData`.
        let bytesRead = self.read()
        if bytesRead > 0 {
            self.log("buffered \(bytesRead) bytes from inbound data available event")
        }

        self.streamRead()
    }

    // Called when data is available to be read on the stream
    // NOTE: New flows with a single read with not get this event and should optimistically read when the flow is started.
    // This event will be called for all future input available after the flow starts.
    internal func handleInboundDataAvailableEvent(_ from: ProtocolInstanceReference) {
        switch self.streamStateMachine.inboundDataAvailable() {
        case .readData:
            self.log("received input room available")
            self.deliverInboundDataDownstreamIfDemand()

        case .doNotRead:
            break
        }
    }

    /// Fires `channelReadComplete` down the pipeline and then attempts issuing a read event on the pipeline.
    private func fireChannelReadComplete() {
        self.log("ChildChannel fire channel read complete")
        self.pipeline.fireChannelReadComplete()

        self.tryToAutoRead()
    }

    /// Issues a `read()` on the pipeline if `autoRead` is enabled and there is no outstanding read request.
    func tryToAutoRead() {
        if self.autoRead, !self.pendingRead {
            self.pipeline.read()
        }
    }

    /// Returns `true` if data was actually delivered
    private func flushBufferedReadData() -> Bool {
        guard self.bufferedReadData.readableBytes > 0 else {
            return false
        }
        // We will now satisfy a pending read request. Reset the flag.
        self.pendingRead = false
        let bytesRead = self.bufferedReadData.readableBytes
        self.log("Read \(bytesRead) bytes from stream")
        self.pipeline.fireChannelRead(self.bufferedReadData)
        self.bufferedReadData.clear()
        return true
    }

    /// Returns `true` if an end-of-stream event was surfaced
    private func surfaceReadCompletion() -> Bool {
        switch self.streamStateMachine.completeRead() {
        case .reportFin(let streamFullyClosed):
            self.pipeline.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
            if streamFullyClosed {
                self.log("stream is now fully closed")
                self.closeStream(mode: .disconnectOnly, error: nil, promise: nil)
                self.shutdownStream(direction: .all, applicationErrorCode: nil)
            }
            return true

        case .reportPeerReset:
            self.pipeline.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
            return true

        case .nothingToReport:
            return false
        }
    }

    func streamRead() {
        self.eventLoop.preconditionInEventLoop()

        // Emit channel read complete when:
        // - any read was fired, or
        // - FIN or peer RESET is emitted
        let flushed = self.flushBufferedReadData()
        let endOfStream = self.surfaceReadCompletion()

        if flushed || endOfStream {
            self.fireChannelReadComplete()
        }
    }

    internal func handleOutboundRoomAvailableEvent(_ from: ProtocolInstanceReference) {
        log("received output available")
    }

    // Stream handler is in the connected state
    internal func handleConnectedEvent(_ from: ProtocolInstanceReference) {
        self.eventLoop.preconditionInEventLoop()
        log("connected")

        guard let metadata: ProtocolMetadata<QUICProtocol> = self.getStreamMetadata() else {
            self.logger.error("connected stream does not have access to metadata")
            self.connectedEventHandler?(nil)
            return
        }

        self._isActive.store(true, ordering: .sequentiallyConsistent)
        self.pipeline.fireChannelRegistered()
        self.pipeline.fireChannelActive()
        log("setting up metadata handlers")
        if let keepAliveTime = self.keepAliveInterval {
            log("setting keepalive interval")
            let keepAlive = UInt16(truncatingIfNeeded: keepAliveTime.components.seconds)
            metadata.connectionMetadata?.setKeepalive(keepAlive: keepAlive)
        }

        guard let rawStreamID = metadata.streamID else {
            self.logger.error("connected stream does not have a stream ID assigned")
            self.connectedEventHandler?(nil)
            return
        }

        let streamID = QUICStreamID(rawValue: rawStreamID)
        self.streamID = streamID
        #if DEBUG
        self.logPrefix = "[\(self.role.description)][S\(streamID.rawValue)]"
        #endif

        switch self.streamStateMachine.streamConnected(direction: self.streamDirection) {
        case .activateStream:
            break

        case .ignoreAlreadyConnected:
            self.logger.warning("\(self.logPrefix) handleConnectedEvent called when already connected")
            assertionFailure("\(self.logPrefix) handleConnectedEvent called when already connected")
            return

        case .ignoreAlreadyClosed:
            self.logger.warning("\(self.logPrefix) handleConnectedEvent called when already closed")
            assertionFailure("\(self.logPrefix) handleConnectedEvent called when already closed")
            return
        }

        self.connectedEventHandler?(self.streamID)
    }

    internal func setNewFlowMetadata(_ newMetadata: ProtocolMetadata<QUICProtocol>) {
        self.originalMetadata = newMetadata
    }

    internal func getStreamMetadata() -> ProtocolMetadata<QUICProtocol>? {
        if let originalMetadata {
            return originalMetadata
        } else {
            if let metadata: ProtocolMetadata<QUICProtocol> = self.getMetadata() {
                self.originalMetadata = metadata
                return metadata
            }
        }
        return nil
    }

    // Get metadata about the stream from the internal QUIC stack
    private final func getMetadata<P: NetworkProtocol>() -> ProtocolMetadata<P>? {
        self.fromExternal {
            switch self.swiftNetworkStreamHandle.invokeGetMetadata() {
            case .proceed(let linkage):
                return linkage.invokeGetMetadata(self.reference) as ProtocolMetadata<P>?
            case .ignore:
                return nil
            }
        }
    }

    var isStreamChannelActive: Bool {
        self.isActive
    }

    // Write data to the QUIC stream, optionally setting the FIN flag.
    internal func writeDataToStream(_ byteBuffer: inout ByteBuffer, fin: Bool = false) -> Bool {
        self.writeBuffersToStream(CollectionOfOne(byteBuffer), fin: fin)
    }

    /// Write multiple buffers to the QUIC stream as a single `FrameArray`, optionally setting FIN.
    ///
    /// - Parameters:
    ///   - buffers: The buffers to write. Each becomes a separate `Frame` in one `invokeSendStreamData` call.
    ///   - fin: Whether to set the FIN flag on the last frame, signalling end of stream.
    /// - Returns: `true` if the write was accepted, `false` if the stream is in a state that
    ///   prevents writing (e.g. already finished or reset).
    internal func writeBuffersToStream(_ buffers: some BidirectionalCollection<ByteBuffer>, fin: Bool = false) -> Bool {
        self.eventLoop.preconditionInEventLoop()
        log("writeBuffersToStream")
        do {
            var frameArray = FrameArray(capacity: buffers.count)
            var totalBytes = 0
            guard buffers.count > 0 || fin else { return true }
            if fin {
                if buffers.count > 0 {
                    var index = 0
                    for buffer in buffers {
                        totalBytes += buffer.readableBytes
                        if index == buffers.count - 1 {
                            var frame = Frame(copyBuffer: buffer.readableBytesUInt8Span)
                            frame.connectionComplete = true
                            frameArray.add(frame: frame)
                        } else {
                            frameArray.add(frame: Frame(copyBuffer: buffer.readableBytesUInt8Span))
                        }
                        index += 1
                    }
                } else {
                    var frame = Frame(count: 0)
                    frame.connectionComplete = true
                    frameArray.add(frame: frame)
                }
                let _ = try self.streamStateMachine.sendFin()
            } else {
                for buffer in buffers {
                    totalBytes += buffer.readableBytes
                    frameArray.add(frame: Frame(copyBuffer: buffer.readableBytesUInt8Span))
                }

                if totalBytes == 0 {
                    return true
                }
            }
            log("write \(frameArray.count) frames, \(totalBytes) bytes, fin: \(fin)")

            switch self.swiftNetworkStreamHandle.invokeSendStreamData() {
            case .proceed(let linkage):
                try linkage.invokeSendStreamData(self.reference, streamData: frameArray)
            case .handleViolation(let reason):
                frameArray.finalizeAllFramesAsFailed()
                throw SwiftNetworkStreamHandle.violationError(operation: "invokeSendStreamData", reason: reason)
            }
            return true
        } catch {
            self.logger.error("\(self.logPrefix) writeBuffersToStream failed: \(error)")
            return false
        }
    }

    internal func sendFIN() throws {
        self.eventLoop.preconditionInEventLoop()

        switch try self.streamStateMachine.sendFin() {
        case .sendFin:
            self.log("sending connection complete")
            var finFrame = Frame(copyBuffer: [])
            finFrame.connectionComplete = true
            var finFrameArray = FrameArray(frame: finFrame)
            switch self.swiftNetworkStreamHandle.invokeSendStreamData() {
            case .proceed(let linkage):
                try linkage.invokeSendStreamData(self.reference, streamData: finFrameArray)
            case .handleViolation(let reason):
                finFrameArray.finalizeAllFramesAsFailed()
                throw SwiftNetworkStreamHandle.violationError(operation: "invokeSendStreamData", reason: reason)
            }

        case .ignore(.alreadyFinished):
            self.log("sendFIN ignored: send side already finished")

        case .ignore(.streamReset):
            self.log("sendFIN ignored: send side already reset")
        }
    }

    // Read from the stream
    internal func read() -> Int {
        self.eventLoop.preconditionInEventLoop()
        log("read attempt")
        switch self.streamStateMachine.attemptRead() {
        case .doNotRead:
            log("read closed")
            return 0
        case .proceedWithRead:
            break
        }

        var frameArray: FrameArray?
        do throws(NetworkError) {
            switch self.swiftNetworkStreamHandle.invokeReceiveStreamData() {
            case .proceed(let linkage):
                frameArray = try linkage.invokeReceiveStreamData(
                    self.reference,
                    minimumBytes: 1,
                    maximumBytes: Int.max
                )
            case .handleViolation(let reason):
                throw SwiftNetworkStreamHandle.violationError(operation: "invokeReceiveStreamData", reason: reason)
            }
        } catch {
            self.log("Failed to read with error: \(error)")
            frameArray = nil
        }

        guard var frames = frameArray, frames.count > 0 else {
            self.log("Failed to receive stream data")
            return 0
        }

        var readDataCount: Int = 0
        frames.iterateMutableFrames { frame in
            // Process data before FIN: a frame may carry both payload and the
            // FIN flag. The FIN transitions the receive state machine to
            // `dataRecvd`, so any data must be buffered first.
            readDataCount +=
                frame.span?.withUnsafeBufferPointer { spanBuffer in
                    guard spanBuffer.count > 0 else { return 0 }
                    // Mark data as received in state machine
                    do {
                        switch try self.streamStateMachine.receiveData() {
                        case .bufferData:
                            return self.bufferedReadData.writeWithUnsafeMutableBytes(
                                minimumWritableBytes: spanBuffer.count
                            ) { buffer in
                                buffer.copyMemory(from: UnsafeRawBufferPointer(spanBuffer))
                                return spanBuffer.count
                            }

                        case .doNotBuffer(.allDataReceived):
                            self.logger.warning(
                                "\(self.logPrefix) receiveData: data arrived after all data received"
                            )
                            assertionFailure("receiveData: data arrived after all data received")
                            return 0

                        case .doNotBuffer(.streamReset):
                            self.logger.warning(
                                "\(self.logPrefix) receiveData: data arrived after stream reset"
                            )
                            assertionFailure("receiveData: data arrived after stream reset")
                            return 0
                        }
                    } catch {
                        self.logger.warning("\(self.logPrefix) read errored: \(error)")
                        return 0
                    }
                } ?? 0

            if frame.connectionComplete {
                // Signals that a FIN was delivered - transition receive state machine
                self.log("connection complete received on frame")
                do {
                    switch try self.streamStateMachine.receiveFin(finalSize: 0) {
                    case .markAllDataReceived:
                        break
                    case .ignore(.alreadyReceivedFin):
                        break
                    case .ignore(.streamReset):
                        self.log("receiveFin: stream was reset")
                    }
                } catch {
                    self.logger.warning("\(self.logPrefix) receiveFin errored: \(error)")
                }
            }
            frame.finalize(success: true)
            return true
        }
        return readDataCount
    }

    // TODO: Workaround compiler crash while evaluating request ExecuteSILPipelineRequest
    @_optimize(none)
    func shutdownStream(
        direction: StreamShutdownDirection,
        applicationErrorCode: QUICApplicationErrorCode?
    ) {
        if let metadata: ProtocolMetadata<QUICProtocol> = self.getStreamMetadata() {
            if let applicationErrorCode {
                metadata.applicationError = applicationErrorCode.rawValue
            }
        }
        switch self.streamStateMachine.shutdownStream(
            direction: direction,
            applicationErrorCode: applicationErrorCode
        ) {
        case .cleanupOnly:
            break
        case .shutdownBoth:
            self.log(
                "shutdownStream shutting down in both directions, applicationErrorCode: \(String(describing: applicationErrorCode))"
            )
            if self.swiftNetworkStreamHandle.isAttached {
                self.closeStream(mode: .disconnectOnly, error: nil, promise: nil)
            }

        case .closeRead(let streamFullyClosed):
            // Send STOP_SENDING so no more data is received from the peer.
            // Per RFC 9000 §3.4, send and receive are independent state machines,
            // so we always use the graceful path regardless of the write side's state.
            self.log(
                "shutdownStream shutting down read, applicationErrorCode: \(String(describing: applicationErrorCode))"
            )
            self.abortInbound(error: NetworkError(quicApplicationError: applicationErrorCode?.rawValue ?? 0))
            if streamFullyClosed {
                self.closeStream(mode: .disconnectOnly, error: nil, promise: nil)
            }

        case .readAlreadyClosed:
            self.log("shutdownStream read side already closed")

        case .readPeerReset:
            self.log("shutdownStream read side already reset by peer")

        case .sendReset(let code, let streamFullyClosed):
            // Close the write side by sending RESET_STREAM.
            // Per RFC 9000 §3.4, send and receive are independent state machines,
            // so we always use the graceful path regardless of the read side's state.
            self.log("shutdownStream shutting down write, errorCode: \(code)")
            // TODO: https://github.com/apple/swift-nio-quic/issues/3
            // When no application error code is provided, we fall back to 0.
            // Per RFC 9000 §19.4, RESET_STREAM carries an application protocol error
            // code — it may not be appropriate to send this frame without one.
            // It appears SwiftNetwork's `abortOutbound` always sends the frame.
            self.abortOutbound(error: NetworkError(quicApplicationError: code.rawValue))
            if streamFullyClosed {
                self.closeStream(mode: .disconnectOnly, error: nil, promise: nil)
            }

        case .writeAlreadyFinished:
            self.log("shutdownStream write side already finished cleanly")

        case .writeAlreadyReset(let code):
            self.log("shutdownStream write side already reset with code \(code)")

        case .cannotShutdown:
            self.logger.error("[S\(String(describing: self.streamID))] cannot shutdown in requested direction")
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICChannelStreamHandler: UpperProtocolHandler {
    internal func handleNetworkProtocolEvent(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        event: SwiftNetwork.NetworkProtocolEvent
    ) {
        // do nothing
    }

    // UpperProtocolHandler conformance
    internal func attachLowerStreamProtocol(
        _ lowerProtocol: SwiftNetwork.ProtocolInstanceReference,
        remote: SwiftNetwork.Endpoint?,
        local: SwiftNetwork.Endpoint?,
        parameters: SwiftNetwork.Parameters?,
        path: SwiftNetwork.PathProperties?
    ) throws(SwiftNetwork.NetworkError) {
        throw NetworkError.posix(ENOTSUP)
    }

    // UpperProtocolHandler conformance
    internal func attachLowerProtocol(
        _ lowerProtocol: SwiftNetwork.ProtocolInstanceReference,
        remote: SwiftNetwork.Endpoint?,
        local: SwiftNetwork.Endpoint?,
        parameters: SwiftNetwork.Parameters?,
        path: SwiftNetwork.PathProperties?
    ) throws(SwiftNetwork.NetworkError) {
        throw NetworkError.posix(ENOTSUP)
    }
}

// MARK: - `Channel` and `ChannelCore` conformance

@available(anyAppleOS 26, *)
extension QUICChannelStreamHandler: Channel, ChannelCore {
    var parent: (any Channel)? {
        self.connectionChannel
    }

    @usableFromInline
    struct SynchronousOptions: NIOSynchronousChannelOptions {
        @usableFromInline
        let channel: QUICChannelStreamHandler

        fileprivate init(channel: QUICChannelStreamHandler) {
            self.channel = channel
        }

        @inlinable
        func setOption<Option: ChannelOption>(_ option: Option, value: Option.Value) throws {
            try self.channel._setOption0(option, value: value)
        }

        @inlinable
        func getOption<Option: ChannelOption>(_ option: Option) throws -> Option.Value {
            try self.channel._getOption0(option)
        }
    }

    @usableFromInline
    var syncOptions: (any NIOSynchronousChannelOptions)? {
        SynchronousOptions(channel: self)
    }
    @inlinable
    func getOption<Option: ChannelOption>(_ option: Option) -> EventLoopFuture<Option.Value> {
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.submit { try self._getOption0(option) }
        }
        return self.eventLoop.makeCompletedFuture { try self._getOption0(option) }
    }

    @inlinable
    func _getOption0<Option: ChannelOption>(_ option: Option) throws -> Option.Value {
        self.eventLoop.preconditionInEventLoop()
        if option is ChannelOptions.Types.QUICStreamIDChannelOption {
            return self.streamID!.rawValue as! Option.Value
        }
        if option is ChannelOptions.Types.HalfCloseOnStopSendingChannelOption {
            return self.understandsStopSending as! Option.Value
        }
        if option is ChannelOptions.Types.AutoReadOption {
            return self.autoRead as! Option.Value
        }
        fatalError()
    }

    @inlinable
    func setOption<Option: ChannelOption>(_ option: Option, value: Option.Value) -> EventLoopFuture<Void>
    where Option.Value: Sendable {
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.submit { try self._setOption0(option, value: value) }
        }
        return self.eventLoop.makeCompletedFuture { try self._setOption0(option, value: value) }
    }

    @inlinable
    func _setOption0<Option: ChannelOption>(_ option: Option, value: Option.Value) throws {
        self.eventLoop.preconditionInEventLoop()
        if option is ChannelOptions.Types.HalfCloseOnStopSendingChannelOption {
            self.understandsStopSending = value as! Bool
            return
        }
        if option is ChannelOptions.Types.AutoReadOption {
            self.autoRead = value as! Bool
            return
        }
        fatalError()
    }

    @usableFromInline
    var closeFuture: EventLoopFuture<Void> {
        self._closePromise.futureResult
    }

    @usableFromInline
    var pipeline: ChannelPipeline {
        self._pipeline
    }

    @usableFromInline
    var isWritable: Bool {
        self._isWritable.load(ordering: .sequentiallyConsistent)
    }

    @usableFromInline
    var isActive: Bool {
        self._isActive.load(ordering: .sequentiallyConsistent)
    }

    @usableFromInline
    var _channelCore: any ChannelCore {
        self.eventLoop.preconditionInEventLoop()
        return self
    }

    @usableFromInline
    var localAddress: SocketAddress? {
        self._localAddress
    }

    @usableFromInline
    var remoteAddress: SocketAddress? {
        self._remoteAddress
    }

    @inlinable
    func localAddress0() throws -> SocketAddress {
        self.eventLoop.preconditionInEventLoop()
        self.log("localAddress0")
        guard let localAddress = self.localAddress else {
            throw ChannelError.unknownLocalAddress
        }

        return localAddress
    }

    @inlinable
    func remoteAddress0() throws -> SocketAddress {
        self.eventLoop.preconditionInEventLoop()
        self.log("remoteAddress0")
        guard let remoteAddress = self.remoteAddress else {
            throw ChannelError.operationUnsupported
        }

        return remoteAddress
    }

    @inlinable
    func write0(_ data: NIOAny, promise: EventLoopPromise<Void>?) {
        self.eventLoop.preconditionInEventLoop()
        self.log("ChildChannel write0")
        // TODO: Make sure the writes are batched
        let write = self.unwrapData(data, as: ByteBuffer.self)
        self.bufferedWriteData.append(write)
        promise?.succeed()
    }

    @inlinable
    func flush0() {
        self.eventLoop.preconditionInEventLoop()
        self.log("ChildChannel flush0")
        if self.writeBuffersToStream(bufferedWriteData, fin: false) {
            self.log("ChildChannel flush0 succeeded")
            bufferedWriteData.removeAll(where: { _ in true })
        }
    }

    @inlinable
    func read0() {
        self.eventLoop.preconditionInEventLoop()
        self.log("ChildChannel read0")

        self.pendingRead = true
        // Try to satisfy the read immediately if there is data already buffered in SwiftNetwork.
        self.deliverInboundDataDownstreamIfDemand()
    }

    private func _close(error: (any Error)?, promise: EventLoopPromise<Void>?) {
        self.eventLoop.preconditionInEventLoop()
        guard self.isActive else {
            promise?.succeed()
            return
        }
        self.log("_close error: \(String(describing: error))")
        self._isActive.store(false, ordering: .sequentiallyConsistent)
        // Put calls to _close on the next runloop tick because it calls fireChannelInactive
        self.eventLoop.assumeIsolated().execute {
            if let error {
                self.pipeline.fireErrorCaught(error)
            }

            self.pipeline.fireChannelInactive()
            self.pipeline.fireChannelUnregistered()
            self.removeHandlers(pipeline: self.pipeline)
            self._closePromise.succeed(())
            promise?.succeed()
            // Fire disconnect if there is no error present
            if let disconnect = self.disconnectedEventHandler {
                disconnect(nil)
            }
            // Break the retain cycle between the ref and self.
            self.reference = ProtocolInstanceReference()
            self.clearHandlers()
        }
    }

    /// Close the send side gracefully: flush any buffered write data with a FIN,
    /// or send a bare FIN when nothing is buffered. Idempotent — `sendFIN` reacts
    /// to the state machine's `sendFin` action, so it is a no-op once the send
    /// side has already finished or been reset.
    private func closeOutputSide() {
        if self.bufferedWriteData.isEmpty {
            do {
                try self.sendFIN()
            } catch {
                self.log("Sendig FIN error: \(error)")
            }
        } else {
            if self.writeBuffersToStream(self.bufferedWriteData, fin: true) {
                self.log("ChildChannel close0 succeeded sending FIN")
                self.bufferedWriteData.removeAll(where: { _ in true })
            }
        }
    }

    @inlinable
    func close0(error: any Error, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        self.eventLoop.preconditionInEventLoop()
        self.log("ChildChannel close0 mode \(mode)")

        switch mode {
        case .output:
            if self.streamStateMachine.canWrite {
                self.closeOutputSide()
                promise?.succeed()
            } else {
                promise?.fail(ChannelError.outputClosed)
                return
            }
        case .input:
            switch self.streamStateMachine.completeRead() {
            case .nothingToReport:
                // `shutdownStream` handles the QUIC-level signaling as needed,
                // e.g. STOP_SENDING for `.read`, RESET_STREAM for `.write`.
                self.shutdownStream(direction: .read, applicationErrorCode: nil)
                if self.streamStateMachine.isWriteClosed {
                    self.log("Input and output for stream are now closed, stream will be queued up for closure")
                }
                promise?.succeed()
            default:
                promise?.fail(ChannelError.inputClosed)
                return
            }
        case .all:
            // Gracefully shutdown both sides.
            if self.streamID != nil {
                var shutdownRead = false
                var shutdownWrite = false

                switch self.streamDirection {
                case .bidirectional:
                    shutdownRead = true
                    shutdownWrite = true
                case .receiveOnly:
                    shutdownRead = true
                case .sendOnly:
                    shutdownWrite = true
                }

                if shutdownRead {
                    switch self.streamStateMachine.completeRead() {
                    case .nothingToReport:
                        self.shutdownStream(direction: .read, applicationErrorCode: nil)
                    default:
                        break
                    }
                }

                if shutdownWrite, self.streamStateMachine.canWrite {
                    self.closeOutputSide()
                }
            }

            // Fire channel inactive so the application observes the close. The lower
            // protocol is left attached; the natural disconnect event from
            // SwiftNetwork (after all ACKs arrive) triggers detach via
            // `handleDisconnectedEvent`.
            self.closeStream(mode: .closeOnly, error: nil, promise: promise)
        }
    }

    @inlinable
    func triggerUserOutboundEvent0(_ event: Any, promise: EventLoopPromise<Void>?) {
        self.eventLoop.preconditionInEventLoop()

        self.log(
            "ChildChannel triggerUserOutboundEvent0: \(event)"
        )

        switch event {
        case let event as NIOQUICHelpers.QUICStopSendingEvent:
            // Send a STOP_SENDING frame to close the read (input) side
            self.shutdownStream(
                direction: .read,
                applicationErrorCode: event.code
            )
            if self.streamStateMachine.isWriteClosed {
                // Both sides now closed
                self.closeStream(mode: .closeOnly, error: nil, promise: promise)
            } else {
                promise?.succeed()
            }
        case let event as NIOQUICHelpers.QUICResetStreamEvent:
            // Send RESET_STREAM to close the write (output) side
            self.shutdownStream(
                direction: .write,
                applicationErrorCode: event.code
            )
            switch self.streamStateMachine.completeRead() {
            case .nothingToReport:
                promise?.succeed()
                break
            default:
                break
            }
        default:
            break
        }
        guard let promise else {
            return
        }
        // We are not expecting any outbound events here so we are simply
        // failing the promise
        promise.fail(ChannelError.operationUnsupported)
    }

    @inlinable
    func channelRead0(_ data: NIOAny) {
        // do nothing
    }

    @inlinable
    func errorCaught0(error: any Error) {
        // do nothing
    }

    @inlinable
    func register0(promise: EventLoopPromise<Void>?) {
        fatalError("not implemented \(#function)")
    }

    @inlinable
    func bind0(to: SocketAddress, promise: EventLoopPromise<Void>?) {
        fatalError("not implemented \(#function)")
    }

    @inlinable
    func connect0(to: SocketAddress, promise: EventLoopPromise<Void>?) {
        fatalError("not implemented \(#function)")
    }
}

@available(anyAppleOS 26, *)
extension QUICChannelStreamHandler {
    /// Appends bytes directly into the NIO read buffer.
    ///
    /// - Note: This method is intended to only be used in tests to simulate a live peer sending data over the network.
    internal func _testOnly_appendToBufferedReadData(_ buffer: ByteBuffer) {
        self.eventLoop.preconditionInEventLoop()
        self.bufferedReadData.writeImmutableBuffer(buffer)
    }
}
