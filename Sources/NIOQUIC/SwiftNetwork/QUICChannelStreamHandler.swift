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
#elseif canImport(Android)
import Android
#elseif canImport(Musl)
import Musl
#endif

enum StreamClosureState {
    case closeAndDisconnect  // Calls close and stop to detach everything
    case closeOnly  // Calls close only
    case disconnectOnly  // Calls stop and detach only, could result in disconnect event as well
}

private enum LogLevel {
    case trace
    case warning
    case error
}

/// `QUICChannelStreamHandler` is the bridge between SwiftNetwork and our code on the application-side;
/// one `QUICChannelStreamHandler` exists for each QUIC stream.
@available(anyAppleOS 26, *)
final class QUICChannelStreamHandler: ExternalInboundStreamLinkage, @unchecked Sendable {

    // MARK: Channel and ChannelCore conformanace

    /// The channel of the QUIC Connection.
    @usableFromInline
    var connectionChannel: (any Channel)?
    /// The event loop of the connection channel.
    @usableFromInline
    let eventLoop: any EventLoop
    /// The allocator of the parent — computed so it does not occupy inline storage.
    @usableFromInline
    var allocator: ByteBufferAllocator {
        self.connectionChannel?.allocator ?? ByteBufferAllocator()
    }
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
    /// The address of the remote peer, which follows the connection's active path.
    @usableFromInline
    let _remoteAddress: SendableAddress
    let _closePromise: EventLoopPromise<Void>

    // MARK: Private Constant state

    internal typealias LowerProtocol = BaseOutboundStreamLinkage

    private let role: Role
    private let logger: Logger
    /// The connection's queued callouts: the pipeline events and promise completions the stack's
    /// events cause are queued there, and delivered once the event context is released.
    let callouts: QUICCallouts
    internal var identifier: InstanceIdentifier = InstanceIdentifier()

    internal var eventManager = ProtocolEventManager()
    internal var streamStateMachine: QUICStreamStateMachine
    private var pipelineStateMachine: QUICStreamPipelineStateMachine
    internal var swiftNetworkStreamHandle: SwiftNetworkStreamHandle

    // Stream state machine (RFC 9000 Section 3 compliant)
    internal var context: SwiftNetwork.NetworkContext
    internal var streamID: QUICStreamID?

    // MARK: Private mutable state

    /// Whether the application has opted in to half-closure semantics for received STOP_SENDING.
    private var understandsStopSending = false
    #if DEBUG
    private var logPrefix: String = ""
    #endif
    private var bufferedReadData: ByteBuffer = ByteBuffer()
    private var bufferedWriteData: TinyArray<ByteBuffer> = []
    /// Stores as a UInt16 to reduce the memory footprint
    private var _keepAliveInterval: UInt16? = nil

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

    /// Whether `_close` has already run.
    private var isClosed: Bool = false

    /// Whether `_close`'s deferred completion has run. The handler unregisters once this is set
    /// and it has detached from its flow, which delivers events to it until then.
    private var isCloseCompleted: Bool = false

    /// Whether the pipeline has been closed: its close was delivered, `channelInactive` included if
    /// it was active. Pipeline events are queued (see `QUICCallouts`), so this trails `isClosed`,
    /// which is set as soon as the stream closes.
    private var isPipelineClosed: Bool = false

    /// Whether a `channelRead` of the buffered bytes is queued and not yet delivered.
    private var isReadQueued: Bool = false

    private var originalMetadata: ProtocolMetadata<QUICProtocol>?
    private var connectedEventHandler: ((QUICStreamID?) -> Void)?
    private var disconnectedEventHandler: ((NetworkError?) -> Void)?

    internal init(
        role: Role,
        parameters: Parameters,
        streamID: QUICStreamID?,
        logger: Logger,
        remoteAddress: SendableAddress,
        localAddress: SocketAddress?,
        connectionChannel: any Channel,
        callouts: QUICCallouts,
        keepAliveInterval: Duration? = nil,
        in eventContext: inout NetworkContext.EventContext
    ) {
        self.role = role
        self.streamID = streamID
        self.logger = logger
        self.callouts = callouts
        self._remoteAddress = remoteAddress
        self._localAddress = localAddress
        self.context = parameters.context
        if let keepAlive = keepAliveInterval {
            self._keepAliveInterval = UInt16(clamping: keepAlive.components.seconds)
        }
        #if DEBUG
        self.logPrefix = "[\(self.role.description)][S\(streamID == nil ? "?" : String(streamID!.rawValue))]"
        #endif
        self.streamStateMachine = QUICStreamStateMachine()
        self.pipelineStateMachine = QUICStreamPipelineStateMachine()
        self.swiftNetworkStreamHandle = SwiftNetworkStreamHandle()

        self.connectionChannel = connectionChannel
        self.eventLoop = connectionChannel.eventLoop
        self._isActive = Atomic(false)
        self._isWritable = Atomic(true)
        self._closePromise = connectionChannel.eventLoop.makePromise(of: Void.self)
        self._pipeline = ChannelPipeline(channel: self)

        self.identifier = InstanceIdentifier(eventManager: &self.eventManager, in: &eventContext)
    }

    internal init?(
        role: Role,
        local: Endpoint,
        remote: Endpoint,
        parameters: Parameters,
        path: PathProperties,
        streamID: QUICStreamID?,
        logger: Logger,
        remoteAddress: SendableAddress,
        localAddress: SocketAddress?,
        listenerProtocol: BaseStreamListenerLinkage,
        connectionChannel: (any Channel)?,
        eventLoop: any EventLoop,
        callouts: QUICCallouts,
        keepAliveInterval: Duration? = nil
    ) {
        #if DEBUG
        assert(
            !callouts.isInsideStack,
            "QUICChannelStreamHandler attaches on construction, which requires that no event context is held"
        )
        #endif
        self.role = role
        self.streamID = streamID
        self.logger = logger
        self.callouts = callouts
        self._remoteAddress = remoteAddress
        self._localAddress = localAddress
        self.context = parameters.context
        if let keepAlive = keepAliveInterval {
            self._keepAliveInterval = UInt16(clamping: keepAlive.components.seconds)
        }
        #if DEBUG
        self.logPrefix = "[\(self.role.description)][S\(streamID == nil ? "?" : String(streamID!.rawValue))]"
        #endif
        self.streamStateMachine = QUICStreamStateMachine()
        self.pipelineStateMachine = QUICStreamPipelineStateMachine()
        self.swiftNetworkStreamHandle = SwiftNetworkStreamHandle()

        self.eventLoop = eventLoop
        self.connectionChannel = connectionChannel
        self._isActive = Atomic(false)
        self._isWritable = Atomic(true)
        self._closePromise = eventLoop.makePromise(of: Void.self)

        self.identifier = InstanceIdentifier(context: self.context, eventManager: &self.eventManager)
        // Before attaching: the calls the attach queues run as it returns, and may reach the pipeline.
        self._pipeline = ChannelPipeline(channel: self)

        do throws(NetworkError) {
            try callouts.callFramework { () throws(NetworkError) in
                try listenerProtocol.invokeAttachUpperProtocolToNewFlow(
                    BaseInboundStreamLinkage(external: self),
                    remote: remote,
                    local: local,
                    parameters: parameters,
                    path: path
                )
            }
        } catch {
            self.log("Error attaching lower protocol: \(error)")
            self.callouts.enter(as: self) { eventContext in
                self.unregisterEventManager(in: &eventContext)
            }
            return nil
        }
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
    /// The handler is called with the event context held, so it must not call into NIO or consumer
    /// code: the connection defers its work to the next loop tick.
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
    fileprivate func log(
        _ logMessage: @autoclosure () -> String,
        metadata: @autoclosure () -> Logger.Metadata? = nil,
        logLevel: LogLevel = .trace
    ) {
        switch logLevel {
        case .error:
            let message = logMessage()
            #if DEBUG
            self.logger.error("\(self.logPrefix) \(message)", metadata: metadata())
            #else
            self.logger.error("\(message)", metadata: metadata())
            #endif
        case .trace:
            #if DEBUG
            let message = logMessage()
            self.logger.trace("\(self.logPrefix) \(message)", metadata: metadata())
            #endif
        case .warning:
            let message = logMessage()
            #if DEBUG
            self.logger.warning("\(self.logPrefix) \(message)", metadata: metadata())
            #else
            self.logger.warning("\(message)", metadata: metadata())
            #endif
        }
    }

    // Event that signals that RESET_STREAM was received
    internal func handleInboundAbortedEvent(
        error: SwiftNetwork.NetworkError?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        if let error, let applicationErrorCode = error.quicApplicationError, applicationErrorCode >= 0 {
            self.log("Received reset stream error", metadata: ["applicationErrorCode": "\(applicationErrorCode)"])
            // applicationErrorCode comes from the wire as a QUIC variable-length
            // integer (< 2^62), so the force-unwrap is safe.
            let errorCode = NIOQUICHelpers.QUICApplicationErrorCode(
                UInt64(applicationErrorCode)
            )!
            do {
                try self.handleReceivedResetStream(applicationErrorCode: errorCode, in: &eventContext)
            } catch {
                self.log("handleInboundAbortedEvent errored: \(error)", logLevel: .warning)
                switch error {
                case .wrongDirection:
                    self.closeStream(mode: .disconnectOnly, error: error, promise: nil, in: &eventContext)
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
        applicationErrorCode errorCode: NIOQUICHelpers.QUICApplicationErrorCode,
        in eventContext: inout NetworkContext.EventContext
    ) throws(QUICStreamStateMachine.InvalidTransition) {
        switch try self.streamStateMachine.receiveResetStream(
            applicationErrorCode: errorCode,
            finalSize: 0
        ) {
        case .closeStream(let code):
            self.closeStream(
                mode: .closeAndDisconnect,
                error: QUICStreamResetError(code: code),
                promise: nil,
                in: &eventContext
            )

        case .surfaceReset(let code):
            #if compiler(<6.4)
            self.surfaceReceivedReset(applicationErrorCode: code, in: &eventContext)
            #else

            if self.pipelineStateMachine.isInitialized {
                self.log("surfaceReset", metadata: ["applicationErrorCode": "\(code)"])
                // yields buffered data and then surfaces the reset
                self.streamRead(in: &eventContext)
            } else {
                // SM has captured the reset; the post-init `streamRead()`
                // will surface it via `completeRead()`.
                self.log("surfaceReset: pipeline not yet initialized, will surface on next read")
            }
            #endif

        case .doNothing(let reason):
            switch reason {
            case .alreadyFullyReceived:
                self.log("receiveResetStream: already fully received")
            case .alreadyReset:
                self.log("receiveResetStream: already reset")
            }
        }
    }

    #if compiler(<6.4)
    // Extracted from `handleReceivedResetStream` to keep that method's SIL
    // small enough to avoid the same `CopyPropagation` `-O` crash on
    // Swift 6.3.
    @inline(never)
    private func surfaceReceivedReset(
        applicationErrorCode code: QUICApplicationErrorCode,
        in eventContext: inout NetworkContext.EventContext
    ) {
        if self.pipelineStateMachine.isInitialized {
            self.log("surfaceReset", metadata: ["applicationErrorCode": "\(code)"])
            // yields buffered data and then surfaces the reset
            self.streamRead(in: &eventContext)
        } else {
            // SM has captured the reset; the post-init `streamRead()`
            // will surface it via `completeRead()`.
            self.log("surfaceReset: pipeline not yet initialized, will surface on next read")
        }
    }
    #endif

    // Event that signals that STOP_SENDING was received
    internal func handleOutboundAbortedEvent(
        error: SwiftNetwork.NetworkError?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
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
                        self.callouts.add(.streamStopSending(self, code: errorCode), in: &eventContext)
                        if self.streamStateMachine.hasReceivedFin {
                            self.closeStream(
                                mode: .closeOnly,
                                error: NIOQUICHelpers.QUICStopSendingError(code: error.errorCode),
                                promise: nil,
                                in: &eventContext
                            )
                        }
                    } else {
                        self.closeStream(
                            mode: .closeAndDisconnect,
                            error: NIOQUICHelpers.QUICStopSendingError(code: error.errorCode),
                            promise: nil,
                            in: &eventContext
                        )
                    }
                case .sendResetAndCloseStream(let error):
                    self.closeStream(
                        mode: .closeAndDisconnect,
                        error: NIOQUICHelpers.QUICStopSendingError(code: error.errorCode),
                        promise: nil,
                        in: &eventContext
                    )
                case .ignore(.alreadyFinished):
                    self.log("receiveStopSending: stream already finished, ignoring")

                case .ignore(.alreadyReset):
                    self.log("receiveStopSending: stream already reset, ignoring")
                }
            } catch {
                self.log("handleOutboundAbortedEvent errored: \(error)", logLevel: .warning)
                switch error {
                case .wrongDirection:
                    self.closeStream(mode: .disconnectOnly, error: error, promise: nil, in: &eventContext)
                case .notConnected:
                    break
                }
            }
        } else {
            self.log("handleOutboundAbortedEvent called with error: \(String(describing: error))")
        }
    }

    // Send a STOP_SENDING frame to close the read (Note, this stream will receive a RESET_STREAM in return)
    internal func abortInbound(error: NetworkError?, in eventContext: inout NetworkContext.EventContext) {
        do throws(NetworkError) {
            self.log("abortInbound")
            switch self.swiftNetworkStreamHandle.invokeAbortInbound() {
            case .proceed(let linkage):
                try linkage.invokeAbortInbound(error: error, for: self.identifier, in: &eventContext)
            case .ignore:
                break
            }
        } catch {
            self.log("Failed to abort inbound: \(error)")
            self.closeStream(mode: .disconnectOnly, error: error, promise: nil, in: &eventContext)
        }
    }

    // Send a RESET_STREAM frame to close the write
    internal func abortOutbound(error: NetworkError?, in eventContext: inout NetworkContext.EventContext) {
        do throws(NetworkError) {
            switch self.swiftNetworkStreamHandle.invokeAbortOutbound() {
            case .proceed(let linkage):
                try linkage.invokeAbortOutbound(error: error, for: self.identifier, in: &eventContext)
            case .ignore:
                break
            }
        } catch {
            self.log("Failed to abort outbound: \(error)")
            self.closeStream(mode: .disconnectOnly, error: error, promise: nil, in: &eventContext)
        }
    }

    // MARK: - Initializer dispatch
    //
    // Called by `QUICConnectionChannel` when a stream needs its application-supplied
    // initializer run. The stream owns its own pipeline state machine: it gates on
    // `startInitializer`, runs the initializer, then on success calls
    // `markInitializerComplete()` and surfaces the now-ready stream. The connection
    // never reaches into the pipeline state machine directly.

    /// Runs the inbound initializer for this stream, driving the per-stream pipeline state
    /// machine and surfacing the stream once it is ready.
    ///
    /// - Returns: A future that completes when an asynchronous initializer finishes, or `nil`
    ///   when there was no asynchronous work to wait for (no initializer, or the pipeline is
    ///   already initialized or inactive). The connection uses this to defer its own
    ///   `channelInactive` while an initializer is in flight.
    func initializeInbound(
        streamID: QUICStreamID,
        initializer: QUICInboundStreamInitializer?
    ) -> EventLoopFuture<Void>? {
        self.eventLoop.preconditionInEventLoop()

        switch self.pipelineStateMachine.startInitializer(channelActive: self.isStreamChannelActive) {
        case .runInitializer:
            break  // continue below
        case .ignore(let reason):
            switch consume reason {
            case .initializerComplete:
                self.tryToAutoRead()
            case .channelInactive:
                break
            case .initializerInProgress:
                break
            }
            return nil
        }

        switch initializer {
        case .multiplexer(let continuation):
            let future = continuation.initialize(channel: self)
                .hop(to: self.eventLoop)
            future.assumeIsolated().whenComplete { result in
                switch result {
                case .success(let output):
                    switch self.pipelineStateMachine.markInitializerComplete() {
                    case .surfaceInitializedStream:
                        continuation.yield(output: output, channel: self)
                        self.tryToAutoRead()
                    case .ignoreAlreadyComplete:
                        break
                    }
                case .failure:
                    self.close(promise: nil)
                }
            }
            return future.map { _ in () }

        case .closure(let initializer):
            let future = initializer(self).hop(to: self.eventLoop)
            future.assumeIsolated().whenComplete { result in
                switch result {
                case .success:
                    switch self.pipelineStateMachine.markInitializerComplete() {
                    case .surfaceInitializedStream:
                        self.tryToAutoRead()
                    case .ignoreAlreadyComplete:
                        break
                    }
                case .failure:
                    self.close(promise: nil)
                }
            }
            return future

        case .none:
            switch self.pipelineStateMachine.markInitializerComplete() {
            case .surfaceInitializedStream:
                self.tryToAutoRead()
            case .ignoreAlreadyComplete:
                break
            }
            return nil
        }
    }

    /// Runs the outbound initializer for this stream, driving the per-stream pipeline state
    /// machine and succeeding `promise` with the stream once it is ready.
    ///
    /// - Note: If the stream's channel has already gone inactive by the time this is called (the
    ///   stream connected but was reset, or the connection closed, before the creation callback
    ///   ran) then the initializer can't be run at all and `promise` is failed rather than
    ///   succeeded with a stream that has no pipeline.
    func initializeOutbound(
        streamID: QUICStreamID,
        initializer: @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>,
        promise: EventLoopPromise<any Channel>
    ) {
        self.eventLoop.preconditionInEventLoop()

        switch self.pipelineStateMachine.startInitializer(channelActive: self.isStreamChannelActive) {
        case .runInitializer:
            initializer(self, streamID)
                .hop(to: self.eventLoop)
                .assumeIsolated()
                .whenComplete { result in
                    switch result {
                    case .success:
                        switch self.pipelineStateMachine.markInitializerComplete() {
                        case .surfaceInitializedStream:
                            promise.succeed(self)
                            self.tryToAutoRead()
                        case .ignoreAlreadyComplete:
                            promise.succeed(self)
                        }
                    case .failure(let error):
                        promise.fail(error)
                    }
                }
        case .ignore(let reason):
            switch consume reason {
            case .initializerComplete:
                // Outbound streams get a fresh handler each time, so the pipeline can't already
                // be initialized here. Surface it rather than dropping the promise on the floor.
                assertionFailure("outbound initializer already ran for stream \(streamID)")
                promise.succeed(self)
                self.tryToAutoRead()
            case .channelInactive:
                // The initializer never ran: the stream has no pipeline, so don't hand it back.
                promise.fail(ChannelError.ioOnClosedChannel)
            case .initializerInProgress:
                // As above: a fresh handler can't have an initializer in flight.
                assertionFailure("outbound initializer already in flight for stream \(streamID)")
                promise.succeed(self)
            }
        }
    }

    // Start the stream handler
    func start(fromNewFlowHandler: Bool = false, in eventContext: inout NetworkContext.EventContext) {
        log("start")
        self.eventLoop.preconditionInEventLoop()
        switch self.swiftNetworkStreamHandle.invokeConnect() {
        case .proceed(let linkage):
            linkage.invokeConnect(for: self.identifier, in: &eventContext)
        case .handleViolation(let reason):
            preconditionFailure("invokeConnect on SwiftNetworkStreamHandle: \(reason)")
        }
        // If started from NewFlowHandler then presumably there is application data waiting in the read queue
        // Do an optimistic read here when the flow is started and then all future data that comes in will get the handleInboundDataAvailableEvent event.
        // The handleInboundDataAvailableEvent signals that its time to read data on the stream.
        if fromNewFlowHandler {
            let bytesRead = self.read(in: &eventContext)
            if bytesRead > 0 {
                log("start new flow optimistic read returning: \(bytesRead) bytes")
            }
        }
    }

    // Stop and disconnect the stream handler
    func stop(detachFromLowerProtocol: Bool = false, in eventContext: inout NetworkContext.EventContext) {
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
            linkage.invokeDisconnect(error: nil, for: self.identifier, in: &eventContext)
        case .ignore:
            break
        }
        if detachFromLowerProtocol {
            switch self.swiftNetworkStreamHandle.invokeDetach() {
            case .proceed(let linkage):
                do throws(NetworkError) {
                    try linkage.invokeDetach(for: self.identifier, in: &eventContext)
                } catch {
                    self.log("Failed to detach lower protocol: \(error)")
                }
                self.unregisterIfCloseCompleted(in: &eventContext)
            case .skipAlreadyDetached:
                break
            }
        }
    }

    /// Closes the channel if it isn't yet closed.
    func closeIfNeeded(in eventContext: inout NetworkContext.EventContext) {
        self._close(error: nil, promise: nil, in: &eventContext)
    }

    /// Unregisters a handler that just detached from its flow, if its close already completed.
    /// `_close` leaves the unregistration to the detach while the flow still delivers events to it.
    private func unregisterIfCloseCompleted(in eventContext: inout NetworkContext.EventContext) {
        if self.isCloseCompleted {
            self.unregisterEventManager(in: &eventContext)
        }
    }

    /// Called when the stream handler receives a disconnected event.
    ///
    /// Queues the close cascade, which runs once the event context is released, before the entry
    /// that delivered the event returns.
    internal func handleDisconnectedEvent(
        error: NetworkError?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        self.log("handleDisconnectedEvent error: \(String(describing: error))")
        switch self.swiftNetworkStreamHandle.invokeDetach() {
        case .proceed(let linkage):
            do throws(NetworkError) {
                try linkage.invokeDetach(for: self.identifier, in: &eventContext)
            } catch {
                self.log("Failed to detach lower protocol: \(error)")
            }
            self.unregisterIfCloseCompleted(in: &eventContext)
        case .skipAlreadyDetached:
            break
        }

        let closeError: (any Error)?
        if error?.isIdleTimeout == true {
            closeError = nil
        } else {
            closeError = error.flatMap { QUICConnectionError(networkError: $0) } ?? error
        }
        self._close(error: closeError, promise: nil, in: &eventContext)
    }

    @inline(__always)
    private func closeStream(
        mode: StreamClosureState,
        error: (any Error)?,
        promise: EventLoopPromise<Void>?,
        in eventContext: inout NetworkContext.EventContext
    ) {
        log("closeStream mode: \(mode)")
        switch mode {
        case .closeAndDisconnect:  // Calls close and stop to detach everything
            self._close(error: error, promise: promise, in: &eventContext)
            self.stop(detachFromLowerProtocol: true, in: &eventContext)
        case .closeOnly:  // Calls close only
            self._close(error: error, promise: promise, in: &eventContext)
        case .disconnectOnly:  // Calls stop and detach only, could result in disconnect event as well
            self.stop(detachFromLowerProtocol: true, in: &eventContext)
        }
    }

    /// Drains SwiftNetwork's stream buffer and delivers bytes downstream, but only when there is an outstanding read
    /// request from the downstream consumer, i.e. `self.pendingRead == true`.
    ///
    /// When there is no demand, we leave bytes inside SwiftNetwork's buffer as SwiftNetwork uses its buffer occupancy
    /// to drive flow control. If we always drained it, SwiftNetwork would think the data has been consumed and keep
    /// advancing its flow control window, defeating backpressure and letting unbounded bytes accumulate here.
    private func deliverInboundDataDownstreamIfDemand(in eventContext: inout NetworkContext.EventContext) {
        // Only continue if there is a pending read.
        guard self.pendingRead else {
            return
        }

        // Pull the latest bytes out of SwiftNetwork into `self.bufferedReadData`.
        let bytesRead = self.read(in: &eventContext)
        if bytesRead > 0 {
            self.log("buffered \(bytesRead) bytes from inbound data available event")
        }

        self.streamRead(in: &eventContext)
    }

    // Called when data is available to be read on the stream
    // NOTE: New flows with a single read with not get this event and should optimistically read when the flow is started.
    // This event will be called for all future input available after the flow starts.
    internal func handleInboundDataAvailableEvent(
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        switch self.streamStateMachine.inboundDataAvailable() {
        case .readData:
            self.log("received input room available")
            self.deliverInboundDataDownstreamIfDemand(in: &eventContext)

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
    ///
    /// May re-enter `read0()`, so it's only called with nothing held.
    func tryToAutoRead() {
        if self.autoRead, !self.pendingRead {
            self.pipeline.read()
        }
    }

    private func flushBufferedReadData() {
        guard self.bufferedReadData.readableBytes > 0 else {
            return
        }
        // We will now satisfy a pending read request. Reset the flag.
        self.pendingRead = false
        let bytesRead = self.bufferedReadData.readableBytes
        self.log("Read \(bytesRead) bytes from stream")
        self.pipeline.fireChannelRead(self.bufferedReadData)
        self.bufferedReadData.clear()
    }

    /// Returns `true` if an end-of-stream event was surfaced
    private func surfaceReadCompletion(in eventContext: inout NetworkContext.EventContext) -> Bool {
        switch self.streamStateMachine.completeRead() {
        case .reportFin(let streamFullyClosed):
            self.callouts.add(.streamInputClosed(self), in: &eventContext)
            if streamFullyClosed {
                self.log("stream is now fully closed")
                self.closeStream(mode: .disconnectOnly, error: nil, promise: nil, in: &eventContext)
                self.shutdownStream(direction: .all, applicationErrorCode: nil, in: &eventContext)
            }
            return true

        case .reportPeerReset(let code):
            self.callouts.add(.streamError(self, QUICStreamResetError(code: code)), in: &eventContext)
            return true

        case .nothingToReport:
            return false
        }
    }

    func streamRead(in eventContext: inout NetworkContext.EventContext) {
        self.eventLoop.preconditionInEventLoop()

        // Emit channel read complete when:
        // - any read was fired, or
        // - FIN or peer RESET is emitted
        let hasReadData = self.bufferedReadData.readableBytes > 0 && !self.isReadQueued
        if hasReadData {
            // The read is satisfied now, as it was when delivered right away: the stack reports data
            // once per frame, and a later frame's report leaves its bytes for the next read.
            self.pendingRead = false
            self.isReadQueued = true
            self.callouts.add(.streamRead(self), in: &eventContext)
        }
        let endOfStream = self.surfaceReadCompletion(in: &eventContext)

        if hasReadData || endOfStream {
            self.callouts.add(.streamReadComplete(self), in: &eventContext)
        }
    }

    internal func handleOutboundRoomAvailableEvent(
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        log("received output available")
    }

    // Stream handler is in the connected state
    internal func handleConnectedEvent(
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        self.eventLoop.preconditionInEventLoop()
        log("connected")

        guard let metadata: ProtocolMetadata<QUICProtocol> = self.getStreamMetadata(in: &eventContext) else {
            self.logger.error("connected stream does not have access to metadata")
            // Only schedules work for the next tick: no NIO code runs here.
            self.connectedEventHandler?(nil)
            return
        }

        self.callouts.add(.streamActive(self), in: &eventContext)
        log("setting up metadata handlers")
        if let keepAliveTime = self._keepAliveInterval, let connectionMetadata = metadata.connectionMetadata {
            log("setting keepalive interval")
            // The setter acquires the event context itself, so it runs once the context is released.
            // The connection unsets it when it closes.
            self.callouts.add(.setKeepalive(connectionMetadata, seconds: keepAliveTime), in: &eventContext)
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
            self.log("handleConnectedEvent called when already connected", logLevel: .warning)
            assertionFailure("handleConnectedEvent called when already connected")
            return

        case .ignoreAlreadyClosed:
            self.log("handleConnectedEvent called when already closed", logLevel: .warning)
            assertionFailure("handleConnectedEvent called when already closed")
            return
        }

        self.connectedEventHandler?(self.streamID)
    }

    internal func setNewFlowMetadata(_ newMetadata: ProtocolMetadata<QUICProtocol>) {
        self.originalMetadata = newMetadata
    }

    internal func getStreamMetadata(
        in eventContext: inout NetworkContext.EventContext
    ) -> ProtocolMetadata<QUICProtocol>? {
        if let originalMetadata {
            return originalMetadata
        } else {
            if let metadata: ProtocolMetadata<QUICProtocol> = self.getMetadata(in: &eventContext) {
                self.originalMetadata = metadata
                return metadata
            }
        }
        return nil
    }

    // Get metadata about the stream from the internal QUIC stack
    private final func getMetadata<P: NetworkProtocol>(
        in eventContext: inout NetworkContext.EventContext
    ) -> ProtocolMetadata<P>? {
        switch self.swiftNetworkStreamHandle.invokeGetMetadata() {
        case .proceed(let linkage):
            return linkage.invokeGetMetadata(for: self.identifier, in: &eventContext) as ProtocolMetadata<P>?
        case .ignore:
            return nil
        }
    }

    var isStreamChannelActive: Bool {
        self.isActive
    }

    /// Write multiple buffers to the QUIC stream as a single `FrameArray`, optionally setting FIN.
    ///
    /// - Parameters:
    ///   - buffers: The buffers to write. Each becomes a separate `Frame` in one `invokeSendStreamData` call.
    ///   - fin: Whether to set the FIN flag on the last frame, signalling end of stream.
    /// - Returns: `true` if the write was accepted, `false` if the stream is in a state that
    ///   prevents writing (e.g. already finished or reset).
    internal func writeBuffersToStream(
        _ buffers: some BidirectionalCollection<ByteBuffer>,
        fin: Bool = false,
        in eventContext: inout NetworkContext.EventContext
    ) -> Bool {
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
                for buffer in buffers where buffer.readableBytes > 0 {
                    totalBytes += buffer.readableBytes
                    frameArray.add(frame: Frame(copyBuffer: buffer.readableBytesUInt8Span))
                }

                if totalBytes == 0 {
                    assert(frameArray.isEmpty, "If there's no bytes to stream we expect the frame array to be empty")
                    return true
                }
            }
            log("write \(frameArray.count) frames, \(totalBytes) bytes, fin: \(fin)")

            switch self.swiftNetworkStreamHandle.invokeSendStreamData() {
            case .proceed(let linkage):
                try linkage.invokeSendStreamData(frameArray, from: self.identifier, in: &eventContext)
            case .handleViolation(let reason):
                frameArray.finalizeAllFramesAsFailed()
                throw SwiftNetworkStreamHandle.violationError(operation: "invokeSendStreamData", reason: reason)
            }
            return true
        } catch {
            self.log("writeBuffersToStream failed: \(error)", logLevel: .error)
            return false
        }
    }

    internal func sendFIN(in eventContext: inout NetworkContext.EventContext) throws {
        self.eventLoop.preconditionInEventLoop()

        switch try self.streamStateMachine.sendFin() {
        case .sendFin:
            self.log("sending connection complete")
            var finFrame = Frame(copyBuffer: [])
            finFrame.connectionComplete = true
            var finFrameArray = FrameArray(frame: finFrame)
            switch self.swiftNetworkStreamHandle.invokeSendStreamData() {
            case .proceed(let linkage):
                try linkage.invokeSendStreamData(finFrameArray, from: self.identifier, in: &eventContext)
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
    internal func read(in eventContext: inout NetworkContext.EventContext) -> Int {
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
                    minimumBytes: 1,
                    maximumBytes: Int.max,
                    for: self.identifier,
                    in: &eventContext
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
                            self.log("receiveData: data arrived after all data received", logLevel: .warning)
                            assertionFailure("receiveData: data arrived after all data received")
                            return 0

                        case .doNotBuffer(.streamReset):
                            self.log("receiveData: data arrived after stream reset", logLevel: .warning)
                            assertionFailure("receiveData: data arrived after stream reset")
                            return 0
                        }
                    } catch {
                        self.log("read errored: \(error)", logLevel: .warning)
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
                    self.log("receiveFin errored: \(error)", logLevel: .warning)
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
        applicationErrorCode: QUICApplicationErrorCode?,
        in eventContext: inout NetworkContext.EventContext
    ) {
        if let metadata: ProtocolMetadata<QUICProtocol> = self.getStreamMetadata(in: &eventContext) {
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
                self.closeStream(mode: .disconnectOnly, error: nil, promise: nil, in: &eventContext)
            }

        case .closeRead(let streamFullyClosed, let deliverEndOfStream):
            // Send STOP_SENDING so no more data is received from the peer.
            // Per RFC 9000 §3.4, send and receive are independent state machines,
            // so we always use the graceful path regardless of the write side's state.
            self.log(
                "shutdownStream shutting down read, applicationErrorCode: \(String(describing: applicationErrorCode))"
            )
            self.abortInbound(
                error: NetworkError(quicApplicationError: applicationErrorCode?.rawValue ?? 0),
                in: &eventContext
            )
            if deliverEndOfStream {
                self.callouts.add(.streamInputClosed(self), in: &eventContext)
            }
            if streamFullyClosed {
                self.closeStream(mode: .disconnectOnly, error: nil, promise: nil, in: &eventContext)
            }

        case .readAlreadyClosed(let deliverEndOfStream):
            self.log("shutdownStream read side already closed")
            if deliverEndOfStream {
                self.callouts.add(.streamInputClosed(self), in: &eventContext)
            }

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
            self.abortOutbound(error: NetworkError(quicApplicationError: code.rawValue), in: &eventContext)
            if streamFullyClosed {
                self.closeStream(mode: .disconnectOnly, error: nil, promise: nil, in: &eventContext)
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
extension QUICChannelStreamHandler: ProtocolInstance {}

@available(anyAppleOS 26, *)
extension QUICChannelStreamHandler {
    func handleNetworkProtocolEvent(
        event: SwiftNetwork.NetworkProtocolEvent,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        // do nothing
    }

    /// Receives this stream's linkage, delivered synchronously from inside the second `init`'s
    /// call to `invokeAttachUpperProtocolToNewFlow`, and completes the pairing: an external upper
    /// makes the second leg of the attach itself, which is what sets the flow up in QUIC.
    func invokeAttachLowerProtocol(
        _ lowerProtocol: BaseOutboundStreamLinkage,
        remote: SwiftNetwork.Endpoint?,
        local: SwiftNetwork.Endpoint?,
        parameters: SwiftNetwork.Parameters?,
        path: SwiftNetwork.PathProperties?
    ) throws(SwiftNetwork.NetworkError) {
        self.swiftNetworkStreamHandle = SwiftNetworkStreamHandle(linkage: lowerProtocol)
        try lowerProtocol.invokeAttachUpperProtocol(
            BaseInboundStreamLinkage(external: self),
            remote: remote,
            local: local,
            parameters: parameters,
            path: path
        )
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
        #if DEBUG
        // Pipeline events from code holding the event context would let handlers re-enter NIOQUIC
        // with it held: queue them instead (see `QUICCallouts`). Other threads only use the pipeline
        // to hop onto the event loop.
        assert(
            !self.eventLoop.inEventLoop || !self.callouts.isInsideStack,
            "Stream pipeline used while the event context may be held"
        )
        #endif
        return self._pipeline
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
        self._remoteAddress.value
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
        let succeeded = self.callouts.enter(as: self) { eventContext in
            self.writeBuffersToStream(self.bufferedWriteData, fin: false, in: &eventContext)
        }
        if succeeded {
            self.log("ChildChannel flush0 succeeded")
            self.bufferedWriteData.removeAll(where: { _ in true })
        }
    }

    @inlinable
    func read0() {
        self.eventLoop.preconditionInEventLoop()
        self.log("ChildChannel read0")

        self.pendingRead = true
        // Try to satisfy the read immediately if there is data already buffered in SwiftNetwork.
        self.callouts.enter(as: self) { eventContext in
            self.deliverInboundDataDownstreamIfDemand(in: &eventContext)
        }
    }

    private func _close(
        error: (any Error)?,
        promise: EventLoopPromise<Void>?,
        in eventContext: inout NetworkContext.EventContext
    ) {
        self.eventLoop.preconditionInEventLoop()
        if self.isClosed {
            if let promise {
                self.callouts.add(.completePromise(promise, .success(())), in: &eventContext)
            }
            return
        }
        self.log("_close error: \(String(describing: error))")
        self.isClosed = true

        // A stream can be closed before it ever became active, e.g. its flow was attached in the
        // same read batch in which the connection tore down. Then it doesn't see 'channelInactive'
        // (it didn't see 'channelActive'), but its close promise is still completed.
        self.callouts.add(.streamClosed(self, promise: promise, error: error), in: &eventContext)

        // Complete the promise on the next loop tick.
        self.eventLoop.assumeIsolated().execute {
            self.removeHandlers(pipeline: self.pipeline)
            self._closePromise.succeed(())
            // Fire disconnect if there is no error present
            if let disconnect = self.disconnectedEventHandler {
                disconnect(nil)
            }
            self.isCloseCompleted = true
            // Still attached, the flow delivers events to this handler, so it stays registered: a
            // stream the application closed stays attached until the flow's disconnect, after all
            // ACKs, and unregisters once that detaches it.
            if !self.swiftNetworkStreamHandle.isAttached {
                self.callouts.enter(as: self) { eventContext in
                    self.unregisterEventManager(in: &eventContext)
                }
            }
            self.clearHandlers()
        }
    }

    /// Close the send side gracefully: flush any buffered write data with a FIN,
    /// or send a bare FIN when nothing is buffered. Idempotent — `sendFIN` reacts
    /// to the state machine's `sendFin` action, so it is a no-op once the send
    /// side has already finished or been reset.
    private func closeOutputSide(in eventContext: inout NetworkContext.EventContext) {
        if self.bufferedWriteData.isEmpty {
            do {
                try self.sendFIN(in: &eventContext)
            } catch {
                self.log("Sendig FIN error: \(error)")
            }
        } else {
            if self.writeBuffersToStream(self.bufferedWriteData, fin: true, in: &eventContext) {
                self.log("ChildChannel close0 succeeded sending FIN")
                self.bufferedWriteData.removeAll(where: { _ in true })
            }
        }
    }

    @inlinable
    func close0(error: any Error, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        self.eventLoop.preconditionInEventLoop()
        self.log("ChildChannel close0 mode \(mode)")
        self.callouts.enter(as: self) { eventContext in
            self.closeStreamSides(mode: mode, promise: promise, in: &eventContext)
        }
    }

    /// Closes the sides of the stream `mode` names, for `close0`.
    private func closeStreamSides(
        mode: CloseMode,
        promise: EventLoopPromise<Void>?,
        in eventContext: inout NetworkContext.EventContext
    ) {
        switch mode {
        case .output:
            if self.streamStateMachine.canWrite {
                self.closeOutputSide(in: &eventContext)
                self.complete(promise, .success(()), in: &eventContext)
            } else {
                self.complete(promise, .failure(ChannelError.outputClosed), in: &eventContext)
                return
            }
        case .input:
            if self.streamStateMachine.isReceiveClosed {
                self.complete(promise, .failure(ChannelError.inputClosed), in: &eventContext)
                return
            }
            // `shutdownStream` handles the QUIC-level signaling as needed,
            // e.g. STOP_SENDING for `.read`, RESET_STREAM for `.write`.
            self.shutdownStream(direction: .read, applicationErrorCode: nil, in: &eventContext)
            if self.streamStateMachine.isSendClosed {
                self.log("Input and output for stream are now closed, stream will be queued up for closure")
            }
            self.complete(promise, .success(()), in: &eventContext)
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

                if shutdownRead, !self.streamStateMachine.isReceiveClosed {
                    self.shutdownStream(direction: .read, applicationErrorCode: nil, in: &eventContext)
                }

                if shutdownWrite, self.streamStateMachine.canWrite {
                    self.closeOutputSide(in: &eventContext)
                }
            }

            // Fire channel inactive so the application observes the close. The lower
            // protocol is left attached; the natural disconnect event from
            // SwiftNetwork (after all ACKs arrive) triggers detach via
            // `handleDisconnectedEvent`.
            self.closeStream(mode: .closeOnly, error: nil, promise: promise, in: &eventContext)
        }
    }

    @inlinable
    func triggerUserOutboundEvent0(_ event: Any, promise: EventLoopPromise<Void>?) {
        self.eventLoop.preconditionInEventLoop()

        self.log(
            "ChildChannel triggerUserOutboundEvent0: \(event)"
        )

        self.callouts.enter(as: self) { eventContext in
            self.handleUserOutboundEvent(event, promise: promise, in: &eventContext)
        }
        guard let promise else {
            return
        }
        // We are not expecting any outbound events here so we are simply
        // failing the promise
        promise.fail(ChannelError.operationUnsupported)
    }

    /// Handles the stop-sending and reset events for `triggerUserOutboundEvent0`.
    private func handleUserOutboundEvent(
        _ event: Any,
        promise: EventLoopPromise<Void>?,
        in eventContext: inout NetworkContext.EventContext
    ) {
        switch event {
        case let event as NIOQUICHelpers.QUICStopSendingEvent:
            // Send a STOP_SENDING frame to close the read (input) side
            self.shutdownStream(
                direction: .read,
                applicationErrorCode: event.code,
                in: &eventContext
            )
            if self.streamStateMachine.isSendClosed {
                // Both sides now closed
                self.closeStream(mode: .closeOnly, error: nil, promise: promise, in: &eventContext)
            } else {
                self.complete(promise, .success(()), in: &eventContext)
            }
        case let event as NIOQUICHelpers.QUICResetStreamEvent:
            // Send RESET_STREAM to close the write (output) side
            self.shutdownStream(
                direction: .write,
                applicationErrorCode: event.code,
                in: &eventContext
            )
            switch self.streamStateMachine.completeRead() {
            case .nothingToReport:
                self.complete(promise, .success(()), in: &eventContext)
                break
            default:
                break
            }
        default:
            break
        }
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

// MARK: - Queued callouts
//
// The stack's events reach this handler while the event context is held, so the pipeline events
// and promise completions they cause are queued (see `QUICCallouts`) and delivered through these
// calls once the event context was released.
//
// Once the pipeline is closed, they fire nothing: a handler may close the channel from an earlier
// call of the same batch, such as a `channelRead`.

@available(anyAppleOS 26, *)
extension QUICChannelStreamHandler {
    /// Queues `promise`'s completion, if there is one.
    private func complete(
        _ promise: EventLoopPromise<Void>?,
        _ result: Result<Void, any Error>,
        in eventContext: inout NetworkContext.EventContext
    ) {
        if let promise {
            self.callouts.add(.completePromise(promise, result), in: &eventContext)
        }
    }

    func deliverActive() {
        // A channel closed before it became active sees neither event.
        if self.isPipelineClosed || self.isActive {
            return
        }
        self._isActive.store(true, ordering: .sequentiallyConsistent)
        self.pipeline.fireChannelRegistered()
        self.pipeline.fireChannelActive()
    }

    func deliverRead() {
        self.isReadQueued = false
        if self.isPipelineClosed {
            return
        }
        self.flushBufferedReadData()
    }

    func deliverReadComplete() {
        if self.isPipelineClosed {
            return
        }
        self.fireChannelReadComplete()
    }

    func deliverInputClosed() {
        if self.isPipelineClosed {
            return
        }
        self.pipeline.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
    }

    func deliverStopSending(code: NIOQUICHelpers.QUICApplicationErrorCode) {
        if self.isPipelineClosed {
            return
        }
        self.pipeline.fireUserInboundEventTriggered(NIOQUICHelpers.QUICStopSendingEvent(code: code))
    }

    func deliverError(_ error: any Error) {
        if self.isPipelineClosed {
            return
        }
        self.pipeline.fireErrorCaught(error)
    }

    func deliverClose(promise: EventLoopPromise<Void>?, error: (any Error)?) {
        if self.isPipelineClosed {
            promise?.succeed()
            return
        }
        // As NIO's channels do: update the state first, then the close promise, then the events.
        self.isPipelineClosed = true
        let wasActive = self._isActive.exchange(false, ordering: .sequentiallyConsistent)
        promise?.succeed()
        if let error {
            self.pipeline.fireErrorCaught(error)
        }
        // A stream closed before it ever became active doesn't see `channelInactive` since it did
        // not see `channelActive` either.
        if wasActive {
            self.pipeline.fireChannelInactive()
            self.pipeline.fireChannelUnregistered()
        }
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

@available(anyAppleOS 26, *)
extension QUICConnectionError {
    init?(networkError error: NetworkError) {
        let errorCode: UInt64
        let isApplication: Bool

        if let code = error.quicApplicationError {
            errorCode = UInt64(code)
            isApplication = true
        } else if let code = error.quicTransportError {
            errorCode = UInt64(code)
            isApplication = false
        } else {
            return nil
        }

        self.init(reason: String(describing: error), isApplication: isApplication, code: errorCode)
    }
}
