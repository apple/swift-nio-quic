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

import DequeModule
import NIOCore
import NIOQUICHelpers
import Synchronization

/// A channel for a QUIC connection.
@available(anyAppleOS 26, *)
final class QUICConnectionChannel: @unchecked Sendable {
    // @unchecked because of the IUO ChannelPipeline, which is never mutated after `init`.
    // The `ChannelPipeline` breaks the retain cycle between it and this channel.
    //
    // IMPORTANT:
    //
    // Because this `Channel` must use `@unchecked Sendable` (see note above), by convention,
    // all state and functions that MUST be accessed from the channel's event-loop have a
    // leading underscore.
    //
    // Note also that some immutable state also has methods prefixed with an underscore: this
    // is to avoid ambiguity with properties of the same name required by the `Channel`
    // protocol.

    /// The pipeline associated with this channel.
    ///
    /// See the note above about the `!`.
    private var _pipeline: ChannelPipeline!

    /// Completed when the channel is closed. Provides `closeFuture` for the `Channel` API.
    private let closePromise: EventLoopPromise<Void>

    /// The address of the local peer.
    private let _localAddress: SocketAddress

    /// The address of the remote peer.
    private let _remoteAddress: SocketAddress

    /// Whether the `Channel` is currently writable.
    private let _isWritable: Atomic<Bool>

    /// Whether the `Channel` is currently active.
    private let _isActive: Atomic<Bool>

    /// Whether this connection is for a server.
    private let isServer: Bool

    // MARK: - Event-loop local state

    // **IMPORTANT:** see note at the top of this class about these fields.

    /// The underlying SwiftNetwork-backed QUIC connection.
    private let _connection: Connection

    /// A registrar for connection IDs.
    private let _registrar: ConnectionIDRegistrar

    /// A view into the QUIC handler in the UDP channel, for outbound operations.
    private let _transport: Transport

    /// The lifecycle state machine for the channel.
    private var _lifecycle: Lifecycle

    /// Initializer for inbound streams.
    private var _streamInitializer: StreamInitializer?

    /// Promise to complete when the channel becomes active (or closed if never active).
    private var _readyPromise: EventLoopPromise<Void>?

    /// Whether auto-read is enabled on this channel.
    private var _autoRead: Bool

    /// Whether the channel is allowed to drain the output from the connection.
    private var _isAllowedToDrain: Bool

    /// Whether the channel is currently in a read loop.
    private var _inReadLoop: Bool

    /// A queue of streams pushed to the channel by the underlying connection. These are dequeued
    /// at the end of the parent channel's read loop (in `_parentChannelReadComplete`).
    private var _pendingStreams: Deque<(QUICStreamID, QUICChannelStreamHandler)>

    /// The negotiation state of the QUIC datagram extension (RFC 9221) for the connection.
    enum DatagramNegotiation {
        /// The peer's `max_datagram_frame_size` transport parameter is not yet known.
        /// Early writes are buffered until the peer's advertisement is received.
        case waitingForPeerAdvertisement(earlyWrites: TinyArray<(ByteBuffer, EventLoopPromise<Void>?)>)
        /// The peer advertised a `max_datagram_frame_size` of 0: it does not support datagrams.
        case peerDoesNotAcceptDatagrams
        /// The peer accepts datagrams up to `maximumSize` bytes.
        case peerAcceptsDatagrams(maximumSize: Int)
    }

    /// The negotiation state of the QUIC datagram extension (RFC 9221).
    private var _datagramNegotiation: DatagramNegotiation

    // MARK: - Channel API

    /// The parent `Channel` (i.e. the UDP channel).
    let parent: (any Channel)?

    /// The `EventLoop` the channel is bound to (identical to `parent.eventLoop`).
    let eventLoop: any EventLoop

    /// A `ByteBuffer` allocator.
    let allocator: ByteBufferAllocator

    init(
        udpChannel: any Channel,
        connection: Connection,
        registrar: ConnectionIDRegistrar,
        transport: Transport,
        isServer: Bool
    ) {
        self.parent = udpChannel

        self.eventLoop = udpChannel.eventLoop
        self.allocator = udpChannel.allocator
        self.closePromise = udpChannel.eventLoop.makePromise()

        self._localAddress = connection.localAddress
        self._remoteAddress = connection.remoteAddress
        self._isWritable = Atomic(true)
        self._isActive = Atomic(false)
        self.isServer = isServer

        self._connection = connection
        self._registrar = registrar
        self._transport = transport
        self._lifecycle = Lifecycle()
        self._autoRead = true
        self._streamInitializer = nil
        self._readyPromise = nil
        self._isAllowedToDrain = true
        self._inReadLoop = false
        self._pendingStreams = []
        self._datagramNegotiation = .waitingForPeerAdvertisement(earlyWrites: TinyArray())

        self._pipeline = ChannelPipeline(channel: self)

        switch connection {
        case .live(let connection):
            connection.setDriver(self)
        case .test:
            ()
        }
    }

    enum StreamInitializer {
        /// Hand new streams to a multiplexer continuation. Used by the typed-output
        /// `QUICConnection<Output>` path.
        case multiplexer(any StreamMultiplexerContinuation)
        /// Initialize new streams via the supplied closure.
        case closure(@Sendable (any Channel) -> EventLoopFuture<Void>)
    }
}

// MARK: Channel conformance

@available(anyAppleOS 26, *)
extension QUICConnectionChannel: Channel {
    var closeFuture: EventLoopFuture<Void> {
        self.closePromise.futureResult
    }

    var pipeline: ChannelPipeline {
        self._pipeline
    }

    var localAddress: SocketAddress? {
        self._localAddress
    }

    var remoteAddress: SocketAddress? {
        self._remoteAddress
    }

    var isWritable: Bool {
        self._isWritable.load(ordering: .acquiring)
    }

    var isActive: Bool {
        self._isActive.load(ordering: .acquiring)
    }

    var _channelCore: any ChannelCore {
        self
    }

    func setOption<Option: ChannelOption>(
        _ option: Option,
        value: Option.Value
    ) -> EventLoopFuture<Void> {
        if self.eventLoop.inEventLoop {
            return self.eventLoop.makeCompletedFuture {
                try self._syncOptions.setOption(option, value: value)
            }
        } else {
            return self.eventLoop.submit {
                try self._syncOptions.setOption(option, value: value)
            }
        }
    }

    func getOption<Option: ChannelOption>(_ option: Option) -> EventLoopFuture<Option.Value> {
        if self.eventLoop.inEventLoop {
            return self.eventLoop.makeCompletedFuture {
                try self._syncOptions.getOption(option)
            }
        } else {
            return self.eventLoop.submit {
                try self._syncOptions.getOption(option)
            }
        }
    }

    struct SyncOptions: NIOSynchronousChannelOptions {
        fileprivate let channel: QUICConnectionChannel

        fileprivate init(_ channel: QUICConnectionChannel) {
            channel.eventLoop.assertInEventLoop()
            self.channel = channel
        }

        func getOption<Option: ChannelOption>(_ option: Option) throws -> Option.Value {
            switch option {
            case is ChannelOptions.Types.AutoReadOption:
                return self.channel._autoRead as! Option.Value
            default:
                throw ChannelError.operationUnsupported
            }
        }

        func setOption<Option: ChannelOption>(_ option: Option, value: Option.Value) throws {
            switch option {
            case is ChannelOptions.Types.AutoReadOption:
                self.channel._autoRead = value as! Bool
            default:
                throw ChannelError.operationUnsupported
            }
        }
    }

    var syncOptions: (any NIOSynchronousChannelOptions)? {
        self._syncOptions
    }

    // Typed version of `syncOptions`, used within the channel. The erased version above
    // is a `Channel` protocol requirement.
    private var _syncOptions: SyncOptions {
        SyncOptions(self)
    }
}

// MARK: ChannelCore conformance

@available(anyAppleOS 26, *)
extension QUICConnectionChannel: ChannelCore {
    func localAddress0() throws -> SocketAddress {
        self.eventLoop.assertInEventLoop()
        return self._localAddress
    }

    func remoteAddress0() throws -> SocketAddress {
        self.eventLoop.assertInEventLoop()
        return self._remoteAddress
    }

    func register0(promise: EventLoopPromise<Void>?) {
        self.eventLoop.assertInEventLoop()
        promise?.succeed()
    }

    func bind0(to: SocketAddress, promise: EventLoopPromise<Void>?) {
        self.eventLoop.assertInEventLoop()
        promise?.fail(ChannelError.operationUnsupported)
    }

    func connect0(to: SocketAddress, promise: EventLoopPromise<Void>?) {
        self.eventLoop.assertInEventLoop()
        promise?.fail(ChannelError.operationUnsupported)
    }

    func write0(_ data: NIOAny, promise: EventLoopPromise<Void>?) {
        self.eventLoop.assertInEventLoop()
        self._writeDatagram(self.unwrapData(data, as: ByteBuffer.self), promise: promise)
    }

    func flush0() {
        self.eventLoop.assertInEventLoop()
        self._flushDatagrams()
    }

    func read0() {
        self.eventLoop.assertInEventLoop()
        self._transport.read()
    }

    func close0(error: any Error, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        self.eventLoop.assertInEventLoop()

        self.closeConnection(
            promise: promise,
            isApplicationClose: false,
            errorCode: QUICTransportErrorCode.noError.rawValue,
            reasonPhrase: ""
        )
    }

    func triggerUserOutboundEvent0(_ event: Any, promise: EventLoopPromise<Void>?) {
        self.eventLoop.assertInEventLoop()

        switch event {
        case let event as QUICCloseConnectionEvent:
            self.closeConnection(
                promise: promise,
                isApplicationClose: true,
                errorCode: Int64(event.code.rawValue),
                reasonPhrase: event.reasonPhrase ?? ""
            )

        case let event as QUICRequestAssociateSCIDEvent:
            self._connection.withLiveOnly(promise: promise) { connection in
                try connection.requestAssociationOfConnectionID(event.scid)
            }

        case let event as QUICRequestRetireDCIDEvent:
            self._connection.withLiveOnly(promise: promise) { connection in
                try connection.requestRetirementOfConnectionID(event.dcid)
            }

        #if DEBUG
        case let event as _QUICForTestingPoisonRetiredSCIDEvent:
            self._connection.withLiveOnly(promise: promise) { connection in
                connection._forTesting_addRetiredSCID(event.scid)
            }

        case let event as _QUICForTestingGetActiveSCIDsEvent:
            self._connection.withLiveOnly(promise: promise) { connection in
                let scids = connection._forTesting_getActiveSCIDs()
                event.result.withLockedValue { $0 = scids }
            }

        case let event as _QUICForTestingRemoveActiveSCIDEvent:
            self._connection.withLiveOnly(promise: promise) { connection in
                connection._forTesting_removeFromActiveSCIDs(event.scid)
            }
        #endif

        default:
            promise?.fail(ChannelError.operationUnsupported)
        }
    }

    func channelRead0(_ data: NIOAny) {
        // Unhandled read, drop it.
    }

    func errorCaught0(error: any Error) {
        // Unhandled error, drop it.
    }
}

// MARK: - Connection view

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    /// A view of the channel used by the underlying connection.
    ///
    /// The channel acts as the connections delegate and is notified of various lifecycle events
    /// such as becoming connected and disconeccted, in addition to new streams being openened
    /// and needing the channel to callback to drain new output.
    struct ConnectionView {
        private let _channel: QUICConnectionChannel

        var channel: any Channel {
            self._channel
        }

        init(_ channel: QUICConnectionChannel) {
            channel.eventLoop.assertInEventLoop()
            self._channel = channel
        }
    }

    var connectionView: ConnectionView {
        ConnectionView(self)
    }
}

@available(anyAppleOS 26, *)
@available(*, unavailable)
extension QUICConnectionChannel.ConnectionView: Sendable {}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel.ConnectionView {
    /// The connection associated a new ID with itself, update the routing table so that the new
    /// ID is routed to this connection.
    func associate(_ newID: QUICConnectionID) -> Bool {
        let associated = self._channel._registrar.associate(newID)

        if associated {
            let event = QUICSCIDAssociatedEvent(scid: newID)
            self._channel.pipeline.fireUserInboundEventTriggered(event)
        }

        return associated
    }

    /// The connection retired the given ID, update the routing table to drop the retired ID.
    func retire(_ id: QUICConnectionID) -> Bool {
        let retired = self._channel._registrar.retire(id)

        if retired {
            self._channel.pipeline.fireUserInboundEventTriggered(QUICSCIDRetiredEvent(scid: id))
        }

        return retired
    }

    /// Generate a new connection ID for the connection.
    func generateID() -> QUICConnectionID {
        self._channel._registrar.generateID()
    }

    /// Notifies the connection that it should call back into the connection and drain its output
    /// buffer. This is used for out-of-band writes.
    func drainOutbound() {
        self._channel.drainAndReconcileLifecycle()
    }

    /// Notifies the connection that the handshake completed.
    ///
    /// - Parameter peerMaxDatagramFrameSize: The peer's advertised `max_datagram_frame_size`
    ///   or `0` if the peer does not accept datagrams.
    func handshakeCompleted(peerMaxDatagramFrameSize: Int) {
        self._channel._connectionActivated(peerMaxDatagramFrameSize: peerMaxDatagramFrameSize)
    }

    /// A datagram was received from the peer; fire it as a `channelRead`.
    func datagramRead(_ datagram: ByteBuffer) {
        self._channel.pipeline.syncOperations.fireChannelRead(NIOAny(datagram))
    }

    /// The connection's datagram flow failed; fire the error into the pipeline.
    func datagramError(_ error: any Error) {
        self._channel.pipeline.syncOperations.fireErrorCaught(error)
    }

    /// The connection closed spontaneously.
    ///
    /// Locally-requested closes do not arrive here, they go via
    /// ``QUICConnectionProtocol/close(isApplicationClose:errorCode:reason:)`` instead.
    ///
    /// - Parameter error: The close error, or `nil` for a clean close.
    func connectionClosed(error: (any Error)?) {
        self._channel._connectionClosed(error: error)
    }

    /// Notify the channel that a peer-initiated inbound stream was created.
    ///
    /// - Parameters:
    ///   - id: The stream's ID.
    ///   - channel: The stream's handler.
    func newInboundStream(id: QUICStreamID, channel: QUICChannelStreamHandler) {
        self._channel._newInboundStream(streamID: id, channel: channel)
    }
}

// MARK: - Transport view

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    /// A view of the channel used by the transport (i.e. UDP channel).
    struct TransportView {
        private let channel: QUICConnectionChannel

        init(_ channel: QUICConnectionChannel) {
            channel.eventLoop.assertInEventLoop()
            self.channel = channel
        }
    }

    var transportView: TransportView {
        TransportView(self)
    }
}

@available(anyAppleOS 26, *)
@available(*, unavailable)
extension QUICConnectionChannel.TransportView: Sendable {}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel.TransportView {
    /// Configure the pipeline, then complete `promise`.
    ///
    /// For a **server** connection the promise is completed when the channel
    /// is initialized, for a **client** connection the promise is completed when
    /// the channel is initialized and the handshake has completed.
    func initialize(
        promise: EventLoopPromise<Void>?,
        initializer: (QUICConnectionChannel) -> EventLoopFuture<Void>
    ) {
        guard self.channel._lifecycle.initialize() else {
            promise?.fail(ChannelError.operationUnsupported)
            return
        }

        // Inherit autoRead from the parent UDP channel; default if unreadable.
        let autoRead = (try? self.channel.parent?.syncOptions?.getOption(.autoRead)) ?? true
        self.channel._autoRead = autoRead

        initializer(self.channel)
            .hop(to: self.channel.eventLoop)
            .assumeIsolated()
            .whenComplete { result in
                self._initializerCompleted(result: result, promise: promise)
            }
    }

    private func _initializerCompleted(
        result: Result<Void, any Error>,
        promise: EventLoopPromise<Void>?
    ) {
        switch result {
        case .success:
            switch self.channel._lifecycle.initialized() {
            case .awaitingActivation:
                if self.channel.isServer {
                    self.channel.drainAndReconcileLifecycle()
                    promise?.succeed()
                } else {
                    self.channel._readyPromise = promise
                    self.channel.drainAndReconcileLifecycle()
                }

            case .closedDuringInit:
                // Closed completed before init; fail the promise now.
                self.channel.drainAndReconcileLifecycle()
                promise?.fail(ChannelError.alreadyClosed)
            }

        case .failure(let error):
            self.channel.failInitialization(error: error)
            promise?.fail(error)
        }
    }

    /// Returns whether the read caused the channel to enter a read loop.
    @discardableResult
    func parentChannelRead(_ buffer: ByteBuffer) -> Bool {
        self.channel._parentChannelRead(buffer)
    }

    func parentChannelReadComplete() {
        self.channel._parentChannelReadComplete()
    }

    func parentChannelInactive() {
        self.channel._parentChannelBecameInactive()
    }

    func parentChannelWritabilityChanged(to isWritable: Bool) {
        self.channel._parentChannelWritabilityChanged(to: isWritable)
    }

    func parentChannelUserInboundEventTriggered(_ event: Any) {
        self.channel._parentChannelUserInboundEvent(event)
    }

    func shutdown(promise: EventLoopPromise<Void>?) {
        self.channel._parentInvokedShutdown(promise: promise)
    }

    /// Tear the channel down immediately.
    ///
    /// Skips waiting for the QUIC close handshake or for per-stream close futures; used when
    /// a graceful shutdown overran its deadline.
    func forceClose() {
        self.channel._forceClose()
    }
}

// MARK: - Close & inactive

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    private func _connectionActivated(peerMaxDatagramFrameSize: Int) {
        self.eventLoop.assertInEventLoop()
        self._lifecycle.connectionActivated()
        self._setPeerMaxDatagramFrameSize(peerMaxDatagramFrameSize)
    }

    private func _connectionClosed(error: (any Error)?) {
        self.eventLoop.assertInEventLoop()
        self._lifecycle.connectionClosed(error: error)
    }

    /// Ask the lifecycle whether to initiate a new close, cascade onto an existing close, or
    /// succeed immediately, then perform the corresponding side effects.
    func closeConnection(
        promise: EventLoopPromise<Void>?,
        isApplicationClose: Bool,
        errorCode: Int64,
        reasonPhrase: String
    ) {
        self.eventLoop.assertInEventLoop()

        let continueClosing: Bool
        switch self._lifecycle.beginClosing(error: nil) {
        case .beganClosing:
            continueClosing = true
        case .alreadyClosing:
            self.closePromise.futureResult.cascade(to: promise)
            continueClosing = false
        case .alreadyClosed:
            // The close promise has already been completed; cascading it completes 'promise'
            // immediately rather than leaving the caller waiting forever.
            self.closePromise.futureResult.cascade(to: promise)
            continueClosing = false
        }

        guard continueClosing else { return }

        if let promise {
            self.closePromise.futureResult.cascade(to: promise)
        }

        // Notify streams of imminent close so handlers can flush final frames during the
        // QUIC close handshake. Only on graceful closes — abrupt error closes don't allow
        // time to wind down.
        if errorCode == QUICTransportErrorCode.noError.rawValue {
            self._connection.quiesceStreams()
        }

        // Close may re-enter the channel and result in a drain; stop the re-entrancy and
        // explicitly drain after this.
        self.withoutEnteringDrainOutput {
            _ = self._connection.close(
                isApplicationClose: isApplicationClose,
                errorCode: errorCode,
                reason: reasonPhrase
            )
        }

        self.drainAndReconcileLifecycle()
    }

    /// Flush finalized output to the UDP side, then apply any pending lifecycle transition.
    ///
    /// Bails if drains are suppressed (`!_isAllowedToDrain`): a re-entrant `drainOutbound()`
    /// fired from inside `withoutEnteringDrainOutput { … }` must not run
    /// `reconcileLifecycle()` here — that would fire `channelActive`/`channelInactive`
    /// mid-batch. The enclosing operation re-drives once its suppressed block returns.
    ///
    /// This is the only way to pair a drain with a lifecycle reconcile: calling `drainOutput()`
    /// and `reconcileLifecycle()` separately takes the drain's suppression but not the
    /// reconcile's, which fires lifecycle events mid-batch when re-entered.
    ///
    /// Note this does not *set* `_isAllowedToDrain`: its own work (pop finalized packets, read
    /// the state machine) never re-enters SwiftNetwork, so there is nothing new to suppress.
    fileprivate func drainAndReconcileLifecycle() {
        self.eventLoop.assertInEventLoop()

        guard self._isAllowedToDrain else { return }

        self.drainOutput()
        self.reconcileLifecycle()
    }

    /// Tear the channel down after the user's initializer threw. Emits CONNECTION\_CLOSE to the
    /// peer, then drives normal pipeline teardown (errorCaught + channelInactive) carrying the
    /// initializer error.
    fileprivate func failInitialization(error: any Error) {
        self.eventLoop.assertInEventLoop()
        self._lifecycle.beginClosing(error: error)

        // Close may re-enter the channel and result in a drain; stop the re-entrancy and
        // explicitly drain after this.
        self.withoutEnteringDrainOutput {
            _ = self._connection.close(
                isApplicationClose: false,
                errorCode: QUICTransportErrorCode.internalError.rawValue,
                reason: ""
            )
        }

        self.drainAndReconcileLifecycle()
    }

    /// Run `body` with the drain guard held, dropping any re-entrant `drainOutbound()`
    /// SwiftNetwork fires while `body` runs (e.g. from `receivePacketsComplete()` finalizing
    /// frames, or `close()` stopping the flow handler).
    ///
    /// The explicit drain after flushes whatever `body` finalized. Without this
    /// the re-entrant wake would run a nested `drainAndReconcileLifecycle()`
    /// mid-batch, firing `channelActive`/`Inactive` before the batch completes.
    private func withoutEnteringDrainOutput<Result>(_ body: () -> Result) -> Result {
        let wasAllowedToDrain = self._isAllowedToDrain
        self._isAllowedToDrain = false
        defer { self._isAllowedToDrain = wasAllowedToDrain }
        return body()
    }

    private func drainOutput() {
        self.eventLoop.assertInEventLoop()

        guard self._isAllowedToDrain else { return }

        // Avoid re-entering this function.
        self.withoutEnteringDrainOutput {
            while let envelope = self._connection.nextPacketToSend() {
                self._transport.writeDatagram(envelope, promise: nil)
            }

            // The view discards unnecessary flushes; no need to track them per-write in the loop.
            self._transport.flush()
        }
    }

    /// Apply any lifecycle transition pushed by the connection since the last
    /// drain. Runs at the post-drain boundary so `channelActive` fires only after
    /// the handshake's final bytes have flushed. The lifecycle owns the decision;
    /// this only performs the side effects it hands back.
    private func reconcileLifecycle() {
        self.eventLoop.assertInEventLoop()

        while let action = self._lifecycle.reconcile() {
            switch action {
            case .fireActive:
                self._isActive.store(true, ordering: .releasing)
                self.pipeline.fireChannelActive()
                self._readyPromise.take()?.succeed()

                if self._autoRead {
                    self._transport.read()
                }

            case .fireInactive(let error):
                self.fireChannelInactiveNow(error: error)
            }
        }
    }

    fileprivate func fireChannelInactiveNow(error: (any Error)? = nil) {
        self.eventLoop.assertInEventLoop()

        // Stop accepting more inbound streams.
        switch self._streamInitializer.take() {
        case .multiplexer(let continuation):
            continuation.finish()
        case .closure, .none:
            ()
        }

        // Surface the close error to before tearing the streams down: a handler may rely on
        // connection level errors to fail streams.
        if let error {
            self.pipeline.fireErrorCaught(error)
        }

        // Wait for all streams to close before firing inactive.
        let streamCloseFutures = self._connection.closeAllStreams()

        if streamCloseFutures.isEmpty {
            self.completeChannelInactive(error: error)
        } else {
            EventLoopFuture
                .andAllComplete(streamCloseFutures, on: self.eventLoop)
                .assumeIsolated()
                .whenComplete { _ in
                    self.completeChannelInactive(error: error)
                }
        }
    }

    private func completeChannelInactive(error: (any Error)?) {
        self.eventLoop.assertInEventLoop()
        self._lifecycle.closed()
        self._isActive.store(false, ordering: .releasing)

        // Datagram writes buffered while waiting for the peer's advertisement will never be sent.
        self._failBufferedDatagramWrites()

        self.pipeline.fireChannelInactive()

        if let readyPromise = self._readyPromise.take() {
            readyPromise.fail(error ?? ChannelError.alreadyClosed)
        }

        // Tear down on the next loop tick.
        self.eventLoop.assumeIsolated().execute {
            self.removeHandlers(pipeline: self.pipeline)
            self._connection.dropChannelReferences()
            self.closePromise.succeed()
        }
    }
}

// MARK: - Parent channel events

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    fileprivate func _parentChannelBecameInactive() {
        self.eventLoop.assertInEventLoop()

        let shouldClose: Bool
        switch self._lifecycle.beginClosing(error: nil) {
        case .beganClosing:
            shouldClose = true
        case .alreadyClosing, .alreadyClosed:
            shouldClose = false
        }

        guard shouldClose else { return }

        // No drain before the close: the parent channel is already inactive so nothing can
        // be written. The close is only here to drive the connection's state machine so streams
        // tear down.
        self.withoutEnteringDrainOutput {
            _ = self._connection.close(
                isApplicationClose: false,
                errorCode: QUICTransportErrorCode.noError.rawValue,
                reason: ""
            )
        }

        self.drainAndReconcileLifecycle()
    }

    /// Force the channel closed without waiting for QUIC peer ack or per-stream close futures.
    /// Used by the registry when graceful shutdown overran its deadline.
    fileprivate func _forceClose() {
        self.eventLoop.assertInEventLoop()

        switch self._lifecycle.forceClosing() {
        case .alreadyClosed:
            return
        case .alreadyCommitted:
            // Another path already committed to firing channelInactive and may be mid-flight
            // (e.g. waiting on closeAllStreams() futures). Only hurry the streams along; let
            // that path's own completeChannelInactive finish the job.
            _ = self._connection.closeAllStreams()
        case .forceThroughNow:
            // Stop the stream handlers aggressively (sets each to disconnected and detaches from
            // the lower protocol). Their own closeFutures may resolve on a later tick, but we
            // don't wait — that's the whole point of force-close.
            _ = self._connection.closeAllStreams()
            self.completeChannelInactive(error: nil)
        }
    }

    fileprivate func _parentChannelUserInboundEvent(_ event: Any) {
        self.eventLoop.assertInEventLoop()
        self.pipeline.syncOperations.fireUserInboundEventTriggered(event)
    }

    fileprivate func _parentChannelRead(_ buffer: ByteBuffer) -> Bool {
        self.eventLoop.assertInEventLoop()
        // Feed packets in, '_parentChannelReadComplete' signals to the connection that
        // it should then consume those packets.
        self._connection.receivePacket(buffer)

        let didEnterReadLoop = !self._inReadLoop
        self._inReadLoop = true
        return didEnterReadLoop
    }

    fileprivate func _parentChannelReadComplete() {
        self.eventLoop.assertInEventLoop()
        self._inReadLoop = false

        // Avoid entering 'drainOutput'; wait for all events to be delivered and then
        // deal with them in 'drainAndReconcileLifecycle' below.
        self.withoutEnteringDrainOutput {
            self._connection.receivePacketsComplete()
        }

        // Disable outbound batching so that SwiftQUIC emits frames into the connection. They'll
        // be picked up when draining starts a few lines below. Re-enable again (i.e. disable
        // temporarily) if there are pending streams as they may also produce output during their
        // init.
        self._connection.withLiveOnly { connection in
            let hasPendingStreams = !self._pendingStreams.isEmpty
            connection.flushOutboundBatch(resumeBatching: hasPendingStreams)
        }

        self.drainAndReconcileLifecycle()
        self.processPendingInboundStreams()

        // Done producing data: exit read loop (which also flushes outbound data.)
        self._connection.withLiveOnly { $0.exitReadLoop() }
        // Stream initializers may produce output; reconcile any pending events again.
        self.drainAndReconcileLifecycle()
    }

    fileprivate func _parentChannelWritabilityChanged(to isWritable: Bool) {
        let (exchanged, _) = self._isWritable.compareExchange(
            expected: !isWritable,
            desired: isWritable,
            ordering: .acquiringAndReleasing
        )

        // Only fire a notification when the value changed.
        if exchanged {
            self.pipeline.fireChannelWritabilityChanged()
        }
    }

    fileprivate func _parentInvokedShutdown(promise: EventLoopPromise<Void>?) {
        self.closeConnection(
            promise: promise,
            isApplicationClose: false,
            errorCode: QUICTransportErrorCode.noError.rawValue,
            reasonPhrase: ""
        )
    }
}

// MARK: - Outbound streams

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    fileprivate func _createOutboundStream(
        type: QUICStreamType,
        promise: EventLoopPromise<any Channel>,
        initializer: @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
    ) {
        self.eventLoop.assertInEventLoop()

        switch self._connection {
        case .live(let connection):
            do {
                try connection.addNewOutboundStreamInputHandler(
                    streamType: type,
                    connectionChannel: self
                ) { result in
                    switch result {
                    case .success(let (streamID, stream)):
                        stream.initializeOutbound(
                            streamID: streamID,
                            initializer: initializer,
                            promise: promise
                        )
                    case .failure(let error):
                        promise.fail(error)
                    }
                }
            } catch {
                // Invalid stream type.
                promise.fail(error)
            }

        case .test:
            promise.fail(ChannelError.operationUnsupported)
        }

    }

    func makeStreamCreator(role: Role) -> QUICStreamCreator {
        QUICStreamCreator(
            eventLoop: self.eventLoop,
            role: role,
            createOutboundStream: NIOLoopBound(
                self.createOutboundStreamForCreator,
                eventLoop: self.eventLoop
            )
        )
    }

    private func createOutboundStreamForCreator(
        _ promise: EventLoopPromise<any Channel>,
        _ streamType: QUICStreamType,
        _ streamChannelInitializer: @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
    ) {
        self.eventLoop.assertInEventLoop()
        self._createOutboundStream(
            type: streamType,
            promise: promise,
            initializer: streamChannelInitializer
        )
    }
}

// MARK: - Streams

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    func setInboundStreamInitializer(_ initializer: StreamInitializer) {
        self.eventLoop.assertInEventLoop()
        self._streamInitializer = initializer
    }

    private func _newInboundStream(streamID: QUICStreamID, channel: QUICChannelStreamHandler) {
        self.eventLoop.assertInEventLoop()
        self._pendingStreams.append((streamID, channel))
    }

    /// Initialize inbound streams pushed during the current read batch, skipping any not yet
    /// connected.
    private func processPendingInboundStreams() {
        self.eventLoop.assertInEventLoop()

        while let (streamID, stream) = self._pendingStreams.popFirst() {
            if stream.streamStateMachine.isConnected {
                self.runInboundStreamInitializer(streamID: streamID, stream: stream)
            }
        }
    }

    private func runInboundStreamInitializer(
        streamID: QUICStreamID,
        stream: QUICChannelStreamHandler
    ) {
        self.eventLoop.assertInEventLoop()

        self._lifecycle.willInitializeStream()

        let initialized = stream.initializeInbound(
            streamID: streamID,
            initializer: self._streamInitializer
        )

        if let initialized {
            initialized.assumeIsolated().whenComplete { _ in
                self._streamInitializerDidComplete()
            }
        } else {
            self._streamInitializerDidComplete()
        }
    }

    fileprivate func _streamInitializerDidComplete() {
        self.eventLoop.assertInEventLoop()

        self._lifecycle.streamInitializerFinished()
        self.reconcileLifecycle()
    }
}

// MARK: - Datagrams

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    /// Writes a datagram, applying the current negotiation state.
    func _writeDatagram(_ datagram: ByteBuffer, promise: EventLoopPromise<Void>?) {
        self.eventLoop.assertInEventLoop()

        switch self._datagramNegotiation {
        case .waitingForPeerAdvertisement(var earlyWrites):
            // The handshake has not finished and the peer's advertised size is yet unknown.
            // Temporarily switch state to avoid CoW-ign earlyWrites.
            self._datagramNegotiation = .peerDoesNotAcceptDatagrams
            earlyWrites.append((datagram, promise))
            self._datagramNegotiation = .waitingForPeerAdvertisement(earlyWrites: earlyWrites)

        case .peerDoesNotAcceptDatagrams:
            // Nope. Drop it.
            promise?.fail(QUICError.peerDoesNotAcceptDatagrams)

        case .peerAcceptsDatagrams(let maximumSize):
            self.sendDatagram(datagram, maximumSize: maximumSize, promise: promise)
        }
    }

    /// Flushes datagrams buffered in the connection since the last flush.
    func _flushDatagrams() {
        self.eventLoop.assertInEventLoop()

        self.withoutEnteringDrainOutput {
            self._connection.flushDatagrams()
        }

        self.drainAndReconcileLifecycle()
    }

    /// Applies the peer's advertised `max_datagram_frame_size` setting.
    ///
    /// - Precondition: Must only be called once per connection; the peer only advertises this once.
    private func _setPeerMaxDatagramFrameSize(_ size: Int) {
        self.eventLoop.assertInEventLoop()

        switch self._datagramNegotiation {
        case .waitingForPeerAdvertisement(let earlyWrites):
            if size == 0 {
                self._datagramNegotiation = .peerDoesNotAcceptDatagrams
                for (_, promise) in earlyWrites {
                    promise?.fail(QUICError.peerDoesNotAcceptDatagrams)
                }
            } else {
                self._datagramNegotiation = .peerAcceptsDatagrams(maximumSize: size)
                for (datagram, promise) in earlyWrites {
                    self.sendDatagram(datagram, maximumSize: size, promise: promise)
                }

                if !earlyWrites.isEmpty {
                    self._flushDatagrams()
                }
            }

        case .peerDoesNotAcceptDatagrams, .peerAcceptsDatagrams:
            assertionFailure("peer max datagram size must not be updated more than once")
        }
    }

    /// Fails any writes buffered while waiting for the peer's advertisement. Called as the channel
    /// tears down: nothing will arrive to resolve them otherwise.
    private func _failBufferedDatagramWrites() {
        self.eventLoop.assertInEventLoop()

        switch self._datagramNegotiation {
        case .waitingForPeerAdvertisement(let earlyWrites):
            self._datagramNegotiation = .peerDoesNotAcceptDatagrams
            for (_, promise) in earlyWrites {
                promise?.fail(ChannelError.ioOnClosedChannel)
            }
        case .peerDoesNotAcceptDatagrams, .peerAcceptsDatagrams:
            ()  // Nothing is buffered here, writes are handed straight to the connection.
        }
    }

    /// Rejects the datagram if it exceeds the peer's advertised limit, then hands it to the
    /// connection (which may still refuse the write, e.g. because it has no datagram flow).
    ///
    /// The size check is payload-only, see `QUICError.datagramTooLarge`.
    private func sendDatagram(
        _ datagram: ByteBuffer,
        maximumSize: Int,
        promise: EventLoopPromise<Void>?
    ) {
        guard datagram.readableBytes <= maximumSize else {
            promise?.fail(QUICError.datagramTooLarge)
            return
        }

        if self._connection.writeDatagram(datagram) {
            promise?.succeed()
        } else {
            promise?.fail(QUICError.datagramWriteFailed)
        }
    }
}
