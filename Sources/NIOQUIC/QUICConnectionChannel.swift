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

import NIOCore
import NIOQUICHelpers
import Synchronization

/// A channel for a single QUIC connection.
///
/// Created by ``QUICHandler`` for each accepted or initiated QUIC connection. Inbound packets
/// are pushed in via the ``ParentView``; outbound datagrams are written back through a
/// ``QUICHandler/ChildView``. Stream channels live one level below this channel.
@available(anyAppleOS 26, *)
final class QUICConnectionChannel: @unchecked Sendable {
    // @unchecked because of the IUO ChannelPipeline, which is never mutated after `init`.
    // The `ChannelPipeline` breaks the retain cycle between it and this channel.

    /// The pipeline associated with this channel.
    private var _pipeline: ChannelPipeline!

    /// Completed when the channel is closed. Provides `closeFuture` for the `Channel` API.
    private let closePromise: EventLoopPromise<Void>

    /// The underlying SwiftNetowork-backed QUIC connection.
    private let connection: SwiftNetworkQUICConnection

    /// A view into the QUIC handler in the UDP channel, for outbound operations.
    private let udpView: QUICHandler.ChildView

    /// The `Channel` for the UDP connection.
    private let udpChannel: any Channel

    /// Whether the `Channel` is currently writable.
    private let _isWritable: Atomic<Bool>

    /// Whether the `Channel` is currently active.
    private let _isActive: Atomic<Bool>

    /// Event loop local state
    private var state: State

    /// The `EventLoop` the channel is bound to.
    let eventLoop: any EventLoop

    /// A `ByteBuffer` allocator.
    let allocator: ByteBufferAllocator

    init(
        udpChannel: any Channel,
        udpView: QUICHandler.ChildView,
        connection: SwiftNetworkQUICConnection
    ) {
        self.udpChannel = udpChannel
        self.udpView = udpView
        self.connection = connection

        self.eventLoop = udpChannel.eventLoop
        self.allocator = udpChannel.allocator
        self.closePromise = udpChannel.eventLoop.makePromise()
        self._isWritable = Atomic(true)
        self._isActive = Atomic(false)
        self.state = State()

        self._pipeline = ChannelPipeline(channel: self)
    }

    // Mutable state, to be mutated from the event loop only.
    struct State {
        var lifecycle: Lifecycle
        var autoRead: Bool
        var deferredInactiveError: (any Error)?
        /// Caller-supplied promise to settle when the channel either reaches `.activated`
        /// (success) or hits `.closed` first (failure). `nil` if no caller asked to await
        /// activation, or if it has already been settled.
        var readyPromise: EventLoopPromise<Void>?

        init() {
            self.lifecycle = .idle
            self.autoRead = true
            self.deferredInactiveError = nil
            self.readyPromise = nil
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    struct SyncView {
        fileprivate let channel: QUICConnectionChannel

        fileprivate init(_ channel: QUICConnectionChannel) {
            channel.eventLoop.assertInEventLoop()
            self.channel = channel
        }
    }

    private var syncView: SyncView {
        SyncView(self)
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel.SyncView: NIOSynchronousChannelOptions {
    func getOption<Option: ChannelOption>(_ option: Option) throws -> Option.Value {
        switch option {
        case is ChannelOptions.Types.AutoReadOption:
            return self.channel.state.autoRead as! Option.Value
        default:
            throw ChannelError.operationUnsupported
        }
    }

    func setOption<Option: ChannelOption>(_ option: Option, value: Option.Value) throws {
        switch option {
        case is ChannelOptions.Types.AutoReadOption:
            self.channel.state.autoRead = value as! Bool
        default:
            throw ChannelError.operationUnsupported
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel: Channel {
    var closeFuture: EventLoopFuture<Void> {
        self.closePromise.futureResult
    }

    var pipeline: ChannelPipeline {
        self._pipeline
    }

    var localAddress: SocketAddress? {
        self.connection.localAddress
    }

    var remoteAddress: SocketAddress? {
        self.connection.remoteAddress
    }

    var parent: (any Channel)? {
        self.udpChannel
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
                try self.syncView.setOption(option, value: value)
            }
        } else {
            return self.eventLoop.submit {
                try self.syncView.setOption(option, value: value)
            }
        }
    }

    func getOption<Option: ChannelOption>(_ option: Option) -> EventLoopFuture<Option.Value> {
        if self.eventLoop.inEventLoop {
            return self.eventLoop.makeCompletedFuture {
                try self.syncView.getOption(option)
            }
        } else {
            return self.eventLoop.submit {
                try self.syncView.getOption(option)
            }
        }
    }

    var syncOptions: (any NIOSynchronousChannelOptions)? {
        self.syncView
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel: ChannelCore {
    func localAddress0() throws -> SocketAddress {
        self.connection.localAddress
    }

    func remoteAddress0() throws -> SocketAddress {
        self.connection.remoteAddress
    }

    func register0(promise: EventLoopPromise<Void>?) {
        promise?.succeed()
    }

    func bind0(to: SocketAddress, promise: EventLoopPromise<Void>?) {
        promise?.fail(ChannelError.operationUnsupported)
    }

    func connect0(to: SocketAddress, promise: EventLoopPromise<Void>?) {
        promise?.fail(ChannelError.operationUnsupported)
    }

    func write0(_ data: NIOAny, promise: EventLoopPromise<Void>?) {
        // TODO: support QUIC datagrams.
        promise?.fail(ChannelError.operationUnsupported)
    }

    func flush0() {
        // TODO: support QUIC datagrams
    }

    func read0() {
        self.udpView.read()
    }

    func close0(error: any Error, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        self.closeConnection(
            promise: promise,
            isApplicationClose: false,
            errorCode: QUICTransportErrorCode.noError.rawValue,
            reasonPhrase: ""
        )
    }

    func triggerUserOutboundEvent0(_ event: Any, promise: EventLoopPromise<Void>?) {
        switch event {
        case let event as QUICCloseConnectionEvent:
            self.closeConnection(
                promise: promise,
                isApplicationClose: true,
                errorCode: Int64(event.code.rawValue),
                reasonPhrase: event.reasonPhrase ?? ""
            )

        #if DEBUG
        case let event as _QUICForTestingPoisonRetiredSCIDEvent:
            self.connection._forTesting_addRetiredSCID(event.scid)
            promise?.succeed()
        case let event as _QUICForTestingGetActiveSCIDsEvent:
            let scids = self.connection._forTesting_getActiveSCIDs()
            event.result.withLockedValue { $0 = scids }
            promise?.succeed()
        case let event as _QUICForTestingRemoveActiveSCIDEvent:
            self.connection._forTesting_removeFromActiveSCIDs(event.scid)
            promise?.succeed()
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

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    /// Switch on lifecycle and decide whether to initiate a new close, cascade onto an existing
    /// close, or succeed immediately.
    func closeConnection(
        promise: EventLoopPromise<Void>?,
        isApplicationClose: Bool,
        errorCode: Int64,
        reasonPhrase: String
    ) {
        self.eventLoop.assertInEventLoop()

        switch self.state.lifecycle {
        case .closing:
            if let promise {
                self.closePromise.futureResult.cascade(to: promise)
            }
            return
        case .closed:
            promise?.succeed()
            return
        case .idle, .initializing, .initialized, .activated:
            break
        }

        if let promise {
            self.closePromise.futureResult.cascade(to: promise)
        }

        // Notify streams of imminent close so handlers can flush final frames during the
        // QUIC close handshake. Only on graceful closes — abrupt error closes don't allow
        // time to wind down.
        if errorCode == QUICTransportErrorCode.noError.rawValue {
            self.connection.fireUserInboundEventOnAllStreams(ChannelShouldQuiesceEvent())
        }

        self.connection.outboundDrainScheduled()
        defer { self.connection.outboundDrainFinished() }

        let action = self.connection.close(
            sendApplicationClose: isApplicationClose,
            errorCode: errorCode,
            reason: reasonPhrase
        )

        switch action {
        case .alreadyClosed:
            self.state.lifecycle.closing()
            self.fireChannelInactiveSoon()
        case .closeInitiated:
            self.state.lifecycle.closing()
            self.drainOutput()
        }
    }

    private func drainOutput() {
        self.eventLoop.assertInEventLoop()

        while let buffer = self.connection.nextOutboundPacket() {
            let envelope = AddressedEnvelope(
                remoteAddress: self.connection.remoteAddress,
                data: buffer
            )
            self.udpView.writeDatagram(envelope, promise: nil)
        }

        // The view discards unnecessary flushes; no need to track them per-write in the loop.
        self.udpView.flush()

        let isInitializing = self.state.lifecycle.outboundDataProcessed()
        switch self.connection.outboundDataProcessed(isChannelInitializing: isInitializing) {
        case .completeActivation:
            self.state.lifecycle.activated()
            self._isActive.store(true, ordering: .releasing)
            self.pipeline.fireChannelActive()
            self.state.readyPromise?.succeed()
            self.state.readyPromise = nil
            // Initial autoRead read.
            if self.state.autoRead {
                self.udpView.read()
            }

        case .closeCleanly:
            self.fireChannelInactiveSoon()

        case .closeWithError(let error):
            self.fireChannelInactiveSoon(error: error)

        case .noAction:
            ()
        }
    }

    fileprivate func fireChannelInactiveSoon(error: (any Error)? = nil) {
        // TODO: deferral (will be done in a later PR.)
        self.fireChannelInactiveNow(error: error)
    }

    fileprivate func fireChannelInactiveNow(error: (any Error)? = nil) {
        self.eventLoop.assertInEventLoop()

        // Wait for all stream handlers before firing inactive.
        let streamCloseFutures = self.connection.closeAllStreamHandlers()

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
        self._isActive.store(false, ordering: .releasing)
        if let error {
            self.pipeline.fireErrorCaught(error)
        }
        self.pipeline.fireChannelInactive()
        self.pipeline.fireChannelUnregistered()
        self.state.lifecycle.closed()

        if let readyPromise = self.state.readyPromise {
            self.state.readyPromise = nil
            readyPromise.fail(error ?? ChannelError.alreadyClosed)
        }

        // closeFuture is an observer of "channel is done"; always succeed it. Errors are
        // surfaced via fireErrorCaught above.
        self.closePromise.succeed()
    }
}
