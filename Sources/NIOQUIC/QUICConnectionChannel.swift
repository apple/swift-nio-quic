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

    /// Whether auto-read is enabled on this channel.
    private var _autoRead: Bool

    // MARK: - Channel API

    /// The parent `Channel` (i.e. the UDP channel).
    let parent: (any Channel)?

    /// The `EventLoop` the channel is bound to (identical to `parent.eventLoop`).
    let eventLoop: any EventLoop

    /// A `ByteBuffer` allocator.
    let allocator: ByteBufferAllocator

    init(
        udpChannel: any Channel,
        isServer: Bool
    ) {
        self.parent = udpChannel

        self.eventLoop = udpChannel.eventLoop
        self.allocator = udpChannel.allocator
        self.closePromise = udpChannel.eventLoop.makePromise()

        // force unwraps will be removed in a later PR (a not-yet-introduced provides them)
        self._localAddress = udpChannel.localAddress!
        self._remoteAddress = udpChannel.remoteAddress!
        self._isWritable = Atomic(true)
        self._isActive = Atomic(false)
        self.isServer = isServer

        self._autoRead = true

        self._pipeline = ChannelPipeline(channel: self)
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
        // TODO: support QUIC datagrams.
        promise?.fail(ChannelError.operationUnsupported)
    }

    func flush0() {
        // TODO: support QUIC datagrams
    }

    func read0() {
        // TODO: read from transport
    }

    func close0(error: any Error, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        self.eventLoop.assertInEventLoop()

        // Added in a later PR:
        // self.closeConnection(
        //     promise: promise,
        //     isApplicationClose: false,
        //     errorCode: QUICTransportErrorCode.noError.rawValue,
        //     reasonPhrase: ""
        // )
    }

    func triggerUserOutboundEvent0(_ event: Any, promise: EventLoopPromise<Void>?) {
        self.eventLoop.assertInEventLoop()
        // Handled in a later PR.
    }

    func channelRead0(_ data: NIOAny) {
        // Unhandled read, drop it.
    }

    func errorCaught0(error: any Error) {
        // Unhandled error, drop it.
    }
}
