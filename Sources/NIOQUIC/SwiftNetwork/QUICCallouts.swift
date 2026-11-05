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
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

/// The calls a connection makes into NIO and consumer code when entering from SwiftNetwork are queued
/// while the stack holds the connection's `EventContext` and run once it's released.
///
/// SwiftNetwork's `NetworkContext.EventContext` is an exclusive token: acquiring it while it's held
/// traps. NIO's pipeline is synchronous and re-entrant: code NIOQUIC calls into (a handler reacting
/// to a `channelRead`, a promise callback, a stream initializer) can call straight back into
/// NIOQUIC and through to Swift Network, which then needs the token again. So NIOQUIC never makes
/// such calls while the token is held:
///
/// - Code that holds the token (recognisable by its `inout NetworkContext.EventContext` parameter)
///   updates NIOQUIC's own state and queues the call with ``add(_:in:)``.
/// - Whoever acquired the token runs the queue once it's released. NIOQUIC's entry points acquire
///   through ``enter(as:_:)``, which runs the queue on the way out. Framework entry points that
///   acquire the token themselves (attaching flows and paths, metadata setters such as keep-alive)
///   are called through ``callFramework(_:)``, which runs what their witnesses queued. Otherwise
///   the stack acquires the token only inside the tasks it schedules on the connection's
///   `EventLoopBackedScheduler`, which runs the queue after each of them.
///
/// So NIO and consumer code only run with nothing held, NIOQUIC's entry points always acquire the
/// token afresh, and the queued calls run in order, in the event loop tick that queued them.
///
/// Consumer code that runs with the token held because NIOQUIC needs its result must not call
/// into NIOQUIC: the connection ID and stateless reset token generators.
@available(anyAppleOS 26, *)
final class QUICCallouts {
    private var queue: Deque<QUICCallout>

    #if DEBUG
    /// How many of ``enter(as:_:)``, ``callFramework(_:)`` and scheduler tasks are running.
    private var stackDepth: Int
    #endif

    init() {
        self.queue = []
        #if DEBUG
        self.stackDepth = 0
        #endif
    }

    #if DEBUG
    /// Whether the stack may hold the event context.
    var isInsideStack: Bool {
        self.stackDepth > 0
    }
    #endif

    /// Queues `callout` to run once the event context is released.
    ///
    /// - Note: `eventContext` is unused. Holding it just proves that the caller is in the stack.
    ///
    /// - Precondition: Must only be called from inside the stack.
    func add(_ callout: QUICCallout, in eventContext: inout NetworkContext.EventContext) {
        #if DEBUG
        assert(
            self.isInsideStack,
            "Callout queued where nothing runs the queue: call the framework through callFramework(_:)"
        )
        #endif
        self.queue.append(callout)
    }

    /// Runs `body` with the connection's event context, entering the stack as `instance`, then runs
    /// the calls queued meanwhile, before returning or rethrowing.
    ///
    /// Enter as the NIOQUIC instance that makes the calls, never as a framework instance it calls
    /// into: a down-call needs an idle callee, and entering as the callee makes it busy.
    ///
    /// - Precondition: Must be called with no event context held.
    func enter<Instance: ProtocolInstance, Value, Failure: Error>(
        as instance: Instance,
        _ body: (inout NetworkContext.EventContext) throws(Failure) -> Value
    ) throws(Failure) -> Value {
        self.willEnterStack()
        defer {
            self.didLeaveStack()
            self.run()
        }
        return try instance.fromExternal(body)
    }

    /// Calls a framework entry point that acquires the event context itself, then runs the calls its
    /// witnesses queued meanwhile, before returning or rethrowing.
    ///
    /// - Precondition: Must be called with no event context held.
    func callFramework<Value, Failure: Error>(_ body: () throws(Failure) -> Value) throws(Failure) -> Value {
        self.willEnterStack()
        defer {
            self.didLeaveStack()
            self.run()
        }
        return try body()
    }

    /// Runs a task the stack scheduled, inside which it acquires the event context itself, then the
    /// calls queued meanwhile.
    ///
    /// - Precondition: Must be called with no event context held.
    func runStackTask(_ task: () -> Void) {
        self.willEnterStack()
        task()
        self.didLeaveStack()
        self.run()
    }

    /// Runs the queued calls, and those they queue in turn.
    func run() {
        #if DEBUG
        assert(!self.isInsideStack, "Queued callouts run only once the event context is released")
        #endif
        while !self.queue.isEmpty {
            // Take the batch, so that a call whose code enters the stack again runs only that
            // entry's calls before it returns.
            var batch = Deque<QUICCallout>()
            swap(&batch, &self.queue)
            while let callout = batch.popFirst() {
                callout.run(through: self)
            }
            // Keep the storage for the next batch, unless calls were queued meanwhile.
            if self.queue.isEmpty {
                swap(&batch, &self.queue)
            }
        }
    }

    private func willEnterStack() {
        #if DEBUG
        assert(!self.isInsideStack, "Entering the stack while it may hold the event context")
        assert(self.queue.isEmpty, "Callouts left queued by an earlier stretch inside the stack")
        self.stackDepth += 1
        #endif
    }

    private func didLeaveStack() {
        #if DEBUG
        self.stackDepth -= 1
        #endif
    }
}

/// A call into NIO or consumer code queued while the event context is held, or a framework entry
/// point that acquires the event context itself. See ``QUICCallouts``.
@available(anyAppleOS 26, *)
enum QUICCallout {
    /// The stream channel became active: `channelRegistered`, then `channelActive`.
    case streamActive(QUICChannelStreamHandler)
    /// The stream channel's buffered inbound bytes, as a `channelRead`.
    case streamRead(QUICChannelStreamHandler)
    /// The end of a burst of reads on the stream channel: `channelReadComplete`, then auto-read.
    case streamReadComplete(QUICChannelStreamHandler)
    /// `ChannelEvent.inputClosed` on the stream channel.
    case streamInputClosed(QUICChannelStreamHandler)
    /// `QUICStopSendingEvent` on the stream channel.
    case streamStopSending(QUICChannelStreamHandler, code: NIOQUICHelpers.QUICApplicationErrorCode)
    /// `errorCaught` on the stream channel.
    case streamError(QUICChannelStreamHandler, any Error)
    /// The stream channel closed: its close promise, `errorCaught` if there's an error, then
    /// `channelInactive` and `channelUnregistered` if it was active.
    case streamClosed(QUICChannelStreamHandler, promise: EventLoopPromise<Void>?, error: (any Error)?)
    /// A locally opened stream's outcome, for whoever opened it.
    case streamReady(
        (Result<(streamID: QUICStreamID, handler: QUICChannelStreamHandler), any Error>) -> Void,
        Result<(streamID: QUICStreamID, handler: QUICChannelStreamHandler), any Error>
    )
    /// Completes a promise.
    case completePromise(EventLoopPromise<Void>, Result<Void, any Error>)
    /// A call into the connection channel.
    case connection(any QUICConnectionCalloutTarget, QUICConnectionCallout)
    /// Sets the QUIC connection's keep-alive interval, in seconds. The setter is a framework entry
    /// point that acquires the event context itself.
    case setKeepalive(SwiftNetwork.QUICConnectionProtocol.QUICConnectionMetadata, seconds: UInt16)

    func run(through callouts: QUICCallouts) {
        switch self {
        case .streamActive(let stream):
            stream.deliverActive()
        case .streamRead(let stream):
            stream.deliverRead()
        case .streamReadComplete(let stream):
            stream.deliverReadComplete()
        case .streamInputClosed(let stream):
            stream.deliverInputClosed()
        case .streamStopSending(let stream, let code):
            stream.deliverStopSending(code: code)
        case .streamError(let stream, let error):
            stream.deliverError(error)
        case .streamClosed(let stream, let promise, let error):
            stream.deliverClose(promise: promise, error: error)
        case .streamReady(let onStreamReady, let result):
            onStreamReady(result)
        case .completePromise(let promise, let result):
            promise.completeWith(result)
        case .connection(let connection, let callout):
            connection.run(callout)
        case .setKeepalive(let metadata, let seconds):
            callouts.callFramework {
                metadata.setKeepalive(keepAlive: seconds)
            }
        }
    }
}

/// A call into the connection channel. See ``QUICCallout/connection(_:_:)``.
@available(anyAppleOS 26, *)
enum QUICConnectionCallout {
    /// The handshake completed. The peer accepts datagrams up to the given size, none if it's zero.
    case handshakeCompleted(peerMaxDatagramFrameSize: Int)
    /// The connection closed, with an error or cleanly.
    case closed((any Error)?)
    /// The connection associated a new connection ID with itself: `QUICSCIDAssociatedEvent`.
    case connectionIDAssociated(QUICConnectionID)
    /// The connection retired one of its connection IDs: `QUICSCIDRetiredEvent`.
    case connectionIDRetired(QUICConnectionID)
    /// A datagram arrived on the connection's datagram flow, for a `channelRead`.
    case datagramRead(ByteBuffer)
    /// The connection's datagram flow failed, for an `errorCaught`.
    case datagramError(any Error)
}

/// The connection a ``QUICConnectionCallout`` runs on. `SwiftNetworkQUICConnection` is generic over
/// its consumer, so the queue refers to it through this.
@available(anyAppleOS 26, *)
protocol QUICConnectionCalloutTarget: AnyObject {
    func run(_ callout: QUICConnectionCallout)
}
