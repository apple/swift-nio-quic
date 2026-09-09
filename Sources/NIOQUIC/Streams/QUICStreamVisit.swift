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

import NIOQUICHelpers

/// A visit to a single QUIC stream returned from a ``QUICStreamIterator``.
///
/// During the visit you can find what events have happened on the stream since the last visit
/// in ``events`` or poll the various properties on the visit object. You can read data from or
/// write data to the network via the ``stream`` property.
///
/// You can also access and modify the consumer state for the stream via ``state``. Note that if
/// you require simulatenous access to ``stream`` and ``state`` you can use ``withStream(execute:)``.
@available(anyAppleOS 26, *)
public struct QUICStreamVisit<Consumer: QUICStreamConsumer & ~Copyable>: ~Copyable, ~Escapable {
    @usableFromInline
    let _table: QUICStreamTable<Consumer>

    @usableFromInline
    let _transport: UnsafeMutablePointer<QUICStreamTransportState>

    @usableFromInline
    let _state: UnsafeMutablePointer<Consumer.StreamState?>

    /// An opaque identifier for this stream.
    public let handle: QUICStreamHandle

    /// Events which happened to the stream since it was last visited.
    public let events: QUICStreamEvents

    @inlinable
    @_lifetime(immortal)
    init(
        table: QUICStreamTable<Consumer>,
        transport: UnsafeMutablePointer<QUICStreamTransportState>,
        state: UnsafeMutablePointer<Consumer.StreamState?>,
        handle: QUICStreamHandle,
        events: QUICStreamEvents
    ) {
        self._table = copy table
        self._transport = transport
        self._state = state
        self.handle = handle
        self.events = events
    }

    /// The ID of the stream, or `nil` if it hasn't been assigned one yet.
    @inlinable
    public var id: QUICStreamID? {
        self._transport.pointee.core.id
    }

    /// Why the stream closed, or `nil` if it closed cleanly or hasn't closed yet.
    ///
    /// This may only be set if ``events`` contains ``QUICStreamEvents/closed``.
    @inlinable
    public var closeError: (any Error)? {
        if self.events.contains(.closed) {
            return self._transport.pointee.closeError
        } else {
            return nil
        }
    }

    /// The error code the peer sent in RESET\_STREAM if ``events`` contains
    /// ``QUICStreamReadyEvents/reset``, `nil` otherwise.
    @inlinable
    public var resetCode: QUICApplicationErrorCode? {
        if self.events.contains(.reset) {
            return self._transport.pointee.resetCode
        } else {
            return nil
        }
    }

    /// The error code the peer sent in STOP\_SENDING if ``events`` contains
    /// ``QUICStreamReadyEvents/stopSending``, `nil` otherwise.
    @inlinable
    public var stopSendingCode: QUICApplicationErrorCode? {
        if self.events.contains(.stopSending) {
            return self._transport.pointee.stopSendingCode
        } else {
            return nil
        }
    }

    /// The stream being visited.
    @inlinable
    public var stream: QUICStream<Consumer> {
        @_lifetime(&self)
        mutating get {
            QUICStream(table: self._table, transport: self._transport, handle: self.handle)
        }
        @_lifetime(&self)
        mutating _modify {
            // Discarded after the yield: the stream doesn't own anything directly, modifying it
            // mutates the underlying storage.
            var stream = QUICStream<Consumer>(
                table: self._table,
                transport: self._transport,
                handle: self.handle
            )

            yield &stream
        }
    }

    /// Access to all streams on this connection.
    @inlinable
    public var streams: QUICStreams<Consumer> {
        @_lifetime(borrow self)
        get {
            QUICStreams(table: self._table)
        }
    }

    /// The consumer's own state for this stream, in place.
    @inlinable
    public var state: Consumer.StreamState {
        _read {
            yield self._state.pointee!
        }
        nonmutating _modify {
            yield &self._state.pointee!
        }
    }

    /// Runs `body` on this stream and the consumer's state for it.
    ///
    /// - Parameter body: Handed the stream and the consumer's state for it.
    /// - Returns: What `body` returned.
    @inlinable
    public mutating func withStream<Result: ~Copyable, Failure: Error>(
        execute body: (
            _ stream: inout QUICStream<Consumer>,
            _ state: inout Consumer.StreamState
        ) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        var stream = QUICStream<Consumer>(
            table: self._table,
            transport: self._transport,
            handle: self.handle
        )

        return try body(&stream, &self._state.pointee!)
    }

    /// Ends the visit, taking the state out of the slot if the stream is closing.
    ///
    /// You can call this function when a stream is finished with if you need to take ownership
    /// of the stream state. There is no requirement to call this function: the stream state will
    /// be cleaned up automatically after the visit if this isn't called.
    ///
    /// - Returns: The state, or `nil` if ``events`` does not contain
    ///   ``QUICStreamEvents/closed``.
    @inlinable
    public consuming func finish() -> Consumer.StreamState? {
        guard self.events.contains(.closed) else { return nil }

        // The visit is finished but its slot and its handle are still valid. Mark it as not having
        // state so that the stream isn't handed out on another path, for example
        // `QUICStreams.withStream(handle:body:)`.
        self._transport.pointee.hasState = false

        return self._state.pointee.take()
    }
}

@available(anyAppleOS 26, *)
@available(*, unavailable)
extension QUICStreamVisit: Sendable where Consumer: ~Copyable {}
