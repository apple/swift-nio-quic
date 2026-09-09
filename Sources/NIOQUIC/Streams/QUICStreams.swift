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

/// Access to the streams of a connection.
@available(anyAppleOS 26, *)
public struct QUICStreams<Consumer: QUICStreamConsumer & ~Copyable>: ~Copyable, ~Escapable {
    @usableFromInline
    let _table: QUICStreamTable<Consumer>

    @inlinable
    @_lifetime(immortal)
    init(table: QUICStreamTable<Consumer>) {
        self._table = table
    }

    /// Opens a stream.
    ///
    /// - Parameters:
    ///   - type: The type of stream to open.
    ///   - state: The consumer's state for the new stream.
    /// - Returns: The handle for the new stream. Its ID is assigned by the stack, which reports it
    ///   as a ``QUICStreamReadyEvents/opened`` visit.
    /// - Throws: If the stream could not be opened, in which case `state` is destroyed.
    public mutating func open(
        _ type: QUICStreamType,
        state: consuming Consumer.StreamState
    ) throws -> QUICStreamHandle {
        try self._table.open(type, state: consume state)
    }

    /// Runs `body` on a stream and its state.
    ///
    /// - Parameters:
    ///   - handle: The stream to run on.
    ///   - body: Handed the stream and the consumer's state for it.
    /// - Returns: What `body` returned, or `nil` if `handle` doesn't address a ready stream.
    @inlinable
    public mutating func withStream<Result: ~Copyable, Failure: Error>(
        handle: QUICStreamHandle,
        execute body: (
            _ stream: inout QUICStream<Consumer>,
            _ state: inout Consumer.StreamState
        ) throws(Failure) -> Result
    ) throws(Failure) -> Result? {
        try self._table.withStream(handle: handle, execute: body)
    }

    /// The handle for a stream ID.
    ///
    /// - Parameter id: The stream ID to look up.
    /// - Returns: The handle for the stream with that ID, or `nil` if there's no valid handle
    ///   for a stream with that ID (for example, the stream is closed).
    @inlinable
    public func handle(forID id: QUICStreamID) -> QUICStreamHandle? {
        self._table.handle(forID: id)
    }
}

@available(anyAppleOS 26, *)
@available(*, unavailable)
extension QUICStreams: Sendable where Consumer: ~Copyable {}
