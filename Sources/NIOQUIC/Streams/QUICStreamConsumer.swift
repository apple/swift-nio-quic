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

/// Consumes streams for a single QUIC connection.
@available(anyAppleOS 27, *)
public protocol QUICStreamConsumer: SendableMetatype, ~Copyable {
    /// Per-stream state stored by the connection.
    associatedtype StreamState: ~Copyable

    /// Create a ``StreamState`` for the given stream.
    ///
    /// This is called once per inbound stream, immediately before it's first visited.
    ///
    /// - Parameter stream: The stream the state is for.
    /// - Returns: The state to store.
    mutating func makeStreamState(_ stream: inout QUICStream<Self>) -> StreamState

    /// Process every stream with unhandled events.
    ///
    /// Use this function to handle streams which have changed state since the last call. If you
    /// don't process a stream (i.e. do not pull it from the iterator) then it will be included
    /// in the next visit.
    ///
    /// - Parameter streams: The streams to visit.
    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>)

    /// Visits the streams with unhandled events: makes the state of each new stream, then calls
    /// ``processStreams(_:)``.
    ///
    /// Don't implement this: the connection calls the default implementation. As a requirement, its
    /// witness is emitted with each conforming type, so the visit runs specialized for that type
    /// instead of as NIOQUIC's generic code, which looks up the metadata of the stream state's storage
    /// at run time.
    mutating func _visitStreams(_ visit: borrowing _QUICStreamsVisit<Self>)
}

@available(anyAppleOS 27, *)
extension QUICStreamConsumer where Self: ~Copyable {
    @inlinable
    public mutating func _visitStreams(_ visit: borrowing _QUICStreamsVisit<Self>) {
        visit._table.drain(into: &self)
    }
}

/// The streams a connection hands to its consumer's ``QUICStreamConsumer/_visitStreams(_:)``.
@available(anyAppleOS 27, *)
public struct _QUICStreamsVisit<Consumer: QUICStreamConsumer & ~Copyable>: ~Copyable {
    @usableFromInline
    let _table: QUICStreamTable<Consumer>

    init(table: QUICStreamTable<Consumer>) {
        self._table = table
    }
}

@available(anyAppleOS 27, *)
@available(*, unavailable)
extension _QUICStreamsVisit: Sendable where Consumer: ~Copyable {}
