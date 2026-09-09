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
@available(anyAppleOS 26, *)
public protocol QUICStreamConsumer: SendableMetatype, ~Copyable {
    /// Per-stream state stored by the connection.
    #if swift(>=6.4)
    associatedtype StreamState: ~Copyable
    #else  // 6.3 doesn't support ~Copyable associatedtypes
    associatedtype StreamState
    #endif

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
}
