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

/// A connection whose streams are serviced by a ``QUICStreamConsumer``.
///
/// Use this to reach the connection's streams out-of-band, rather than in a visit. Inside a visit,
/// use ``QUICStreamVisit/streams`` instead. Open outbound streams with
/// ``QUICStreams/open(_:state:)``.
@available(anyAppleOS 26, *)
public struct QUICStreamConnection<Consumer: QUICStreamConsumer & ~Copyable>: @unchecked Sendable {
    // @unchecked because all methods hop to the right event loop.

    /// The underlying channel.
    private let _channel: QUICConnectionChannel<Consumer>

    /// The event loop this connection is on.
    public let eventLoop: any EventLoop

    /// The connection's channel.
    public var channel: (any Channel) {
        self._channel
    }

    init(channel: QUICConnectionChannel<Consumer>) {
        self.eventLoop = channel.eventLoop
        self._channel = channel
    }

    /// Runs `body` on the connection's streams.
    ///
    /// Streams stay reachable once the connection has closed, but no more can be opened.
    ///
    /// - Parameter body: Handed the connection's streams. They don't outlive the call.
    /// - Returns: What `body` returned, or `nil` if the connection has no stream table.
    public func withStreams<Result: Sendable>(
        _ body: @escaping @Sendable (_ streams: inout QUICStreams<Consumer>) throws -> Result
    ) -> EventLoopFuture<Result?> {
        if self.eventLoop.inEventLoop {
            return self.eventLoop.makeCompletedFuture {
                try self._channel.withStreams(body)
            }
        } else {
            return self.eventLoop.submit {
                try self._channel.withStreams(body)
            }
        }
    }

    /// Returns a view over the connection assuming the caller is on the ``eventLoop``.
    public func assumeIsolated() -> Isolated {
        self.eventLoop.assertInEventLoop()
        return Isolated(self)
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamConnection where Consumer: ~Copyable {
    /// A view over the connection which is isolated to its concurrency domain.
    public struct Isolated {
        let connection: QUICStreamConnection<Consumer>

        fileprivate init(_ connection: QUICStreamConnection<Consumer>) {
            self.connection = connection
        }

        /// The event loop this connection is on.
        public var eventLoop: any EventLoop {
            self.connection.eventLoop
        }

        /// The connection's channel.
        public var channel: (any Channel) {
            self.connection._channel
        }

        /// Runs `body` on this connection's streams, out of band of any drain.
        ///
        /// - Returns: What `body` returned, or `nil` if the connection has no stream table.
        public func withStreams<Result: ~Copyable, Failure: Error>(
            _ body: (inout QUICStreams<Consumer>) throws(Failure) -> Result
        ) throws(Failure) -> Result? {
            try self.connection._channel.withStreams(body)
        }
    }
}

@available(anyAppleOS 26, *)
@available(*, unavailable)
extension QUICStreamConnection.Isolated: Sendable where Consumer: ~Copyable {}
