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

/// Tracks the number of open connections and imposes limits.
///
/// There are three configuration parameters:
/// * `activeLimit` limits all connections. While at this limit all new connections will be directly rejected.
/// * `handshakeLimit` limits the connections in the process of handshaking. Even if this limit is not met,
///     the `activeLimit` will also reject connections.
/// * `newConnectionRateLimit` imposes a rate limit accepting handshake in addition to
///     the `handshakeLimit`.
///
/// Callers must call `finishedHandshake()` and `closingConnection()` for each
/// connection `acceptNewConnection()` admits.
///
/// Confined to the event loop of the owning `QUICHandler`; not `Sendable`.
struct ConnectionAdmissionController: ~Copyable {

    /// Inform why a connection was dropped.
    enum DropReason: Equatable {
        case activeLimitReached
        case handshakeLimitReached
        case rateLimited
    }

    /// Result type for `acceptNewConnection()`.
    enum Decision: Equatable {
        case accept
        case drop(_ reason: DropReason)
    }

    /// Maximum active connections allowed, or `nil` for unbounded.
    private let activeLimit: Int?
    /// Maximum connections allowed to be mid-handshake at once, or `nil` for unbounded.
    private let handshakeLimit: Int?
    /// Throttles how fast new connection attempts are admitted. `nil` when unbounded.
    private var rateLimiter: TokenBucket?
    /// Supplies the current time for the rate limit.
    private let eventLoop: any EventLoop

    /// Number of connections admitted and not yet closed.
    private var activeCount: Int
    /// Number of admitted connections still mid-handshake. Always `<= activeCount`.
    private var handshakeCount: Int

    /// - Parameters:
    ///   - activeLimit: Maximum active connections allowed, or `nil` for unbounded.
    ///   - handshakeLimit: Maximum connections allowed to be mid-handshake at once, or `nil` for
    ///     unbounded.
    ///   - newConnectionRateLimit: Maximum new-connection attempts per second, or `nil` for
    ///     unbounded.
    ///   - eventLoop: Supplies the current time for the rate limit, so `EmbeddedEventLoop`-based
    ///     tests can control it with `advanceTime(by:)`.
    init(
        activeLimit: Int?,
        handshakeLimit: Int?,
        newConnectionRateLimit: Int?,
        eventLoop: any EventLoop
    ) {
        self.activeLimit = activeLimit
        self.handshakeLimit = handshakeLimit
        self.eventLoop = eventLoop
        if let rateLimit = newConnectionRateLimit {
            let refillInterval: TimeAmount
            if rateLimit == 0 {
                // Avoid division by 0.
                refillInterval = .nanoseconds(.max)
            } else {
                // Round up so tokens never refill faster than the configured rate. A rate of 0 never refills.
                refillInterval = .nanoseconds((1_000_000_000 + Int64(rateLimit) - 1) / Int64(rateLimit))
            }
            self.rateLimiter = TokenBucket(
                capacity: rateLimit,
                refillInterval: refillInterval,
                now: eventLoop.now
            )
        } else {
            self.rateLimiter = nil
        }
        self.activeCount = 0
        self.handshakeCount = 0
    }

    /// Decides whether a new connection may be admitted, and if it may, immediately begins
    /// tracking it as active and mid-handshake.
    ///
    /// Checks the active-connection limit, then the handshake limit, then the rate limit, in that
    /// priority order.
    ///
    /// - Returns `Decision.accept` if a connection can be accepted (this will consume the respective slots)
    ///     or `Decision.drop` when a connection limit was reached.
    mutating func acceptNewConnection() -> Decision {
        if let activeLimit = self.activeLimit, self.activeCount >= activeLimit {
            return .drop(.activeLimitReached)
        }
        if let handshakeLimit = self.handshakeLimit, self.handshakeCount >= handshakeLimit {
            return .drop(.handshakeLimitReached)
        }
        if let withinRateLimit = self.rateLimiter?.tryConsume(now: self.eventLoop.now), !withinRateLimit {
            return .drop(.rateLimited)
        }
        self.activeCount += 1
        self.handshakeCount += 1
        return .accept
    }

    /// Call when the connection's handshake completes or the connection closes.
    mutating func finishedHandshake() {
        assert(self.handshakeCount > 0)
        self.handshakeCount -= 1
    }

    /// Call when the connection closes.
    mutating func closingConnection() {
        assert(self.activeCount > 0)
        self.activeCount -= 1
    }
}

@available(*, unavailable)
extension ConnectionAdmissionController: Sendable {}
