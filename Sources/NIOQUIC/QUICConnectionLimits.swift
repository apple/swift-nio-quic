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

/// Limits on the inbound connections a server ``QUICHandler`` admits. Clients ignore them.
@available(anyAppleOS 26, *)
public struct QUICConnectionLimits: Sendable {

    enum Mode {
        // No limits will be enforced.
        case unlimited
        // Connection limits are tracked separately by each `QUICHandler`.
        case perHandler(activeLimit: Int, handshakeLimit: Int, newConnectionRateLimit: Int)
    }

    let mode: Mode

    /// Admits every new connection.
    public static var unlimited: Self { Self(mode: .unlimited) }

    /// Limits that each ``QUICHandler`` enforces on its own. `0` means no limit.
    ///
    /// Note: Limits must not be negative.
    ///
    /// - Parameters:
    ///   - activeLimit: Maximum number of connections the server may process at once, including those
    ///   still completing their handshake. New connection attempts beyond this limit are dropped.
    ///   - handshakeLimit: Maximum number of connections that may be mid-handshake at once.
    ///   - newConnectionRateLimit: Maximum number of new connections accepted per second. Must be
    ///     between `0` and `1_000_000_000`.
    public static func perHandler(
        activeLimit: Int = 0,
        handshakeLimit: Int = 0,
        newConnectionRateLimit: Int = 0
    ) -> Self {
        precondition(activeLimit >= 0, "activeLimit must not be negative")
        precondition(handshakeLimit >= 0, "handshakeLimit must not be negative")
        precondition(
            newConnectionRateLimit >= 0 && newConnectionRateLimit <= 1_000_000_000,
            "newConnectionRateLimit must be between 0 and 1,000,000,000"
        )
        return Self(
            mode: .perHandler(
                activeLimit: activeLimit,
                handshakeLimit: handshakeLimit,
                newConnectionRateLimit: newConnectionRateLimit
            )
        )
    }
}
