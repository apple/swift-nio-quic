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
    // Internal on purpose: new modes, such as limits shared across handlers, can then be added
    // without breaking clients.
    enum Mode {
        case unlimited
        case perHandler(activeLimit: Int, handshakeLimit: Int, newConnectionRateLimit: Int)
    }

    let mode: Mode

    /// Admits every new connection.
    public static var unlimited: Self { Self(mode: .unlimited) }

    /// Limits that each ``QUICHandler`` enforces on its own. `0` means no limit.
    ///
    /// - Parameters:
    ///   - activeLimit: Maximum number of active connections, including those still completing
    ///     their handshake. Must not be negative.
    ///   - handshakeLimit: Maximum number of connections that may be mid-handshake at once. Must
    ///     not be negative.
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
