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

/// A snapshot of one handler's connection establishment and admission metrics.
///
/// Counters describe connection instances, not unique peers. A snapshot is a copy
/// and can be used independently of the handler's event loop.
/// Rejection counters count admission decisions, so repeated packets can count repeatedly.
@available(anyAppleOS 26, *)
struct InternalQUICHandlerMetrics: Sendable, Equatable {
    /// Connection creation attempts started, excluding admission rejections.
    let establishmentAttempts: UInt64
    /// Attempts whose connection channel completed establishment.
    let establishmentSuccesses: UInt64
    /// Attempts that ended before establishment completed, including local cancellation.
    let establishmentFailures: UInt64
    /// Started connections not yet closed, including those still establishing.
    let activeConnections: Int
    /// Started attempts with no terminal establishment outcome.
    let handshakesInProgress: Int
    /// Admission decisions rejected by the active connection limit.
    let rejectedByActiveLimit: UInt64
    /// Admission decisions rejected by the concurrent handshake limit.
    let rejectedByHandshakeLimit: UInt64
    /// Admission decisions rejected by the new connection rate limit.
    let rejectedByRateLimit: UInt64
}
