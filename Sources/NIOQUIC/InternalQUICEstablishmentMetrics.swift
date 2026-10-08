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

/// Timing values captured when the QUIC connection was established.
@available(anyAppleOS 26, *)
struct InternalQUICEstablishmentMetrics: Sendable, Equatable {
    let handshakeDuration: Duration
    /// The current path's smoothed RTT at establishment.
    let handshakeRTT: Duration
}
