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

/// A snapshot of the current QUIC path's RTT and congestion window, and connection packet counters.
///
/// RTT values preserve the underlying estimates, including initial values before
/// measurement. In particular, the unmeasured minimum is `UInt32.max` seconds.
/// A snapshot is a copy and can be used independently of the connection's event loop.
@available(anyAppleOS 26, *)
struct InternalQUICConnectionMetrics: Sendable, Equatable {
    /// The latest ACK-delay-adjusted RTT.
    let currentRTT: Duration
    let minimumRTT: Duration
    let smoothedRTT: Duration
    let rttVariance: Duration
    let congestionWindowInBytes: UInt64
    /// Datagram frames processed by the connection, including frames rejected during parsing.
    /// A datagram containing multiple QUIC packets is counted once.
    let receivedDatagrams: UInt64
    /// QUIC packet send attempts counted before sealing and transport submission.
    let sentPacketAttempts: UInt64
    /// QUIC packets declared lost, including declarations later found to be spurious.
    let lostPackets: UInt64
    /// ECN-capable packets sent by this connection.
    let ecnCapablePacketsSent: UInt64
    /// Sent ECN-capable packets acknowledged by the peer.
    let ecnCapablePacketsAcknowledged: UInt64
    /// The underlying transport's accumulated validated CE feedback count.
    /// This can count a marked packet more than once across acknowledgments.
    let ecnMarkedPackets: UInt64
    /// Sent ECN-capable packets declared lost.
    let ecnCapablePacketsLost: UInt64
}
