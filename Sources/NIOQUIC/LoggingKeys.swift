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

/// Keys used for logging to ensure consistent naming.
enum LoggingKeys {
    static let channelWritability = "channel.writability"
    static let channelOutboundBytes = "channel.outboundBytes"
    static let addressLocal = "quic.localAddress"
    static let addressRemote = "quic.remoteAddress"
    static let connectionSCID = "quic.connection.scid"
    static let connectionOriginalSCID = "quic.connection.originalSCID"
    static let connectionDCID = "quic.connection.dcid"
    static let packetType = "quic.packet.type"
    static let packetVersion = "quic.packet.version"
}
