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

@testable import NIOQUIC

@available(anyAppleOS 26, *)
extension QUICConnectionID {
    var asBytes: [UInt8] {
        self.withUnsafeBufferPointer { buffer in
            Array(buffer)
        }
    }
}

@available(anyAppleOS 26, *)
enum QUICPackets {
    static func shortHeader(destinationID: QUICConnectionID, payloadLength: Int = 0) -> [UInt8] {
        [
            // Header Form, Key Phase, etc.
            [0b0110_0000],
            // Destination ID
            destinationID.asBytes,
            // Payload, standing in for the packet number and protected frames
            Array(repeating: 0xAA, count: payloadLength),
        ].reduce([], +)
    }

    /// - Parameters:
    ///   - version: Defaults to the literal Version Negotiation marker `0x00000000`. Pass the
    ///     RFC 9000 §15 pattern `0x?a?a?a?a` (e.g. `[0x1a, 0x2a, 0x3a, 0x4a]`) to build a packet
    ///     that forces a version negotiation exchange instead.
    static func versionNegotiation(
        destinationID: QUICConnectionID?,
        sourceID: QUICConnectionID?,
        version: [UInt8] = [0, 0, 0, 0],
        payloadLength: Int = 0
    ) -> [UInt8] {
        // Note: Differently built to appease the type checker.
        var packet: [UInt8] = [0b1000_0000]
        packet.append(contentsOf: version)
        // Destination Connection ID length
        packet.append(contentsOf: destinationID.flatMap { [UInt8(exactly: $0.length)!] } ?? [0])
        // Destination ID
        if let dcidBytes = destinationID.flatMap({ $0.asBytes }) {
            packet.append(contentsOf: dcidBytes)
        }
        // Source Connection ID length
        packet.append(contentsOf: sourceID.flatMap { [UInt8(exactly: $0.length)!] } ?? [0])
        // Source ID
        if let scidBytes = sourceID.flatMap({ $0.asBytes }) {
            packet.append(contentsOf: scidBytes)
        }
        // Payload, standing in for the packet number and protected frames
        packet.append(contentsOf: Array(repeating: 0xFF, count: payloadLength))
        return packet
    }

    static func initial(
        destinationID: QUICConnectionID,
        sourceID: QUICConnectionID,
        token: [UInt8],
        version: Int
    ) -> [UInt8] {
        switch version {
        case 1:
            return [
                // Header Form
                [0b1100_0000],
                // Version
                [0x00, 0x00, 0x00, 0x01],
                // Destination Connection ID length
                [UInt8(exactly: destinationID.length)!],
                // Destination ID
                destinationID.asBytes,
                // Source Connection ID length
                [UInt8(exactly: sourceID.length)!],
                // Source ID
                sourceID.asBytes,
                // Token length
                [UInt8(exactly: token.count)!],
                // Token
                token,
            ].reduce([], +)
        case 2:
            return [
                // Header Form
                [0b1101_0000],
                // Version
                [0x6b, 0x33, 0x43, 0xcf],
                // Destination Connection ID length
                [UInt8(exactly: destinationID.length)!],
                // Destination ID
                destinationID.asBytes,
                // Source Connection ID length
                [UInt8(exactly: sourceID.length)!],
                // Source ID
                sourceID.asBytes,
                // Token length
                [UInt8(exactly: token.count)!],
                // Token
                token,
            ].reduce([], +)
        default:
            fatalError("Unknown version: \(version)")
        }
    }

    static func zeroRTT(
        destinationID: QUICConnectionID,
        sourceID: QUICConnectionID,
        version: Int
    ) -> [UInt8] {
        switch version {
        case 1:
            return [
                // Header Form
                [0b1101_0000],
                // Version
                [0x00, 0x00, 0x00, 0x01],
                // Destination Connection ID length
                [UInt8(exactly: destinationID.length)!],
                // Destination ID
                destinationID.asBytes,
                // Source Connection ID length
                [UInt8(exactly: sourceID.length)!],
                // Source ID
                sourceID.asBytes,
            ].reduce([], +)
        case 2:
            return [
                // Header Form
                [0b1110_0000],
                // Version
                [0x6b, 0x33, 0x43, 0xcf],
                // Destination Connection ID length
                [UInt8(exactly: destinationID.length)!],
                // Destination ID
                destinationID.asBytes,
                // Source Connection ID length
                [UInt8(exactly: sourceID.length)!],
                // Source ID
                sourceID.asBytes,
            ].reduce([], +)
        default:
            fatalError("Unknown version: \(version)")
        }
    }

    static func handshake(
        destinationID: QUICConnectionID,
        sourceID: QUICConnectionID,
        version: Int
    ) -> [UInt8] {
        switch version {
        case 1:
            return [
                // Header Form
                [0b1110_0000],
                // Version
                [0x00, 0x00, 0x00, 0x01],
                // Destination Connection ID length
                [UInt8(exactly: destinationID.length)!],
                // Destination ID
                destinationID.asBytes,
                // Source Connection ID length
                [UInt8(exactly: sourceID.length)!],
                // Source ID
                sourceID.asBytes,
            ].reduce([], +)
        case 2:
            return [
                // Header Form
                [0b1111_0000],
                // Version
                [0x6b, 0x33, 0x43, 0xcf],
                // Destination Connection ID length
                [UInt8(exactly: destinationID.length)!],
                // Destination ID
                destinationID.asBytes,
                // Source Connection ID length
                [UInt8(exactly: sourceID.length)!],
                // Source ID
                sourceID.asBytes,
            ].reduce([], +)
        default:
            fatalError("Unknown version: \(version)")
        }
    }

    static func retry(
        destinationID: QUICConnectionID,
        sourceID: QUICConnectionID,
        token: [UInt8],
        version: Int
    ) -> [UInt8] {
        switch version {
        case 1:
            return [
                // Header Form
                [0b1111_0000],
                // Version
                [0x00, 0x00, 0x00, 0x01],
                // Destination Connection ID length
                [UInt8(exactly: destinationID.length)!],
                // Destination ID
                destinationID.asBytes,
                // Source Connection ID length
                [UInt8(exactly: sourceID.length)!],
                // Source ID
                sourceID.asBytes,
                // Token
                token,
                // Integrity token
                [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            ].reduce([], +)
        case 2:
            return [
                // Header Form
                [0b1100_0000],
                // Version
                [0x6b, 0x33, 0x43, 0xcf],
                // Destination Connection ID length
                [UInt8(exactly: destinationID.length)!],
                // Destination ID
                destinationID.asBytes,
                // Source Connection ID length
                [UInt8(exactly: sourceID.length)!],
                // Source ID
                sourceID.asBytes,
                // Token
                token,
                // Integrity token
                [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            ].reduce([], +)
        default:
            fatalError("Unknown version: \(version)")
        }
    }
}
