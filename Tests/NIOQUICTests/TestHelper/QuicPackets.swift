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
    static func shortHeader(destinationID: QUICConnectionID) -> [UInt8] {
        [
            // Header Form, Key Phase, etc.
            [0b0011_0000],
            // Destination ID
            destinationID.asBytes,
        ].reduce([], +)
    }

    static func versionNegotiation(
        destinationID: QUICConnectionID?,
        sourceID: QUICConnectionID?
    ) -> [UInt8] {
        [
            // Header Form
            [0b1000_0000],
            // Version
            [0, 0, 0, 0],
            // Destination Connection ID length
            destinationID.flatMap { [UInt8(exactly: $0.length)!] } ?? [0],
            // Destination ID
            destinationID.flatMap { $0.asBytes },
            // Source Connection ID length
            sourceID.flatMap { [UInt8(exactly: $0.length)!] } ?? [0],
            // Source ID
            sourceID.flatMap { $0.asBytes },
        ].compactMap { $0 }.reduce([], +)
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
