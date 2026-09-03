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
@_spi(ProtocolProvider) import SwiftNetwork

@available(anyAppleOS 26, *)
extension UInt8 {
    /// RFC 9000 § 17.2: The most significant bit (0x80) of byte 0 (the first byte) is set to 1 for long headers.
    fileprivate var indicatesLongHeader: Bool {
        (self & 0x80) != 0
    }

    /// Only keep the bits that identify long header packet types and shift them the right.
    fileprivate var maskedLongPacketType: UInt8 {
        (self & QUICPacketHeader.PacketType.typeMask) >> 4
    }
}

/// A QUIC packet's header.
@available(anyAppleOS 26, *)
struct QUICPacketHeader: Hashable, Sendable {

    /// QUIC packet header version.
    struct Version: Hashable, Sendable {
        private var backing: UInt32

        static let negotiation = Version(0x0000_0000)
        static let v1 = Version(0x0000_0001)
        static let v2 = Version(0x6b33_43cf)

        var headerVersionField: UInt32 {
            self.backing
        }

        init(_ headerVersionField: UInt32) {
            self.backing = headerVersionField
        }
    }

    /// QUIC packet type.
    struct PacketType: Hashable, Sendable {
        enum Base: UInt8, Hashable {
            /// Initial packet.
            case initial = 1
            /// Retry packet.
            case retry
            /// Handshake packet.
            case handshake
            /// 0-RTT packet.
            case zeroRTT
            /// 1-RTT short header packet.
            case short
            /// Version negotiation packet.
            case versionNegotiation
        }

        fileprivate static let typeMask: UInt8 = 0x30

        let base: Base

        fileprivate init(_ base: Base) {
            self.base = base
        }

        init?(rawValue: UInt8) {
            guard let base = Base(rawValue: rawValue) else {
                return nil
            }
            self.init(base)
        }

        /// Parse the packet type from the byte of the packet.
        init(firstByte: UInt8, version: Version) {
            // Check if the byte indicates a short header.
            guard firstByte.indicatesLongHeader else {
                self = .short
                return
            }

            // Must be VN or a long header.
            let maskedByte = firstByte.maskedLongPacketType

            switch version {
            case Version.negotiation:
                self = .versionNegotiation

            case Version.v1:
                switch maskedByte {
                case 0b00:
                    self = .initial
                case 0b01:
                    self = .zeroRTT
                case 0b10:
                    self = .handshake
                case 0b11:
                    self = .retry
                default:
                    fatalError("Unknown packet type: \(maskedByte)")
                }

            case Version.v2:
                switch maskedByte {
                case 0b01:
                    self = .initial
                case 0b10:
                    self = .zeroRTT
                case 0b11:
                    self = .handshake
                case 0b00:
                    self = .retry
                default:
                    fatalError("Unknown packet type: \(maskedByte)")
                }

            default:
                // We pass other cases as a version negotiation request to SwiftQUIC and let it decide
                // how to handle them. This includes:
                // * Forced version negotiation (RFC 9000, Section 15): "Versions that follow the pattern
                //   0x?a?a?a?a are reserved for use in forcing version negotiation to be exercised -- that
                //   is, any version number where the low four bits of all bytes is 1010 (in binary)."
                // * Unrecognized version numbers (RFC 9000, Section 5.2.2): "If a server receives a packet
                //   that indicates an unsupported version and if the packet is large enough to initiate a
                //   new connection for any supported version, the server Version Negotiation packet as
                //   described in Section 6.1."
                self = .versionNegotiation
            }

        }

        /// Initial packet.
        static let initial = PacketType(.initial)
        /// Retry packet.
        static let retry = PacketType(.retry)
        /// Handshake packet.
        static let handshake = PacketType(.handshake)
        /// 0-RTT packet.
        static let zeroRTT = PacketType(.zeroRTT)
        /// 1-RTT short header packet.
        static let short = PacketType(.short)
        /// Version negotiation packet.
        static let versionNegotiation = PacketType(.versionNegotiation)

        var rawValue: UInt8 {
            self.base.rawValue
        }
    }

    /// The type of the packet.
    var type: PacketType
    /// The version of the packet.
    var version: Version?
    /// The destination connection ID of the packet.
    var destinationConnectionID: QUICConnectionID
    /// The source connection ID of the packet.
    var sourceConnectionID: QUICConnectionID?
    /// The address verification token of the packet. Only present when the type is `initial`
    /// or `retry` .
    var token: [UInt8]
}

@available(anyAppleOS 26, *)
extension NIOCore.ByteBuffer {

    /// A method to parse a `QUICPacketHeader` from the `ByteBuffer`, mainly to parse the destinationConnectionID only for routing.
    ///
    /// - Parameters:
    ///   - shortHeaderDCIDLength: The length of the destination connection ID. Required to parse short header packets.
    /// - Returns: The parsed `QUICPacketHeader` or `nil`.
    func parseQUICPacketHeader(
        destinationIDLength shortHeaderDCIDLength: Int
    ) -> QUICPacketHeader? {
        self.withUnsafeReadableBytes { buffer in
            // Needed to decide if this is a long or short header packet.
            guard let firstByte = buffer.first else {
                return nil
            }

            let header = QUICConnectionUtilities.parseInboundPacket(
                buffer,
                shortHeaderDestinationCIDLength: Int(shortHeaderDCIDLength)
            )

            guard let header, let dcid = header.destinationConnectionID else {
                return nil
            }

            // NOTE: This will swollow unknown versions. Since the header is only parsed
            // for connection routing (accept new, forward to existing, stateless reset),
            // the accuracy here is not important.
            var version: QUICPacketHeader.Version = .v1
            if let rawVersion = header.version {
                version = QUICPacketHeader.Version(rawVersion)
            }

            let packetType: QUICPacketHeader.PacketType = QUICPacketHeader.PacketType(
                firstByte: firstByte,
                version: version
            )

            var scid: QUICConnectionID? = nil
            if let parsedSCID = header.sourceConnectionID {
                scid = QUICConnectionID(parsedSCID)
            }

            return QUICPacketHeader(
                type: packetType,
                version: version,
                destinationConnectionID: QUICConnectionID(dcid),
                sourceConnectionID: scid,
                token: header.token
            )
        }
    }
}
