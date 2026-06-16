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

/// A QUIC packet's header.
@available(macOS 11, iOS 14.0, tvOS 14.0, watchOS 7.0, macCatalyst 14.0, *)
public struct QUICPacketHeader: Hashable, Sendable {

    static func form(_ firstByte: UInt8) -> PacketForm {
        PacketForm(firstByte)
    }

    enum PacketForm {
        case short
        case long

        private static let formBit: UInt8 = 0x80

        // The most significant bit (0x80) of byte 0 (the first byte) is set to 1 for long headers.
        init(_ firstByte: UInt8) {
            if (firstByte & Self.formBit) == 0 {
                self = .short
            } else {
                self = .long
            }
        }
    }

    /// QUIC packet header version.
    public struct Version: Hashable, Sendable {
        private var backing: UInt32

        public static let negotiation = Version(0x0000_0000)
        public static let v1 = Version(0x0000_0001)
        public static let v2 = Version(0x6b33_43cf)

        var headerVersionField: UInt32 {
            self.backing
        }

        init(_ headerVersionField: UInt32) {
            self.backing = headerVersionField
        }
    }

    /// QUIC packet type.
    public struct PacketType: Hashable, Sendable {
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

        private static let typeMask: UInt8 = 0x30

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

        init(firstByteAlreadyMasked: UInt8, version: Version) throws {
            switch version {
            case Version.negotiation:
                self = .versionNegotiation

            case Version.v1:
                switch firstByteAlreadyMasked {
                case 0b00:
                    self = .initial
                case 0b01:
                    self = .zeroRTT
                case 0b10:
                    self = .handshake
                case 0b11:
                    self = .retry
                default:
                    fatalError("Unknown packet type: \(firstByteAlreadyMasked)")
                }

            case Version.v2:
                switch firstByteAlreadyMasked {
                case 0b01:
                    self = .initial
                case 0b10:
                    self = .zeroRTT
                case 0b11:
                    self = .handshake
                case 0b00:
                    self = .retry
                default:
                    fatalError("Unknown packet type: \(firstByteAlreadyMasked)")
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

        init(firstByte: UInt8, version: Version) throws {
            let firstByteAlreadyMasked: UInt8 = (firstByte & Self.typeMask) >> 4
            try self.init(firstByteAlreadyMasked: firstByteAlreadyMasked, version: version)
        }

        /// Initial packet.
        public static let initial = PacketType(.initial)
        /// Retry packet.
        public static let retry = PacketType(.retry)
        /// Handshake packet.
        public static let handshake = PacketType(.handshake)
        /// 0-RTT packet.
        public static let zeroRTT = PacketType(.zeroRTT)
        /// 1-RTT short header packet.
        public static let short = PacketType(.short)
        /// Version negotiation packet.
        public static let versionNegotiation = PacketType(.versionNegotiation)

        public var rawValue: UInt8 {
            self.base.rawValue
        }
    }

    /// The type of the packet.
    public var type: PacketType
    /// The version of the packet.
    public var version: Version?
    /// The destination connection ID of the packet.
    public var destinationConnectionID: QUICConnectionID
    /// The source connection ID of the packet.
    public var sourceConnectionID: QUICConnectionID?
    /// The address verification token of the packet. Only present when the type is `initial`
    /// or `retry` .
    public var token: [UInt8]
    /// Returns if the version of the header is supported by SwiftQUIC.
    var isVersionSupported: Bool {
        version?.headerVersionField == QUICVersion.v1.rawValue
    }

    // TODO: grab this from somewhere?
    // Possibly SwiftTLS.TLSRecordProtector.aesTagLengthBytes
    fileprivate static let AES128GCMTagLength: Int = 16
}

@available(macOS 11, iOS 14.0, tvOS 14.0, watchOS 7.0, macCatalyst 14.0, *)
extension NIOCore.ByteBuffer {

    /// A method to parse a `QUICPacketHeader` from the `ByteBuffer`, mainly to parse the destinationConnectionID only for routing.
    /// Falls back to getQUICPacketHeader if the dcid cannot be parsed
    ///
    /// - Parameters:
    ///   - shortHeaderDCIDLength: The length of the destination connection ID. Required to parse short header packets.
    /// - Returns: The parsed `QUICPacketHeader` or `nil` if the not enough bytes were readable.
    public func parseQUICPacketHeader(
        destinationIDLength shortHeaderDCIDLength: Int
    ) throws -> QUICPacketHeader? {
        let routingHeader: QUICPacketHeader? = try self.withUnsafeReadableBytes { buffer in
            let header = QUICConnectionUtilities.parseInboundPacket(
                buffer,
                shortHeaderDestinationCIDLength: Int(shortHeaderDCIDLength)
            )

            guard let header, let dcid = header.destinationConnectionID else {
                return nil
            }

            let headerTypeBits = header.type ?? 0
            var version: QUICPacketHeader.Version = .v1
            if let rawVersion = header.version {
                version = QUICPacketHeader.Version(rawVersion)
            }
            let packetType: QUICPacketHeader.PacketType = try QUICPacketHeader.PacketType(
                firstByteAlreadyMasked: headerTypeBits,
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

        if let routingHeader {
            return routingHeader
        } else {
            return try self.getQUICPacketHeader(
                destinationIDLength: shortHeaderDCIDLength
            )
        }
    }

    /// A method to parse a `QUICPacketHeader` from the `ByteBuffer`.
    ///
    /// - Parameters:
    ///   - shortHeaderDCIDLength: The length of the destination connection ID. Required to parse short header packets.
    /// - Returns: The parsed `QUICPacketHeader` or `nil` if the not enough bytes were readable.
    public func getQUICPacketHeader(
        destinationIDLength shortHeaderDCIDLength: Int
    ) throws -> QUICPacketHeader? {
        let packetType: QUICPacketHeader.PacketType
        var sourceConnectionID: QUICConnectionID? = nil
        var destinationConnectionID = QUICConnectionID.zero
        var version: QUICPacketHeader.Version?
        var token: [UInt8] = []

        // to avoid advancing the read index
        var localBuffer = self

        // READ FIRST BYTE: Extract the first byte to determine packet type
        guard let firstByteSlice = localBuffer.readBytes(length: 1) else {
            return nil
        }
        let first = firstByteSlice[0]

        // Determine if this is a short or long header packet
        switch QUICPacketHeader.PacketForm(first) {
        // SHORT HEADER PARSING: Simpler packet format
        case .short:
            packetType = .short
            let dcidLength = shortHeaderDCIDLength

            let dcidWrittenBytes = destinationConnectionID.withUnsafeMutableBufferPointer {
                destinationConnectionIDBytes in
                localBuffer.readSlice(length: dcidLength)!.withUnsafeReadableBytes { pointer in
                    pointer.copyBytes(to: destinationConnectionIDBytes)
                }
            }
            precondition(dcidWrittenBytes == dcidLength)
            destinationConnectionID.length = dcidLength

        // LONG HEADER PARSING: More complex packet format with version info
        case .long:
            // READ VERSION: 32-bit version number
            guard let longHeaderVersion = localBuffer.readInteger(as: UInt32.self) else {
                return nil
            }
            let parsedHeaderVersion = QUICPacketHeader.Version(longHeaderVersion)
            version = parsedHeaderVersion

            packetType = try QUICPacketHeader.PacketType(firstByte: first, version: parsedHeaderVersion)

            // DESTINATION CONNECTION ID PARSING
            guard let longHeaderDCIDLength = localBuffer.readInteger(as: UInt8.self) else {
                return nil
            }
            let dcidLength = Int(longHeaderDCIDLength)

            if longHeaderDCIDLength > QUICConnectionID.maxLength {
                throw QUICError.invalidConnectionIDLength(Int(longHeaderDCIDLength))
            }
            let dcidWrittenBytes = destinationConnectionID.withUnsafeMutableBufferPointer {
                destinationConnectionIDBytes in
                localBuffer.readSlice(length: dcidLength)!.withUnsafeReadableBytes { pointer in
                    pointer.copyBytes(to: destinationConnectionIDBytes)
                }
            }
            precondition(dcidWrittenBytes == dcidLength)
            destinationConnectionID.length = dcidLength

            // SOURCE CONNECTION ID PARSING: Same pattern as destination ID
            let headerSCIDLength = localBuffer.readInteger(as: UInt8.self)
            guard let headerSCIDLength else {
                return nil
            }
            let scidLength = Int(headerSCIDLength)

            if headerSCIDLength > QUICConnectionID.maxLength {
                throw QUICError.invalidConnectionIDLength(scidLength)
            }

            var scid = QUICConnectionID.zero

            let scidWrittenBytes = scid.withUnsafeMutableBufferPointer { sourceConnectionIDBytes in
                localBuffer.readSlice(length: scidLength)!.withUnsafeReadableBytes { pointer in
                    pointer.copyBytes(to: sourceConnectionIDBytes)
                }
            }
            precondition(scidWrittenBytes == scidLength)
            scid.length = scidLength

            sourceConnectionID = scid

            var versions: [UInt32] = []  // TODO: not currently used?
            switch packetType {
            case .initial:
                // INITIAL PACKET: Contains a token with variable length encoding
                guard let tokenLength = localBuffer.readEncodedInteger(as: Int.self, strategy: .quic) else {
                    return nil
                }
                token = localBuffer.readBytes(length: tokenLength) ?? []

            case .retry:
                // RETRY PACKET: Contains a token but with integrity tag at end
                let tokenLength = localBuffer.readableBytes - QUICPacketHeader.AES128GCMTagLength
                token = localBuffer.readBytes(length: tokenLength) ?? []

            case .versionNegotiation:
                // VERSION NEGOTIATION: Contains list of supported versions
                while let version = localBuffer.readInteger(as: UInt32.self) {
                    versions.append(version)
                }

            default:
                ()  // do nothing
            }
        }

        return QUICPacketHeader(
            type: packetType,
            version: version,
            destinationConnectionID: destinationConnectionID,
            sourceConnectionID: sourceConnectionID,
            token: token
        )
    }
}
