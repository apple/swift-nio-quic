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
import Testing

@testable import NIOQUIC

final class HeaderIDTests {
    @available(anyAppleOS 26, *)
    @Test(
        "short header"
    )
    func shortHeader() throws {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 0, 0, 0, 0,
            ],
            length: 16
        )
        let packet = QUICPackets.shortHeader(destinationID: connectionID)
        var buffer = ByteBuffer(bytes: packet)
        buffer.addPadding()

        let header = buffer.parseQUICPacketHeader(destinationIDLength: 16)

        try #require(header?.type == .short)
        try #require(header?.destinationConnectionID == connectionID)
    }

    @available(anyAppleOS 26, *)
    @Test(
        "version negotiation"
    )
    func versionNegotiation() throws {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
            ],
            length: 20
        )
        let packet = QUICPackets.versionNegotiation(destinationID: connectionID, sourceID: connectionID)
        let buffer = ByteBuffer(bytes: packet)

        let header = buffer.parseQUICPacketHeader(destinationIDLength: 16)

        try #require(header?.type == .versionNegotiation)
        try #require(header?.destinationConnectionID == connectionID)
        try #require(header?.sourceConnectionID == connectionID)
    }

    @available(anyAppleOS 26, *)
    @Test(
        "initial",
        arguments: [1]  // SwiftNetwork only supports QUIC version 1
    )
    func initial(version: Int) throws {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
            ],
            length: 20
        )
        let token: [UInt8] = [1, 2, 3, 4]
        let packet = QUICPackets.initial(
            destinationID: connectionID,
            sourceID: connectionID,
            token: token,
            version: version
        )
        let buffer = ByteBuffer(bytes: packet)

        let header = buffer.parseQUICPacketHeader(destinationIDLength: 16)

        try #require(header?.type == .initial)
        try #require(header?.destinationConnectionID == connectionID)
        try #require(header?.sourceConnectionID == connectionID)
        // Tokens will only be read from retry packets
        try #require(header?.token == [])
    }

    @available(anyAppleOS 26, *)
    @Test(
        "zeroRTT",
        arguments: [1]  // SwiftNetwork only supports QUIC version 1
    )
    func zeroRTT(version: Int) throws {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
            ],
            length: 20
        )
        let packet = QUICPackets.zeroRTT(destinationID: connectionID, sourceID: connectionID, version: version)
        let buffer = ByteBuffer(bytes: packet)

        let header = buffer.parseQUICPacketHeader(destinationIDLength: 16)

        try #require(header?.type == .zeroRTT)
        try #require(header?.destinationConnectionID == connectionID)
        try #require(header?.sourceConnectionID == connectionID)
    }

    @available(anyAppleOS 26, *)
    @Test(
        "handshake",
        arguments: [1]  // SwiftNetwork only supports QUIC version 1
    )
    func handshake(version: Int) throws {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
            ],
            length: 20
        )
        let packet = QUICPackets.handshake(destinationID: connectionID, sourceID: connectionID, version: version)
        let buffer = ByteBuffer(bytes: packet)

        let header = buffer.parseQUICPacketHeader(destinationIDLength: 16)

        try #require(header?.type == .handshake)
        try #require(header?.destinationConnectionID == connectionID)
        try #require(header?.sourceConnectionID == connectionID)
    }

    @available(anyAppleOS 26, *)
    @Test(
        "retry",
        arguments: [1]  // SwiftNetwork only supports QUIC version 1
    )
    func retry(version: Int) throws {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
            ],
            length: 20
        )
        let token: [UInt8] = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
        let packet = QUICPackets.retry(
            destinationID: connectionID,
            sourceID: connectionID,
            token: token,
            version: version
        )
        let buffer = ByteBuffer(bytes: packet)

        let header = buffer.parseQUICPacketHeader(destinationIDLength: 16)

        try #require(header?.type == .retry)
        try #require(header?.destinationConnectionID == connectionID)
        try #require(header?.sourceConnectionID == connectionID)
        try #require(header?.token == token)
    }

    @available(anyAppleOS 26, *)
    @Test(
        "dcid"
    )
    func dcid() throws {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 0, 0,
                0, 0, 0, 0, 0,
                0, 0, 0, 0, 0,
            ],
            length: 8
        )

        let token: [UInt8] = []
        let packet = QUICPackets.initial(
            destinationID: connectionID,
            sourceID: connectionID,
            token: token,
            version: 1
        )
        let buffer = ByteBuffer(bytes: packet)
        let header = buffer.parseQUICPacketHeader(destinationIDLength: 8)
        try #require(header?.destinationConnectionID == connectionID)
    }

    // MARK: - Zero-length connection ID tests (RFC 9000 Section 5.1)

    @available(anyAppleOS 26, *)
    @Test(
        "initial with zero-length SCID",
        arguments: [1]  // SwiftNetwork only supports QUIC version 1
    )
    func initialWithZeroLengthSCID(version: Int) throws {
        let dcid = QUICConnectionID(
            bytes: [
                1, 2, 3, 4, 5, 6, 7, 8, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
            length: 8
        )
        let scid = QUICConnectionID(
            bytes: InlineArray(repeating: 0),
            length: 0
        )
        let token: [UInt8] = [1, 2, 3, 4]
        let packet = QUICPackets.initial(
            destinationID: dcid,
            sourceID: scid,
            token: token,
            version: version
        )
        var buffer = ByteBuffer(bytes: packet)
        buffer.addPadding()

        let header = buffer.parseQUICPacketHeader(destinationIDLength: 8)

        try #require(header?.type == .initial)
        try #require(header?.destinationConnectionID == dcid)
        let parsedSCID = try #require(header?.sourceConnectionID)
        #expect(parsedSCID.length == 0)
        #expect(parsedSCID == scid)
        // Tokens will only be read from retry packets
        try #require(header?.token == [])
    }

    @available(anyAppleOS 26, *)
    @Test(
        "initial with zero-length DCID and SCID",
        arguments: [1]  // SwiftNetwork only supports QUIC version 1
    )
    func initialWithZeroLengthDCIDAndSCID(version: Int) throws {
        let zeroLengthCID = QUICConnectionID(
            bytes: InlineArray(repeating: 0),
            length: 0
        )
        let token: [UInt8] = []
        let packet = QUICPackets.initial(
            destinationID: zeroLengthCID,
            sourceID: zeroLengthCID,
            token: token,
            version: version
        )
        var buffer = ByteBuffer(bytes: packet)
        buffer.addPadding()

        let header = try #require(buffer.parseQUICPacketHeader(destinationIDLength: 0))

        try #require(header.type == .initial)
        let parsedDCID = header.destinationConnectionID
        #expect(parsedDCID.length == 0)
        let parsedSCID = try #require(header.sourceConnectionID)
        #expect(parsedSCID.length == 0)
        #expect(parsedDCID == parsedSCID)
    }

    @available(anyAppleOS 26, *)
    @Test(
        "short header with zero-length DCID"
    )
    func shortHeaderWithZeroLengthDCID() throws {
        let zeroLengthCID = QUICConnectionID(
            bytes: InlineArray(repeating: 0),
            length: 0
        )
        let packet = QUICPackets.shortHeader(destinationID: zeroLengthCID)
        var buffer = ByteBuffer(bytes: packet)
        buffer.addPadding()

        let header = try #require(buffer.parseQUICPacketHeader(destinationIDLength: 0))

        #expect(header.type == .short)
        #expect(header.destinationConnectionID.length == 0)
        // Short headers have no SCID
        #expect(header.sourceConnectionID == nil)
    }

    // MARK: - Undersized datagrams

    @available(anyAppleOS 26, *)
    @Test(
        "short header too small to contain the destination connection ID"
    )
    func shortHeaderTooSmallForDCID() {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 0, 0, 0, 0,
            ],
            length: 16
        )
        let packet = Array(QUICPackets.shortHeader(destinationID: connectionID).prefix(4))
        let buffer = ByteBuffer(bytes: packet)

        let header = buffer.parseQUICPacketHeader(destinationIDLength: 16)

        #expect(header == nil)
    }

    @available(anyAppleOS 26, *)
    @Test(
        "long header too small to contain the destination connection ID"
    )
    func longHeaderTooSmallForDCID() throws {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
            ],
            length: 20
        )
        let packet = Array(
            QUICPackets.initial(destinationID: connectionID, sourceID: connectionID, token: [], version: 1)
                .prefix(6)
        )
        let buffer = ByteBuffer(bytes: packet)

        let header = buffer.parseQUICPacketHeader(destinationIDLength: 16)

        #expect(header == nil)
    }

    @available(anyAppleOS 26, *)
    @Test(
        "long header too small to contain the source connection ID"
    )
    func longHeaderTooSmallForSCID() {
        let zeroLengthCID = QUICConnectionID(
            bytes: InlineArray(repeating: 0),
            length: 0
        )
        let sourceID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
                1, 1, 1, 1, 1,
            ],
            length: 20
        )
        let packet = Array(
            QUICPackets.initial(destinationID: zeroLengthCID, sourceID: sourceID, token: [], version: 1)
                .prefix(7)
        )
        let buffer = ByteBuffer(bytes: packet)

        let header = buffer.parseQUICPacketHeader(destinationIDLength: 16)

        #expect(header == nil)
    }
}

extension ByteBuffer {
    /// Add padding to the byte buffer to ensure it has at least a certain length.
    /// In the tests this is used to make sure our packets meet the minimum QUIC
    /// requirements of 21 bytes (the default argument).
    mutating func addPadding(ensuringMinimumLengthOf minimumLength: Int = 21, paddingByte: UInt8 = 0xAA) {
        let currentLength = self.readableBytes
        if currentLength < minimumLength {
            self.writeRepeatingByte(paddingByte, count: minimumLength - currentLength)
        }
    }
}
