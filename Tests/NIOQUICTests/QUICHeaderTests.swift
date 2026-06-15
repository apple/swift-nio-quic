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
        let buffer = ByteBuffer(bytes: packet)

        let header = try buffer.getQUICPacketHeader(destinationIDLength: 16)

        try #require(header?.type == .short)
        try #require(header?.destinationConnectionID == connectionID)
    }

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

        let header = try buffer.getQUICPacketHeader(destinationIDLength: 16)

        try #require(header?.type == .versionNegotiation)
        try #require(header?.destinationConnectionID == connectionID)
        try #require(header?.sourceConnectionID == connectionID)
    }

    @Test(
        "initial",
        arguments: [1, 2]
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

        let header = try buffer.getQUICPacketHeader(destinationIDLength: 16)

        try #require(header?.type == .initial)
        try #require(header?.destinationConnectionID == connectionID)
        try #require(header?.sourceConnectionID == connectionID)
        try #require(header?.token == token)
    }

    @Test(
        "zeroRTT",
        arguments: [1, 2]
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

        let header = try buffer.getQUICPacketHeader(destinationIDLength: 16)

        try #require(header?.type == .zeroRTT)
        try #require(header?.destinationConnectionID == connectionID)
        try #require(header?.sourceConnectionID == connectionID)
    }

    @Test(
        "handshake",
        arguments: [1, 2]
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

        let header = try buffer.getQUICPacketHeader(destinationIDLength: 16)

        try #require(header?.type == .handshake)
        try #require(header?.destinationConnectionID == connectionID)
        try #require(header?.sourceConnectionID == connectionID)
    }

    @Test(
        "retry",
        arguments: [1, 2]
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

        let header = try buffer.getQUICPacketHeader(destinationIDLength: 16)

        try #require(header?.type == .retry)
        try #require(header?.destinationConnectionID == connectionID)
        try #require(header?.sourceConnectionID == connectionID)
        try #require(header?.token == token)
    }

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
        let header = try buffer.parseQUICPacketHeader(destinationIDLength: 8)
        try #require(header?.destinationConnectionID == connectionID)
    }

    // MARK: - Zero-length connection ID tests (RFC 9000 Section 5.1)

    @Test(
        "initial with zero-length SCID",
        arguments: [1, 2]
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
        let buffer = ByteBuffer(bytes: packet)

        let header = try buffer.getQUICPacketHeader(destinationIDLength: 8)

        try #require(header?.type == .initial)
        try #require(header?.destinationConnectionID == dcid)
        let parsedSCID = try #require(header?.sourceConnectionID)
        #expect(parsedSCID.length == 0)
        #expect(parsedSCID == scid)
        try #require(header?.token == token)
    }

    @Test(
        "initial with zero-length DCID and SCID",
        arguments: [1, 2]
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
        let buffer = ByteBuffer(bytes: packet)

        let header = try #require(try buffer.getQUICPacketHeader(destinationIDLength: 0))

        try #require(header.type == .initial)
        let parsedDCID = header.destinationConnectionID
        #expect(parsedDCID.length == 0)
        let parsedSCID = try #require(header.sourceConnectionID)
        #expect(parsedSCID.length == 0)
        #expect(parsedDCID == parsedSCID)
        try #require(header.token == token)
    }

    @Test(
        "short header with zero-length DCID"
    )
    func shortHeaderWithZeroLengthDCID() throws {
        let zeroLengthCID = QUICConnectionID(
            bytes: InlineArray(repeating: 0),
            length: 0
        )
        let packet = QUICPackets.shortHeader(destinationID: zeroLengthCID)
        let buffer = ByteBuffer(bytes: packet)

        let header = try #require(try buffer.getQUICPacketHeader(destinationIDLength: 0))

        #expect(header.type == .short)
        #expect(header.destinationConnectionID.length == 0)
        // Short headers have no SCID
        #expect(header.sourceConnectionID == nil)
    }
}
