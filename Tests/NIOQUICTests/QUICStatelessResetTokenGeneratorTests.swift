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
import XCTest

@testable import NIOQUIC

@available(anyAppleOS 26, *)
final class QUICStatelessResetTokenGeneratorTests: XCTestCase {
    private static let key = [UInt8](repeating: 0xAB, count: 32)
    private static let otherKey = [UInt8](repeating: 0xCD, count: 32)

    private func makeConnectionID(_ byte: UInt8) -> QUICConnectionID {
        QUICConnectionID(bytes: InlineArray<20, UInt8>(repeating: byte), length: 8)
    }

    func testTokenIsSixteenBytes() {
        let generator = QUICStatelessResetTokenGenerator(key: Self.key)

        XCTAssertEqual(generator.tokenBytes(for: self.makeConnectionID(1)).count, 16)
    }

    func testTokenIsStableForTheSameKeyAndConnectionID() {
        let connectionID = self.makeConnectionID(1)
        let generator = QUICStatelessResetTokenGenerator(key: Self.key)
        let other = QUICStatelessResetTokenGenerator(key: Self.key)

        // The whole point of deriving tokens: a second generator with the same key produces the
        // same token, so a reset stays valid after the connection state is gone.
        XCTAssertEqual(generator.tokenBytes(for: connectionID), other.tokenBytes(for: connectionID))
    }

    func testTokenDiffersPerConnectionID() {
        let generator = QUICStatelessResetTokenGenerator(key: Self.key)

        XCTAssertNotEqual(
            generator.tokenBytes(for: self.makeConnectionID(1)),
            generator.tokenBytes(for: self.makeConnectionID(2))
        )
    }

    func testTokenDiffersPerKey() {
        let connectionID = self.makeConnectionID(1)

        XCTAssertNotEqual(
            QUICStatelessResetTokenGenerator(key: Self.key).tokenBytes(for: connectionID),
            QUICStatelessResetTokenGenerator(key: Self.otherKey).tokenBytes(for: connectionID)
        )
    }

    func testKeyIsGeneratedWhenNoneIsConfigured() {
        let connectionID = self.makeConnectionID(1)

        XCTAssertNotEqual(
            QUICStatelessResetTokenGenerator(key: nil).tokenBytes(for: connectionID),
            QUICStatelessResetTokenGenerator(key: nil).tokenBytes(for: connectionID)
        )
    }

    func testResetPacketEndsWithTheTokenAndLooksLikeAShortHeader() throws {
        let connectionID = self.makeConnectionID(1)
        let generator = QUICStatelessResetTokenGenerator(key: Self.key)

        let packet = try XCTUnwrap(
            generator.statelessResetPacket(
                for: connectionID,
                triggeringPacketLength: 1200,
                allocator: ByteBufferAllocator()
            )
        )

        XCTAssertEqual(
            Array(packet.readableBytesView.suffix(16)),
            generator.tokenBytes(for: connectionID)
        )
        // Form bit clear, fixed bit set: indistinguishable from a 1-RTT packet (RFC 9000 § 10.3).
        let firstByte = try XCTUnwrap(packet.getInteger(at: packet.readerIndex, as: UInt8.self))
        XCTAssertEqual(firstByte & 0b1100_0000, 0b0100_0000)
        XCTAssertGreaterThanOrEqual(packet.readableBytes, 21)
    }

    func testResetPacketIsSmallerThanThePacketThatTriggeredIt() throws {
        let generator = QUICStatelessResetTokenGenerator(key: Self.key)

        // RFC 9000 § 10.3.3: every reset must be smaller than its trigger, so an exchange of
        // resets between two stateless endpoints dies out instead of looping.
        for triggeringPacketLength in [22, 30, 43, 1200] {
            let packet = try XCTUnwrap(
                generator.statelessResetPacket(
                    for: self.makeConnectionID(1),
                    triggeringPacketLength: triggeringPacketLength,
                    allocator: ByteBufferAllocator()
                )
            )
            XCTAssertLessThan(packet.readableBytes, triggeringPacketLength)
        }
    }

    func testNoResetPacketWhenTheTriggerIsTooSmall() {
        let generator = QUICStatelessResetTokenGenerator(key: Self.key)

        // A reset needs 5 unpredictable bytes plus the 16-byte token, so nothing valid fits into
        // fewer than 21 bytes.
        for triggeringPacketLength in [0, 1, 21] {
            XCTAssertNil(
                generator.statelessResetPacket(
                    for: self.makeConnectionID(1),
                    triggeringPacketLength: triggeringPacketLength,
                    allocator: ByteBufferAllocator()
                )
            )
        }
    }
}
