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

struct QUICStatelessResetTokenGeneratorTests {
    private static let key = [UInt8](repeating: 0xAB, count: 32)
    private static let otherKey = [UInt8](repeating: 0xCD, count: 32)

    @available(anyAppleOS 26, *)
    private func makeConnectionID(_ byte: UInt8) -> QUICConnectionID {
        QUICConnectionID(bytes: InlineArray<20, UInt8>(repeating: byte), length: 8)
    }

    @Test
    @available(anyAppleOS 26, *)
    func tokenIsStableForTheSameKeyAndConnectionID() {
        let connectionID = self.makeConnectionID(1)
        let generator = QUICStatelessResetTokenGenerator(key: Self.key)
        let other = QUICStatelessResetTokenGenerator(key: Self.key)

        // The whole point of deriving tokens: a second generator with the same key produces the
        // same token, so a reset stays valid after the connection state is gone.
        #expect(generator.token(for: connectionID) == other.token(for: connectionID))
    }

    @Test
    @available(anyAppleOS 26, *)
    func tokenDiffersPerConnectionID() {
        let generator = QUICStatelessResetTokenGenerator(key: Self.key)

        #expect(generator.token(for: self.makeConnectionID(1)) != generator.token(for: self.makeConnectionID(2)))
    }

    @Test
    @available(anyAppleOS 26, *)
    func tokenDiffersPerKey() {
        let connectionID = self.makeConnectionID(1)

        #expect(
            QUICStatelessResetTokenGenerator(key: Self.key).token(for: connectionID)
            != QUICStatelessResetTokenGenerator(key: Self.otherKey).token(for: connectionID)
        )
    }

    @Test
    @available(anyAppleOS 26, *)
    func keyIsGeneratedWhenNoneIsConfigured() {
        let connectionID = self.makeConnectionID(1)

        #expect(
            QUICStatelessResetTokenGenerator(key: nil).token(for: connectionID)
            != QUICStatelessResetTokenGenerator(key: nil).token(for: connectionID)
        )
    }

    @Test
    @available(anyAppleOS 26, *)
    func resetPacketEndsWithTheTokenAndLooksLikeAShortHeader() throws {
        let connectionID = self.makeConnectionID(1)
        let generator = QUICStatelessResetTokenGenerator(key: Self.key)

        let packet = try #require(
            generator.statelessResetPacket(
                for: connectionID,
                triggeringPacketLength: 1200,
                allocator: ByteBufferAllocator()
            )
        )

        // Form bit clear, fixed bit set: indistinguishable from a 1-RTT packet (RFC 9000 § 10.3).
        let firstByte = try #require(packet.getInteger(at: packet.readerIndex, as: UInt8.self))
        #expect(firstByte & 0b1100_0000 == 0b0100_0000)
        #expect(packet.readableBytes >= 21)
    }

    @Test
    @available(anyAppleOS 26, *)
    func resetPacketIsSmallerThanThePacketThatTriggeredIt() throws {
        let generator = QUICStatelessResetTokenGenerator(key: Self.key)

        // RFC 9000 § 10.3.3: every reset must be smaller than its trigger, so an exchange of
        // resets between two stateless endpoints dies out instead of looping.
        for triggeringPacketLength in [22, 30, 43, 1200] {
            let packet = try #require(
                generator.statelessResetPacket(
                    for: self.makeConnectionID(1),
                    triggeringPacketLength: triggeringPacketLength,
                    allocator: ByteBufferAllocator()
                )
            )
            #expect(packet.readableBytes < triggeringPacketLength)
        }
    }

    @Test
    @available(anyAppleOS 26, *)
    func noResetPacketWhenTheTriggerIsTooSmall() {
        let generator = QUICStatelessResetTokenGenerator(key: Self.key)

        // A reset needs 5 unpredictable bytes plus the 16-byte token, so nothing valid fits into
        // fewer than 21 bytes.
        for triggeringPacketLength in [0, 1, 21] {
            #expect(
                generator.statelessResetPacket(
                    for: self.makeConnectionID(1),
                    triggeringPacketLength: triggeringPacketLength,
                    allocator: ByteBufferAllocator()
                )
                == nil
            )
        }
    }
}
