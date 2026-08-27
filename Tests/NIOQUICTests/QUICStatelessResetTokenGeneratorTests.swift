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
}
