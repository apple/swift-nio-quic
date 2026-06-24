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

import XCTest

@testable import NIOQUIC

@available(anyAppleOS 26, *)
final class QUICConnectionIDGeneratorTests: XCTestCase {
    func testRandomGeneratorDefaultLength() {
        var generator = RandomQUICConnectionIDGenerator()
        XCTAssertEqual(generator.connectionIDLength, Int(QUICConnectionID.randomIDLength))
        let cid = generator.next()
        XCTAssertEqual(cid.length, Int(QUICConnectionID.randomIDLength))
    }

    func testRandomGeneratorCustomLength() {
        var generator = RandomQUICConnectionIDGenerator(connectionIDLength: 16)
        XCTAssertEqual(generator.connectionIDLength, 16)
        let cid = generator.next()
        XCTAssertEqual(cid.length, 16)
    }

    func testRandomGeneratorZeroLength() {
        var generator = RandomQUICConnectionIDGenerator(connectionIDLength: 0)
        XCTAssertEqual(generator.connectionIDLength, 0)
        let cid = generator.next()
        XCTAssertEqual(cid.length, 0)
    }

    func testCustomGenerator() {
        struct CountingGenerator: QUICConnectionIDGenerator {
            var connectionIDLength: Int = 4
            var counter: UInt8 = 0

            mutating func next() -> QUICConnectionID {
                defer { self.counter += 1 }
                var bytes = InlineArray<20, UInt8>(repeating: 0)
                bytes[0] = self.counter
                return QUICConnectionID(bytes: bytes, length: UInt8(self.connectionIDLength))
            }

            mutating func next(
                sourceConnectionID: QUICConnectionID,
                destinationConnectionID: QUICConnectionID
            ) -> QUICConnectionID {
                self.next()
            }
        }

        var generator = CountingGenerator()
        let cid0 = generator.next()
        let cid1 = generator.next()
        let cid2 = generator.next()

        XCTAssertEqual(cid0.length, 4)
        XCTAssertEqual(cid0.bytes[0], 0)
        XCTAssertEqual(cid1.bytes[0], 1)
        XCTAssertEqual(cid2.bytes[0], 2)
    }
}
