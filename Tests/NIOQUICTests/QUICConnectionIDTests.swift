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
final class ConnectionIDTests: XCTestCase {
    func testEquatable() {
        let firstConnectionID = QUICConnectionID(
            bytes: [
                1, 2, 3, 4, 5,
                6, 7, 8, 9, 10,
                11, 12, 13, 14, 15,
                16, 17, 18, 19, 20,
            ],
            length: 20
        )
        let secondConnectionID = QUICConnectionID(
            bytes: [
                0, 1, 2, 3, 4,
                5, 6, 7, 8, 9,
                10, 11, 12, 13, 14,
                15, 16, 17, 18, 19,
            ],
            length: 20
        )

        XCTAssertEqual(firstConnectionID, firstConnectionID)
        XCTAssertEqual(secondConnectionID, secondConnectionID)
        XCTAssertNotEqual(firstConnectionID, secondConnectionID)
    }

    func testEquatable_whenDifferentLength() {
        let firstConnectionID = QUICConnectionID(
            bytes: [
                1, 2, 3, 4, 5,
                6, 7, 8, 9, 10,
                11, 12, 13, 14, 15,
                16, 17, 18, 19, 20,
            ],
            length: 20
        )
        let secondConnectionID = QUICConnectionID(
            bytes: [
                1, 2, 3, 4, 5,
                6, 7, 8, 9, 10,
                11, 12, 13, 14, 15,
                16, 17, 18, 19, 20,
            ],
            length: 16
        )

        XCTAssertEqual(firstConnectionID, firstConnectionID)
        XCTAssertEqual(secondConnectionID, secondConnectionID)
        XCTAssertNotEqual(firstConnectionID, secondConnectionID)
    }

    func testHashable() {
        let firstConnectionID = QUICConnectionID(
            bytes: [
                1, 2, 3, 4, 5,
                6, 7, 8, 9, 10,
                11, 12, 13, 14, 15,
                16, 17, 18, 19, 20,
            ],
            length: 20
        )
        let secondConnectionID = QUICConnectionID(
            bytes: [
                0, 1, 2, 3, 4,
                5, 6, 7, 8, 9,
                10, 11, 12, 13, 14,
                15, 16, 17, 18, 19,
            ],
            length: 20
        )

        XCTAssertEqual(firstConnectionID.hashValue, firstConnectionID.hashValue)
        XCTAssertEqual(secondConnectionID.hashValue, secondConnectionID.hashValue)
        XCTAssertNotEqual(firstConnectionID.hashValue, secondConnectionID.hashValue)
    }

    func testHashable_whenSamePrefixDifferentLength() {
        let shorter = QUICConnectionID(
            bytes: [
                1, 2, 3, 4, 5, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
            length: 5
        )
        let longer = QUICConnectionID(
            bytes: [
                1, 2, 3, 4, 5, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
            length: 10
        )

        XCTAssertNotEqual(shorter, longer)
        XCTAssertNotEqual(shorter.hashValue, longer.hashValue)
    }

    func testWithUnsafeBufferPointer() {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 2, 3, 4, 5,
                6, 7, 8, 9, 10,
                11, 12, 13, 14, 15,
                16, 17, 18, 19, 20,
            ],
            length: 20
        )

        let expectedBytes: [UInt8] = Array(1...20)
        connectionID.withUnsafeBufferPointer { buffer in
            XCTAssertEqual(Array(buffer), expectedBytes)
        }
    }

    func testWithUnsafeMutableBufferPointer() {
        var connectionID = QUICConnectionID(
            bytes: [
                1, 2, 3, 4, 5,
                6, 7, 8, 9, 10,
                11, 12, 13, 14, 15,
                16, 17, 18, 19, 20,
            ],
            length: 20
        )

        let expectedBytes: [UInt8] = Array(1...20)
        connectionID.withUnsafeMutableBufferPointer { buffer in
            XCTAssertEqual(Array(buffer), expectedBytes)
        }
    }

    func testStaticZero() {
        let zero = QUICConnectionID.zero
        zero.withUnsafeBufferPointer { buffer in
            for byte in buffer {
                XCTAssertEqual(byte, 0)
            }
        }
        XCTAssertEqual(zero.description, "0x0000000000000000000000000000000000000000")
    }

    // MARK: - Zero-length connection ID tests (RFC 9000 Section 5.1)

    func testZeroLengthConnectionID() {
        let cid = QUICConnectionID(
            bytes: InlineArray(repeating: 0),
            length: 0
        )
        XCTAssertEqual(cid.length, 0)
        XCTAssertEqual(cid.description, "")
    }

    func testZeroLengthConnectionID_equality() {
        let cid1 = QUICConnectionID(
            bytes: InlineArray(repeating: 0),
            length: 0
        )
        let cid2 = QUICConnectionID(
            bytes: InlineArray(repeating: 0xFF),
            length: 0
        )
        // Two zero-length CIDs are always equal regardless of backing bytes
        XCTAssertEqual(cid1, cid2)
    }

    func testZeroLengthConnectionID_notEqualToNonZeroLength() {
        let zeroLength = QUICConnectionID(
            bytes: InlineArray(repeating: 0),
            length: 0
        )
        let oneLength = QUICConnectionID(
            bytes: InlineArray(repeating: 0),
            length: 1
        )
        XCTAssertNotEqual(zeroLength, oneLength)
    }

    func testZeroLengthConnectionID_hashable() {
        let cid1 = QUICConnectionID(
            bytes: InlineArray(repeating: 0),
            length: 0
        )
        let cid2 = QUICConnectionID(
            bytes: InlineArray(repeating: 0xFF),
            length: 0
        )
        // Two zero-length CIDs must hash to the same value
        XCTAssertEqual(cid1.hashValue, cid2.hashValue)

        // Can be used as a dictionary key
        var dict = [QUICConnectionID: String]()
        dict[cid1] = "test"
        XCTAssertEqual(dict[cid2], "test")
    }

    func testZeroLengthConnectionID_withUnsafeBufferPointer() {
        let cid = QUICConnectionID(
            bytes: InlineArray(repeating: 0),
            length: 0
        )
        cid.withUnsafeBufferPointer { buffer in
            XCTAssertEqual(buffer.count, 0)
        }
    }

    func testStaticRandom() {
        struct NotSoRandomGenerator: RandomNumberGenerator {
            let values: [UInt64]
            var iterator: IndexingIterator<[UInt64]>

            init(values: [UInt64]) {
                self.values = values
                self.iterator = values.makeIterator()
            }

            mutating func next() -> UInt64 {
                self.iterator.next()!
            }
        }
        let bytes: [UInt8] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        let firstNumber = bytes[...9].reversed().reduce(0) { $0 << 8 + UInt64($1) }
        var generator: any RandomNumberGenerator = NotSoRandomGenerator(
            values: [firstNumber]
        )
        let random = QUICConnectionID.random(using: &generator)

        var expectedBytes: [UInt8] = [1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        random.withUnsafeBufferPointer { buffer in
            for byte in buffer {
                XCTAssertEqual(byte, expectedBytes.removeFirst())
            }
        }
        XCTAssertEqual(random.description, "0x0102030405060708")
    }
}
