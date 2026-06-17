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
import Testing

@testable import NIOQUIC

private let testBuffer: [UInt8] = (0..<2048).flatMap { _ in [7, 6, 5, 4, 3, 2, 1, 9, 8] }

struct ByteBuffer_FrameTests {
    @available(anyAppleOS 26, *)
    @Test(
        "init(frame:) empty buffer"
    )
    func emptyBuffer() throws {
        let bytes: [UInt8] = []
        let frame = Frame(copyBuffer: bytes)

        // testing this initializer
        let byteBuffer = ByteBuffer(frame: frame)

        #expect(byteBuffer == ByteBuffer(bytes: bytes))
    }

    @available(anyAppleOS 26, *)
    @Test(
        "init(frame:) buffer consistency"
    )
    func bufferConsistency() throws {
        let frame = Frame(copyBuffer: testBuffer)

        // testing this initializer
        let byteBuffer = ByteBuffer(frame: frame)

        #expect(byteBuffer == ByteBuffer(bytes: testBuffer))
    }

    @available(anyAppleOS 26, *)
    @Test(
        "init(frame:) independent backing storage"
    )
    func objectSemantics() throws {
        // this test is possibly overly cautious given ByteBuffer's public API guarantees
        var frame = Frame(copyBuffer: testBuffer)

        let extractedBytes = frame.extractBytes()
        var byteBuffer = extractedBytes.span.withUnsafeBytes {
            NIOCore.ByteBuffer(bytes: $0)
        }
        frame.finalize(success: true)
        #expect(byteBuffer == ByteBuffer(bytes: testBuffer))

        // ensure different backing storage
        try byteBuffer.withVeryUnsafeBytes { unsafeRawBufferPointer in
            try extractedBytes.span.withUnsafeBytes { frameUnsafeRawBufferPointer in
                let pointer = try #require(unsafeRawBufferPointer.baseAddress)
                #expect(frameUnsafeRawBufferPointer.baseAddress != pointer)
            }
        }
        // ensure modifying the new buffer succeeds an doesn't go bang
        byteBuffer.setBytes([0x13], at: 0)
        #expect(byteBuffer != ByteBuffer(bytes: testBuffer))
    }
}
