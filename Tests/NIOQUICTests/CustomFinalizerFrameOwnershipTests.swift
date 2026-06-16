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
@_spi(CustomByteBufferAllocator) import NIOCore
import Testing

@_spi(ProtocolProvider) @testable import NIOQUIC
@_spi(ProtocolProvider) @testable import SwiftNetwork

struct CustomFinalizerFrameOwnershipTests {

    private let bufferSize = 10
    private let deallocator: (UnsafeMutableRawBufferPointer) -> Void = { $0.baseAddress?.deallocate() }

    @Test("Empty frame: fully claimed from start, unclaimedLength == 0")
    func emptyFrame() {
        let buf = allocateAndFillBuffer(of: bufferSize)
        var frame = Frame(
            buffer: buf,
            finalizer: deallocator
        )
        let claimed = frame.claim(fromStart: bufferSize)
        #expect(claimed)
        #expect(frame.unclaimedLength == 0)

        guard let info = frame.takeOwnershipOfCustomFinalizerBuffer() else {
            frame.finalize(success: true)
            Issue.record("takeOwnershipOfCustomFinalizerBuffer returned nil for empty frame")
            return
        }

        frame.finalize(success: true)

        #expect(info.readerOffset == bufferSize)
        #expect(info.writerOffset == bufferSize)

        let byteBuffer = ByteBuffer(
            takingOwnershipOf: info.bufferPointer,
            allocator: FrameMemory.allocator,
            readerIndex: info.readerOffset,
            writerIndex: info.writerOffset
        )
        #expect(byteBuffer.readableBytes == 0)
        #expect(byteBuffer.capacity == bufferSize)
    }

    @Test("Partially filled frame: claimed from end")
    func partiallyFilledFrame() {
        let buf = allocateAndFillBuffer(of: bufferSize)
        var frame = Frame(
            buffer: buf,
            finalizer: deallocator
        )
        let claimed = frame.claim(fromStart: 0, fromEnd: 4)
        #expect(claimed)
        #expect(frame.unclaimedLength == 6)

        guard let info = frame.takeOwnershipOfCustomFinalizerBuffer() else {
            frame.finalize(success: true)
            Issue.record("takeOwnershipOfCustomFinalizerBuffer returned nil")
            return
        }

        frame.finalize(success: true)

        #expect(info.readerOffset == 0)
        #expect(info.writerOffset == 6)

        var byteBuffer = ByteBuffer(
            takingOwnershipOf: info.bufferPointer,
            allocator: FrameMemory.allocator,
            readerIndex: info.readerOffset,
            writerIndex: info.writerOffset
        )
        #expect(byteBuffer.capacity == bufferSize)
        #expect(byteBuffer.readableBytes == 6)
        #expect(byteBuffer.readBytes(length: 6) == [0, 1, 2, 3, 4, 5])
    }

    @Test("Partially filled + consumed: claimed from start and end")
    func partiallyFilledAndConsumedFrame() {
        let buf = allocateAndFillBuffer(of: bufferSize)
        var frame = Frame(
            buffer: buf,
            finalizer: deallocator
        )
        let claimedEnd = frame.claim(fromStart: 0, fromEnd: 4)
        #expect(claimedEnd)
        let claimedStart = frame.claim(fromStart: 2)
        #expect(claimedStart)
        #expect(frame.unclaimedLength == 4)

        guard let info = frame.takeOwnershipOfCustomFinalizerBuffer() else {
            frame.finalize(success: true)
            Issue.record("takeOwnershipOfCustomFinalizerBuffer returned nil")
            return
        }

        frame.finalize(success: true)

        #expect(info.readerOffset == 2)
        #expect(info.writerOffset == 6)

        var byteBuffer = ByteBuffer(
            takingOwnershipOf: info.bufferPointer,
            allocator: FrameMemory.allocator,
            readerIndex: info.readerOffset,
            writerIndex: info.writerOffset
        )
        #expect(byteBuffer.capacity == bufferSize)
        #expect(byteBuffer.readableBytes == 4)
        #expect(byteBuffer.readBytes(length: 4) == [2, 3, 4, 5])
    }

    @Test("Fully filled frame: no claims")
    func fullyFilledFrame() {
        let buf = allocateAndFillBuffer(of: bufferSize)
        var frame = Frame(
            buffer: buf,
            finalizer: deallocator
        )
        #expect(frame.unclaimedLength == bufferSize)

        guard let info = frame.takeOwnershipOfCustomFinalizerBuffer() else {
            frame.finalize(success: true)
            Issue.record("takeOwnershipOfCustomFinalizerBuffer returned nil")
            return
        }

        frame.finalize(success: true)

        #expect(info.readerOffset == 0)
        #expect(info.writerOffset == bufferSize)

        var byteBuffer = ByteBuffer(
            takingOwnershipOf: info.bufferPointer,
            allocator: FrameMemory.allocator,
            readerIndex: info.readerOffset,
            writerIndex: info.writerOffset
        )
        #expect(byteBuffer.capacity == bufferSize)
        #expect(byteBuffer.readableBytes == bufferSize)
        #expect(byteBuffer.readBytes(length: bufferSize) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    }

    @Test("Fully filled + fully consumed: all bytes claimed from start")
    func fullyFilledAndConsumedFrame() {
        let buf = allocateAndFillBuffer(of: bufferSize)
        var frame = Frame(
            buffer: buf,
            finalizer: deallocator
        )
        let claimed = frame.claim(fromStart: bufferSize)
        #expect(claimed)
        #expect(frame.unclaimedLength == 0)

        guard let info = frame.takeOwnershipOfCustomFinalizerBuffer() else {
            frame.finalize(success: true)
            Issue.record("takeOwnershipOfCustomFinalizerBuffer returned nil for fully consumed frame")
            return
        }

        frame.finalize(success: true)

        #expect(info.readerOffset == bufferSize)
        #expect(info.writerOffset == bufferSize)

        let byteBuffer = ByteBuffer(
            takingOwnershipOf: info.bufferPointer,
            allocator: FrameMemory.allocator,
            readerIndex: info.readerOffset,
            writerIndex: info.writerOffset
        )
        #expect(byteBuffer.capacity == bufferSize)
        #expect(byteBuffer.readableBytes == 0)
    }

    // MARK: Helpers

    private func allocateAndFillBuffer(of bufferSize: Int) -> UnsafeMutableRawBufferPointer {
        let ptr = UnsafeMutableRawPointer.allocate(
            byteCount: bufferSize,
            alignment: MemoryLayout<UInt8>.alignment
        )
        let buf = UnsafeMutableRawBufferPointer(start: ptr, count: bufferSize)
        for i in 0..<bufferSize {
            buf[i] = UInt8(i)
        }
        return buf
    }
}
