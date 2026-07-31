//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2023 Apple Inc. and the SwiftNIO project authors
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

struct BufferPoolTests {
    @Test
    func poolFillsToCapacity() {
        var pool = BufferPool(capacity: 3)
        #expect(pool.count == 0)
        #expect(pool.capacity == 3)

        // The pool recycles buffers which are unique. Store each buffer to ensure that's not the
        // case.
        let buffers: [ByteBuffer] = (1...pool.capacity).map { _ in
            let (buffer, _) = pool.withBuffer(minimumCapacity: 1024) {
                #expect($0.readableBytes == 0)
                #expect($0.writableBytes == 1024)
            }
            return buffer
        }

        let bufferIDs = Set(buffers.map { $0.storagePointerIntegerValue() })
        #expect(buffers.count == bufferIDs.count)

        // Keep the buffers alive.
        withExtendedLifetime(buffers) {
            for _ in 1...pool.capacity {
                let (buffer, _) = pool.withBuffer(minimumCapacity: 1024) {
                    #expect($0.readableBytes == 0)
                    #expect($0.writableBytes == 1024)
                }
                #expect(pool.count == pool.capacity)
                #expect(!bufferIDs.contains(buffer.storagePointerIntegerValue()))
            }
        }
    }

    @Test
    func buffersAreRecycled() {
        var pool = BufferPool(capacity: 3)

        let (_, storageID) = pool.withBuffer(minimumCapacity: 1024) { buffer in
            buffer.storagePointerIntegerValue()
        }

        #expect(pool.count == 1)

        for _ in 0..<100 {
            _ = pool.withBuffer(minimumCapacity: 1024) { buffer in
                #expect(buffer.storagePointerIntegerValue() == storageID)
            }
            #expect(pool.count == 1)
        }
    }

    @Test
    func firstAvailableBufferUsed() {
        var pool = BufferPool(capacity: 3)

        var buffers: [ByteBuffer] = (0..<pool.capacity).map { _ in
            let (buffer, _) = pool.withBuffer(minimumCapacity: 1024) { _ in }
            return buffer
        }

        let bufferIDs = Set(buffers.map { $0.storagePointerIntegerValue() })
        #expect(bufferIDs.count == pool.capacity)

        // Drop the ref to the middle buffer.
        buffers.remove(at: 1)

        _ = pool.withBuffer(minimumCapacity: 1024) { buffer in
            #expect(bufferIDs.contains(buffer.storagePointerIntegerValue()))
        }
    }

    @Test
    func buffersAreClearedBetweenCalls() {
        var pool = BufferPool(capacity: 3)

        // The pool recycles buffers which are unique. Store each buffer to ensure that's not the
        // case.
        var buffers: [ByteBuffer] = (1...pool.capacity).map { _ in
            let (buffer, _) = pool.withBuffer(minimumCapacity: 1024) {
                $0.writeRepeatingByte(42, count: 1024)
            }
            return buffer
        }

        // Grab the storage pointers; check against them below.
        var storagePointers = Set(buffers.map { $0.storagePointerIntegerValue() })
        #expect(storagePointers.count == pool.capacity)

        // Drop the buffer storage refs so they can be reused.
        buffers.removeAll()

        // Loop of the pool again.
        var moreBuffers: [ByteBuffer] = storagePointers.map { _ in
            let (buffer, _) = pool.withBuffer(minimumCapacity: 1024) {
                let storagePointer = $0.storagePointerIntegerValue()
                #expect(storagePointers.remove(storagePointer) != nil)
                #expect($0.readerIndex == 0)
                #expect($0.writerIndex == 0)
            }
            return buffer
        }
        moreBuffers.removeAll()
    }
}

extension ByteBuffer {
    func storagePointerIntegerValue() -> UInt {
        var pointer: UInt = 0
        self.withVeryUnsafeBytes { ptr in
            pointer = UInt(bitPattern: ptr.baseAddress!)
        }
        return pointer
    }
}
