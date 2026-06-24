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

@_spi(CustomByteBufferAllocator) import NIOCore
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

@available(anyAppleOS 26, *)
enum FrameMemory {
    static let allocator = ByteBufferAllocator(
        allocate: { UnsafeMutableRawPointer.allocate(byteCount: Int($0), alignment: 1) },
        reallocate: { ptr, oldSize, newSize in
            let new = UnsafeMutableRawPointer.allocate(byteCount: Int(newSize), alignment: 1)
            if let ptr {
                new.copyMemory(from: ptr, byteCount: min(Int(oldSize), Int(newSize)))
                ptr.deallocate()
            }
            return new
        },
        deallocate: { $0.deallocate() },
        copy: { $0.copyMemory(from: $1, byteCount: Int($2)) }
    )
}

@available(anyAppleOS 26, *)
extension Frame {
    // Wrap creation of a new Frame with a .customFinalizer buffer. Allocates `size` bytes
    // and sets a finalizer that deallocates the memory.
    init(allocatingCustomFinalizerBufferOfSize size: Int) {
        let ptr = UnsafeMutableRawPointer.allocate(
            byteCount: size,
            alignment: MemoryLayout<UInt8>.alignment
        )
        self.init(
            buffer: UnsafeMutableRawBufferPointer(start: ptr, count: size),
            finalizer: { $0.baseAddress?.deallocate() }
        )
    }
}
