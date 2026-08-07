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

import BasicContainers
@_spi(ProtocolProvider) import SwiftNetwork

/// A pool of `Frame`s.
///
/// The pool stores two category sizes of frame: small and large. To be pooled, small and large
/// frames must have exactly the capacity as required by their size class. The pool limits also how
/// many of each category size can be stored at a given time. This avoids situations where bursty
/// traffic results in the pool overcomitting memory indefinitely.
@available(anyAppleOS 26, *)
final class FramePool {
    private var smallStorage: BufferPool
    private var largeStorage: BufferPool

    private let smallFrameSize: Int
    private let largeFrameSize: Int

    init(
        smallFrameSize: Int,
        maxSmallFrames: Int,
        largeFrameSize: Int,
        maxLargeFrames: Int,
    ) {
        precondition(maxLargeFrames >= 0 && maxSmallFrames >= 0)
        precondition(largeFrameSize >= 0 && smallFrameSize >= 0)
        precondition(largeFrameSize > smallFrameSize || largeFrameSize == 0)

        self.smallStorage = BufferPool(capacity: maxSmallFrames)
        self.smallFrameSize = smallFrameSize

        self.largeStorage = BufferPool(capacity: maxLargeFrames)
        self.largeFrameSize = largeFrameSize
    }

    /// The number of small frames in the pool.
    var smallFrameCount: Int {
        self.smallStorage.count
    }

    /// The number of large frames in the pool.
    var largeFrameCount: Int {
        self.largeStorage.count
    }

    static func makePool(forGSO gso: Bool) -> Self {
        Self(
            // Small frames are approx MSS sized.
            smallFrameSize: 1500,
            // GSO expects fewer small frames.
            maxSmallFrames: gso ? 8 : 64,
            // 64KB is the upper limit on what can be sent with GSO.
            largeFrameSize: gso ? 64 * 1024 : 0,
            // 4 * 64KB is 256KB: the maximum amount retained per connection for GSO.
            maxLargeFrames: gso ? 4 : 0
        )
    }

    private func storeBufferPointer(_ buffer: consuming UnsafeMutableRawBufferPointer) -> Bool {
        let stored: Bool

        switch buffer.count {
        case self.smallFrameSize:
            stored = self.smallStorage.storeBuffer(buffer)
        case self.largeFrameSize:
            stored = self.largeStorage.storeBuffer(buffer)
        default:
            // Not a pooled size.
            buffer.deallocate()
            stored = false
        }

        return stored
    }

    /// Stores the backing storage from the frame.
    ///
    /// - Parameter frame: The frame to store.
    /// - Returns: Whether the frame was stored or not.
    @discardableResult
    func storeFrame(_ frame: consuming Frame) -> Bool {
        let stored: Bool

        if let customFinalizer = frame.takeOwnershipOfCustomFinalizerBuffer() {
            stored = self.storeBufferPointer(customFinalizer.bufferPointer)
        } else {
            stored = false
        }

        if !stored {
            frame.finalize(success: false)
        }

        return stored
    }

    /// Take a frame from the pool if one is available.
    ///
    /// - Parameter minimumSize: The minimum size of the frame to take.
    /// - Returns: A pooled frame, if one was available.
    func takeFrame(minimumSize: Int) -> Frame? {
        let buffer: UnsafeMutableRawBufferPointer?

        if minimumSize <= self.smallFrameSize {
            buffer = self.smallStorage.takeBuffer()
        } else if minimumSize <= self.largeFrameSize {
            buffer = self.largeStorage.takeBuffer()
        } else {
            buffer = nil
        }

        if let buffer {
            return Frame(buffer: buffer) { $0.baseAddress?.deallocate() }
        } else {
            return nil
        }
    }

    /// Takes a frame from the pool or creates one if none were available.
    ///
    /// - Parameter minimumSize: The minimum size of the frame to take or create.
    /// - Returns: A frame.
    func takeOrCreateFrame(minimumSize: Int) -> Frame {
        if let frame = self.takeFrame(minimumSize: minimumSize) {
            return frame
        }

        // Create a frame sized for one of the buckets.
        let size: Int
        if minimumSize <= self.smallFrameSize {
            size = self.smallFrameSize
        } else if minimumSize <= self.largeFrameSize {
            size = self.largeFrameSize
        } else {
            size = minimumSize
        }

        return Frame(allocatingCustomFinalizerBufferOfSize: size)
    }

    private struct BufferPool: ~Copyable {
        private var storage: UniqueArray<UnsafeMutableRawBufferPointer>
        let capacity: Int

        var count: Int {
            self.storage.count
        }

        init(capacity: Int) {
            self.storage = UniqueArray()
            self.storage.reserveCapacity(capacity)
            self.capacity = capacity
        }

        deinit {
            for index in self.storage.indices {
                self.storage[index].deallocate()
            }
        }

        mutating func storeBuffer(_ buffer: consuming UnsafeMutableRawBufferPointer) -> Bool {
            if self.storage.count < self.capacity {
                self.storage.append(buffer)
                return true
            } else {
                buffer.deallocate()
                return false
            }
        }

        mutating func takeBuffer() -> UnsafeMutableRawBufferPointer? {
            self.storage.popLast()
        }
    }
}
