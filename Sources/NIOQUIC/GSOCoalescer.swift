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

import DequeModule
@_spi(CustomByteBufferAllocator) import NIOCore
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

/// Groups an ordered burst of outbound datagrams for one peer into runs which can be sent to the
/// kernel as a single UDP Generic Segmentation Offload write.
@available(anyAppleOS 26, *)
struct GSOCoalescer: ~Copyable {
    /// The maximum number of segments to coalesce.
    var maxSegments: Int

    /// The maximum size of a coalesced datagram.
    var maxCoalescedSize: Int

    /// Frames to coalesce.
    private var frames: UniqueDeque<Frame>

    /// The address of the remote peer to send datagrams to.
    private let remoteAddress: SocketAddress

    /// A pool of byte buffers.
    private var pool: BufferPool

    /// A pool of `Frame`s.
    private let framePool: FramePool

    init(
        remoteAddress: SocketAddress,
        framePool: FramePool,
        maxSegments: Int = 64,
        maxCoalescedSize: Int = 65535,
        bufferPoolCapacity: Int = 8
    ) {
        self.remoteAddress = remoteAddress
        self.maxSegments = maxSegments
        self.maxCoalescedSize = maxCoalescedSize

        self.frames = UniqueDeque<Frame>()
        self.frames.reserveCapacity(16)

        self.pool = BufferPool(capacity: bufferPoolCapacity, allocator: ByteBufferAllocator())
        self.framePool = framePool
    }

    var isEmpty: Bool {
        self.frames.isEmpty
    }

    mutating func finalizeAllFramesAsFailed() {
        while var frame = self.frames.popFirst() {
            frame.finalize(success: false)
        }
    }

    mutating func append(frames: consuming FrameArray) {
        self.frames.reserveCapacity(self.frames.count + frames.count)

        while var frame = frames.popFirst() {
            if frame.unclaimedLength == 0 {
                frame.finalize(success: true)
            } else {
                self.frames.append(frame)
            }
        }
    }

    private mutating func datagram(from frame: consuming Frame) -> AddressedEnvelope<ByteBuffer> {
        let buffer: ByteBuffer

        var buf = ByteBuffer()
        buf.reserveCapacity(frame.unclaimedLength)
        frame.span?.withUnsafeBufferPointer { _ = buf.writeBytes($0) }
        self.framePool.storeFrame(frame)
        buffer = buf

        return AddressedEnvelope(remoteAddress: self.remoteAddress, data: buffer)
    }

    /// Removes and returns the next run of pending datagrams, or `nil` if there are none left.
    mutating func next() -> AddressedEnvelope<ByteBuffer>? {
        if self.frames.isEmpty {
            return nil
        } else if self.maxSegments == 1, let frame = self.frames.popFirst() {
            return self.datagram(from: frame)
        }

        // Try to coalesce as many sequential frames as possible without exceeding:
        // - the max segment length,
        // - the max coalesced size
        //
        // Frames must be the same length to coalesce although the final frame in a
        // run may be shorter than the rest.
        var index = self.frames.startIndex
        let segmentSize = self.frames[index].unclaimedLength
        self.frames.formIndex(after: &index)

        var totalSize = segmentSize
        var runLength = 1

        // Max segments may be less than the configured max based on the size of the first
        // segment in a run.
        let effectiveMaxSegments = min(self.maxSegments, max(1, self.maxCoalescedSize / segmentSize))

        while index != self.frames.endIndex, runLength < effectiveMaxSegments {
            let size = self.frames[index].unclaimedLength

            // Bigger; end run without including this frame.
            if size > segmentSize { break }

            totalSize &+= size
            runLength &+= 1

            // Smaller; end run the run.
            if size < segmentSize { break }

            // Same size; continue iterating.
            self.frames.formIndex(after: &index)
        }

        if runLength == 1 {
            // Nothing to coalesce; so just remove and return.
            let frame = self.frames.popFirst()!
            return self.datagram(from: frame)
        } else {
            let (buffer, ()) = self.pool.withBuffer(minimumCapacity: totalSize) { buffer in
                while runLength > 0 {
                    runLength &-= 1
                    Self.write(self.frames.removeFirst(), to: &buffer, returningFrameTo: self.framePool)
                }
            }

            return AddressedEnvelope(
                remoteAddress: self.remoteAddress,
                data: buffer,
                metadata: AddressedEnvelope.Metadata(
                    ecnState: .transportNotCapable,
                    packetInfo: nil,
                    segmentSize: segmentSize
                )
            )
        }
    }

    private static func write(
        _ frame: consuming Frame,
        to buffer: inout ByteBuffer,
        returningFrameTo pool: FramePool
    ) {
        frame.span?.withUnsafeBufferPointer { _ = buffer.writeBytes($0) }
        pool.storeFrame(frame)
    }
}
