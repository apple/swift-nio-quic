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

/// Groups an ordered burst of outbound datagrams for one peer into runs which can be sent to the
/// kernel as a single UDP Generic Segmentation Offload write.
@available(anyAppleOS 26, *)
struct GSOCoalescer: ~Copyable {
    /// The maximum number of segments to coalesce.
    var maxSegments: Int

    /// The maximum size of a coalesced datagram.
    var maxCoalescedSize: Int

    /// Frames to coalesce.
    private var frames: FrameArray

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

        self.frames = FrameArray(capacity: 16)

        self.pool = BufferPool(capacity: bufferPoolCapacity, allocator: ByteBufferAllocator())
        self.framePool = framePool
    }

    var isEmpty: Bool {
        self.frames.isEmpty
    }

    mutating func finalizeAllFramesAsFailed() {
        self.frames.finalizeAllFramesAsFailed()
    }

    mutating func append(frames: consuming FrameArray) {
        self.frames.add(frames: frames)
    }

    private mutating func datagram(from frame: inout Frame) -> AddressedEnvelope<ByteBuffer> {
        var buffer = ByteBuffer()
        buffer.reserveCapacity(frame.unclaimedLength)
        frame.span?.withUnsafeBufferPointer { _ = buffer.writeBytes($0) }
        self.framePool.storeFrame(&frame)

        return AddressedEnvelope(remoteAddress: self.remoteAddress, data: buffer)
    }

    /// Removes and returns the next run of pending datagrams, or `nil` if there are none left.
    mutating func next() -> AddressedEnvelope<ByteBuffer>? {
        // Drop leading empty frames (if they exist.)
        while !self.frames.isEmpty, self.frames.peekFirstFrame({ $0.unclaimedLength }) == 0 {
            assertionFailure("Unexpected empty frame")
            var frame = self.frames.popFirst()!
            frame.finalize(success: true)
        }

        if self.frames.isEmpty {
            return nil
        } else if self.maxSegments == 1, var frame = self.frames.popFirst() {
            return self.datagram(from: &frame)
        }

        // Try to coalesce as many sequential frames as possible without exceeding:
        // - the max segment length,
        // - the max coalesced size
        //
        // Frames must be the same length to coalesce although the final frame in a
        // run may be shorter than the rest.
        let maxSegments = self.maxSegments
        let maxCoalescedSize = self.maxCoalescedSize

        var segmentSize = 0
        var totalSize = 0
        var runLength = 0
        // Max segments may be less than the configured max based on the size of the first
        // segment in a run.
        var effectiveMaxSegments = 0

        self.frames.iterateImmutableFrames { frame in
            let size = frame.unclaimedLength

            if runLength == 0 {
                segmentSize = size
                effectiveMaxSegments = min(maxSegments, max(1, maxCoalescedSize / size))
                totalSize = size
                runLength = 1
                return runLength < effectiveMaxSegments
            }

            if size == 0 {
                assertionFailure("Unexpected empty frame")
                return false
            }

            // Bigger; end run without including this frame.
            if size > segmentSize { return false }

            totalSize &+= size
            runLength &+= 1

            // Smaller; end run the run.
            if size < segmentSize { return false }

            // Same size; continue iterating.
            return runLength < effectiveMaxSegments
        }

        if runLength == 1 {
            // Nothing to coalesce; so just remove and return.
            var frame = self.frames.popFirst()!
            return self.datagram(from: &frame)
        } else {
            let (buffer, ()) = self.pool.withBuffer(minimumCapacity: totalSize) { buffer in
                while runLength > 0 {
                    runLength &-= 1
                    var frame = self.frames.popFirst()!
                    frame.span?.withUnsafeBufferPointer { _ = buffer.writeBytes($0) }
                    self.framePool.storeFrame(&frame)
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
}
