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
import NIOCore

/// Groups an ordered burst of outbound datagrams for one peer into runs which can be sent to the
/// kernel as a single UDP Generic Segmentation Offload write.
struct GSOCoalescer {
    /// The maximum number of segments to coalesce.
    var maxSegments: Int

    /// The maximum size of a coalesced datagram.
    var maxCoalescedSize: Int

    private var buffers: Deque<ByteBuffer>
    private let remoteAddress: SocketAddress

    init(remoteAddress: SocketAddress, maxSegments: Int = 64, maxCoalescedSize: Int = 65535) {
        self.remoteAddress = remoteAddress
        self.maxSegments = maxSegments
        self.maxCoalescedSize = maxCoalescedSize
        self.buffers = Deque()
        self.buffers.reserveCapacity(16)
    }

    var isEmpty: Bool {
        self.buffers.isEmpty
    }

    mutating func append(_ buffer: ByteBuffer) {
        if buffer.readableBytes > 0 {
            self.buffers.append(buffer)
        }
    }

    /// Removes and returns the next run of pending datagrams, or `nil` if there are none left.
    mutating func next() -> AddressedEnvelope<ByteBuffer>? {
        if self.buffers.isEmpty {
            return nil
        } else if self.maxSegments == 1, let buffer = self.buffers.popFirst() {
            return AddressedEnvelope(remoteAddress: self.remoteAddress, data: buffer)
        }

        // Try to coalesce as many sequential buffers as possible without exceeding:
        // - the max segment length,
        // - the max coalesced size
        //
        // Buffers must be the same length to coalesce although the final buffer in a
        // run may be shorter than the rest.
        var index = self.buffers.startIndex
        let segmentSize = self.buffers[index].readableBytes
        self.buffers.formIndex(after: &index)

        var totalSize = segmentSize
        var runLength = 1

        // Max segments may be less than the configured max based on the size of the first
        // segment in a run.
        let effectiveMaxSegments = min(self.maxSegments, max(1, self.maxCoalescedSize / segmentSize))

        while index != self.buffers.endIndex, runLength < effectiveMaxSegments {
            let size = self.buffers[index].readableBytes

            // Bigger; end run without including this buffer.
            if size > segmentSize { break }

            totalSize &+= size
            runLength &+= 1

            // Smaller; end run the run.
            if size < segmentSize { break }

            // Same size; continue iterating.
            self.buffers.formIndex(after: &index)
        }

        if runLength == 1 {
            // Nothing to coalesce; so just remove and return.
            return AddressedEnvelope(
                remoteAddress: self.remoteAddress,
                data: self.buffers.removeFirst()
            )
        }

        // Coalesce the run into the first datagram's buffer.
        var coalesced = self.buffers.removeFirst()
        coalesced.reserveCapacity(minimumWritableBytes: totalSize &- segmentSize)
        runLength &-= 1

        while runLength > 0 {
            runLength &-= 1
            coalesced.writeImmutableBuffer(self.buffers.removeFirst())
        }

        return AddressedEnvelope(
            remoteAddress: self.remoteAddress,
            data: coalesced,
            metadata: AddressedEnvelope.Metadata(
                ecnState: .transportNotCapable,
                packetInfo: nil,
                segmentSize: segmentSize
            )
        )
    }
}
