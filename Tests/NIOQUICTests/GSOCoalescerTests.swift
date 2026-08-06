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
import Testing

@_spi(Essentials) @_spi(ProtocolProvider) @testable import NIOQUIC
@_spi(Essentials) @_spi(ProtocolProvider) @testable import SwiftNetwork

struct GSOCoalescerTests {
    private static let peer = try! SocketAddress(ipAddress: "127.0.0.1", port: 4433)

    @available(anyAppleOS 26, *)
    private func makeCoalescer(
        maxSegments: Int = 64,
        maxCoalescedSize: Int = 65535
    ) -> GSOCoalescer {
        GSOCoalescer(
            remoteAddress: Self.peer,
            framePool: .makePool(forGSO: false),
            maxSegments: maxSegments,
            maxCoalescedSize: maxCoalescedSize
        )
    }

    /// Builds a batch of frames of the given packet sizes, each filled with a distinct byte.
    ///
    /// Frames are allocated with `allocation` bytes (defaulting to the packet size) to mimic the
    /// QUIC stack, which allocates a full-MSS frame and then collapses it down to the length of
    /// the packet it actually wrote.
    @available(anyAppleOS 26, *)
    private func frames(_ sizes: [Int], allocation: Int? = nil) -> FrameArray {
        var array = FrameArray(capacity: sizes.count)
        for (index, size) in sizes.enumerated() {
            let allocation = allocation ?? size
            var frame = Frame(allocatingCustomFinalizerBufferOfSize: allocation)
            if allocation != size {
                let collapsed = frame.collapse(to: size)
                #expect(collapsed)
            }
            if var span = frame.mutableSpan {
                for spanIndex in span.indices {
                    span[spanIndex] = UInt8(truncatingIfNeeded: index + 1)
                }
            }
            array.add(frame: frame)
        }
        return array
    }

    /// Drains the coalescer into a list of (segment size, total size) pairs.
    @available(anyAppleOS 26, *)
    private func drain(_ coalescer: inout GSOCoalescer) -> [(segmentSize: Int?, totalSize: Int)] {
        var runs: [(segmentSize: Int?, totalSize: Int)] = []
        while let envelope = coalescer.next() {
            #expect(envelope.remoteAddress == Self.peer)
            runs.append(
                (segmentSize: envelope.metadata?.segmentSize, totalSize: envelope.data.readableBytes)
            )
        }
        return runs
    }

    @available(anyAppleOS 26, *)
    @Test
    func emptyCoalescer() {
        var coalescer = self.makeCoalescer()
        let isEmpty = coalescer.isEmpty
        #expect(isEmpty)
        #expect(coalescer.next() == nil)
    }

    @available(anyAppleOS 26, *)
    @Test
    func emptyBatchIsIgnored() {
        var coalescer = self.makeCoalescer()
        coalescer.append(frames: FrameArray())
        let isEmpty = coalescer.isEmpty
        #expect(isEmpty)
        #expect(coalescer.next() == nil)
    }

    @available(anyAppleOS 26, *)
    @Test
    func singleDatagramIsNotSegmented() {
        var coalescer = self.makeCoalescer()
        coalescer.append(frames: self.frames([1200]))

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [nil])
        #expect(runs.map { $0.totalSize } == [1200])
        let isEmpty = coalescer.isEmpty
        #expect(isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func equalSizedDatagramsCoalesce() {
        var coalescer = self.makeCoalescer()
        coalescer.append(frames: self.frames(Array(repeating: 1200, count: 5)))

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [1200])
        #expect(runs.map { $0.totalSize } == [6000])
    }

    /// The QUIC stack hands back frames allocated at full MSS regardless of how many bytes the
    /// packet written into them uses: the run must be sized by the packet, not the allocation.
    @available(anyAppleOS 26, *)
    @Test
    func packetLengthNotAllocationSizeDrivesTheRun() {
        var coalescer = self.makeCoalescer()
        coalescer.append(frames: self.frames([1200, 1200, 900, 1200], allocation: 1400))

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [1200, nil])
        #expect(runs.map { $0.totalSize } == [3300, 1200])
    }

    @available(anyAppleOS 26, *)
    @Test
    func shortDatagramEndsRun() {
        var coalescer = self.makeCoalescer()
        coalescer.append(frames: self.frames([1200, 1200, 500, 1200]))

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [1200, nil])
        #expect(runs.map { $0.totalSize } == [2900, 1200])
    }

    @available(anyAppleOS 26, *)
    @Test
    func largerDatagramStartsNewRun() {
        var coalescer = self.makeCoalescer()
        coalescer.append(frames: self.frames([800, 800, 1200, 1200]))

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [800, 1200])
        #expect(runs.map { $0.totalSize } == [1600, 2400])
    }

    @available(anyAppleOS 26, *)
    @Test
    func runIsCappedAtMaxSegments() {
        var coalescer = self.makeCoalescer()
        let maxSegments = coalescer.maxSegments
        coalescer.append(frames: self.frames(Array(repeating: 100, count: maxSegments + 3)))

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [100, 100])
        #expect(runs.map { $0.totalSize } == [maxSegments * 100, 300])
    }

    @available(anyAppleOS 26, *)
    @Test
    func runIsCappedAtMaxCoalescedSize() throws {
        let maxSegments = 64
        let maxCoalescedSize = 65_536

        var coalescer = self.makeCoalescer(
            maxSegments: maxSegments,
            maxCoalescedSize: maxCoalescedSize
        )

        // 64 * 1400 = 89600 bytes: more than maxCoalescedSize
        coalescer.append(frames: self.frames(Array(repeating: 1400, count: maxSegments)))

        let runs = self.drain(&coalescer)
        try #require(runs.count == 2)

        #expect(runs[0].totalSize == (maxCoalescedSize / 1400) * 1400)
        #expect(runs[0].segmentSize == 1400)

        #expect(runs[1].totalSize == (maxSegments * 1400) - runs[0].totalSize)
        #expect(runs[1].segmentSize == 1400)
    }

    @available(anyAppleOS 26, *)
    @Test
    func oversizedDatagramIsEmittedAlone() {
        var coalescer = self.makeCoalescer()
        let oversized = coalescer.maxCoalescedSize + 1  // read before the borrow: GSOCoalescer is ~Copyable
        coalescer.append(frames: self.frames([oversized, 1200]))

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [nil, nil])
        #expect(runs.map { $0.totalSize } == [oversized, 1200])
    }

    @available(anyAppleOS 26, *)
    @Test
    func maxSegmentsOfOneNeverCoalesces() {
        var coalescer = GSOCoalescer(
            remoteAddress: Self.peer,
            framePool: .makePool(forGSO: false),
            maxSegments: 1
        )
        coalescer.append(frames: self.frames([1200, 1200, 1200]))

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [nil, nil, nil])
        #expect(runs.map { $0.totalSize } == [1200, 1200, 1200])
    }

    @available(anyAppleOS 26, *)
    @Test
    func coalescedRunPreservesBytesInOrder() {
        var coalescer = self.makeCoalescer()
        coalescer.append(frames: self.frames([4, 4, 4]))

        let envelope = coalescer.next()
        #expect(envelope?.metadata?.segmentSize == 4)
        #expect(envelope.map { Array($0.data.readableBytesView) } == [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3])
        #expect(coalescer.next() == nil)
    }
}
