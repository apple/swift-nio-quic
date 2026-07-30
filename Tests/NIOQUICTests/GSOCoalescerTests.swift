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

@testable import NIOQUIC

struct GSOCoalescerTests {
    private static let peer = try! SocketAddress(ipAddress: "127.0.0.1", port: 4433)

    private func makeCoalescer(
        maxSegments: Int = 64,
        maxCoalescedSize: Int = 65535
    ) -> GSOCoalescer {
        GSOCoalescer(
            remoteAddress: Self.peer,
            maxSegments: maxSegments,
            maxCoalescedSize: maxCoalescedSize
        )
    }

    private func buffer(_ size: Int) -> ByteBuffer {
        ByteBuffer(repeating: UInt8(size % 256), count: size)
    }

    /// Drains the coalescer into a list of (segment size, total size) pairs.
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

    @Test
    func emptyCoalescer() {
        var coalescer = self.makeCoalescer()
        #expect(coalescer.isEmpty)
        #expect(coalescer.next() == nil)
    }

    @Test
    func emptyBufferIsIgnored() {
        var coalescer = self.makeCoalescer()
        coalescer.append(ByteBuffer())
        #expect(coalescer.isEmpty)
        #expect(coalescer.next() == nil)
    }

    @Test
    func singleDatagramIsNotSegmented() {
        var coalescer = self.makeCoalescer()
        coalescer.append(self.buffer(1200))

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [nil])
        #expect(runs.map { $0.totalSize } == [1200])
        #expect(coalescer.isEmpty)
    }

    @Test
    func equalSizedDatagramsCoalesce() {
        var coalescer = self.makeCoalescer()
        for _ in 0..<5 {
            coalescer.append(self.buffer(1200))
        }

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [1200])
        #expect(runs.map { $0.totalSize } == [6000])
    }

    @Test
    func shortDatagramEndsRun() {
        var coalescer = self.makeCoalescer()
        coalescer.append(self.buffer(1200))
        coalescer.append(self.buffer(1200))
        coalescer.append(self.buffer(500))
        coalescer.append(self.buffer(1200))

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [1200, nil])
        #expect(runs.map { $0.totalSize } == [2900, 1200])
    }

    @Test
    func largerDatagramStartsNewRun() {
        var coalescer = self.makeCoalescer()
        coalescer.append(self.buffer(800))
        coalescer.append(self.buffer(800))
        coalescer.append(self.buffer(1200))
        coalescer.append(self.buffer(1200))

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [800, 1200])
        #expect(runs.map { $0.totalSize } == [1600, 2400])
    }

    @Test
    func runIsCappedAtMaxSegments() {
        var coalescer = self.makeCoalescer()
        for _ in 0..<(coalescer.maxSegments + 3) {
            coalescer.append(self.buffer(100))
        }

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [100, 100])
        #expect(runs.map { $0.totalSize } == [coalescer.maxSegments * 100, 300])
    }

    @Test
    func runIsCappedAtMaxCoalescedSize() throws {
        let maxSegments = 64
        let maxCoalescedSize = 65_536

        var coalescer = self.makeCoalescer(
            maxSegments: maxSegments,
            maxCoalescedSize: maxCoalescedSize
        )

        // 64 * 1400 = 89600 bytes: more than maxCoalescedSize
        for _ in 0..<coalescer.maxSegments {
            coalescer.append(self.buffer(1400))
        }

        let runs = self.drain(&coalescer)
        try #require(runs.count == 2)

        #expect(runs[0].totalSize == (maxCoalescedSize / 1400) * 1400)
        #expect(runs[0].segmentSize == 1400)

        #expect(runs[1].totalSize == (maxSegments * 1400) - runs[0].totalSize)
        #expect(runs[1].segmentSize == 1400)
    }

    @Test
    func oversizedDatagramIsEmittedAlone() {
        var coalescer = self.makeCoalescer()
        let oversized = coalescer.maxCoalescedSize + 1
        coalescer.append(self.buffer(oversized))
        coalescer.append(self.buffer(1200))

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [nil, nil])
        #expect(runs.map { $0.totalSize } == [oversized, 1200])
    }

    @Test
    func maxSegmentsOfOneNeverCoalesces() {
        var coalescer = GSOCoalescer(remoteAddress: Self.peer, maxSegments: 1)
        for _ in 0..<3 {
            coalescer.append(self.buffer(1200))
        }

        let runs = self.drain(&coalescer)
        #expect(runs.map { $0.segmentSize } == [nil, nil, nil])
        #expect(runs.map { $0.totalSize } == [1200, 1200, 1200])
    }

    @Test
    func coalescedRunPreservesBytesInOrder() {
        var coalescer = self.makeCoalescer()
        for byte in UInt8(1)...UInt8(3) {
            coalescer.append(ByteBuffer(repeating: byte, count: 4))
        }

        let envelope = coalescer.next()
        #expect(envelope?.metadata?.segmentSize == 4)
        #expect(envelope.map { Array($0.data.readableBytesView) } == [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3])
        #expect(coalescer.next() == nil)
    }
}
