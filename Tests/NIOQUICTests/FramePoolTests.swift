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
import Testing

@testable import NIOQUIC

struct FramePoolTests {
    @available(anyAppleOS 26, *)
    func makeFramePool(
        smallFrameSize: Int = 100,
        maxSmallFrames: Int = 16,
        largeFrameSize: Int = 200,
        maxLargeFrames: Int = 16
    ) -> FramePool {
        FramePool(
            smallFrameSize: smallFrameSize,
            maxSmallFrames: maxSmallFrames,
            largeFrameSize: largeFrameSize,
            maxLargeFrames: maxLargeFrames
        )
    }

    @Test
    @available(anyAppleOS 26, *)
    func empty() {
        let pool = self.makeFramePool(smallFrameSize: 100, largeFrameSize: 200)
        #expect(pool.smallFrameCount == 0)
        #expect(pool.largeFrameCount == 0)

        expectNil(pool.takeFrame(minimumSize: 0))
        expectNil(pool.takeFrame(minimumSize: 100))
        expectNil(pool.takeFrame(minimumSize: 200))
        expectNil(pool.takeFrame(minimumSize: 300))
    }

    @Test
    @available(anyAppleOS 26, *)
    func initPreconditions() async {
        // Sizes must be >= 0
        await #expect(processExitsWith: .failure) {
            _ = FramePool(smallFrameSize: -1, maxSmallFrames: 1, largeFrameSize: 1, maxLargeFrames: 1)
        }
        await #expect(processExitsWith: .failure) {
            _ = FramePool(smallFrameSize: 1, maxSmallFrames: 1, largeFrameSize: -1, maxLargeFrames: 1)
        }

        // max frame counts must be >= 0
        await #expect(processExitsWith: .failure) {
            _ = FramePool(smallFrameSize: 1, maxSmallFrames: -1, largeFrameSize: 2, maxLargeFrames: 1)
        }
        await #expect(processExitsWith: .failure) {
            _ = FramePool(smallFrameSize: 1, maxSmallFrames: 1, largeFrameSize: 2, maxLargeFrames: -1)
        }

        // large frame size must be > small frame size
        await #expect(processExitsWith: .failure) {
            _ = FramePool(smallFrameSize: 10, maxSmallFrames: 1, largeFrameSize: 9, maxLargeFrames: 1)
        }
    }

    @Test(
        arguments: [
            // Small
            (0, 100),
            (100, 100),
            // Large
            (101, 200),
            (200, 200),
            // Unpooled
            (201, 201),
            (202, 202),
        ]
    )
    @available(anyAppleOS 26, *)
    func appropriateSizeFrameIsCreated(requestedSize: Int, expectedSize: Int) {
        let pool = self.makeFramePool(smallFrameSize: 100, largeFrameSize: 200)

        var frame = pool.takeOrCreateFrame(minimumSize: requestedSize)
        #expect(frame.unclaimedLength == expectedSize)
        frame.finalize(success: true)
    }

    @Test
    @available(anyAppleOS 26, *)
    func returnedFrameGoesInRightBucket() {
        let pool = self.makeFramePool(smallFrameSize: 100, largeFrameSize: 200)

        let small = pool.takeOrCreateFrame(minimumSize: 100)
        let storedSmall = pool.storeFrame(small)
        #expect(storedSmall)
        #expect(pool.smallFrameCount == 1)

        let large = pool.takeOrCreateFrame(minimumSize: 200)
        let storedLarge = pool.storeFrame(large)
        #expect(storedLarge)
        #expect(pool.largeFrameCount == 1)

        let tooLarge = pool.takeOrCreateFrame(minimumSize: 201)
        let storedTooLarge = pool.storeFrame(tooLarge)
        #expect(!storedTooLarge)
        // Still 1 each.
        #expect(pool.smallFrameCount == 1)
        #expect(pool.largeFrameCount == 1)
    }

    @Test(arguments: [true, false])
    @available(anyAppleOS 26, *)
    func capacityIsRespected(small: Bool) {
        let size = small ? 100 : 200
        let pool = self.makeFramePool(
            smallFrameSize: 100,
            maxSmallFrames: 4,
            largeFrameSize: 200,
            maxLargeFrames: 4
        )

        // Create 5 frames.
        var frames = UniqueArray<Frame>()
        for _ in 1...5 {
            let frame = pool.takeOrCreateFrame(minimumSize: size)
            frames.append(frame)
        }

        for expectedCount in 1...4 {
            let frame = frames.removeLast()
            let stored = pool.storeFrame(frame)
            #expect(stored)

            if small {
                #expect(pool.smallFrameCount == expectedCount)
                #expect(pool.largeFrameCount == 0)
            } else {
                #expect(pool.smallFrameCount == 0)
                #expect(pool.largeFrameCount == expectedCount)
            }
        }

        let frame = frames.removeLast()
        let stored = pool.storeFrame(frame)
        #expect(!stored)
        if small {
            #expect(pool.smallFrameCount == 4)
            #expect(pool.largeFrameCount == 0)
        } else {
            #expect(pool.smallFrameCount == 0)
            #expect(pool.largeFrameCount == 4)
        }
    }

    @Test
    @available(anyAppleOS 26, *)
    func foreignFrameIsNotStored() {
        let pool = self.makeFramePool(smallFrameSize: 100, largeFrameSize: 200)
        // Right size, wrong backing type.
        let frame = Frame(count: 100)
        let stored = pool.storeFrame(frame)
        #expect(!stored)
    }
}

private func expectNil<T: ~Copyable>(_ expression: @autoclosure () -> T?) {
    let value = expression()
    switch value {
    case .some:
        Issue.record("Expected nil but found a value")
    case .none:
        ()
    }
}
