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

struct TokenBucketTests {
    @Test
    func burstUpToCapacityIsAllowedInstantly() {
        var bucket = TokenBucket(capacity: 2, refillInterval: .milliseconds(500), now: .uptimeNanoseconds(0))
        let first = bucket.tryConsume(now: .uptimeNanoseconds(0))
        let second = bucket.tryConsume(now: .uptimeNanoseconds(0))
        #expect(first)
        #expect(second)
    }

    @Test
    func exceedingCapacityIsRejected() {
        var bucket = TokenBucket(capacity: 2, refillInterval: .milliseconds(500), now: .uptimeNanoseconds(0))
        let first = bucket.tryConsume(now: .uptimeNanoseconds(0))
        let second = bucket.tryConsume(now: .uptimeNanoseconds(0))
        let third = bucket.tryConsume(now: .uptimeNanoseconds(0))
        #expect(first)
        #expect(second)
        #expect(!third)
    }

    @Test
    func tokensRefillProportionallyToElapsedTime() {
        var bucket = TokenBucket(capacity: 2, refillInterval: .milliseconds(500), now: .uptimeNanoseconds(0))
        let first = bucket.tryConsume(now: .uptimeNanoseconds(0))
        let second = bucket.tryConsume(now: .uptimeNanoseconds(0))
        let third = bucket.tryConsume(now: .uptimeNanoseconds(0))
        #expect(first)
        #expect(second)
        #expect(!third)

        // One refill interval (500ms) later, exactly one token is back.
        let afterOneInterval = bucket.tryConsume(now: .uptimeNanoseconds(500_000_000))
        let afterOneIntervalAgain = bucket.tryConsume(now: .uptimeNanoseconds(500_000_000))
        #expect(afterOneInterval)
        #expect(!afterOneIntervalAgain)
    }

    @Test
    func refillNeverExceedsCapacity() {
        var bucket = TokenBucket(capacity: 2, refillInterval: .milliseconds(500), now: .uptimeNanoseconds(0))

        // An hour of idle time shouldn't bank more than the 2-token capacity.
        let first = bucket.tryConsume(now: .uptimeNanoseconds(3_600_000_000_000))
        let second = bucket.tryConsume(now: .uptimeNanoseconds(3_600_000_000_000))
        let third = bucket.tryConsume(now: .uptimeNanoseconds(3_600_000_000_000))
        #expect(first)
        #expect(second)
        #expect(!third)
    }

    @Test
    func capacityAndRefillIntervalAreIndependent() {
        var bucket = TokenBucket(capacity: 5, refillInterval: .seconds(1), now: .uptimeNanoseconds(0))
        for _ in 0..<5 {
            let consumed = bucket.tryConsume(now: .uptimeNanoseconds(0))
            #expect(consumed)
        }
        let afterBurst = bucket.tryConsume(now: .uptimeNanoseconds(0))
        #expect(!afterBurst)

        // One second later, only 1 token (one refill interval) has come back, not 5.
        let afterOneSecond = bucket.tryConsume(now: .uptimeNanoseconds(1_000_000_000))
        let afterOneSecondAgain = bucket.tryConsume(now: .uptimeNanoseconds(1_000_000_000))
        #expect(afterOneSecond)
        #expect(!afterOneSecondAgain)
    }

    @Test
    func partialIntervalsAccumulateAcrossCalls() {
        // Polling more often than the refill interval (500ms) must not lose progress: each
        // individual call sees less than one interval's worth of elapsed time, but a token should
        // still appear once their sum crosses one interval.
        var bucket = TokenBucket(capacity: 1, refillInterval: .milliseconds(500), now: .uptimeNanoseconds(0))
        let first = bucket.tryConsume(now: .uptimeNanoseconds(0))
        #expect(first)

        let at200ms = bucket.tryConsume(now: .uptimeNanoseconds(200_000_000))
        let at400ms = bucket.tryConsume(now: .uptimeNanoseconds(400_000_000))
        #expect(!at200ms)
        #expect(!at400ms)

        // 600ms since the token was consumed at t=0: a full 500ms interval has now elapsed.
        let at600ms = bucket.tryConsume(now: .uptimeNanoseconds(600_000_000))
        #expect(at600ms)
    }

    @Test
    func noTokenIsGrantedOneNanosecondShortOfAnInterval() {
        var bucket = TokenBucket(capacity: 1, refillInterval: .milliseconds(500), now: .uptimeNanoseconds(0))
        let first = bucket.tryConsume(now: .uptimeNanoseconds(0))
        #expect(first)

        // One nanosecond short of the 500ms interval: integer division must truncate, not round.
        let justShort = bucket.tryConsume(now: .uptimeNanoseconds(499_999_999))
        #expect(!justShort)

        // The previous call gained zero tokens, so it must not have advanced `lastRefill` either
        // — elapsed time is still measured from t=0, and exactly 500ms have now passed.
        let exactlyOneInterval = bucket.tryConsume(now: .uptimeNanoseconds(500_000_000))
        #expect(exactlyOneInterval)
    }

    @Test
    func timeSpentAtCapacityDoesNotCountTowardsTheNextToken() {
        // Refill progress made while the bucket is full must be discarded,
        // not banked towards the token after the next consumption.
        var bucket = TokenBucket(capacity: 1, refillInterval: .seconds(1), now: .uptimeNanoseconds(0))

        // The initial token is held until t=999ms.
        let at999ms = bucket.tryConsume(now: .uptimeNanoseconds(999_000_000))
        #expect(at999ms)

        // Only 1ms has passed since the token was consumed.
        let at1000ms = bucket.tryConsume(now: .uptimeNanoseconds(1_000_000_000))
        #expect(!at1000ms)

        // One full interval after the consumption at t=999ms, the token is back.
        let at1999ms = bucket.tryConsume(now: .uptimeNanoseconds(1_999_000_000))
        #expect(at1999ms)
    }

    @Test
    func timeSpentAtCapacityAfterRefillingDoesNotCountTowardsTheNextToken() {
        // Like `timeSpentAtCapacityDoesNotCountTowardsTheNextToken`, but the bucket reaches
        // capacity by refilling instead of starting full.
        var bucket = TokenBucket(capacity: 1, refillInterval: .seconds(1), now: .uptimeNanoseconds(0))
        let atStart = bucket.tryConsume(now: .uptimeNanoseconds(0))
        #expect(atStart)

        // The bucket is full again from t=1000ms, and the token is held until t=1999ms.
        let at1999ms = bucket.tryConsume(now: .uptimeNanoseconds(1_999_000_000))
        #expect(at1999ms)

        // Only 1ms has passed since the token was consumed.
        let at2000ms = bucket.tryConsume(now: .uptimeNanoseconds(2_000_000_000))
        #expect(!at2000ms)
    }
}
