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

/// A token bucket: holds at most `capacity` tokens, refilled continuously at one token every
/// `refillInterval`.
struct TokenBucket {
    /// The maximum number of tokens the bucket can hold.
    private let capacity: Int
    /// How long it takes to generate one token.
    private let refillInterval: TimeAmount
    private var availableTokens: Int
    private var lastRefill: NIODeadline

    /// - Parameters:
    ///   - capacity: The maximum number of tokens the bucket can hold. Must be positive.
    ///   - refillInterval: How long it takes to generate one token. Must be positive.
    init(capacity: Int, refillInterval: TimeAmount, now: NIODeadline = .now()) {
        precondition(capacity > 0, "capacity must be positive")
        precondition(refillInterval.nanoseconds > 0, "refillInterval must be positive")
        self.capacity = capacity
        self.refillInterval = refillInterval
        self.availableTokens = capacity
        self.lastRefill = now
    }

    /// Consumes one token if one is available, refilling first based on elapsed time.
    mutating func tryConsume(now: NIODeadline = .now()) -> Bool {
        let elapsed = now - self.lastRefill
        let refillIntervalNanoseconds = self.refillInterval.nanoseconds
        let tokensGained = max(0, Int(elapsed.nanoseconds / refillIntervalNanoseconds))
        self.availableTokens = min(self.capacity, self.availableTokens + tokensGained)
        self.lastRefill = self.lastRefill + .nanoseconds(Int64(tokensGained) * refillIntervalNanoseconds)

        if self.availableTokens >= 1 {
            self.availableTokens -= 1
            return true
        }
        return false
    }
}

@available(*, unavailable)
extension TokenBucket: Sendable {}
