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

import NIOQUICHelpers
import Testing

@testable import NIOQUIC

struct QUICStreamIDCacheTests {
    @Test(arguments: [(1, 1), (5, 8), (16, 16), (17, 32)])
    func capacityIsRoundedUpToPowerOfTwo(capacity: Int, rounded: Int) {
        #expect(QUICStreamIDCache<Int>(capacity: capacity, threshold: 0.6).capacity == rounded)
    }

    @Test func insertThenLookup() {
        var cache = Self.cache(capacity: 8)

        for (index, id) in Self.streamIDs(count: 8).enumerated() {
            #expect(cache.updateValue(index, forID: id).isInserted)
            #expect(cache.contains(id))
            #expect(cache[id] == index)
        }

        #expect(cache.count == 8)
    }

    @Test func lookupUnknownID() {
        var cache = Self.cache(capacity: 8)
        cache.updateValue(42, forID: QUICStreamID(rawValue: 0))

        #expect(cache[QUICStreamID(rawValue: 4)] == nil)
        #expect(!cache.contains(QUICStreamID(rawValue: 4)))
    }

    @Test func insertReplacesValueForSameID() {
        var cache = Self.cache(capacity: 8)
        let id = QUICStreamID(rawValue: 4)
        cache.updateValue(1, forID: id)

        #expect(cache.updateValue(2, forID: id).replaced == 1)
        #expect(cache[id] == 2)
        #expect(cache.count == 1)
    }

    @Test func insertEvictsSlotOccupant() {
        var cache = Self.cache(capacity: 4)
        let (first, second) = Self.collidingIDs
        cache.updateValue(1, forID: first)

        let evicted = cache.updateValue(2, forID: second).evicted
        #expect(evicted?.id == first)
        #expect(evicted?.value == 1)

        #expect(cache[second] == 2)
        #expect(cache[first] == nil)
        #expect(cache.count == 1)
    }

    @Test func removeValue() {
        var cache = Self.cache(capacity: 8)
        let id = QUICStreamID(rawValue: 8)
        cache.updateValue(1, forID: id)

        #expect(cache.removeValue(forID: id) == 1)
        #expect(cache[id] == nil)
        #expect(cache.isEmpty)
        #expect(cache.removeValue(forID: id) == nil)
    }

    @Test func removeValueForIDOccupyingNoSlot() {
        var cache = Self.cache(capacity: 4)
        let (first, second) = Self.collidingIDs
        cache.updateValue(1, forID: first)

        #expect(cache.removeValue(forID: second) == nil)
        #expect(cache[first] == 1)
    }

    @Test func growthPreservesValues() {
        var cache = QUICStreamIDCache<Int>(capacity: 4, threshold: 0.6)
        let ids = Self.streamIDs(count: 32)

        for (index, id) in ids.enumerated() {
            #expect(cache.updateValue(index, forID: id).isInserted, "unexpected eviction of \(id)")
        }

        #expect(cache.count == 32)
        #expect(cache.capacity >= 32)

        for (index, id) in ids.enumerated() {
            #expect(cache[id] == index)
        }
    }

    @Test func removeAll() {
        var cache = Self.cache(capacity: 8)
        let ids = Self.streamIDs(count: 4)
        for (index, id) in ids.enumerated() {
            cache.updateValue(index, forID: id)
        }

        cache.removeAll()

        #expect(cache.isEmpty)
        for id in ids {
            #expect(cache[id] == nil)
        }
    }

    @Test func iterator() throws {
        var cache = Self.cache(capacity: 8)
        let ids = Self.streamIDs(count: 4)
        for (index, id) in ids.enumerated() {
            cache.updateValue(index, forID: id)
        }
        let values = Array(cache)
        try #require(values.count == 4)

        #expect(values[0] == (ids[0], 0))
        #expect(values[1] == (ids[1], 1))
        #expect(values[2] == (ids[2], 2))
        #expect(values[3] == (ids[3], 3))
    }

    /// Client-initiated bidirectional stream IDs which share a slot in a cache with four slots.
    private static var collidingIDs: (QUICStreamID, QUICStreamID) {
        (QUICStreamID(rawValue: 0), QUICStreamID(rawValue: 16))
    }

    private static func streamIDs(count: Int) -> [QUICStreamID] {
        (0..<count).map { QUICStreamID(rawValue: UInt64($0) << 2) }
    }

    /// A cache which evicts on a collision rather than growing.
    private static func cache(capacity: Int) -> QUICStreamIDCache<Int> {
        QUICStreamIDCache(capacity: capacity, threshold: 1.0)
    }
}

extension QUICStreamIDCache.UpdateResult {
    fileprivate var isInserted: Bool {
        switch self {
        case .inserted:
            return true
        case .replaced, .evicted:
            return false
        }
    }

    fileprivate var replaced: Value? {
        switch self {
        case .replaced(let value):
            return value
        case .inserted, .evicted:
            return nil
        }
    }

    fileprivate var evicted: (id: QUICStreamID, value: Value)? {
        switch self {
        case .evicted(let id, let value):
            return (id, value)
        case .inserted, .replaced:
            return nil
        }
    }
}
