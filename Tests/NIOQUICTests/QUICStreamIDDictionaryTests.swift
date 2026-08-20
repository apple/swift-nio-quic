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

struct QUICStreamIDDictionaryTests {
    @available(anyAppleOS 26, *)
    @Test func empty() {
        let dictionary = QUICStreamIDDictionary<Int>()
        #expect(dictionary.count == 0)
        #expect(dictionary.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test func updateNewValue() {
        var dictionary = QUICStreamIDDictionary<Int>()
        let previous = dictionary.updateValue(42, forID: 1)
        #expect(previous == nil)
        #expect(dictionary.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test func updateExistingValue() {
        var dictionary = QUICStreamIDDictionary<Int>()
        dictionary.updateValue(42, forID: 1)
        let previous = dictionary.updateValue(41, forID: 1)
        #expect(previous == 42)
        #expect(dictionary.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test func setNewValue() {
        var dictionary = QUICStreamIDDictionary<Int>()
        dictionary[1] = 42
        #expect(dictionary.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test func setExistingValue() {
        var dictionary = QUICStreamIDDictionary<Int>()
        dictionary[1] = 42
        #expect(dictionary.count == 1)
        dictionary[1] = 41
        #expect(dictionary.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test func setRemoveExistingValue() {
        var dictionary = QUICStreamIDDictionary<Int>()
        dictionary[1] = 42
        #expect(dictionary.count == 1)
        dictionary[1] = nil
        #expect(dictionary.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test func getValue() {
        var dictionary = QUICStreamIDDictionary<Int>()
        dictionary[1] = 42
        #expect(dictionary[1] == 42)
    }

    @available(anyAppleOS 26, *)
    @Test func removeValue() {
        var dictionary = QUICStreamIDDictionary<Int>()
        dictionary[1] = 42
        #expect(dictionary.removeValue(forID: 0) == nil)
        #expect(dictionary.removeValue(forID: 1) == 42)
        #expect(dictionary.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test func removeUnknownValue() {
        var dictionary = QUICStreamIDDictionary<Int>()
        #expect(dictionary.removeValue(forID: 0) == nil)
        #expect(dictionary.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test func streamTypesHaveIndependentKeys() {
        var dictionary = QUICStreamIDDictionary<Int>()

        // The IDs share a numeric part and differ only in their type bits.
        for typeBits in UInt64(0)..<4 {
            dictionary[QUICStreamID(rawValue: typeBits)] = Int(typeBits)
        }

        for typeBits in UInt64(0)..<4 {
            #expect(dictionary[QUICStreamID(rawValue: typeBits)] == Int(typeBits))
        }

        #expect(dictionary.count == 4)
    }

    @available(anyAppleOS 26, *)
    @Test func evictedValuesRemainReadable() {
        var dictionary = Self.evictingDictionary()
        let (evicted, evictor) = Self.collidingIDs

        dictionary[evicted] = 1
        dictionary[evictor] = 2

        #expect(dictionary[evicted] == 1)
        #expect(dictionary[evictor] == 2)
        #expect(dictionary.contains(evicted))
        #expect(dictionary.count == 2)
    }

    @available(anyAppleOS 26, *)
    @Test func evictedValuesAreUpdatedInPlace() {
        var dictionary = Self.evictingDictionary()
        let (evicted, evictor) = Self.collidingIDs

        dictionary[evicted] = 1
        dictionary[evictor] = 2

        #expect(dictionary.updateValue(3, forID: evicted) == 1)
        #expect(dictionary[evicted] == 3)
        #expect(dictionary.count == 2)
    }

    @available(anyAppleOS 26, *)
    @Test func evictedValuesCanBeRemoved() {
        var dictionary = Self.evictingDictionary()
        let (evicted, evictor) = Self.collidingIDs

        dictionary[evicted] = 1
        dictionary[evictor] = 2

        #expect(dictionary.removeValue(forID: evicted) == 1)
        #expect(dictionary[evicted] == nil)
        #expect(dictionary[evictor] == 2)
        #expect(dictionary.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test func manyStreamsOfEachType() {
        var dictionary = QUICStreamIDDictionary<Int>(initialCacheCapacity: 4)
        let ids = (0..<256).map { QUICStreamID(rawValue: UInt64($0)) }

        for (index, id) in ids.enumerated() {
            dictionary[id] = index
        }

        #expect(dictionary.count == ids.count)
        for (index, id) in ids.enumerated() {
            #expect(dictionary[id] == index)
        }

        for id in ids {
            dictionary.removeValue(forID: id)
        }

        #expect(dictionary.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test func removeAll() {
        var dictionary = Self.evictingDictionary()
        let (evicted, evictor) = Self.collidingIDs
        dictionary[evicted] = 1
        dictionary[evictor] = 2

        dictionary.removeAll()

        #expect(dictionary.isEmpty)
        #expect(dictionary[evicted] == nil)
        #expect(dictionary[evictor] == nil)
    }

    @available(anyAppleOS 26, *)
    @Test func updatingCollidingStreamsDoesNotSwapThemInAndOutOfTheCache() {
        var dictionary = Self.evictingDictionary()
        let (evicted, evictor) = Self.collidingIDs
        dictionary[evicted] = 1
        dictionary[evictor] = 2
        #expect(dictionary._testOnly_isCached(evictor))
        #expect(!dictionary._testOnly_isCached(evicted))

        for value in 3...6 {
            dictionary[evicted] = value

            #expect(dictionary._testOnly_isCached(evictor))
            #expect(!dictionary._testOnly_isCached(evicted))
            #expect(dictionary[evicted] == value)
            #expect(dictionary[evictor] == 2)
            #expect(dictionary.count == 2)
        }
    }

    @available(anyAppleOS 26, *)
    @Test func evictedStreamStaysInTheOverflowDictionary() {
        var dictionary = Self.evictingDictionary()
        let (evicted, evictor) = Self.collidingIDs
        dictionary[evicted] = 1
        dictionary[evictor] = 2

        dictionary.removeValue(forID: evictor)
        #expect(dictionary.updateValue(3, forID: evicted) == 1)

        #expect(!dictionary._testOnly_isCached(evicted))
        #expect(dictionary[evicted] == 3)
        #expect(dictionary.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test func overflowSwitchesToDictionaryWhenArrayIsFull() {
        var dictionary = Self.evictingDictionary()
        let ids = Self.collidingIDs(Self.overflowArrayCapacity + 2)

        for (index, id) in ids.enumerated() {
            dictionary[id] = index
        }

        // Each ID evicts its predecessor: the last stays in the cache and every other ID
        // overflows, which is one more than the array holds, so all of them move to the dictionary.
        #expect(dictionary._testOnly_isCached(ids.last!))
        for id in ids.dropLast() {
            #expect(!dictionary._testOnly_isInOverflowArray(id))
        }

        for (index, id) in ids.enumerated() {
            #expect(dictionary[id] == index)
        }
        #expect(dictionary.count == ids.count)
    }

    @available(anyAppleOS 26, *)
    @Test func overflowSwitchesBackToArrayWhenDictionaryIsEmptied() throws {
        var dictionary = Self.evictingDictionary()
        // One more than is needed to spill into the overflow dictionary; the extra ID triggers a
        // final eviction once the dictionary has been emptied.
        let ids = Self.collidingIDs(Self.overflowArrayCapacity + 3)
        let spilling = ids.dropLast()

        for (index, id) in spilling.enumerated() {
            dictionary[id] = index
        }
        try #require(!dictionary._testOnly_isInOverflowArray(ids[0]))

        // Remove everything but the cached ID.
        for id in spilling.dropLast() {
            #expect(dictionary.removeValue(forID: id) != nil)
        }

        // The dictionary is empty again, so the next evicted value goes back into the array.
        let cached = spilling.last!
        let cachedValue = spilling.count - 1
        dictionary[ids.last!] = ids.count - 1
        #expect(dictionary._testOnly_isInOverflowArray(cached))
        #expect(dictionary[cached] == cachedValue)
        #expect(dictionary[ids.last!] == ids.count - 1)
        #expect(dictionary.count == 2)
    }

    @available(anyAppleOS 26, *)
    @Test func spilledOverflowValuesCanBeUpdatedAndRemoved() {
        var dictionary = Self.evictingDictionary()
        let ids = Self.collidingIDs(Self.overflowArrayCapacity + 2)

        for (index, id) in ids.enumerated() {
            dictionary[id] = index
        }

        #expect(dictionary.updateValue(42, forID: ids[2]) == 2)
        #expect(dictionary[ids[2]] == 42)
        #expect(dictionary.removeValue(forID: ids[2]) == 42)
        #expect(dictionary[ids[2]] == nil)
        #expect(dictionary.count == ids.count - 1)
    }

    @available(anyAppleOS 26, *)
    @Test func iteratorEmpty() {
        let dictionary = QUICStreamIDDictionary<Int>()
        #expect(Array(dictionary).isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test func iteratorVisitsEachCache() throws {
        var dictionary = QUICStreamIDDictionary<Int>(initialCacheCapacity: 8)
        // Cover all stream ID types
        let ids = (0..<16).map { QUICStreamID(rawValue: UInt64($0)) }
        for (index, id) in ids.enumerated() {
            dictionary[id] = index
        }

        let elements = Array(dictionary)
        try #require(elements.count == ids.count)
        let expected = Dictionary(uniqueKeysWithValues: zip(ids, ids.indices))
        #expect(Dictionary(uniqueKeysWithValues: elements) == expected)
    }

    @available(anyAppleOS 26, *)
    @Test func iteratorIncludesOverflowValues() throws {
        var dictionary = Self.evictingDictionary()
        let (evicted, evictor) = Self.collidingIDs
        dictionary[evicted] = 1
        dictionary[evictor] = 2
        try #require(!dictionary._testOnly_isCached(evicted))

        let elements = Array(dictionary)
        try #require(elements.count == 2)
        #expect(Dictionary(uniqueKeysWithValues: elements) == [evicted: 1, evictor: 2])
    }

    /// Client-initiated bidirectional stream IDs which share a slot in a cache with four slots.
    private static var collidingIDs: (QUICStreamID, QUICStreamID) {
        let ids = Self.collidingIDs(2)
        return (ids[0], ids[1])
    }

    /// Client-initiated bidirectional stream IDs which share a slot in a cache with four slots.
    private static func collidingIDs(_ count: Int) -> [QUICStreamID] {
        (0..<count).map { QUICStreamID(rawValue: UInt64($0) * 16) }
    }

    /// Mirrors `QUICStreamIDDictionary.overflowArrayCapacity`, which is private.
    private static let overflowArrayCapacity = 32

    /// A dictionary which evicts on a collision rather than growing its caches.
    @available(anyAppleOS 26, *)
    private static func evictingDictionary() -> QUICStreamIDDictionary<Int> {
        QUICStreamIDDictionary(initialCacheCapacity: 4, cacheGrowthThreshold: 1.0)
    }
}
