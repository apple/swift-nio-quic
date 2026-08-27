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

/// A specialized dictionary where QUIC stream IDs are used as keys.
///
/// The dictionary takes advantage of the layout of stream IDs and how they are used in a QUIC
/// connection to provide significantly faster operations than a regular dictionary keyed by stream
/// ID by avoiding hashing in the majority of cases.
///
/// It works by maintaining a cache per stream type which is indexed by the stream ID's numeric bytes
/// (i.e. `rawStreamID >> 2`) modulo the capacity of the cache at the time. Any collisions (in other
/// words, long-lived streams) get evicted and stored into overflow storage.
///
/// Each cache is separately sized and doubles in capacity when its load factor (ratio of occupied
/// slots to capacity) exceeds a threshold. This is naturally bounded by the connection's limit on
/// concurrent streams.
///
/// Overflow storage has two tiers: when there are no more than 32 elements in overflow storage an
/// array is used and a linear scan is used to find the appropriate element. For 33 elements and
/// above a regular dictionary is used. Only one storage tier is used at any one time.
@available(anyAppleOS 26, *)
struct QUICStreamIDDictionary<Value> {
    /// Caches keyed by the "type bits" of a QUIC stream ID (i.e. `rawValue & 0b11`).
    private var caches: InlineArray<4, QUICStreamIDCache<Value>>

    private struct OverflowEntry {
        var id: QUICStreamID
        var value: Value
    }

    /// Array of overflow values. Used when the number of overflow values is low.
    ///
    /// Values are moved to the overflow dictionary when `overflowArrayCapacity` are stored. The
    /// order of elements has no semantic meaning.
    ///
    /// Note: Empty when `overflowDictionary` is non-empty.
    private var overflowArray: [OverflowEntry]

    /// Dictionary of overflow values.
    ///
    /// Note: Empty when `overflowArray` is non-empty.
    private var overflowDictionary: [QUICStreamID: Value]

    /// Number of items in overflow storage.
    ///
    /// Avoids loading a value from the array/dictionary's heap storage on the hot-path.
    private var overflowCount: Int

    /// The number of values held in `overflowArray` before switching to `overflowDictionary`.
    private static var overflowArrayCapacity: Int { 32 }

    /// Returns the cache index to use for a given stream ID.
    private func cacheIndex(of id: QUICStreamID) -> Int {
        // Bottom two bits are the type bits.
        Int(id.rawValue & 0b11)
    }

    /// Returns the number of elements in the dictionary.
    var count: Int {
        var total = self.overflowCount
        total += self.caches[0].count
        total += self.caches[1].count
        total += self.caches[2].count
        total += self.caches[3].count
        return total
    }

    /// Returns whether the dictionary is empty.
    var isEmpty: Bool {
        self.count == 0
    }

    /// Create a new dictionary keyed by QUIC stream IDs.
    ///
    /// - Parameters:
    ///   - initialCacheCapacity: The initial capacity of each cache, rounded up to the next power
    ///     of two. Defaults to 16.
    ///   - cacheGrowthThreshold: The utilisation threshold above which a cache will grow, defaults
    ///     to 0.6.
    init(initialCacheCapacity: Int = 16, cacheGrowthThreshold: Double = 0.6) {
        self.caches = [
            QUICStreamIDCache(capacity: initialCacheCapacity, threshold: cacheGrowthThreshold),
            QUICStreamIDCache(capacity: initialCacheCapacity, threshold: cacheGrowthThreshold),
            QUICStreamIDCache(capacity: initialCacheCapacity, threshold: cacheGrowthThreshold),
            QUICStreamIDCache(capacity: initialCacheCapacity, threshold: cacheGrowthThreshold),
        ]
        self.overflowArray = []
        self.overflowDictionary = [:]
        self.overflowCount = 0
    }

    /// Returns whether the dictionary contains a value for the given stream ID.
    func contains(_ id: QUICStreamID) -> Bool {
        if self.caches[self.cacheIndex(of: id)].contains(id) {
            return true
        } else {
            return self.overflowIndex(of: id) != nil
        }
    }

    /// Returns or updates the value associated with a given ID.
    subscript(id: QUICStreamID) -> Value? {
        get {
            if let value = self.caches[self.cacheIndex(of: id)][id] {
                return value
            } else if let position = self.overflowIndex(of: id) {
                return self.overflowValue(at: position)
            } else {
                return nil
            }
        }
        set {
            if let newValue {
                self.updateValue(newValue, forID: id)
            } else {
                self.removeValue(forID: id)
            }
        }
    }

    /// Updates the value for a given ID, returning the previously set value.
    ///
    /// - Parameters:
    ///   - value: The new value.
    ///   - id: The stream ID to update.
    /// - Returns: The value previously set for the given ID.
    @discardableResult
    mutating func updateValue(_ value: Value, forID id: QUICStreamID) -> Value? {
        let previous: Value?

        if self.overflowCount == 0 || self.caches[self.cacheIndex(of: id)].contains(id) {
            // Fast-path: no-overflow values.
            previous = self.insert(value, forID: id)
        } else if let position = self.overflowIndex(of: id) {
            // Value was in the overflow storage.
            previous = self.overflowValue(at: position)
            self.setOverflowValue(value, at: position)
        } else {
            // Value wasn't in overflow.
            previous = self.insert(value, forID: id)
        }

        return previous
    }

    /// Removes the value associated with the given ID.
    @discardableResult
    mutating func removeValue(forID id: QUICStreamID) -> Value? {
        if let removed = self.caches[self.cacheIndex(of: id)].removeValue(forID: id) {
            return removed
        } else if self.overflowCount == 0 {
            return nil
        } else {
            return self.removeOverflowValue(forID: id)
        }
    }

    /// Insert a value to the cache, moving evicted values into the overflow storage.
    private mutating func insert(_ value: Value, forID id: QUICStreamID) -> Value? {
        switch self.caches[self.cacheIndex(of: id)].updateValue(value, forID: id) {
        case .replaced(let previous):
            return previous
        case .inserted:
            return nil
        case .evicted(let evictedID, let evictedValue):
            self.insertOverflow(evictedValue, forID: evictedID)
            return nil
        }
    }

    /// Returns whether the given ID is held in a cache rather than the overflow storage.
    func _testOnly_isCached(_ id: QUICStreamID) -> Bool {
        self.caches[self.cacheIndex(of: id)].contains(id)
    }

    /// Returns whether the given ID is held in the linearly scanned part of the overflow storage.
    func _testOnly_isInOverflowArray(_ id: QUICStreamID) -> Bool {
        switch self.overflowIndex(of: id) {
        case .array:
            return true
        case .dictionary, .none:
            return false
        }
    }

    /// Removes all values.
    mutating func removeAll() {
        for index in self.caches.indices {
            self.caches[index].removeAll()
        }
        self.overflowArray.removeAll(keepingCapacity: true)
        self.overflowDictionary.removeAll()
        self.overflowCount = 0
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamIDDictionary {
    fileprivate enum OverflowIndex {
        case array(Int)
        case dictionary(Dictionary<QUICStreamID, Value>.Index)
    }

    @inline(__always)
    private func overflowIndex(of id: QUICStreamID) -> OverflowIndex? {
        if self.overflowDictionary.isEmpty {
            for index in self.overflowArray.indices {
                if self.overflowArray[index].id == id {
                    return .array(index)
                }
            }
            return nil
        } else if let index = self.overflowDictionary.index(forKey: id) {
            return .dictionary(index)
        } else {
            return nil
        }
    }

    @inline(__always)
    private func overflowValue(at position: OverflowIndex) -> Value {
        switch position {
        case .array(let index):
            return self.overflowArray[index].value
        case .dictionary(let index):
            return self.overflowDictionary.values[index]
        }
    }

    @inline(never)
    private mutating func setOverflowValue(_ value: Value, at position: OverflowIndex) {
        switch position {
        case .array(let index):
            self.overflowArray[index].value = value
        case .dictionary(let index):
            self.overflowDictionary.values[index] = value
        }
    }

    @inline(never)
    private mutating func insertOverflow(_ value: Value, forID id: QUICStreamID) {
        if self.overflowDictionary.isEmpty {
            if self.overflowArray.count < Self.overflowArrayCapacity {
                if self.overflowArray.isEmpty {
                    self.overflowArray.reserveCapacity(Self.overflowArrayCapacity)
                }
                self.overflowArray.append(OverflowEntry(id: id, value: value))
            } else {
                self.switchOverflowToDictionary(inserting: value, forID: id)
            }
        } else {
            self.overflowDictionary[id] = value
        }
        self.overflowCount &+= 1
    }

    @inline(never)
    private mutating func switchOverflowToDictionary(
        inserting value: Value,
        forID id: QUICStreamID
    ) {
        self.overflowDictionary.reserveCapacity(self.overflowArray.count + 1)
        for entry in self.overflowArray {
            self.overflowDictionary[entry.id] = entry.value
        }
        self.overflowDictionary[id] = value
        self.overflowArray.removeAll(keepingCapacity: true)
    }

    @inline(never)
    private mutating func removeOverflowValue(forID id: QUICStreamID) -> Value? {
        switch self.overflowIndex(of: id) {
        case .array(let index):
            self.overflowCount &-= 1
            // Order has no meaning: swap with the final element to make removal O(1).
            let lastIndex = self.overflowArray.index(before: self.overflowArray.endIndex)
            self.overflowArray.swapAt(index, lastIndex)
            return self.overflowArray.removeLast().value

        case .dictionary(let index):
            self.overflowCount &-= 1
            return self.overflowDictionary.remove(at: index).value

        case .none:
            return nil
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamIDDictionary: Sequence {
    typealias Element = (QUICStreamID, Value)

    func makeIterator() -> Iterator {
        Iterator(storage: self)
    }

    struct Iterator: IteratorProtocol {
        private let storage: QUICStreamIDDictionary<Value>
        private var state: State

        private enum State {
            case iteratingCache(Int, QUICStreamIDCache<Value>.Iterator)
            case iteratingOverflowArray([OverflowEntry].Iterator)
            case iteratingOverflowDictionary([QUICStreamID: Value].Iterator)
            case finished
        }

        fileprivate init(storage: QUICStreamIDDictionary<Value>) {
            let index = storage.caches.startIndex
            self.state = .iteratingCache(index, storage.caches[index].makeIterator())
            self.storage = storage
        }

        mutating func next() -> (QUICStreamID, Value)? {
            while true {
                switch self.state {
                case .iteratingCache(let index, var iterator):
                    self.state = .finished

                    if let value = iterator.next() {
                        self.state = .iteratingCache(index, iterator)
                        return value
                    } else {
                        let nextIndex = self.storage.caches.index(after: index)

                        if nextIndex == self.storage.caches.endIndex {
                            let iterator = self.storage.overflowArray.makeIterator()
                            self.state = .iteratingOverflowArray(iterator)
                        } else {
                            // Next cache.
                            let nextIterator = self.storage.caches[nextIndex].makeIterator()
                            self.state = .iteratingCache(nextIndex, nextIterator)
                        }
                    }

                case .iteratingOverflowArray(var iterator):
                    self.state = .finished

                    if let entry = iterator.next() {
                        self.state = .iteratingOverflowArray(iterator)
                        return (entry.id, entry.value)
                    } else {
                        let iterator = self.storage.overflowDictionary.makeIterator()
                        self.state = .iteratingOverflowDictionary(iterator)
                    }

                case .iteratingOverflowDictionary(var iterator):
                    self.state = .finished

                    if let value = iterator.next() {
                        self.state = .iteratingOverflowDictionary(iterator)
                        return value
                    } else {
                        self.state = .finished
                    }

                case .finished:
                    return nil
                }
            }
        }
    }
}
