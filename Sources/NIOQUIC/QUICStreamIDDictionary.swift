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
/// words, long-lived streams) get evicted and stored into an overflow dictionary (a regular
/// dictionary). Each cache is separately sized and doubles in capacity when its load factor (ratio
/// of occupied slots to capacity) exceeds a threshold. This is naturally bounded by the connections
/// limit on concurrent streams.
@available(anyAppleOS 26, *)
struct QUICStreamIDDictionary<Value> {
    /// An overflow dictionary for when a value is evicted from a cache.
    private var overflow: [QUICStreamID: Value]

    /// Caches keyed by the "type bits" of a QUIC stream ID (i.e. `rawValue & 0b11`).
    private var caches: InlineArray<4, QUICStreamIDCache<Value>>

    /// Returns the cache index to use for a given stream ID.
    private func cacheIndex(of id: QUICStreamID) -> Int {
        // Bottom two bits are the type bits.
        Int(id.rawValue & 0b11)
    }

    /// Returns the number of elements in the dictionary.
    var count: Int {
        var total = self.overflow.count
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
        self.overflow = [:]
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
            } else if let index = self.overflowIndex(of: id) {
                return self.overflow.values[index]
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

        if self.overflow.isEmpty {
            // Fast-path: no-overflow values.
            previous = self.insert(value, forID: id)
        } else if let index = self.overflow.index(forKey: id) {
            // Value was in the overflow storage.
            previous = self.overflow.values[index]
            self.overflow.values[index] = value
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
        } else if self.overflow.isEmpty {
            return nil
        } else {
            return self.overflow.removeValue(forKey: id)
        }
    }

    /// Insert a value to the cache, moving evicted values into the cachee
    private mutating func insert(_ value: Value, forID id: QUICStreamID) -> Value? {
        switch self.caches[self.cacheIndex(of: id)].updateValue(value, forID: id) {
        case .replaced(let previous):
            return previous
        case .inserted:
            return nil
        case .evicted(let evictedID, let evictedValue):
            self.overflow[evictedID] = evictedValue
            return nil
        }
    }

    /// Returns whether the given ID is held in a cache rather than the overflow dictionary.
    func _testOnly_isCached(_ id: QUICStreamID) -> Bool {
        self.caches[self.cacheIndex(of: id)].contains(id)
    }

    /// Removes all values.
    mutating func removeAll() {
        for index in self.caches.indices {
            self.caches[index].removeAll()
        }
        self.overflow.removeAll()
    }

    private func overflowIndex(of id: QUICStreamID) -> Dictionary<QUICStreamID, Value>.Index? {
        self.overflow.isEmpty ? nil : self.overflow.index(forKey: id)
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
            case iteratingOverflow([QUICStreamID: Value].Iterator)
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
                            // Move on the overflow dictionary.
                            self.state = .iteratingOverflow(self.storage.overflow.makeIterator())
                        } else {
                            // Next cache.
                            let nextIterator = self.storage.caches[nextIndex].makeIterator()
                            self.state = .iteratingCache(nextIndex, nextIterator)
                        }
                    }

                case .iteratingOverflow(var iterator):
                    self.state = .finished

                    if let value = iterator.next() {
                        self.state = .iteratingOverflow(iterator)
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
