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

/// A cache of values keyed by `QUICStreamID`. See ``QUICStreamIDDictionary``.
///
/// Slots are indexed by the numeric part of a stream ID (i.e. `rawValue >> 2`) modulo the capacity
/// of the cache, which is always a power of two so that the modulo can be done by masking.
struct QUICStreamIDCache<Value> {
    fileprivate struct Slot {
        private(set) var id: UInt64
        private(set) var value: Optional<Value>

        init(id: QUICStreamID, value: Value) {
            self.id = id.rawValue
            self.value = value
        }

        init() {
            self.id = .max  // Not a valid QUICStreamID
            self.value = nil
        }

        var isEmpty: Bool {
            self.id == .max
        }

        func containsID(_ id: QUICStreamID) -> Bool {
            self.id == id.rawValue
        }

        func value(forID id: QUICStreamID) -> Value? {
            self.id == id.rawValue ? self.value : nil
        }

        mutating func removeValue(forID id: QUICStreamID) -> Value? {
            var value: Value? = nil

            if self.id == id.rawValue {
                self.id = .max
                swap(&value, &self.value)
            }

            return value
        }
    }

    /// The underlying slots in the cache.
    private var slots: [Slot]

    /// A mask applied to the numeric part of a stream ID (i.e. top 62 bits) to get its slot index.
    /// Stored rather than recomputed to avoid the `Int` to `UInt64` conversion on every lookup.
    private var mask: UInt64

    /// The value of `count` at which the cache doubles in size.
    private var nextGrowthCount: Int

    /// The utilisation threshold above which the cache will double in size.
    private let threshold: Double

    /// The number of elements currently stored in the cache.
    private(set) var count: Int

    /// The number of elements that can be stored in the cache.
    var capacity: Int { self.slots.count }

    /// Whether the cache is empty.
    var isEmpty: Bool { self.count == 0 }

    init(capacity: Int, threshold: Double) {
        precondition((0.0...1.0).contains(threshold))
        let capacity = capacity.nextPowerOfTwo
        self.slots = Array(repeating: Slot(), count: capacity)
        self.mask = UInt64(capacity - 1)
        self.threshold = threshold
        self.nextGrowthCount = capacity.scaled(by: threshold)
        self.count = 0
    }

    /// Index of the slot for the given stream ID.
    func slotIndex(of id: QUICStreamID) -> Int {
        // Drop the type bits and then mask. The mask can be used instead of '%' as the capacity is
        // guaranteed to be a power of two (and the mask is just `capacity - 1`).
        Int((id.rawValue >> 2) & self.mask)
    }

    /// Returns the value for the given stream ID, if it exists in the cache.
    subscript(id: QUICStreamID) -> Value? {
        let index = self.slotIndex(of: id)
        return self.slots[index].value(forID: id)
    }

    /// Returns whether the cache contains the given stream ID.
    func contains(_ id: QUICStreamID) -> Bool {
        self.slots[self.slotIndex(of: id)].containsID(id)
    }

    enum UpdateResult {
        /// The value was inserted into an empty slot.
        case inserted
        /// The value replaced a value for the same stream ID.
        case replaced(Value)
        /// The value was inserted but evicted a value for a different stream ID.
        case evicted(id: QUICStreamID, value: Value)
    }

    /// Updates the value stored for the given stream ID.
    ///
    /// - Parameters:
    ///   - value: The value to store.
    ///   - id: The ID of the stream.
    /// - Returns: Whether the value was inserted, replaced an existed value, or evicted a value
    ///   for another stream.
    @discardableResult
    mutating func updateValue(_ value: Value, forID id: QUICStreamID) -> UpdateResult {
        let index = self.slotIndex(of: id)

        var slot = Slot(id: id, value: value)
        swap(&self.slots[index], &slot)

        if let previous = slot.value {
            if slot.containsID(id) {
                return .replaced(previous)
            } else {
                assert(!slot.isEmpty)
                return .evicted(id: QUICStreamID(rawValue: slot.id), value: previous)
            }
        } else {
            assert(slot.isEmpty)
            self.count &+= 1
            self.doubleCapacityIfNeeded()
            return .inserted
        }
    }

    /// Removes the value associated with the given ID, if one exists.
    @discardableResult
    mutating func removeValue(forID id: QUICStreamID) -> Value? {
        let index = self.slotIndex(of: id)

        if let value = self.slots[index].removeValue(forID: id) {
            self.count &-= 1
            return value
        } else {
            return nil
        }
    }

    /// Remove all values in the cache.
    mutating func removeAll() {
        if self.isEmpty { return }

        self.count = 0
        for index in self.slots.indices {
            self.slots[index] = Slot()
        }
    }

    private mutating func doubleCapacityIfNeeded() {
        if self.count < self.nextGrowthCount { return }

        // Compute the new capacity, mask and growth count.
        let oldCapacity = self.capacity
        let capacity = oldCapacity * 2
        self.mask = UInt64(capacity - 1)
        self.nextGrowthCount = capacity.scaled(by: self.threshold)

        // Add the new empty slots.
        self.slots.append(contentsOf: repeatElement(Slot(), count: oldCapacity))

        // Doubling capacity effectively splits each slot into two. One slots maintains its place
        // and the other entry either moves by `oldCapacity` slots. This is determined by the bit
        // for the `oldCapacity` (only one bit, because it was a power of two). All of the new slots
        // are vacant.
        let bit = UInt64(oldCapacity)
        for index in 0..<oldCapacity {
            if !self.slots[index].isEmpty && (self.slots[index].id >> 2 & bit) != 0 {
                self.slots.swapAt(index, index | oldCapacity)
            }
        }
    }
}

extension QUICStreamIDCache: Sequence {
    typealias Element = (QUICStreamID, Value)

    func makeIterator() -> Iterator {
        Iterator(self.slots.makeIterator())
    }

    struct Iterator: IteratorProtocol {
        private var iterator: [QUICStreamIDCache<Value>.Slot].Iterator

        fileprivate init(_ iterator: [QUICStreamIDCache<Value>.Slot].Iterator) {
            self.iterator = iterator
        }

        mutating func next() -> (QUICStreamID, Value)? {
            while let slot = self.iterator.next() {
                if let value = slot.value {
                    return (QUICStreamID(rawValue: slot.id), value)
                }
            }
            return nil
        }
    }
}

extension Int {
    fileprivate var nextPowerOfTwo: Int {
        precondition(self > 0)
        if self.nonzeroBitCount == 1 {
            return self
        } else {
            return 1 << (Int.bitWidth - self.leadingZeroBitCount)
        }
    }

    fileprivate func scaled(by factor: Double) -> Int {
        let scaled = (Double(self) * factor).rounded(.up)
        return Swift.max(1, Int(scaled))
    }
}
