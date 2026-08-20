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
    fileprivate struct Entry {
        var id: QUICStreamID
        var value: Value
    }

    /// The underlying slots in the cache, `nil` means the slot is free.
    private var slots: [Entry?]

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
        self.slots = Array(repeating: nil, count: capacity)
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
        if let entry = self.slots[self.slotIndex(of: id)], entry.id == id {
            return entry.value
        } else {
            return nil
        }
    }

    /// Returns whether the cache contains the given stream ID.
    func contains(_ id: QUICStreamID) -> Bool {
        self.slots[self.slotIndex(of: id)]?.id == id
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

        var entry: Entry? = Entry(id: id, value: value)
        swap(&self.slots[index], &entry)

        switch entry {
        case .none:
            self.count &+= 1
            self.doubleCapacityIfNeeded()
            return .inserted

        case .some(let previous):
            if previous.id == id {
                return .replaced(previous.value)
            } else {
                return .evicted(id: previous.id, value: previous.value)
            }
        }
    }

    /// Removes the value associated with the given ID, if one exists.
    @discardableResult
    mutating func removeValue(forID id: QUICStreamID) -> Value? {
        let index = self.slotIndex(of: id)

        var entry: Entry? = nil
        swap(&entry, &self.slots[index])

        switch entry {
        case .some(let entry) where entry.id == id:
            self.count &-= 1
            return entry.value

        case .some:
            // Different ID, put back the entry.
            swap(&entry, &self.slots[index])
            return nil

        case .none:
            return nil
        }
    }

    /// Remove all values in the cache.
    mutating func removeAll() {
        if self.isEmpty { return }

        self.count = 0
        for index in self.slots.indices {
            self.slots[index] = nil
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
        self.slots.append(contentsOf: repeatElement(nil, count: oldCapacity))

        // Doubling capacity effectively splits each slot into two. One slots maintains its place
        // and the other entry either moves by `oldCapacity` slots. This is determined by the bit
        // for the `oldCapacity` (only one bit, because it was a power of two). All of the new slots
        // are vacant.
        let bit = UInt64(oldCapacity)
        for index in 0..<oldCapacity {
            switch self.slots[index] {
            case .some(let entry):
                if (entry.id.rawValue >> 2) & bit != 0 {
                    self.slots.swapAt(index, index | oldCapacity)
                }
            case .none:
                ()
            }
        }
    }
}

extension QUICStreamIDCache: Sequence {
    typealias Element = (QUICStreamID, Value)

    func makeIterator() -> Iterator {
        Iterator(iterator: self.slots.makeIterator())
    }

    struct Iterator: IteratorProtocol {
        private var iterator: [QUICStreamIDCache<Value>.Entry?].Iterator

        fileprivate init(iterator: [QUICStreamIDCache<Value>.Entry?].Iterator) {
            self.iterator = iterator
        }

        mutating func next() -> (QUICStreamID, Value)? {
            while let entry = self.iterator.next() {
                if let entry {
                    return (entry.id, entry.value)
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
