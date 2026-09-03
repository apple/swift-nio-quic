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

/// Paged storage for stream state, keyed by a ``QUICStreamHandle``.
///
/// Values live in a ``PagedBuffer`` so a value's address is stable while its slot is occupied.
/// When a slot is vacated and later reused its generation is bumped which invalidates any handle
/// previously used for addressing the same slot.
@usableFromInline
struct QUICStreamSlots<Value: ~Copyable>: ~Copyable {
    @usableFromInline
    var _storage: PagedBuffer<Value>

    /// A record of each allocated slot, indexed by the `Index` of a `QUICStreamHandle`.
    @usableFromInline
    var _slots: [QUICStreamSlotRecord]

    /// The indexes of the vacant slots.
    @usableFromInline
    var _freeList: [QUICStreamHandle.Index]

    @usableFromInline
    var _count: Int

    /// The number of occupied slots.
    @inlinable
    var count: Int {
        self._count
    }

    /// The number of slots ever allocated.
    @inlinable
    var allocated: Int {
        self._slots.count
    }

    @inlinable
    init() {
        self._storage = PagedBuffer()
        self._slots = []
        self._freeList = []
        self._count = 0
    }

    deinit {
        for index in self._slots.indices {
            if self._slots[index].isOccupied {
                self._storage.pointer(at: index).deinitialize(count: 1)
            }
        }
    }

    /// Stores `value` in a free slot, returning the handle which addresses it.
    @inlinable
    mutating func insert(_ value: consuming Value) -> QUICStreamHandle {
        let index: QUICStreamHandle.Index
        let pointer: UnsafeMutablePointer<Value>

        if let reused = self._freeList.popLast() {
            index = reused
            pointer = self._storage.pointer(at: index.rawValue)
        } else {
            pointer = self._storage.append()
            index = QUICStreamHandle.Index(self._slots.count)
            self._slots.append(QUICStreamSlotRecord())
            // Storage and slots share indexes, double check they align.
            assert(self._slots.count == self._storage.count)
        }

        pointer.initialize(to: consume value)
        let generation = self._slots[index.rawValue].occupy()
        self._count &+= 1

        return QUICStreamHandle(index: index, generation: generation)
    }

    /// Runs `body` on the value `handle` addresses, or returns `nil` if it addresses nothing.
    @inlinable
    mutating func withValue<Result: ~Copyable>(
        for handle: QUICStreamHandle,
        execute body: (_ value: inout Value) -> Result
    ) -> Result? {
        if let pointer = self.pointer(for: handle) {
            return body(&pointer.pointee)
        } else {
            return nil
        }
    }

    /// Vacates the slot `handle` addresses and returns what was in it.
    @inlinable
    @discardableResult
    mutating func removeValue(for handle: QUICStreamHandle) -> Value? {
        guard let pointer = self.pointer(for: handle) else { return nil }

        self._slots[handle.index.rawValue].vacate()
        self._freeList.append(handle.index)
        self._count &-= 1
        return pointer.move()
    }

    /// Vacates every slot, handing each value to `body`.
    @inlinable
    mutating func removeAll(_ body: (_ value: consuming Value) -> Void) {
        for index in self._slots.indices {
            let wasOccupied = self._slots[index].vacateIfOccupied()

            if wasOccupied {
                let slotIndex = QUICStreamHandle.Index(index)
                self._freeList.append(slotIndex)
                self._count &-= 1
                body(self._storage.pointer(at: slotIndex.rawValue).move())
            }
        }
    }

    /// Whether `handle` addresses an occupied slot of a matching generation.
    @inlinable
    func containsValue(for handle: QUICStreamHandle) -> Bool {
        if handle.index.rawValue < self._slots.count {
            return self._slots[handle.index.rawValue].isOccupied(by: handle.generation)
        } else {
            return false
        }
    }

    /// The handle addressing whatever occupies `index`, or `nil` if that slot is vacant or was
    /// never allocated.
    @inlinable
    func handle(at index: QUICStreamHandle.Index) -> QUICStreamHandle? {
        guard self._slots.indices.contains(index.rawValue) else { return nil }

        let slot = self._slots[index.rawValue]

        if slot.isOccupied {
            return QUICStreamHandle(index: index, generation: slot.generation)
        } else {
            return nil
        }
    }

    /// Resolves `handle` to the value it addresses, or `nil` if it addresses nothing.
    ///
    /// Resolving takes no lasting access to the store, so unlike `withValue` a caller may hold
    /// pointers to several slots at once and work through them together. In exchange the caller
    /// owns the lifetime: nothing detects a pointer used after its slot was vacated, and if the
    /// slot has since been reused the pointer addresses its successor.
    @inlinable
    func pointer(for handle: QUICStreamHandle) -> UnsafeMutablePointer<Value>? {
        if self.containsValue(for: handle) {
            return self._storage.pointer(at: handle.index.rawValue)
        } else {
            return nil
        }
    }

    /// Resolves `index` to the value it addresses, without a generation check, or `nil` if the slot
    /// is vacant or out of range.
    @inlinable
    func pointer(at index: QUICStreamHandle.Index) -> UnsafeMutablePointer<Value>? {
        if self._slots.indices.contains(index.rawValue) && self._slots[index.rawValue].isOccupied {
            return self._storage.pointer(at: index.rawValue)
        } else {
            return nil
        }
    }
}
