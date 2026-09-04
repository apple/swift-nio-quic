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

import Testing

@testable import NIOQUIC

@Suite
struct QUICStreamSlotsTests {
    @Test
    func insertThenAccess() {
        var store = QUICStreamSlots<Int>()
        let handle = store.insert(42)
        #expect(store.count == 1)
        let value = store.withValue(for: handle) { $0 }
        #expect(value == 42)
    }

    @Test
    func mutateInPlace() {
        var store = QUICStreamSlots<Int>()
        let handle = store.insert(1)
        // Non-overlapping visits to one slot are the ordinary case: only nesting is forbidden.
        store.withValue(for: handle) { $0 += 1 }
        store.withValue(for: handle) { $0 += 1 }
        #expect(store.withValue(for: handle) { $0 } == 3)
    }

    @Test
    func removeReturnsValueAndEmptiesSlot() {
        var store = QUICStreamSlots<Int>()
        let handle = store.insert(7)
        #expect(store.removeValue(for: handle) == 7)
        #expect(store.count == 0)
        #expect(store.withValue(for: handle) { $0 } == nil)
    }

    @Test
    func staleHandleDoesNotResolve() {
        var store = QUICStreamSlots<Int>()
        let first = store.insert(1)
        _ = store.removeValue(for: first)
        _ = store.insert(2)
        #expect(store.withValue(for: first) { $0 } == nil)
        #expect(store.removeValue(for: first) == nil)
    }

    @Test
    func growsBeyondOnePage() {
        var store = QUICStreamSlots<Int>()
        var handles: [QUICStreamHandle] = []
        for value in 0..<10 {
            handles.append(store.insert(value))
        }
        #expect(store.count == 10)
        for (value, handle) in handles.enumerated() {
            #expect(store.withValue(for: handle) { $0 } == value)
        }
    }

    @Test
    func removeTwiceReturnsNilTheSecondTime() {
        var store = QUICStreamSlots<Int>()
        let handle = store.insert(7)
        #expect(store.removeValue(for: handle) == 7)
        #expect(store.count == 0)

        // The slot is vacant but its generation still matches, so the absent value is the only
        // thing standing between a second remove and a double free-list push.
        #expect(store.removeValue(for: handle) == nil)
        #expect(store.count == 0)

        // A double push would hand the same index out twice.
        let first = store.insert(1)
        let second = store.insert(2)
        #expect(first.index != second.index)
        #expect(store.count == 2)
    }

    @Test
    func resolvedPointerReadsAndWritesTheValue() {
        var store = QUICStreamSlots<Int>()
        let handle = store.insert(1)
        let pointer = store.pointer(for: handle)!
        #expect(pointer.pointee == 1)

        pointer.pointee += 41
        #expect(store.withValue(for: handle) { $0 } == 42)
    }

    @Test
    func resolvedPointerSurvivesAPageAllocatingInsert() {
        var store = QUICStreamSlots<Int>()
        let first = store.insert(1)
        // Enough to spill onto later pages, so the inserts below must allocate more of them.
        for value in 2...4 { _ = store.insert(value) }
        let pointer = store.pointer(for: first)!

        // Pages are never moved, so growing the store cannot invalidate a pointer into one.
        for value in 5...12 { _ = store.insert(value) }
        #expect(store.count == 12)
        #expect(pointer.pointee == 1)

        pointer.pointee = 99
        #expect(store.withValue(for: first) { $0 } == 99)
    }

    @Test
    func twoResolvedPointersCanBeHeldAndMutatedTogether() {
        var store = QUICStreamSlots<Int>()
        let first = store.insert(1)
        for value in 2...3 { _ = store.insert(value) }
        let second = store.insert(4)

        // On different pages, so the two pointers are independent of each other's storage.
        #expect(
            Page.position(of: first.index.rawValue).pageIndex
                != Page.position(of: second.index.rawValue).pageIndex
        )

        // Resolving takes no lasting access to the store, so both pointers can be live at once —
        // which the closure form cannot express, since it holds the store for the whole visit.
        let firstPointer = store.pointer(for: first)!
        let secondPointer = store.pointer(for: second)!
        firstPointer.pointee += 10
        secondPointer.pointee += 20

        #expect(firstPointer.pointee == 11)
        #expect(secondPointer.pointee == 24)
        #expect(store.withValue(for: first) { $0 } == 11)
        #expect(store.withValue(for: second) { $0 } == 24)
    }

    @Test
    func pointerForStaleHandleIsNil() {
        var store = QUICStreamSlots<Int>()
        let first = store.insert(1)
        _ = store.removeValue(for: first)
        let vacant = store.pointer(for: first)
        #expect(vacant == nil)

        _ = store.insert(2)
        let stale = store.pointer(for: first)
        #expect(stale == nil)
    }

    @Test
    func pointerAtVacantOrOutOfRangeIndexIsNil() {
        var store = QUICStreamSlots<Int>()
        let handle = store.insert(1)
        let occupied = store.pointer(at: handle.index)
        #expect(occupied?.pointee == 1)

        _ = store.removeValue(for: handle)
        let vacant = store.pointer(at: handle.index)
        #expect(vacant == nil)

        let outOfRange = store.pointer(at: .init(999))
        #expect(outOfRange == nil)
    }

    @Test
    func removeAllVisitsEveryValue() {
        var store = QUICStreamSlots<Int>()
        for value in 0..<5 { _ = store.insert(value) }
        var seen: [Int] = []
        store.removeAll { seen.append($0) }
        #expect(seen.sorted() == [0, 1, 2, 3, 4])
        #expect(store.count == 0)
    }

    @Test
    func containsValueOnlyForALiveHandle() {
        var store = QUICStreamSlots<Int>()
        let first = store.insert(1)
        // The results are bound before being expected because #expect decomposes a bare call into
        // a helper which takes the receiver by value, and the store is noncopyable.
        let liveIsOccupied = store.containsValue(for: first)
        #expect(liveIsOccupied)

        _ = store.removeValue(for: first)
        let removedIsOccupied = store.containsValue(for: first)
        #expect(removedIsOccupied == false)

        // Reusing the slot must not resurrect the handle which used to address it.
        let second = store.insert(2)
        let reusedIsOccupied = store.containsValue(for: second)
        let staleIsOccupied = store.containsValue(for: first)
        #expect(reusedIsOccupied)
        #expect(staleIsOccupied == false)

        let outOfRangeIsOccupied = store.containsValue(for: QUICStreamHandle(index: .init(999), generation: 1))
        #expect(outOfRangeIsOccupied == false)
    }

    @Test
    func handleAtIndexMatchesTheIssuedHandle() {
        var store = QUICStreamSlots<Int>()
        let handle = store.insert(1)
        #expect(store.handle(at: handle.index) == handle)

        _ = store.removeValue(for: handle)
        #expect(store.handle(at: handle.index) == nil)

        // The reused slot reports its own handle, not the one it displaced.
        let second = store.insert(2)
        #expect(store.handle(at: handle.index) == second)

        #expect(store.handle(at: .init(999)) == nil)
    }

    /// The generation counts a slot's reuses rather than its occupants, which is what makes a
    /// handle from an earlier occupant fail to resolve instead of addressing its successor.
    @Test
    func generationsCountSlotReuse() {
        var store = QUICStreamSlots<Int>()
        // Zero is never handed out, so an all-zero handle addresses nothing.
        #expect(store.containsValue(for: QUICStreamHandle(index: .first, generation: 0)) == false)

        let first = store.insert(1)
        _ = store.removeValue(for: first)

        let second = store.insert(2)
        #expect(second.index == first.index)
        #expect(second.generation == first.generation &+ 1)

        _ = store.removeValue(for: second)
        let third = store.insert(3)
        #expect(third.index == first.index)
        #expect(third.generation == first.generation &+ 2)
    }
}
