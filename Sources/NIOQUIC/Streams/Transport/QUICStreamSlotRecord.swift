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

/// A bookkeeping record for a slot in ``QUICStreamSlots``.
@usableFromInline
struct QUICStreamSlotRecord {
    @usableFromInline
    var _generation: UInt32
    @usableFromInline
    var _isOccupied: Bool

    /// How many times the slot has been reused.
    @inlinable
    var generation: UInt32 { self._generation }

    /// Whether the slot is currently occupied.
    @inlinable
    var isOccupied: Bool { self._isOccupied }

    @inlinable
    init() {
        self._generation = 0
        self._isOccupied = false
    }

    @inlinable
    func isOccupied(by generation: UInt32) -> Bool {
        self._isOccupied && self._generation == generation
    }

    @inlinable
    mutating func occupy() -> UInt32 {
        assert(!self._isOccupied)
        self._isOccupied = true
        // Wrapping is fine: it takes ~4 billion reuses of one slot, by which point any handle old
        // enough to alias is long gone.
        self._generation &+= 1
        return self._generation
    }

    @inlinable
    mutating func vacate() {
        assert(self._isOccupied)
        self._isOccupied = false
    }

    @inlinable
    mutating func vacateIfOccupied() -> Bool {
        let wasOccupied = self._isOccupied
        self._isOccupied = false
        return wasOccupied
    }
}
