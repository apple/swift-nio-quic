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
    var _generation: QUICStreamHandle.Generation
    @usableFromInline
    var _isOccupied: Bool

    /// How many times the slot has been reused.
    @inlinable
    var generation: QUICStreamHandle.Generation { self._generation }

    /// Whether the slot is currently occupied.
    @inlinable
    var isOccupied: Bool { self._isOccupied }

    @inlinable
    init() {
        self._generation = .first
        self._isOccupied = false
    }

    @inlinable
    func isOccupied(by generation: QUICStreamHandle.Generation) -> Bool {
        self._isOccupied && self._generation == generation
    }

    @inlinable
    mutating func occupy() -> QUICStreamHandle.Generation {
        assert(!self._isOccupied)
        self._isOccupied = true
        self._generation.advance()
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
