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

/// Storage for the consumer's per-stream state.
///
/// The indexing space is shared with a ``QUICStreamSlots`` instance so state is addressed via a
/// ``QUICStreamHandle/Index``. There is *no generation checking* here; this should be done via
/// ``QUICStreamSlots`` prior to accessing state here.
@usableFromInline
struct QUICStreamConsumerStates<State: ~Copyable>: ~Copyable {
    @usableFromInline
    var _storage: PagedBuffer<State?>

    @inlinable
    init() {
        self._storage = PagedBuffer()
    }

    deinit {
        self._storage.deinitializeAll()
    }

    /// The pointer for the value at `index`, growing the storage by one slot if `index` is a slot
    /// the slots have only just allocated.
    @inlinable
    mutating func reserve(at index: QUICStreamHandle.Index) -> UnsafeMutablePointer<State?> {
        if index.rawValue < self._storage.count {
            return self._storage.pointer(at: index.rawValue)
        } else {
            return self._storage.appendNil()
        }
    }

    /// The pointer for `index`, which must have been reserved.
    @inlinable
    func pointer(at index: QUICStreamHandle.Index) -> UnsafeMutablePointer<State?> {
        self._storage.pointer(at: index.rawValue)
    }

    /// Destroys the state at the given `index`, if it has any.
    @inlinable
    func removeValue(at index: QUICStreamHandle.Index) {
        self.pointer(at: index).pointee = nil
    }

    /// Removes all values from the store, keeping capacity.
    @inlinable
    func removeAll() {
        self._storage.setAllToNil()
    }
}
