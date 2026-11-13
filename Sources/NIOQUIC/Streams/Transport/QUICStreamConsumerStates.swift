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
///
/// States are stored as they are, not as optionals, and which slots hold one is kept apart: this
/// code runs unspecialized, and every access to an optional state would look up the optional's
/// metadata at run time.
@usableFromInline
struct QUICStreamConsumerStates<State: ~Copyable>: ~Copyable {
    @usableFromInline
    var _storage: PagedBuffer<State>

    /// Whether each slot holds a state.
    @usableFromInline
    var _holdsState: [Bool]

    @inlinable
    init() {
        self._storage = PagedBuffer()
        self._holdsState = []
    }

    @inlinable
    deinit {
        for index in self._holdsState.indices where self._holdsState[index] {
            self._storage.pointer(at: index).deinitialize(count: 1)
        }
    }

    /// Makes room for a state at `index`, and at any index before it, if the slots have allocated
    /// them since.
    ///
    /// The slots can allocate several before any is reserved: an inbound stream's state is only
    /// reserved when the consumer makes it, before its first visit.
    @inlinable
    mutating func reserve(at index: QUICStreamHandle.Index) {
        while index.rawValue >= self._storage.count {
            self._storage.append()
            self._holdsState.append(false)
        }
    }

    /// Whether `index`, which must have been reserved, holds a state.
    @inlinable
    func holdsState(at index: QUICStreamHandle.Index) -> Bool {
        self._holdsState[index.rawValue]
    }

    /// Stores `state` at `index`, which must have been reserved and hold no state.
    @inlinable
    mutating func initialize(at index: QUICStreamHandle.Index, to state: consuming State) {
        // Initializing over a state would leak it.
        precondition(!self._holdsState[index.rawValue], "slot \(index.rawValue) already holds a state")
        self._storage.pointer(at: index.rawValue).initialize(to: state)
        self._holdsState[index.rawValue] = true
    }

    /// The state at `index`, which must hold one.
    @inlinable
    func pointer(at index: QUICStreamHandle.Index) -> UnsafeMutablePointer<State> {
        // A slot without a state is uninitialized memory.
        precondition(self._holdsState[index.rawValue], "slot \(index.rawValue) holds no state")
        return self._storage.pointer(at: index.rawValue)
    }

    /// Moves the state out of `index`, which must hold one.
    @inlinable
    mutating func move(at index: QUICStreamHandle.Index) -> State {
        // A slot without a state is uninitialized memory.
        precondition(self._holdsState[index.rawValue], "slot \(index.rawValue) holds no state")
        self._holdsState[index.rawValue] = false
        return self._storage.pointer(at: index.rawValue).move()
    }

    /// Destroys the state at `index`, if it holds one.
    @inlinable
    mutating func removeValue(at index: QUICStreamHandle.Index) {
        if self._holdsState[index.rawValue] {
            self._holdsState[index.rawValue] = false
            self._storage.pointer(at: index.rawValue).deinitialize(count: 1)
        }
    }

    /// Destroys every state, keeping capacity.
    @inlinable
    mutating func removeAll() {
        for index in self._holdsState.indices where self._holdsState[index] {
            self._holdsState[index] = false
            self._storage.pointer(at: index).deinitialize(count: 1)
        }
    }
}
