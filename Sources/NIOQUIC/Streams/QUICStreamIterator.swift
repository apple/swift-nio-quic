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

/// An iterator for streams to visit.
///
/// Consuming a stream from the iterator means its events are considered consumer and won't be
/// presented again. If a stream _isn't_ consumed by the iterator then it will be included in the
/// next visit.
@available(anyAppleOS 26, *)
public struct QUICStreamIterator<Consumer: QUICStreamConsumer & ~Copyable>: ~Copyable, ~Escapable {
    /// The handle and events that the last call to `next()` returned.
    @usableFromInline
    struct Pending {
        @usableFromInline
        var handle: QUICStreamHandle
        @usableFromInline
        var presented: QUICStreamEvents

        @inlinable
        init(handle: QUICStreamHandle, presented: QUICStreamEvents) {
            self.handle = handle
            self.presented = presented
        }
    }

    @usableFromInline
    let _table: QUICStreamTable<Consumer>

    /// The handle and events which are currently being visited.
    @usableFromInline
    var _pending: Pending?

    @inlinable
    @_lifetime(immortal)
    init(table: QUICStreamTable<Consumer>) {
        self._table = table
        self._pending = nil
    }

    deinit {
        // Finish the last visit (in case the iterator wasn't completely consumed).
        if let pending = self._pending {
            self._table.finishVisit(pending.handle, presented: pending.presented)
        }
    }

    /// The next stream to service, or `nil` if there are none left.
    ///
    /// - Returns: A stream visit.
    @inlinable
    @_lifetime(&self)
    public mutating func next() -> QUICStreamVisit<Consumer>? {
        // Finish the previous visit (which may close the stream). Doing this in a defer would be
        // to early.
        self._finishVisit()

        while let handle = self._table.nextReadyHandle() {
            guard let resolved = self._table.readySlot(for: handle) else { continue }

            // Stash the pending state (for cleanup in `_finishVisit()`)
            self._pending = Pending(handle: handle, presented: resolved.events)

            return QUICStreamVisit(
                table: self._table,
                transport: resolved.transport,
                state: resolved.state,
                handle: handle,
                events: resolved.events
            )
        }

        return nil
    }

    /// Finishes the visit for the stream that was last handed over.
    ///
    /// Clear any events it presented and recycle its slot if the stream was closed.
    @inlinable
    mutating func _finishVisit() {
        if let pending = self._pending.take() {
            self._table.finishVisit(pending.handle, presented: pending.presented)
        }
    }
}

@available(anyAppleOS 26, *)
@available(*, unavailable)
extension QUICStreamIterator: Sendable where Consumer: ~Copyable {}
