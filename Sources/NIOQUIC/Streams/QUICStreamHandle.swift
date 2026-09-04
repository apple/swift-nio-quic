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

/// An opaque handle for a stream on a connection.
public struct QUICStreamHandle: Hashable, Sendable {
    @usableFromInline
    var _index: Index
    @usableFromInline
    var _generation: UInt32

    @inlinable
    var rawValue: UInt64 {
        // Index asserts it's no greater than a UInt32.
        let indexBits = UInt64(UInt32(truncatingIfNeeded: self._index.rawValue))
        return indexBits | UInt64(self._generation) << 32
    }

    /// The index of the slot the stream occupies in the connection's stream table.
    @inlinable
    var index: Index {
        self._index
    }

    /// How many times that slot has been reused.
    @inlinable
    var generation: UInt32 {
        self._generation
    }

    @inlinable
    init(index: Index, generation: UInt32) {
        self._index = index
        self._generation = generation
    }

    /// Rebuilds a handle which was flattened to its raw bits.
    @inlinable
    init(rawValue: UInt64) {
        self._index = Index(Int(UInt32(truncatingIfNeeded: rawValue)))
        self._generation = UInt32(truncatingIfNeeded: rawValue >> 32)
    }
}

extension QUICStreamHandle {
    /// The index into ``QUICStreamSlots`` for the handle the stream occupies.
    @usableFromInline
    struct Index: Hashable, Comparable, Sendable {
        /// The slot the stream occupies, as a subscript into storage which is addressed by `Int`.
        @usableFromInline
        var rawValue: Int

        @inlinable
        init(_ rawValue: Int) {
            assert(rawValue <= Int(UInt32.max))
            self.rawValue = rawValue
        }

        /// The first slot a store hands out.
        @inlinable
        static var first: Index {
            Index(0)
        }

        @inlinable
        mutating func advance() {
            self.rawValue &+= 1
            assert(self.rawValue <= Int(UInt32.max))
        }

        @inlinable
        static func < (lhs: Index, rhs: Index) -> Bool {
            lhs.rawValue < rhs.rawValue
        }
    }
}
