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
    var _generation: Generation

    @inlinable
    var rawValue: UInt {
        let indexBits = UInt(self._index._rawValue)
        return indexBits | UInt(self._generation.rawValue) << UIntHalf.bitWidth
    }

    /// The index of the slot the stream occupies in the connection's stream table.
    @inlinable
    var index: Index {
        self._index
    }

    /// How many times that slot has been reused.
    @inlinable
    var generation: Generation {
        self._generation
    }

    /// Creates a new handle from its slot index and generation.
    @inlinable
    init(index: Index, generation: Generation) {
        self._index = index
        self._generation = generation
    }

    /// Rebuilds a handle which was flattened to its raw bits.
    @inlinable
    init(rawValue: UInt) {
        self._index = Index(UIntHalf(truncatingIfNeeded: rawValue))
        let generationBits = rawValue >> UIntHalf.bitWidth
        self._generation = Generation(UIntHalf(truncatingIfNeeded: generationBits))
    }
}

extension QUICStreamHandle {
    @usableFromInline
    struct Generation: Hashable, Sendable {
        @usableFromInline
        var rawValue: UIntHalf

        @inlinable
        init(_ rawValue: UIntHalf) {
            self.rawValue = rawValue
        }

        /// The first generation of a slot.
        @inlinable
        static var first: Generation {
            Generation(0)
        }

        @inlinable
        mutating func advance() {
            // Wrapping is fine: it takes a full generation counter's worth of reuses of one slot,
            // by which point any handle old enough _should_ be long gone.
            self.rawValue &+= 1
        }
    }
}

extension QUICStreamHandle {
    /// The index into ``QUICStreamSlots`` for the handle the stream occupies.
    @usableFromInline
    struct Index: Hashable, Comparable, Sendable {
        @usableFromInline
        var _rawValue: UIntHalf

        /// The slot the stream occupies, as a subscript into storage which is addressed by `Int`.
        @inlinable
        var rawValue: Int { Int(self._rawValue) }

        @inlinable
        init(_ rawValue: UIntHalf) {
            self._rawValue = rawValue
        }

        @inlinable
        init(_ rawValue: Int) {
            self._rawValue = UIntHalf(rawValue)
        }

        /// The first slot a store hands out.
        @inlinable
        static var first: Index {
            Index(0)
        }

        @inlinable
        mutating func advance() {
            self._rawValue &+= 1
        }

        @inlinable
        static func < (lhs: Index, rhs: Index) -> Bool {
            lhs.rawValue < rhs.rawValue
        }
    }
}

// MARK: - NetworkFramework indexing

// When creating a `ProtocolInstanceReference` for a given stream you can specify the container and
// an index. That is, a `ProtocolInstanceContainer` can service many `ProtocolInstance`s as each is
// keyed by an `Int` index. Since the index is only an `Int` we need to pack the whole handle into
// it: its slot index and generation.
//
// Note that this will change in SwiftNetwork so this is unlikely to be the end state and will need
// to be revisited then. (That's no bad thing because the slot indexing and generation are a lot
// smaller on 32-bit platforms to squeeze them into an `Int`.)

#if _pointerBitWidth(_64)
@usableFromInline
/// An unsigned integer with half the bytes of a `UInt`.
typealias UIntHalf = UInt32
#elseif _pointerBitWidth(_32)
@usableFromInline
/// An unsigned integer with half the bytes of a `UInt`.
typealias UIntHalf = UInt16
#else
#error("Unsupported pointer size")
#endif

@available(anyAppleOS 26, *)
extension QUICStreamHandle {
    /// The index stored by SwiftNetwork for a given handle.
    @inlinable
    var protocolInstanceReferenceIndex: Int {
        Int(bitPattern: self.rawValue)
    }

    /// Rebuilds the handle the stack was given as a container index.
    @inlinable
    init(protocolInstanceReferenceIndex: Int) {
        self.init(rawValue: UInt(bitPattern: protocolInstanceReferenceIndex))
    }
}
