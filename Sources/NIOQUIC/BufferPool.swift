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

import NIOCore

/// A pool of `ByteBuffer`s.
struct BufferPool: Sendable {
    /// The pooled buffers.
    @usableFromInline
    internal var _buffers: [ByteBuffer]
    /// The index into `buffers` of the index which was last used.
    @usableFromInline
    internal var _lastUsedIndex: Int

    @usableFromInline
    internal let _allocator: ByteBufferAllocator

    /// Maximum number of buffers to store in the pool.
    internal let capacity: Int

    /// Creates a new `BufferPool`.
    ///
    /// - Parameters:
    ///   - capacity: Maximum number of buffers to store in the pool.
    ///   - allocator: An allocator for byte buffers.
    init(capacity: Int, allocator: ByteBufferAllocator = ByteBufferAllocator()) {
        precondition(capacity > 0)
        self.capacity = capacity
        self._allocator = allocator
        self._lastUsedIndex = 0
        self._buffers = []
        self._buffers.reserveCapacity(capacity)
    }

    /// Returns the number of buffers in the pool.
    var count: Int {
        self._buffers.count
    }

    /// Provides a buffer with enough writable capacity as determined by the underlying
    /// receive allocator to the given closure.
    ///
    /// - Parameters:
    ///    - body: Closure where the caller can use the new or existing buffer
    /// - Returns: A tuple containing the `ByteBuffer` used and the `Result` yielded by the closure provided.
    @inlinable
    mutating func withBuffer<Result>(
        minimumCapacity: Int,
        _ body: (_ buffer: inout ByteBuffer) throws -> Result
    ) rethrows -> (ByteBuffer, Result) {
        // Reuse an existing buffer if we can do so without CoWing.
        if let bufferAndResult = try self._reuseExistingBuffer(minimumCapacity: minimumCapacity, body) {
            return bufferAndResult
        } else {
            // No available buffers or the allocator does not offer up buffer sizes; directly
            // allocate a new one.
            return try self._allocateNewBuffer(minimumCapacity: minimumCapacity, body)
        }
    }

    @inlinable
    internal mutating func _reuseExistingBuffer<Result>(
        minimumCapacity: Int,
        _ body: (_ buffer: inout ByteBuffer) throws -> Result
    ) rethrows -> (ByteBuffer, Result)? {
        // Cycle through the buffers starting at the last used buffer.
        let resultAndIndex = try self._buffers._loopingFirstIndexWithResult(startingAt: self._lastUsedIndex) {
            buffer in
            try buffer._modifyIfUniquelyOwned(minimumCapacity: minimumCapacity, body)
        }

        if let (result, index) = resultAndIndex {
            self._lastUsedIndex = index
            return (self._buffers[index], result)
        } else {
            // Couldn't reuse an existing buffer.
            return nil
        }
    }

    @inlinable
    internal mutating func _allocateNewBuffer<Result>(
        minimumCapacity: Int,
        _ body: (_ buffer: inout ByteBuffer) throws -> Result
    ) rethrows -> (ByteBuffer, Result) {
        // Couldn't reuse a buffer; create a new one and store it if there's capacity.
        var newBuffer = self._allocator.buffer(capacity: minimumCapacity)

        if self._buffers.count < self.capacity {
            self._buffers.append(newBuffer)
            self._lastUsedIndex = self._buffers.index(before: self._buffers.endIndex)
            return try self._modifyBuffer(atIndex: self._lastUsedIndex, body)
        } else {
            let result = try body(&newBuffer)
            return (newBuffer, result)
        }
    }

    @inlinable
    internal mutating func _modifyBuffer<Result>(
        atIndex index: Int,
        _ body: (_ buffer: inout ByteBuffer) throws -> Result
    ) rethrows -> (ByteBuffer, Result) {
        let result = try body(&self._buffers[index])
        return (self._buffers[index], result)
    }
}

extension ByteBuffer {
    @inlinable
    internal mutating func _modifyIfUniquelyOwned<Result>(
        minimumCapacity: Int,
        _ body: (_ buffer: inout ByteBuffer) throws -> Result
    ) rethrows -> Result? {
        try self.modifyIfUniquelyOwned { buffer in
            buffer.clear(minimumCapacity: minimumCapacity)
            return try body(&buffer)
        }
    }
}

extension Array {
    /// Iterate over all elements in the array starting at the given index and looping back to the start
    /// if the end is reached. The `body` is applied to each element and iteration is stopped when
    /// `body` returns a non-nil value or all elements have been iterated.
    ///
    /// - Returns: The result and index of the first element passed to `body` which returned
    ///   non-nil, or `nil` if no such element exists.
    @inlinable
    internal mutating func _loopingFirstIndexWithResult<Result>(
        startingAt middleIndex: Index,
        whereNonNil body: (inout Element) throws -> Result?
    ) rethrows -> (Result, Index)? {
        if let result = try self._firstIndexWithResult(in: middleIndex..<self.endIndex, whereNonNil: body) {
            return result
        }

        return try self._firstIndexWithResult(in: self.startIndex..<middleIndex, whereNonNil: body)
    }

    @inlinable
    internal mutating func _firstIndexWithResult<Result>(
        in indices: Range<Index>,
        whereNonNil body: (inout Element) throws -> Result?
    ) rethrows -> (Result, Index)? {
        for index in indices {
            if let result = try body(&self[index]) {
                return (result, index)
            }
        }
        return nil
    }
}
