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

/// Discontiguous storage for values addressed by index.
///
/// The storage is made up of pages which begin at one slot and double in size up to 64. Pages are
/// allocated once and never moved, so the address of each value is stable until the buffer is
/// destroyed and can be accessed via a pointer. By itself this construct is _not_ safe; callers
/// must take care to ensure that pointers do not outlive the buffer.
///
/// Slots are handed out densely from zero: the buffer grows by one slot at a time and never has a
/// hole, so its ``count`` is the next index it will hand out.
@usableFromInline
struct PagedBuffer<Value: ~Copyable>: ~Copyable {
    /// The allocated pages.
    @usableFromInline
    var _pages: [UnsafeMutablePointer<Value>]

    /// The number of slots handed out.
    @usableFromInline
    var _count: Int

    @inlinable
    init() {
        self._pages = []
        self._count = 0
    }

    @inlinable
    deinit {
        for page in self._pages {
            page.deallocate()
        }
    }

    /// The number of slots handed out, which is also the index the next ``append()`` returns.
    @inlinable
    var count: Int {
        self._count
    }

    /// The number of pages allocated so far.
    @inlinable
    var pageCount: Int {
        self._pages.count
    }

    /// The base pointer for a given `page`.
    ///
    /// - Precondition: `page` must exist.
    @inlinable
    func basePointer(ofPage page: Int) -> UnsafeMutablePointer<Value> {
        self._pages[page]
    }

    /// Grows the buffer by one slot and returns it, allocating a page if the last one is full.
    ///
    /// The slot's storage is allocated but not initialized. It is the caller's responsibility to
    /// initialize it. If `Value` is an `Optional` (or expressible by `nil`) then you can grow the
    /// buffer with `nil`-initialized storage using ``appendNil()``.
    @inlinable
    @discardableResult
    mutating func append() -> UnsafeMutablePointer<Value> {
        let position = Page.position(of: self._count)

        // Slots are dense, so the new one lands either on the last page or on the very next one.
        if position.pageIndex == self._pages.count {
            let capacity = Page.capacity(ofPage: position.pageIndex)
            self._pages.append(UnsafeMutablePointer<Value>.allocate(capacity: capacity))
        } else {
            assert(position.pageIndex == self._pages.count - 1)
        }

        self._count &+= 1
        return self._pages[position.pageIndex].advanced(by: position.offset)
    }

    /// The slot for `index`, which must have already been handed out.
    ///
    /// - Precondition: `index` must be below ``count``.
    @inlinable
    func pointer(at index: Int) -> UnsafeMutablePointer<Value> {
        assert(index < self._count, "slot \(index) was never handed out")
        let position = Page.position(of: index)
        return self._pages[position.pageIndex].advanced(by: position.offset)
    }
}

extension PagedBuffer where Value: ExpressibleByNilLiteral {
    /// Grows the buffer by one `nil`-initialized slot and returns it.
    ///
    /// The rest of any page this allocates is `nil`-initialized too: the whole-page operations
    /// below rely on every slot of an allocated page holding a value.
    @inlinable
    @discardableResult
    mutating func appendNil() -> UnsafeMutablePointer<Value> {
        let allocated = self._pages.count
        let pointer = self.append()

        if self._pages.count > allocated {
            self._pages[allocated].initialize(
                repeating: nil,
                count: Page.capacity(ofPage: allocated)
            )
        }

        return pointer
    }

    /// Sets every slot to `nil`.
    @inlinable
    func setAllToNil() {
        for page in 0..<self._pages.count {
            self._pages[page].update(repeating: nil, count: Page.capacity(ofPage: page))
        }
    }

    /// Deinitializes every slot of every page.
    @inlinable
    func deinitializeAll() {
        for index in self._pages.indices {
            self._pages[index].deinitialize(count: Page.capacity(ofPage: index))
        }
    }
}

@usableFromInline
enum Page {
    /// The capacity every page has once doubling stops.
    @usableFromInline
    static var maxPageCapacity: Int { 64 }

    /// The first page which holds ``maxPageCapacity`` slots.
    @inlinable
    static var firstFixedCapacityPage: Int { 6 }

    /// The first index of a fixed-capacity page.
    ///
    /// I.e. how many slots the doubling pages hold between them: `1 + 2 + 4 + ... + 32`.
    @inlinable
    static var firstFixedCapacityIndex: Int {
        (1 << Page.firstFixedCapacityPage) - 1
    }

    /// The number of slots in the given `page`.
    @inlinable
    static func capacity(ofPage page: Int) -> Int {
        if page < Self.firstFixedCapacityPage {
            return 1 << page
        } else {
            return Self.maxPageCapacity
        }
    }

    /// The page `index` lives on, and where on that page it is.
    @inlinable
    static func position(of index: Int) -> Position {
        let page: Int
        let offset: Int

        if index < Self.firstFixedCapacityIndex {
            // Page p holds 2^p slots and begins at index 2^p - 1, so:
            //
            //     2^p - 1 <= index < 2^(p+1) - 1  ==>  2^p <= index + 1 < 2^(p+1)
            //
            // i.e. index + 1 has its highest set bit at position p. The page number is that bit's
            // position; clearing it leaves the distance into the page.
            let oneBased = index + 1
            page = oneBased.bitWidth - 1 - oneBased.leadingZeroBitCount
            offset = oneBased - (1 << page)
        } else {
            let beyond = index - Self.firstFixedCapacityIndex
            page = Self.firstFixedCapacityPage + beyond / Self.maxPageCapacity
            offset = beyond % Self.maxPageCapacity
        }

        return Position(page: page, offset: offset)
    }

    @usableFromInline
    struct Position {
        @usableFromInline
        var pageIndex: Int
        @usableFromInline
        var offset: Int

        @inlinable
        init(page: Int, offset: Int) {
            self.pageIndex = page
            self.offset = offset
        }
    }
}
