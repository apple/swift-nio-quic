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
struct PageLayoutTests {
    @Test
    func capacitiesDoubleAndThenLevelOff() {
        #expect((0..<8).map { Page.capacity(ofPage: $0) } == [1, 2, 4, 8, 16, 32, 64, 64])
        #expect(Page.capacity(ofPage: 100) == 64)
    }

    @Test
    func doublingPagesCoverEveryIndexBelowTheFixedCapacityOnes() {
        let doubling = (0..<Page.firstFixedCapacityPage).map {
            Page.capacity(ofPage: $0)
        }

        #expect(doubling.reduce(0, +) == Page.firstFixedCapacityIndex)
    }

    @Test
    func positionsAtThePageBoundaries() {
        let expected: [(index: Int, page: Int, offset: Int)] = [
            (0, 0, 0),
            (1, 1, 0),
            (2, 1, 1),
            (3, 2, 0),
            (6, 2, 3),
            (7, 3, 0),
            (62, 5, 31),
            (63, 6, 0),
            (126, 6, 63),
            (127, 7, 0),
            (190, 7, 63),
            (191, 8, 0),
        ]

        for (index, page, offset) in expected {
            let position = Page.position(of: index)
            #expect(position.pageIndex == page)
            #expect(position.offset == offset)
        }
    }

    @Test
    func indicesFillPagesInOrderWithoutGapsOrRepeats() {
        var seen = Set<Int>()
        var pageCount = 0
        var filled: [Int: Int] = [:]

        for index in 0..<1000 {
            let position = Page.position(of: index)
            #expect(position.offset < Page.capacity(ofPage: position.pageIndex))

            if position.offset == 0 {
                #expect(position.pageIndex == pageCount)
                pageCount += 1
            } else {
                #expect(position.pageIndex == pageCount - 1)
            }

            #expect(seen.insert(position.pageIndex * Page.maxPageCapacity + position.offset).inserted)
            filled[position.pageIndex, default: 0] &+= 1
        }

        // Every page but the last is full.
        for page in 0..<(pageCount - 1) {
            #expect(filled[page] == Page.capacity(ofPage: page))
        }
    }
}

@Suite
struct PagedBufferTests {
    @Test
    func appendingAllocatesAPageOnlyWhenTheLastOneIsFull() {
        var buffer = PagedBuffer<Int>()
        #expect(buffer.count == 0)
        #expect(buffer.pageCount == 0)

        for index in 0..<200 {
            buffer.append()
            #expect(buffer.count == index + 1)
            #expect(buffer.pageCount == Page.position(of: index).pageIndex + 1)
        }
    }

    @Test
    func slotsKeepTheirAddressAndValueAsTheBufferGrows() {
        var buffer = PagedBuffer<Int>()
        var pointers: [UnsafeMutablePointer<Int>] = []

        for index in 0..<300 {
            let slot = buffer.append()
            slot.initialize(to: index)
            pointers.append(slot)
        }

        for index in 0..<300 {
            #expect(buffer.pointer(at: index) == pointers[index], "slot \(index) moved")
            #expect(pointers[index].pointee == index)
        }

        for pointer in pointers {
            pointer.deinitialize(count: 1)
        }
    }

    @Test
    func slotsOfOnePageAreContiguous() {
        var buffer = PagedBuffer<Int>()
        for _ in 0...300 {
            buffer.append()
        }

        for index in 0..<300 {
            let here = Page.position(of: index)
            let next = Page.position(of: index + 1)
            if here.pageIndex == next.pageIndex {
                #expect(buffer.pointer(at: index + 1) == buffer.pointer(at: index) + 1)
            } else {
                #expect(buffer.pointer(at: index + 1) == buffer.basePointer(ofPage: next.pageIndex))
                #expect(here.offset == Page.capacity(ofPage: here.pageIndex) - 1)
            }
        }
    }

    @Test
    func appendingNilInitializesWholePages() {
        var buffer = PagedBuffer<Int?>()
        for _ in 0...200 {
            buffer.appendNil()
        }

        // Whole-page operations rely on every slot of an allocated page holding a value, including
        // the ones past the end which were never handed out.
        for page in 0..<buffer.pageCount {
            let base = buffer.basePointer(ofPage: page)
            for offset in 0..<Page.capacity(ofPage: page) {
                #expect(base[offset] == nil, "page \(page), slot \(offset)")
            }
        }

        buffer.pointer(at: 200).pointee = 7
        buffer.setAllToNil()
        #expect(buffer.pointer(at: 200).pointee == nil)

        buffer.deinitializeAll()
    }

    @Test
    func appendingNilDoesNotClobberEarlierSlots() {
        var buffer = PagedBuffer<Int?>()
        for index in 0..<10 {
            buffer.appendNil().pointee = index
        }

        // Slot 3 shares a page with slots the later appends nil-initialized around it.
        for index in 0..<10 {
            #expect(buffer.pointer(at: index).pointee == index)
        }

        #expect(buffer.count == 10)
        buffer.deinitializeAll()
    }
}
