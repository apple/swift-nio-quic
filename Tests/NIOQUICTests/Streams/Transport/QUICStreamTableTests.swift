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
import NIOEmbedded
import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
import Testing

@testable import NIOQUIC

@available(anyAppleOS 26, *)
private enum TestConsumer: QUICStreamConsumer {
    typealias StreamState = Int
}

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// The events recorded against a slot, or `nil` if the handle doesn't address one.
    fileprivate func events(for handle: QUICStreamHandle) -> QUICStreamEvents? {
        self._transportStates.pointer(for: handle)?.pointee.events
    }
}

@Suite
struct QUICStreamTableTests {
    @available(anyAppleOS 26, *)
    private static func makeTable(role: Role = .client) -> QUICStreamTable<TestConsumer> {
        QUICStreamTable(
            role: role,
            context: NetworkContext(
                identifier: "quic-stream-table-tests",
                externalScheduler: QUICChannelEventLoop(eventLoop: EmbeddedEventLoop())
            )
        )
    }

    // MARK: - ID indexing

    @available(anyAppleOS 26, *)
    @Test
    func insertWithIDIsIndexedByID() {
        let table = Self.makeTable()
        let handle = table.insertSlot(id: 4, state: 1)
        #expect(table.handle(forID: 4) == handle)
        #expect(table.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test
    func insertWithoutIDIsNotIndexedUntilAssigned() {
        let table = Self.makeTable()
        let handle = table.insertSlot(id: nil, state: 1)
        #expect(table.handle(forID: 4) == nil)

        table.assignID(4, to: handle)
        #expect(table.handle(forID: 4) == handle)

        let id = table.withStream(handle: handle) { stream, _ in stream.id }
        #expect(id == 4)
    }

    @available(anyAppleOS 26, *)
    @Test
    func idLookupFailsOnceTheSlotIsVacated() {
        let table = Self.makeTable()
        let handle = table.insertSlot(id: 4, state: 1)
        table.vacateSlot(handle)
        #expect(table.handle(forID: 4) == nil)
        #expect(table.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func assignIDToVacatedSlotIsIgnored() {
        let table = Self.makeTable()
        let handle = table.insertSlot(id: nil, state: 1)
        table.vacateSlot(handle)
        table.assignID(4, to: handle)
        #expect(table.handle(forID: 4) == nil)
    }

    // MARK: - Generations

    @available(anyAppleOS 26, *)
    @Test
    func eventsForARecycledSlotAreDroppedForTheStaleHandle() {
        let table = Self.makeTable()
        let stale = table.insertSlot(id: nil, state: 1)
        table.vacateSlot(stale)

        // 'live' should reuse the slot that 'stale' used.
        let live = table.insertSlot(id: nil, state: 2)
        #expect(stale.index == live.index)
        #expect(stale.generation != live.generation)

        table.markReady(handle: stale, events: .readable)
        #expect(table.events(for: stale) == nil)
        #expect(table.events(for: live) == [])
    }

    // MARK: - Ready queue

    @available(anyAppleOS 26, *)
    @Test
    func repeatedMarkReadyEnqueuesOnceAndUnionsEvents() {
        let table = Self.makeTable()
        let handle = table.insertSlot(id: nil, state: 1)

        table.markReady(handle: handle, events: .readable)
        table.markReady(handle: handle, events: .writable)

        #expect(table.hasPendingWork)
        #expect(table.events(for: handle) == [.readable, .writable])
    }

    @available(anyAppleOS 26, *)
    @Test
    func markReadyWithNoEventsDoesNotEnqueue() {
        let table = Self.makeTable()
        let handle = table.insertSlot(id: nil, state: 1)
        table.markReady(handle: handle, events: [])
        #expect(!table.hasPendingWork)
    }

    @available(anyAppleOS 26, *)
    @Test
    func markReadyOnAVacatedSlotIsIgnored() {
        let table = Self.makeTable()
        let handle = table.insertSlot(id: nil, state: 1)

        table.vacateSlot(handle)
        table.markReady(handle: handle, events: .readable)

        #expect(!table.hasPendingWork)
    }

    // MARK: - Consumer state

    @available(anyAppleOS 26, *)
    @Test
    func slotWithoutStateIsNotVisited() {
        let table = Self.makeTable()
        let handle = table.insertSlot(id: nil, state: nil)
        #expect(table.withStream(handle: handle) { _, state in state } == nil)
    }

    @available(anyAppleOS 26, *)
    @Test
    func stateMutationsPersistBetweenVisits() {
        let table = Self.makeTable()
        let handle = table.insertSlot(id: nil, state: 1)
        table.withStream(handle: handle) { _, state in state += 1 }
        #expect(table.withStream(handle: handle) { _, state in state } == 2)
    }

    @available(anyAppleOS 26, *)
    @Test
    func nestedVisitsToOneStreamTrap() async {
        await #expect(processExitsWith: .failure) {
            let table = Self.makeTable()
            let handle = table.insertSlot(id: nil, state: 1)

            table.withStream(handle: handle) { _, _ in
                table.withStream(handle: handle) { _, _ in
                }
            }
        }
    }

    // MARK: - Output pending

    @available(anyAppleOS 26, *)
    @Test
    func clearOutputPendingReportsOnce() {
        let table = Self.makeTable()
        table.markOutputPending()

        #expect(table.clearOutputPending())
        #expect(!table.clearOutputPending())
    }

    @available(anyAppleOS 26, *)
    @Test
    func writeDoesNotMarkPendingOutput() {
        let table = Self.makeTable()
        let handle = table.insertSlot(id: nil, state: 1)
        table.withStream(handle: handle) { stream, _ in
            stream.write(ByteBuffer(string: "hello"))
        }
        #expect(!table.clearOutputPending())
    }
}
