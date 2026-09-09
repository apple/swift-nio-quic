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
import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
import Testing

@testable import NIOQUIC

@available(anyAppleOS 26, *)
private struct TestConsumer: QUICStreamConsumer {
    typealias StreamState = Int

    mutating func makeStreamState(_ stream: inout NIOQUIC.QUICStream<Self>) -> Int {
        Issue.record("state requested for a table which is never drained")
        return 0
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        Issue.record("drained a table which is never drained")
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    fileprivate func events(for handle: QUICStreamHandle) -> QUICStreamEvents? {
        self._transportStates.pointer(for: handle)?.pointee.events
    }
}

@available(anyAppleOS 26, *)
private struct RecordingConsumer: QUICStreamConsumer {
    struct StreamState {
        /// Which ``makeStreamState(_:)`` call produced this, counting from one.
        var madeAt: Int
    }

    struct Sighting: Equatable {
        var handle: QUICStreamHandle
        var events: QUICStreamEvents
        var stateMadeAt: Int
    }

    init(
        pullLimit: Int? = nil,
        onVisit: ((_ visit: consuming QUICStreamVisit<Self>) -> Void)? = nil
    ) {
        self.pullLimit = pullLimit
        self.onVisit = onVisit
        self.madeStates = 0
        self.sightings = []
    }

    /// How many states have been made.
    var madeStates: Int

    /// One entry per visit, across every drain.
    var sightings: [Sighting]

    /// The most visits to pull per drain, or `nil` to pull them all.
    var pullLimit: Int?

    /// Called for each visit.
    var onVisit: ((_ visit: consuming QUICStreamVisit<Self>) -> Void)?

    mutating func makeStreamState(_ stream: inout NIOQUIC.QUICStream<Self>) -> StreamState {
        self.madeStates &+= 1
        return StreamState(madeAt: self.madeStates)
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        var pulled = 0

        while self.pullLimit != pulled, let visit = streams.next() {
            pulled &+= 1

            let sighting = Sighting(
                handle: visit.handle,
                events: visit.events,
                stateMadeAt: visit.state.madeAt
            )

            self.sightings.append(sighting)

            if let onVisit = self.onVisit {
                onVisit(visit)
            }
        }
    }
}

private struct Boom: Error {}

@Suite
struct QUICStreamTableTests {
    @available(anyAppleOS 26, *)
    private static func makeTable(role: Role = .client) -> QUICStreamTable<TestConsumer> {
        QUICStreamTable(role: role)
    }

    @available(anyAppleOS 26, *)
    private static func makeRecordingTable(role: Role = .client) -> QUICStreamTable<RecordingConsumer> {
        QUICStreamTable(role: role)
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

    // MARK: - Draining

    @available(anyAppleOS 26, *)
    @Test
    func readyStreamIsVisitedOnceAndNotAgain() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)
        table.markReady(handle: handle, events: .readable)

        table.drain(into: &consumer)
        let sighting = RecordingConsumer.Sighting(handle: handle, events: .readable, stateMadeAt: 1)
        #expect(consumer.sightings == [sighting])

        // Shouldn't be seen a second time.
        table.drain(into: &consumer)
        #expect(consumer.sightings == [sighting])
    }

    @available(anyAppleOS 26, *)
    @Test
    func visitedOncePerTick() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)
        table.markReady(handle: handle, events: .readable)
        table.markReady(handle: handle, events: [.stopSending, .writable])

        table.drain(into: &consumer)
        #expect(consumer.sightings.count == 1)
        #expect(consumer.sightings[0].events == [.readable, .writable, .stopSending])
    }

    @available(anyAppleOS 26, *)
    @Test
    func ignoredVisitIsOfferedAgain() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()

        // Don't pull any streams.
        consumer.pullLimit = 0
        let handle = table.insertSlot(id: 0, state: nil)
        table.markReady(handle: handle, events: .readable)

        // Run the first drain.
        table.drain(into: &consumer)
        #expect(consumer.sightings.isEmpty)

        // Remove the pull limit and drain again.
        consumer.pullLimit = nil
        table.drain(into: &consumer)
        #expect(consumer.sightings.count == 1)
        #expect(consumer.sightings[0].events == .readable)
    }

    @available(anyAppleOS 26, *)
    @Test
    func lastVisitIsFinishedWhenTheConsumerNeverSeesNil() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer(pullLimit: 1)
        let handle = table.insertSlot(id: 0, state: nil)
        table.markReady(handle: handle, events: .readable)
        table.drain(into: &consumer)
        #expect(consumer.sightings.count == 1)

        // The visit should've been finished by the iterator: on events, and no pending work.
        #expect(table.events(for: handle) == [])
        #expect(!table.hasPendingWork)
    }

    @available(anyAppleOS 26, *)
    @Test
    func makeStreamStateIsCalledOncePerStream() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)

        table.markReady(handle: handle, events: .readable)
        table.drain(into: &consumer)
        table.markReady(handle: handle, events: .readable)
        table.drain(into: &consumer)

        #expect(consumer.madeStates == 1)
        #expect(consumer.sightings.map { $0.stateMadeAt } == [1, 1])
    }

    @available(anyAppleOS 26, *)
    @Test
    func slotInsertedWithStateIsNotOfferedToTheConsumer() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: RecordingConsumer.StreamState(madeAt: 42))
        table.markReady(handle: handle, events: [.opened, .readable])
        table.drain(into: &consumer)

        #expect(consumer.madeStates == 0)
        #expect(consumer.sightings.map { $0.stateMadeAt } == [42])
    }

    @available(anyAppleOS 26, *)
    @Test
    func nestedAccessToAnotherStreamFromInsideAVisit() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let first = table.insertSlot(id: 0, state: nil)
        let second = table.insertSlot(id: 4, state: nil)

        // Reach the second stream from inside every visit, which is the case the exclusivity rules
        // make impossible through the slot store's closure-taking members.
        var visitedBy: [Int] = []
        consumer.onVisit = { visit in
            var streams = visit.streams
            let outer = visit.state.madeAt

            if let inner = streams.withStream(handle: second, execute: { _, state in state.madeAt }) {
                visitedBy.append(outer)
                visitedBy.append(inner)
            }
        }

        table.markReady(handle: first, events: .readable)
        table.markReady(handle: second, events: .readable)
        table.drain(into: &consumer)

        #expect(consumer.sightings.count == 2)
        // Each visit sees two streams: the outer stream (first and second) and then the second
        // stream.
        #expect(visitedBy == [1, 2, 2, 2])
    }

    @available(anyAppleOS 26, *)
    @Test
    func withStreamHandsOverTheStreamAndItsStateTogether() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        consumer.onVisit = { visit in
            var visit = consume visit
            visit.withStream { stream, state in
                stream.write(ByteBuffer(string: "hello"))
                try? stream.flush()
                state.madeAt &+= 100
            }
        }

        let handle = table.insertSlot(id: 0, state: nil)
        table.markReady(handle: handle, events: .readable)
        table.drain(into: &consumer)

        consumer.onVisit = nil
        table.markReady(handle: handle, events: .writable)
        table.drain(into: &consumer)
        #expect(consumer.sightings.map { $0.stateMadeAt } == [1, 101])
    }

    // MARK: - Closing

    @available(anyAppleOS 26, *)
    @Test
    func closedIsDeliveredOnceAndRecyclesTheSlot() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)

        var takenStates: [Int] = []
        var reachedAfterTake: [Int?] = []
        consumer.onVisit = { visit in
            guard let state = visit.finish() else { return }

            takenStates.append(state.madeAt)

            // The state's been taken but the handle is still valid (the visit hasn't finished
            // yet). Reaching back through the table shouldn't give us access to the stream
            // (i.e. this should return `nil`).
            let madeAt = table.withStream(handle: handle) { _, state in state.madeAt }
            reachedAfterTake.append(madeAt)
        }

        table.markReady(handle: handle, events: [.readable, .closed])
        table.drain(into: &consumer)
        #expect(consumer.sightings.count == 1)
        #expect(consumer.sightings[0].events == [.readable, .closed])
        #expect(takenStates == [1])
        #expect(reachedAfterTake == [nil])
        #expect(table.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func closeAllDeliversClosedForEveryLiveStream() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let first = table.insertSlot(id: 0, state: nil)
        let second = table.insertSlot(id: 4, state: nil)
        let third = table.insertSlot(id: 8, state: nil)
        table.markReady(handle: second, events: .readable)
        table.closeAll(error: Boom(), disconnect: nil, into: &consumer)

        #expect(consumer.sightings.count == 3)
        // Second is enqueued first (when marked as ready), first and third are added to the
        // queue by 'closeAll'.
        #expect(consumer.sightings.map { $0.handle } == [second, first, third])
        #expect(consumer.sightings.allSatisfy { $0.events.contains(.closed) })
        #expect(table.count == 0)

        table.drain(into: &consumer)
    }

    @available(anyAppleOS 26, *)
    @Test
    func closeAllRecyclesSlotsTheConsumerNeverPulls() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer(pullLimit: 1)

        _ = table.insertSlot(id: 0, state: nil)
        _ = table.insertSlot(id: 4, state: nil)  // ignored because of pullLimit

        table.closeAll(error: Boom(), disconnect: nil, into: &consumer)
        #expect(consumer.sightings.count == 1)
        #expect(table.count == 0)
    }

    // MARK: - Stack events

    @available(anyAppleOS 26, *)
    @Test
    func roomAvailableMarksTheStreamWritable() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)

        let proxy = QUICStreamProxy(table: table, handle: handle)
        proxy.handleOutboundRoomAvailableEvent(table.reference(for: handle))

        table.drain(into: &consumer)
        #expect(consumer.sightings.count == 1)
        #expect(consumer.sightings[0].events == .writable)
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamTable {
    convenience init(role: Role) {
        self.init(role: role, context: Parameters().context)
    }
}
