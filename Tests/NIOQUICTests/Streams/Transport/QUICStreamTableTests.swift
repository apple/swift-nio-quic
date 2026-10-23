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

@_spi(Essentials) @_spi(ProtocolProvider) @testable import NIOQUIC

@available(anyAppleOS 27, *)
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

@available(anyAppleOS 27, *)
extension QUICStreamTable where Consumer: ~Copyable {
    fileprivate func events(for handle: QUICStreamHandle) -> QUICStreamEvents? {
        self._transportStates.pointer(for: handle)?.pointee.events
    }
}

@available(anyAppleOS 27, *)
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
    @available(anyAppleOS 27, *)
    private static func makeTable(role: Role = .client) -> QUICStreamTable<TestConsumer> {
        QUICStreamTable(role: role)
    }

    @available(anyAppleOS 27, *)
    private static func makeRecordingTable(role: Role = .client) -> QUICStreamTable<RecordingConsumer> {
        QUICStreamTable(role: role)
    }

    // MARK: - ID indexing

    @available(anyAppleOS 27, *)
    @Test
    func insertWithIDIsIndexedByID() {
        let table = Self.makeTable()
        defer { table.retireAnchor() }
        let handle = table.insertSlot(id: 4, state: 1)
        #expect(table.handle(forID: 4) == handle)
        #expect(table.count == 1)
    }

    @available(anyAppleOS 27, *)
    @Test
    func insertWithoutIDIsNotIndexedUntilAssigned() {
        let table = Self.makeTable()
        defer { table.retireAnchor() }
        let handle = table.insertSlot(id: nil, state: 1)
        #expect(table.handle(forID: 4) == nil)

        table.assignID(4, to: handle)
        #expect(table.handle(forID: 4) == handle)

        let id = table.withStream(handle: handle) { stream, _ in stream.id }
        #expect(id == 4)
    }

    @available(anyAppleOS 27, *)
    @Test
    func idLookupFailsOnceTheSlotIsVacated() {
        let table = Self.makeTable()
        defer { table.retireAnchor() }
        let handle = table.insertSlot(id: 4, state: 1)
        table.vacateSlot(handle)
        #expect(table.handle(forID: 4) == nil)
        #expect(table.count == 0)
    }

    @available(anyAppleOS 27, *)
    @Test
    func assignIDToVacatedSlotIsIgnored() {
        let table = Self.makeTable()
        defer { table.retireAnchor() }
        let handle = table.insertSlot(id: nil, state: 1)
        table.vacateSlot(handle)
        table.assignID(4, to: handle)
        #expect(table.handle(forID: 4) == nil)
    }

    // MARK: - Generations

    @available(anyAppleOS 27, *)
    @Test
    func eventsForARecycledSlotAreDroppedForTheStaleHandle() {
        let table = Self.makeTable()
        defer { table.retireAnchor() }
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

    @available(anyAppleOS 27, *)
    @Test
    func repeatedMarkReadyEnqueuesOnceAndUnionsEvents() {
        let table = Self.makeTable()
        defer { table.retireAnchor() }
        let handle = table.insertSlot(id: nil, state: 1)

        table.markReady(handle: handle, events: .readable)
        table.markReady(handle: handle, events: .writable)

        #expect(table.hasPendingWork)
        #expect(table.events(for: handle) == [.readable, .writable])
    }

    @available(anyAppleOS 27, *)
    @Test
    func markReadyWithNoEventsDoesNotEnqueue() {
        let table = Self.makeTable()
        defer { table.retireAnchor() }
        let handle = table.insertSlot(id: nil, state: 1)
        table.markReady(handle: handle, events: [])
        #expect(!table.hasPendingWork)
    }

    @available(anyAppleOS 27, *)
    @Test
    func markReadyOnAVacatedSlotIsIgnored() {
        let table = Self.makeTable()
        defer { table.retireAnchor() }
        let handle = table.insertSlot(id: nil, state: 1)

        table.vacateSlot(handle)
        table.markReady(handle: handle, events: .readable)

        #expect(!table.hasPendingWork)
    }

    // MARK: - Consumer state

    @available(anyAppleOS 27, *)
    @Test
    func slotWithoutStateIsNotVisited() {
        let table = Self.makeTable()
        defer { table.retireAnchor() }
        let handle = table.insertSlot(id: nil, state: nil)
        #expect(table.withStream(handle: handle) { _, state in state } == nil)
    }

    @available(anyAppleOS 27, *)
    @Test
    func stateMutationsPersistBetweenVisits() {
        let table = Self.makeTable()
        defer { table.retireAnchor() }
        let handle = table.insertSlot(id: nil, state: 1)
        table.withStream(handle: handle) { _, state in state += 1 }
        #expect(table.withStream(handle: handle) { _, state in state } == 2)
    }

    @available(anyAppleOS 27, *)
    @Test
    func nestedVisitsToOneStreamTrap() async throws {
        #if os(Android)
        try Test.cancel("Exit tests cannot be run on Android yet")
        #else
        await #expect(processExitsWith: .failure) {
            let table = Self.makeTable()
            let handle = table.insertSlot(id: nil, state: 1)

            table.withStream(handle: handle) { _, _ in
                table.withStream(handle: handle) { _, _ in
                }
            }
        }
        #endif
    }

    // MARK: - Output pending

    @available(anyAppleOS 27, *)
    @Test
    func clearOutputPendingReportsOnce() {
        let table = Self.makeTable()
        defer { table.retireAnchor() }
        table.markOutputPending()

        #expect(table.clearOutputPending())
        #expect(!table.clearOutputPending())
    }

    @available(anyAppleOS 27, *)
    @Test
    func writeDoesNotMarkPendingOutput() {
        let table = Self.makeTable()
        defer { table.retireAnchor() }
        let handle = table.insertSlot(id: nil, state: 1)
        table.withStream(handle: handle) { stream, _ in
            stream.write(ByteBuffer(string: "hello"))
        }
        #expect(!table.clearOutputPending())
    }

    // MARK: - Draining

    @available(anyAppleOS 27, *)
    @Test
    func readyStreamIsVisitedOnceAndNotAgain() {
        let table = Self.makeRecordingTable()
        defer { table.retireAnchor() }
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

    @available(anyAppleOS 27, *)
    @Test
    func visitedOncePerTick() {
        let table = Self.makeRecordingTable()
        defer { table.retireAnchor() }
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)
        table.markReady(handle: handle, events: .readable)
        table.markReady(handle: handle, events: [.stopSending, .writable])

        table.drain(into: &consumer)
        #expect(consumer.sightings.count == 1)
        #expect(consumer.sightings[0].events == [.readable, .writable, .stopSending])
    }

    @available(anyAppleOS 27, *)
    @Test
    func ignoredVisitIsOfferedAgain() {
        let table = Self.makeRecordingTable()
        defer { table.retireAnchor() }
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

    @available(anyAppleOS 27, *)
    @Test
    func lastVisitIsFinishedWhenTheConsumerNeverSeesNil() {
        let table = Self.makeRecordingTable()
        defer { table.retireAnchor() }
        var consumer = RecordingConsumer(pullLimit: 1)
        let handle = table.insertSlot(id: 0, state: nil)
        table.markReady(handle: handle, events: .readable)
        table.drain(into: &consumer)
        #expect(consumer.sightings.count == 1)

        // The visit should've been finished by the iterator: on events, and no pending work.
        #expect(table.events(for: handle) == [])
        #expect(!table.hasPendingWork)
    }

    @available(anyAppleOS 27, *)
    @Test
    func makeStreamStateIsCalledOncePerStream() {
        let table = Self.makeRecordingTable()
        defer { table.retireAnchor() }
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)

        table.markReady(handle: handle, events: .readable)
        table.drain(into: &consumer)
        table.markReady(handle: handle, events: .readable)
        table.drain(into: &consumer)

        #expect(consumer.madeStates == 1)
        #expect(consumer.sightings.map { $0.stateMadeAt } == [1, 1])
    }

    @available(anyAppleOS 27, *)
    @Test
    func slotInsertedWithStateIsNotOfferedToTheConsumer() {
        let table = Self.makeRecordingTable()
        defer { table.retireAnchor() }
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: RecordingConsumer.StreamState(madeAt: 42))
        table.markReady(handle: handle, events: [.opened, .readable])
        table.drain(into: &consumer)

        #expect(consumer.madeStates == 0)
        #expect(consumer.sightings.map { $0.stateMadeAt } == [42])
    }

    @available(anyAppleOS 27, *)
    @Test
    func nestedAccessToAnotherStreamFromInsideAVisit() {
        let table = Self.makeRecordingTable()
        defer { table.retireAnchor() }
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

    @available(anyAppleOS 27, *)
    @Test
    func withStreamHandsOverTheStreamAndItsStateTogether() {
        let table = Self.makeRecordingTable()
        defer { table.retireAnchor() }
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

    @available(anyAppleOS 27, *)
    @Test
    func closedIsDeliveredOnceAndRecyclesTheSlot() {
        let table = Self.makeRecordingTable()
        defer { table.retireAnchor() }
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

    @available(anyAppleOS 27, *)
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
    }

    @available(anyAppleOS 27, *)
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

    // MARK: - Opening

    @available(anyAppleOS 27, *)
    @Test(
        arguments: [
            (Role.client, QUICStreamType.serverInitiatedBidirectional),
            (.client, .serverInitiatedUnidirectional),
            (.server, .clientInitiatedBidirectional),
            (.server, .clientInitiatedUnidirectional),
        ]
    )
    func openRejectsAStreamTypeThisRoleCannotInitiate(role: Role, type: QUICStreamType) {
        let table = Self.makeRecordingTable(role: role)
        defer { table.retireAnchor() }

        #expect(throws: QUICError.invalidStreamTypeForRole) {
            _ = try table.open(type, state: .init(madeAt: 1))
        }
        #expect(table.count == 0)
    }

    @available(anyAppleOS 27, *)
    @Test
    func openWithoutAConnectionBehindTheTableThrows() {
        let table = Self.makeRecordingTable(role: .client)
        defer { table.retireAnchor() }

        #expect(throws: QUICError.invalidStreamState) {
            _ = try table.open(.clientInitiatedBidirectional, state: .init(madeAt: 1))
        }
        #expect(table.count == 0)
    }

    @available(anyAppleOS 27, *)
    @Test
    func openAfterCloseAllThrowsAndAddsNothing() {
        let table = Self.makeRecordingTable(role: .client)
        table.opener = .unreachable(context: table.context)
        var consumer = RecordingConsumer()
        table.closeAll(error: nil, disconnect: nil, into: &consumer)

        #expect(throws: QUICError.invalidStreamState) {
            _ = try table.open(.clientInitiatedBidirectional, state: .init(madeAt: 1))
        }
        #expect(table.count == 0)
    }

    // MARK: - Stack events

    @available(anyAppleOS 27, *)
    @Test
    func roomAvailableMarksTheStreamWritable() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)

        // The stack delivers a stream's events to the linkage the stream attached with. There's no
        // flow here, so attach the stream to an empty linkage, which ignores the stream's calls.
        let identifier = table.registerIdentifier(for: handle)
        table.attach(handle, identifier: identifier, linkage: BaseOutboundStreamLinkage())
        let upperLinkage = QUICStreamUpperLinkage(table: table, handle: handle, identifier: identifier)
        table.withEventContext { eventContext in
            upperLinkage.handleOutboundRoomAvailableEvent(for: InstanceIdentifier(), in: &eventContext)
        }

        table.drain(into: &consumer)
        #expect(consumer.sightings.count == 1)
        #expect(consumer.sightings[0].events == .writable)

        // The slot registered with the stack, so it must be removed before the table is released.
        table.closeAll(error: nil, disconnect: nil, into: &consumer)
    }

    // MARK: - Reading

    /// Each pull enters the stack. Nothing follows a FIN, so a read that brings one pulls once; any
    /// other read pulls again to find the stack has nothing more.
    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func readPullsAgainOnlyWithoutTheFin(fin: Bool) {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: .init(madeAt: 0))
        let identifier = table.registerIdentifier(for: handle)
        let lowerLinkage = TestLowerLinkage(context: table.context)
        table.attach(handle, identifier: identifier, linkage: BaseOutboundStreamLinkage(external: lowerLinkage))
        table.assignID(0, to: handle)

        var frame = Frame(copyBuffer: Array("hello".utf8))
        frame.connectionComplete = fin
        lowerLinkage.inbound = FrameArray(frame: frame)

        let outcome = table.withStream(handle: handle) { stream, _ in
            stream.read { bytes in bytes.byteCount }
        }

        let expected: QUICStreamReadOutcome = fin ? .endOfStream(5) : .read(5)
        #expect(outcome == expected)
        #expect(lowerLinkage.receiveCalls == (fin ? 1 : 2))

        table.closeAll(error: nil, disconnect: nil, into: &consumer)
    }

    @available(anyAppleOS 27, *)
    @Test(arguments: [true, false])
    func readOnlyDeliversFINWhenEverythingWasConsumed(fin: Bool) {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: .init(madeAt: 0))
        let identifier = table.registerIdentifier(for: handle)
        let lowerLinkage = TestLowerLinkage(context: table.context)
        table.attach(handle, identifier: identifier, linkage: BaseOutboundStreamLinkage(external: lowerLinkage))
        table.assignID(0, to: handle)

        // Configure lengths to read.
        let data = Array("hello".utf8)
        let totalLength = data.count
        let firstReadLength = 3
        let secondReadLength = totalLength - firstReadLength

        var frame = Frame(copyBuffer: data)
        frame.connectionComplete = fin
        lowerLinkage.inbound = FrameArray(frame: frame)

        let outcomePartialRead = table.withStream(handle: handle) { stream, _ in
            stream.read { bytes in
                if bytes.byteCount == totalLength {
                    return firstReadLength
                } else {
                    return 0
                }
            }
        }

        let expectedPartialRead: QUICStreamReadOutcome = .read(firstReadLength)
        #expect(outcomePartialRead == expectedPartialRead)
        #expect(lowerLinkage.receiveCalls == 1)

        let outcomeFinalRead = table.withStream(handle: handle) { stream, _ in
            stream.read { bytes in bytes.byteCount }
        }

        let expectedFinalRead: QUICStreamReadOutcome = fin ? .endOfStream(secondReadLength) : .read(secondReadLength)
        #expect(outcomeFinalRead == expectedFinalRead)
        #expect(lowerLinkage.receiveCalls == (fin ? 1 : 2))

        let outcomeEmptyRead = table.withStream(handle: handle) { stream, _ in
            stream.read { bytes in bytes.byteCount }
        }

        let expectedEmptyRead: QUICStreamReadOutcome = fin ? .endOfStream(0) : .nothingAvailable
        #expect(outcomeEmptyRead == expectedEmptyRead)
        #expect(lowerLinkage.receiveCalls == (fin ? 1 : 3))

        table.closeAll(error: nil, disconnect: nil, into: &consumer)
    }

    /// A stream's linkage outlives its slot: the stack holds it until the flow detaches and its
    /// queued events run. An event through it must then not reach the stream that took the slot over.
    @available(anyAppleOS 27, *)
    @Test
    func eventsThroughAStaleLinkageAreDropped() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let stale = table.insertSlot(id: 0, state: nil)
        let staleIdentifier = table.registerIdentifier(for: stale)
        table.attach(stale, identifier: staleIdentifier, linkage: BaseOutboundStreamLinkage())
        let staleLinkage = QUICStreamUpperLinkage(table: table, handle: stale, identifier: staleIdentifier)
        table.vacateSlot(stale)

        let live = table.insertSlot(id: 4, state: nil)
        #expect(stale.index == live.index)
        let liveIdentifier = table.registerIdentifier(for: live)
        table.attach(live, identifier: liveIdentifier, linkage: BaseOutboundStreamLinkage())

        table.withEventContext { eventContext in
            staleLinkage.handleOutboundRoomAvailableEvent(for: InstanceIdentifier(), in: &eventContext)
        }
        #expect(table.events(for: live) == [])
        #expect(!table.hasPendingWork)

        table.closeAll(error: nil, disconnect: nil, into: &consumer)
    }

    /// Closing a stream disconnects it from its flow, and the stack can deliver the stream's events
    /// before that call returns. While the table vacates the stream's slot, they must find nothing.
    @available(anyAppleOS 27, *)
    @Test
    func eventsDeliveredWhileAStreamIsVacatedAreDropped() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer()
        let handle = table.insertSlot(id: 0, state: nil)
        let identifier = table.registerIdentifier(for: handle)
        let lowerLinkage = TestLowerLinkage(context: table.context)
        table.attach(handle, identifier: identifier, linkage: BaseOutboundStreamLinkage(external: lowerLinkage))
        lowerLinkage.upper = QUICStreamUpperLinkage(table: table, handle: handle, identifier: identifier)

        table.vacateSlot(handle)
        #expect(lowerLinkage.reportedDisconnect)
        #expect(!table.hasPendingWork)

        table.closeAll(error: nil, disconnect: nil, into: &consumer)
    }

    /// A consumer may stop pulling, so `closeAll` tears the streams it didn't pull down all at once.
    /// Events the stack delivers meanwhile must find nothing.
    @available(anyAppleOS 27, *)
    @Test
    func eventsDeliveredWhileAllSlotsAreTornDownAreDropped() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer(pullLimit: 0)
        let handle = table.insertSlot(id: 0, state: nil)
        let identifier = table.registerIdentifier(for: handle)
        let lowerLinkage = TestLowerLinkage(context: table.context)
        table.attach(handle, identifier: identifier, linkage: BaseOutboundStreamLinkage(external: lowerLinkage))
        lowerLinkage.upper = QUICStreamUpperLinkage(table: table, handle: handle, identifier: identifier)

        table.closeAll(error: nil, disconnect: nil, into: &consumer)
        #expect(lowerLinkage.reportedDisconnect)
        #expect(consumer.sightings.isEmpty)
        #expect(table.count == 0)
    }

    /// Closing one stream can make the stack report events for another before the close returns.
    /// While the table tears all its slots down, those must find nothing either.
    @available(anyAppleOS 27, *)
    @Test
    func eventsForOtherStreamsDeliveredWhileAllSlotsAreTornDownAreDropped() {
        let table = Self.makeRecordingTable()
        var consumer = RecordingConsumer(pullLimit: 0)

        let first = table.insertSlot(id: 0, state: nil)
        let firstIdentifier = table.registerIdentifier(for: first)
        let firstLinkage = TestLowerLinkage(context: table.context)
        table.attach(first, identifier: firstIdentifier, linkage: BaseOutboundStreamLinkage(external: firstLinkage))

        let second = table.insertSlot(id: 4, state: nil)
        let secondIdentifier = table.registerIdentifier(for: second)
        let secondLinkage = TestLowerLinkage(context: table.context)
        table.attach(second, identifier: secondIdentifier, linkage: BaseOutboundStreamLinkage(external: secondLinkage))
        let secondUpper = QUICStreamUpperLinkage(table: table, handle: second, identifier: secondIdentifier)

        // The first stream closes first. Its flow reports room available on the second, as the
        // stack's connection does when a disconnect sends frames.
        var secondEvents: QUICStreamEvents?
        firstLinkage.onDisconnect = { eventContext in
            secondUpper.handleOutboundRoomAvailableEvent(for: InstanceIdentifier(), in: &eventContext)
            secondEvents = table.events(for: second)
        }

        table.closeAll(error: nil, disconnect: nil, into: &consumer)
        #expect(secondEvents == [.closed, .readable])
    }

}

/// A stream's lower linkage standing in for the stack: it hands over the inbound frames a test
/// queues, and reports the stream's disconnect back up before the disconnect call returns, the way
/// the stack can.
@available(anyAppleOS 27, *)
private final class TestLowerLinkage: ExternalOutboundStreamLinkage, ProtocolInstance {
    let context: NetworkContext
    var identifier: InstanceIdentifier
    var eventManager: ProtocolEventManager

    /// The stream's upper linkage, until the stack tears this linkage down.
    var upper: QUICStreamUpperLinkage<RecordingConsumer>?

    private(set) var reportedDisconnect: Bool

    /// The frames the next pull hands over.
    var inbound: FrameArray?

    /// How many times the stream pulled inbound frames.
    private(set) var receiveCalls: Int

    /// Called at the end of the stream's disconnect, before the stack's call returns.
    var onDisconnect: ((inout NetworkContext.EventContext) -> Void)?

    /// Registers with `context`, which acquires the event context: must be called with none held.
    init(context: NetworkContext) {
        self.context = context
        var eventManager = ProtocolEventManager()
        self.identifier = InstanceIdentifier(context: context, eventManager: &eventManager)
        self.eventManager = eventManager
        self.upper = nil
        self.reportedDisconnect = false
        self.inbound = nil
        self.receiveCalls = 0
        self.onDisconnect = nil
    }

    func disconnect(
        error: NetworkError?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        self.reportedDisconnect = true
        self.upper?.handleDisconnectedEvent(error: error, for: self.identifier, in: &eventContext)
        self.onDisconnect?(&eventContext)
    }

    func teardown(in eventContext: inout NetworkContext.EventContext) {
        self.upper = nil
        self.onDisconnect = nil
        self.unregisterEventManager(in: &eventContext)
    }

    func protocolIsConnected(in eventContext: inout NetworkContext.EventContext) -> Bool {
        true
    }

    func connect(for instance: InstanceIdentifier, in eventContext: inout NetworkContext.EventContext) {}

    func detach(
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) throws(NetworkError) {}

    func handleApplicationEvent(
        event: ApplicationEvent,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {}

    func getMetadata<P: NetworkProtocol>(
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) -> ProtocolMetadata<P>? {
        nil
    }

    func getMetrics(
        requestedNetworkMetric: RequestedNetworkMetrics,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) -> NetworkMetrics? {
        nil
    }

    func invokeAttachUpperProtocol(
        _ upperProtocol: BaseInboundStreamLinkage,
        remote: Endpoint?,
        local: Endpoint?,
        parameters: Parameters?,
        path: PathProperties?
    ) throws(NetworkError) {}

    func receiveStreamData(
        minimumBytes: Int,
        maximumBytes: Int,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) throws(NetworkError) -> FrameArray? {
        self.receiveCalls += 1
        return self.inbound.take()
    }

    func getOutboundStreamDataRoomAvailable(
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) throws(NetworkError) -> Int {
        0
    }

    func sendStreamData(
        _ streamData: consuming FrameArray,
        from instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) throws(NetworkError) {
        streamData.finalizeAllFramesAsFailed()
    }
}

@available(anyAppleOS 27, *)
extension QUICStreamTable {
    /// Builds a table on an event-loop-backed context: acquiring the default scheduler's context
    /// traps off its queue. Its anchor must be retired, or the table closed, before it's released.
    convenience init(role: Role) {
        let callouts = QUICCallouts()
        let context = NetworkContext(
            identifier: "QUICStreamTableTests",
            externalScheduler: EventLoopBackedScheduler(eventLoop: EmbeddedEventLoop(), callouts: callouts)
        )
        self.init(role: role, context: context, callouts: callouts)
    }
}

@available(anyAppleOS 27, *)
extension QUICStreamOpener {
    /// An opener with no QUIC connection behind it, so that `open` gets past its opener check.
    /// Attaching through it traps, so `open` must refuse before it gets here.
    static func unreachable(context: NetworkContext) -> Self {
        QUICStreamOpener(listener: BaseStreamListenerLinkage(), connection: InstanceIdentifier(), context: context)
    }
}
