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

public import DequeModule
public import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

@available(anyAppleOS 27, *)
@usableFromInline
final class QUICStreamTable<Consumer: QUICStreamConsumer & ~Copyable> {
    /// The transport state for each stream.
    @usableFromInline
    var _transportStates: QUICStreamSlots<QUICStreamTransportState>

    /// The consumer's state for each stream.
    @usableFromInline
    var _consumerStates: QUICStreamConsumerStates<Consumer.StreamState>

    /// The streams which are ready to visit.
    ///
    /// Each stream appears at most once.
    @usableFromInline
    var _ready: Deque<QUICStreamHandle>

    /// The streams the consumer hasn't yet made state for.
    @usableFromInline
    var _needsState: Deque<QUICStreamHandle>

    /// Stream handles keyed by their ID.
    @usableFromInline
    var _byID: QUICStreamIDDictionary<QUICStreamHandle>

    /// Stream handles keyed by the identifier of the flow each stream is attached to.
    ///
    /// The stack delivers a stream's events `for` the flow that sent them, not for the identifier
    /// this table registered for the stream, so this is how the `ExternalInboundStreamLinkage`
    /// witnesses resolve an event back to its stream.
    var _byFlowIdentifier: [InstanceIdentifier: QUICStreamHandle]

    /// The identifier of whichever stream is currently being attached to the stack.
    ///
    /// `BaseInboundStreamLinkage.init(external:)` reads `identifier` exactly once, at
    /// construction, to seed its own immutable field — it is never consulted again after that.
    /// This is therefore only ever read immediately after being set, from directly within the
    /// same (single-threaded, per-connection) call that just registered it.
    ///
    /// Not `private`: the `identifier` witness that reads this lives in
    /// `QUICStreamTable+ExternalInboundStreamLinkage.swift`, a different file. Only the witness
    /// has to be `@usableFromInline`.
    var _pendingAttachIdentifier: InstanceIdentifier

    /// Store the lower linkage for a stream during calls to `open`.
    ///
    /// A `QUICStreamOpener`'s attach calls `ListenerLinkage.invokeAttachUpperProtocolToNewFlow`,
    /// which delivers the new stream's `BaseOutboundStreamLinkage` synchronously via a callback to
    /// this table's own `invokeAttachLowerProtocol`, fired from inside that same call. The callback
    /// stashes the linkage here once its attach has succeeded, and `open` takes it as soon as that
    /// call returns, so this is `nil` outside an `open`. `_pendingAttachIdentifier`, by contrast,
    /// keeps the identifier registered last.
    ///
    /// Not `private`, for the same reason as `_pendingAttachIdentifier` above: written from
    /// `invokeAttachLowerProtocol` in `QUICStreamTable+ExternalInboundStreamLinkage.swift`, a
    /// different file.
    var _pendingAttachLinkage: BaseOutboundStreamLinkage?

    /// Whether there is any pending output that needs to be drained by the connection.
    @usableFromInline
    var _outputPending: Bool

    /// Used for attaching locally opened streams to the `SwiftNetwork` stack. `nil` if
    /// the table isn't associated with a connection.
    var opener: QUICStreamOpener?

    /// Requests that the owning connection drains this table out of band.
    @usableFromInline
    var outOfBandDrain: (() -> Void)?

    /// Whether an out-of-band drain has been requested.
    @usableFromInline
    var outOfBandDrainRequested: Bool

    /// Whether the connection is in a read loop which is yet to drain the table.
    ///
    /// This is used to avoid unnecessary out-of-band drain calls.
    @usableFromInline
    var inReadLoop: Bool

    /// The role of the local peer.
    let role: Role

    /// Context for the `SwiftNetwork` stack.
    let context: NetworkContext

    /// The connection's queued callouts. The table's operations acquire the context through it.
    let callouts: QUICCallouts

    /// The instance the table's operations enter the stack as.
    ///
    /// Registered for the table's lifetime, and retired by ``retireAnchor()``, which must run
    /// before the table is released: the anchor's event manager traps on deinit otherwise.
    let anchor: EventContextAnchor

    /// Creates a table. Must be called with no event context held: it registers the table's anchor.
    init(role: Role, context: NetworkContext, callouts: QUICCallouts) {
        self._transportStates = QUICStreamSlots()
        self._consumerStates = QUICStreamConsumerStates()
        self._ready = []
        self._needsState = []
        self._byID = QUICStreamIDDictionary()
        self._byFlowIdentifier = [:]
        self._pendingAttachIdentifier = InstanceIdentifier()
        self._pendingAttachLinkage = nil
        self._outputPending = false
        self.opener = nil
        self.inReadLoop = false
        self.outOfBandDrainRequested = false
        self.role = role
        self.context = context
        self.callouts = callouts
        self.anchor = EventContextAnchor(context: context)
    }

    deinit {
        precondition(
            self.isClosed,
            "QUICStreamTable released before closeAll(error:disconnect:into:) or retireAnchor()"
        )
        // Ensure all buffered data is cleaned up (i.e., finalize the frames).
        self.vacateAllSlots()
    }

    /// The number of streams in the table.
    @inlinable
    var count: Int {
        self._transportStates.count
    }

    /// Whether a drain would have anything to report.
    @inlinable
    var hasPendingWork: Bool {
        !self._ready.isEmpty
    }

    /// Whether the table has closed: its anchor is retired, and it takes no new streams.
    var isClosed: Bool {
        self.anchor.identifier.isNone
    }
}

// MARK: - Bookkeeping

@available(anyAppleOS 27, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Marks the stream for the given handle as ready, enqueueing it if it isn't already enqueued.
    @inlinable
    func markReady(handle: QUICStreamHandle, events: QUICStreamEvents) {
        if let transport = self._transportStates.pointer(for: handle) {
            self._markReady(handle: handle, transport: transport, events: events)
        }  // else: handle was invalid; no-op.
    }

    @inlinable
    func _markReady(
        handle: QUICStreamHandle,
        transport: UnsafeMutablePointer<QUICStreamTransportState>,
        events: QUICStreamEvents
    ) {
        let hadNoEvents = transport.pointee.events.isEmpty
        transport.pointee.events.formUnion(events)

        if hadNoEvents && !events.isEmpty {
            self._ready.append(handle)

            if !self.inReadLoop && !self.outOfBandDrainRequested {
                self.outOfBandDrainRequested = true
                self.outOfBandDrain?()
            }
        }
    }

    /// Records that a stream handed something to the stack which has to be flushed.
    @inlinable
    func markOutputPending() {
        self._outputPending = true
    }

    /// Returns whether there is output pending, and clears the pending output state.
    func clearOutputPending() -> Bool {
        let hadOutputPending = self._outputPending
        self._outputPending = false
        return hadOutputPending
    }

    /// Adds a slot for a stream, with the consumer's state for it.
    ///
    /// - Parameters:
    ///   - id: The stream ID, if the stack has already assigned one.
    ///   - state: The consumer's state for the stream. Streams which don't provide state here
    ///     will make state via ``QUICStreamConsumer/makeStreamState(_:)`` before their first visit.
    @inlinable
    func insertSlot(
        id: QUICStreamID?,
        state: consuming Consumer.StreamState?
    ) -> QUICStreamHandle {
        let transportState = QUICStreamTransportState(core: QUICStreamCore(id: id))
        let handle = self._transportStates.insert(transportState)
        let consumerStatePointer = self._consumerStates.reserve(at: handle.index)

        if let state {
            consumerStatePointer.pointee = consume state
            self._transportStates.withValue(for: handle) { $0.hasState = true }
        } else {
            self._needsState.append(handle)
        }

        if let id {
            self._byID[id] = handle
        }  // else local peer opened the stream, id is assigned later.

        return handle
    }

    /// Registers a fresh identifier for `handle`'s stream, using that stream's own event
    /// manager. Also stashes it as the identifier `ExternalInboundStreamLinkage.identifier` will
    /// report on the very next `BaseInboundStreamLinkage(external: self)` construction — call
    /// this immediately before that construction, not before.
    func registerIdentifier(
        for handle: QUICStreamHandle,
        in eventContext: inout NetworkContext.EventContext
    ) -> InstanceIdentifier {
        guard let transport = self._transportStates.pointer(for: handle) else {
            preconditionFailure("slot for \(handle) was recycled before it could be registered")
        }
        let identifier = InstanceIdentifier(eventManager: &transport.pointee.eventManager, in: &eventContext)
        self._pendingAttachIdentifier = identifier
        return identifier
    }

    /// As ``registerIdentifier(for:in:)``, for use with no event context held: registering then
    /// acquires the context itself.
    func registerIdentifier(for handle: QUICStreamHandle) -> InstanceIdentifier {
        guard let transport = self._transportStates.pointer(for: handle) else {
            preconditionFailure("slot for \(handle) was recycled before it could be registered")
        }
        let identifier = InstanceIdentifier(context: self.context, eventManager: &transport.pointee.eventManager)
        self._pendingAttachIdentifier = identifier
        return identifier
    }

    /// Attaches `handle`'s stream to the flow `linkage` leads to, and indexes the stream by that
    /// flow's identifier, which is what the stack's events for the stream arrive `for`.
    ///
    /// Call before connecting the stream: connecting hands over the events the flow buffered.
    func attach(_ handle: QUICStreamHandle, identifier: InstanceIdentifier, linkage: BaseOutboundStreamLinkage) {
        guard let transport = self._transportStates.pointer(for: handle) else {
            preconditionFailure("slot for \(handle) was recycled while its flow was attaching")
        }
        transport.pointee.core.attach(identifier: identifier, linkage: linkage)
        transport.pointee.flowIdentifier = linkage.identifier
        assert(self._byFlowIdentifier[linkage.identifier] == nil)  // A flow attaches to at most one stream.
        self._byFlowIdentifier[linkage.identifier] = handle
    }

    /// Records the ID the stack assigned, indexes it, and connects the stream's state machine.
    @usableFromInline
    func assignID(_ id: QUICStreamID, to handle: QUICStreamHandle) {
        guard let pointer = self._transportStates.pointer(for: handle) else { return }

        switch pointer.pointee.core.connected(id: id, direction: self.direction(of: id)) {
        case .activateStream:
            ()
        case .ignoreAlreadyConnected:
            ()  // A second connected event for the same stream; the ID cannot have changed.
        case .ignoreAlreadyClosed:
            ()  // Closed while the event was in flight.
        }

        self._byID.updateValue(handle, forID: id)
    }

    /// The handle for a stream ID, or `nil` if no live stream has it.
    @usableFromInline
    func handle(forID id: QUICStreamID) -> QUICStreamHandle? {
        if let handle = self._byID[id], self._transportStates.containsValue(for: handle) {
            return handle
        } else {
            return nil
        }
    }

    /// Runs `body` on a stream and the consumer's state for it.
    ///
    /// > Important: You **MUST NOT** call `withStream` re-entrantly for the same handle. Doing so
    /// > will result in a runtime trap.
    ///
    /// - Parameters:
    ///   - handle: The handle for the stream to access.
    ///   - body: Called with the stream and state for the stream aliased by the given handle.
    ///     Called at most once. The closure won't be called if the handle doesn't alias an open
    ///     stream.
    /// - Returns: the result of `body`, or `nil` if the stream wasn't open.
    @inlinable
    func withStream<Result: ~Copyable, Failure: Error>(
        handle: QUICStreamHandle,
        execute body: (
            _ stream: inout QUICStream<Consumer>,
            _ state: inout Consumer.StreamState
        ) throws(Failure) -> Result
    ) throws(Failure) -> Result? {
        guard let pointer = self._transportStates.pointer(for: handle) else { return nil }
        guard pointer.pointee.hasState else { return nil }

        // The slot is "borrowed" for the duration of this call. Attempting to re-entrantly call
        // `withStream` will trap here. Doing this would be very surprising user behavior (they
        // are already accessing the stream in the same lexical scope) and potentially unsafe as
        // both streams would be accessing the same underlying memory for the transport and consumer
        // state.
        pointer.pointee.beginBorrow()
        defer { pointer.pointee.endBorrow() }

        let consumerState = self._consumerStates.pointer(at: handle.index)
        var stream = QUICStream<Consumer>(table: self, transport: pointer, handle: handle)
        return try body(&stream, &consumerState.pointee!)  // checked above
    }

    /// The direction of a stream from its ID and this endpoint's role.
    private func direction(of id: QUICStreamID) -> QUICStreamDirection {
        switch id.type {
        case .clientInitiatedBidirectional, .serverInitiatedBidirectional:
            return .bidirectional
        case .clientInitiatedUnidirectional:
            return self.role == .client ? .sendOnly : .receiveOnly
        case .serverInitiatedUnidirectional:
            return self.role == .server ? .sendOnly : .receiveOnly
        }
    }
}

// MARK: - Stack events

@available(anyAppleOS 27, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// The stack assigned the stream its ID.
    func streamConnected(_ handle: QUICStreamHandle, in eventContext: inout NetworkContext.EventContext) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        // Inbound streams have the stream core created with an ID; only consult metadata if the ID
        // isn't already set.
        let id: QUICStreamID?
        if let known = transport.pointee.core.id {
            id = known
        } else if let rawID = transport.pointee.core.metadata(in: &eventContext)?.streamID {
            id = QUICStreamID(rawValue: rawID)
        } else {
            id = nil
        }

        if let id {
            self.assignID(id, to: handle)
            self._markReady(handle: handle, transport: transport, events: .opened)
        }
    }

    /// The stream disconnected.
    func streamDisconnected(_ handle: QUICStreamHandle, error: NetworkError?) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        transport.pointee.closeError = error
        transport.pointee.disconnectError = error

        // `.readable` as well: there may be leftover data and this will be the last chance to
        // get it.
        self._markReady(handle: handle, transport: transport, events: [.closed, .readable])
    }

    /// The peer sent RESET\_STREAM.
    func peerResetStream(_ handle: QUICStreamHandle, code: QUICApplicationErrorCode) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        transport.pointee.core.receiveResetStream(code: code)
        transport.pointee.resetCode = code

        self._markReady(handle: handle, transport: transport, events: .reset)
    }

    /// The peer sent STOP\_SENDING.
    func peerStoppedSending(
        _ handle: QUICStreamHandle,
        code: QUICApplicationErrorCode,
        in eventContext: inout NetworkContext.EventContext
    ) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        transport.pointee.core.receiveStopSending(code: code, in: &eventContext)
        transport.pointee.stopSendingCode = code

        self._markReady(handle: handle, transport: transport, events: .stopSending)
    }
}

// MARK: - Draining

@available(anyAppleOS 27, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Hand each ready stream to the consumer.
    ///
    /// Holds no event context while the consumer runs: its stream operations each acquire one.
    func drain(into consumer: inout Consumer) {
        self.outOfBandDrainRequested = false

        while let handle = self._needsState.popFirst() {
            guard let transport = self._transportStates.pointer(for: handle) else { continue }
            assert(!transport.pointee.hasState)

            var stream = QUICStream<Consumer>(table: self, transport: transport, handle: handle)
            let consumerState = self._consumerStates.pointer(at: handle.index)
            consumerState.pointee = consumer.makeStreamState(&stream)
            transport.pointee.hasState = true
        }

        var iterator = QUICStreamIterator(table: self)
        consumer.processStreams(&iterator)
        // Finish any visit the consumer didn't pull to completion, so `QUICStreamIterator.deinit`'s
        // precondition holds once the iterator's scope ends right after this.
        iterator._finishVisit()

        // A read loop drains the table once, so if this drain was it, a stream marked ready from
        // now on (say, from `channelReadComplete` or the read loop's final flush) needs a drain of
        // its own.
        self.inReadLoop = false
    }

    /// Marks every stream as closed and visits each of them, then retires the table's anchor.
    ///
    /// The table's operations can't be used afterwards.
    ///
    /// - Parameters:
    ///   - error: Reported to the consumer as the reason each stream closed.
    ///   - disconnect: Sent to the stack, and through it the peer. Separate from `error` because a
    ///     connection-level failure is not always a `NetworkError`, and narrowing to one would lose
    ///     the more useful of the two on the consumer's side.
    ///   - consumer: The consumer to drain into.
    func closeAll(
        error: (any Error)?,
        disconnect: NetworkError?,
        into consumer: inout Consumer
    ) {
        var index = QUICStreamHandle.Index.first
        while index.rawValue < self._transportStates.allocated {
            let transport = self._transportStates.pointer(at: index)
            let handle = self._transportStates.handle(at: index)

            if let transport, let handle {
                transport.pointee.closeError = error
                transport.pointee.disconnectError = disconnect
                // `.readable` too: this is the last chance for the consumer to pull any data.
                self._markReady(handle: handle, transport: transport, events: [.closed, .readable])
            }

            index.advance()
        }

        self.drain(into: &consumer)
        self.withEventContext { eventContext in
            self.forceCloseAll(error: disconnect, in: &eventContext)
        }
        self.retireAnchor()
        // Closed first, and with nothing held: the states' deinits are consumer code, and an `open`
        // from one now throws.
        self._consumerStates.removeAll()
    }

    /// Removes and returns the next stream from the ready queue.
    @inlinable
    func nextReadyHandle() -> QUICStreamHandle? {
        self._ready.popFirst()
    }

    @usableFromInline
    struct ReadySlot {
        @usableFromInline
        var transport: UnsafeMutablePointer<QUICStreamTransportState>
        @usableFromInline
        var state: UnsafeMutablePointer<Consumer.StreamState?>
        @usableFromInline
        var events: QUICStreamEvents

        @inlinable
        init(
            transport: UnsafeMutablePointer<QUICStreamTransportState>,
            state: UnsafeMutablePointer<Consumer.StreamState?>,
            events: QUICStreamEvents
        ) {
            self.transport = transport
            self.state = state
            self.events = events
        }
    }

    /// Returns the ``ReadySlot` for a handle, or `nil` if the handle is invalid.
    ///
    /// This is used only by ``QUICStreamIterator`` immediately prior to visiting the stream.
    @inlinable
    func readySlot(
        for handle: QUICStreamHandle
    ) -> ReadySlot? {
        guard let transport = self._transportStates.pointer(for: handle) else {
            // Possible if the stream gets closed by SwiftNetwork before the first visit.
            return nil
        }

        // Streams only get added to the ready queue when they have events.
        precondition(!transport.pointee.events.isEmpty)
        // State is created for streams before creating the iterator (which is the only caller of
        // this function).
        precondition(transport.pointee.hasState)

        return ReadySlot(
            transport: transport,
            state: self._consumerStates.pointer(at: handle.index),
            events: transport.pointee.events
        )
    }

    /// Clears the flags a visit presented and, if that visit was the stream's last, recycles its
    /// slot.
    ///
    /// Acquires the event context only to recycle the slot.
    @usableFromInline
    func finishVisit(_ handle: QUICStreamHandle, presented: QUICStreamEvents) {
        if presented.contains(.closed) {
            self.vacateSlot(handle)
        } else if let transport = self._transportStates.pointer(for: handle) {
            // Clear the events which were shown in the last visit.
            transport.pointee.events.subtract(presented)

            // Visit didn't consume all bytes: it should be readable in the next visit.
            if transport.pointee.core.needsReadVisit {
                transport.pointee.events.insert(.readable)
            }

            if !transport.pointee.events.isEmpty {
                self._ready.append(handle)
            }
        }
    }
}

// MARK: - Slot teardown

@available(anyAppleOS 27, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Closes and vacates a slot, without a visit.
    ///
    /// - Precondition: The slot has no consumer state. `_remove` would release it with the event
    ///   context held, and its deinit is consumer code: use ``vacateSlot(_:)`` for such a slot.
    func vacateSlot(_ handle: QUICStreamHandle, in eventContext: inout NetworkContext.EventContext) {
        assert(
            self._transportStates.pointer(for: handle)?.pointee.hasState != true,
            "A slot with consumer state must be vacated with nothing held"
        )
        // Note: this is helpful for a stream which never reached the stack, the consumer wasn't
        // told about it so there's nothing to report.
        self._remove(handle: handle, in: &eventContext)
    }

    /// As ``vacateSlot(_:in:)``, but acquires the event context itself, and releases the consumer's
    /// state for the stream only once that acquisition has ended.
    func vacateSlot(_ handle: QUICStreamHandle) {
        let state = self.takeConsumerState(for: handle)
        self.withEventContext { eventContext in
            self._remove(handle: handle, in: &eventContext)
        }
        // Only now, with nothing held: the state's deinit is consumer code.
        _ = consume state
    }

    /// Moves the consumer's state for `handle`'s stream out of the table, for the caller to release
    /// once no event context is held.
    private func takeConsumerState(for handle: QUICStreamHandle) -> Consumer.StreamState? {
        if self._transportStates.pointer(for: handle) == nil {
            return nil
        }
        return self._consumerStates.pointer(at: handle.index).pointee.take()
    }

    /// Closes a slot's stream core and vacates the slot.
    ///
    /// - Parameter handle: The handle the visit was made with.
    private func _remove(handle: QUICStreamHandle, in eventContext: inout NetworkContext.EventContext) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        if let id = transport.pointee.core.id {
            let removed = self._byID.removeValue(forID: id)
            assert(removed == handle)
        }

        let removedFlow = self._byFlowIdentifier.removeValue(forKey: transport.pointee.flowIdentifier)
        assert(removedFlow == nil || removedFlow == handle)  // nil: the slot never attached.

        // Close in case it isn't already (close is idempotent).
        let error = transport.pointee.disconnectError
        transport.pointee.core.close(error: error, in: &eventContext)
        self.unregisterEventManager(of: &transport.pointee, in: &eventContext)

        self._transportStates.removeValue(for: handle)
        self._consumerStates.removeValue(at: handle.index)
    }

    /// Retires the stack's registration of a slot's event manager, which traps on deinit otherwise.
    /// A no-op for a slot that never registered.
    ///
    /// Only a `ProtocolInstance` can unregister an event manager, and a slot isn't one, so the
    /// manager moves into a `SlotRegistration` to be unregistered.
    private func unregisterEventManager(
        of transport: inout QUICStreamTransportState,
        in eventContext: inout NetworkContext.EventContext
    ) {
        var registration = SlotRegistration(
            identifier: InstanceIdentifier(),
            eventManager: exchange(&transport.eventManager, with: ProtocolEventManager())
        )
        registration.unregisterEventManager(in: &eventContext)
    }

    /// Vacates every remaining slot, running `perSlot` on each slot just before it's dropped.
    /// Shared by `forceCloseAll(error:in:)` (detaches from the stack) and `vacateAllSlots()`
    /// (doesn't) so a future field added to one of these bulk-teardown paths isn't forgotten in
    /// the other. Consumer states are the caller's to release.
    fileprivate func removeAllSlots(perSlot: (inout QUICStreamTransportState) -> Void) {
        // First, so no event resolves a stream while `removeAll` has the slots: closing a stream
        // can deliver its events synchronously.
        self._byFlowIdentifier.removeAll()
        self._transportStates.removeAll { transport in
            var transport = consume transport
            perSlot(&transport)
        }
        self._ready.removeAll(keepingCapacity: true)
        self._needsState.removeAll(keepingCapacity: true)
        self._byID.removeAll()
    }

    /// Closes and vacates every remaining slot, without a visit.
    private func forceCloseAll(error: NetworkError?, in eventContext: inout NetworkContext.EventContext) {
        self.removeAllSlots { transport in
            transport.core.close(error: error, in: &eventContext)
            self.unregisterEventManager(of: &transport, in: &eventContext)
        }
    }

    /// Vacates every remaining slot without touching the stack.
    ///
    /// Used only from `deinit`, which has no event context, so unlike `forceCloseAll(error:in:)`
    /// this doesn't call `core.close(error:in:)`. Only slots that never reached the stack are
    /// expected by then: a stream still attached means `closeAll` never ran, and its registered
    /// event manager traps.
    private func vacateAllSlots() {
        self.removeAllSlots { transport in
            transport.core.discardBufferedReadsAndWrites()
        }
    }
}

// MARK: - Event context

@available(anyAppleOS 27, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Runs `body` with the connection's event context, entering the stack as the table's anchor.
    ///
    /// The anchor is never a callee. The operations here call QUIC stream flows, whose parent, the
    /// `QUICConnection`, is idle wherever consumer code runs.
    func withEventContext<R, E: Error>(
        _ body: (inout NetworkContext.EventContext) throws(E) -> R
    ) throws(E) -> R {
        precondition(!self.isClosed, "stream table used after it was closed")
        return try self.callouts.enter(as: self.anchor, body)
    }

    /// Retires the table's anchor, after which its operations can't acquire the event context.
    /// Idempotent.
    func retireAnchor() {
        self.anchor.retire(through: self.callouts)
    }
}

// MARK: - Stream operations

/// `QUICStream`'s operations that reach the stack. Each acquires the event context only while it
/// needs the stack: `read` for each pull from it, the others for the whole call.
@available(anyAppleOS 27, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Reads from the stream `transport` belongs to.
    ///
    /// Acquires the event context only for each pull from the stack, so `body` runs with nothing
    /// held.
    @usableFromInline
    func read(
        _ transport: UnsafeMutablePointer<QUICStreamTransportState>,
        minContiguous: Int,
        _ body: (_ span: borrowing RawSpan) -> Int
    ) -> QUICStreamReadOutcome {
        transport.pointee.core.read(
            minContiguous: minContiguous,
            withEventContext: { pull in self.withEventContext(pull) },
            body
        )
    }

    /// How many bytes the stream `transport` belongs to will accept right now.
    @usableFromInline
    func writableBytes(_ transport: UnsafeMutablePointer<QUICStreamTransportState>) -> Int {
        self.withEventContext { eventContext in
            transport.pointee.core.writableBytes(in: &eventContext)
        }
    }

    /// Writes the queued data of the stream `transport` belongs to.
    @usableFromInline
    func flush(_ transport: UnsafeMutablePointer<QUICStreamTransportState>, fin: Bool) throws {
        try self.withEventContext { eventContext in
            try transport.pointee.core.flush(fin: fin, in: &eventContext)
        }
    }

    /// Resets the send side of the stream `transport` belongs to.
    @usableFromInline
    func sendResetStream(_ transport: UnsafeMutablePointer<QUICStreamTransportState>, code: QUICApplicationErrorCode) {
        self.withEventContext { eventContext in
            transport.pointee.core.sendResetStream(code: code, in: &eventContext)
        }
    }

    /// Closes the receive side of the stream `transport` belongs to.
    @usableFromInline
    func sendStopSending(_ transport: UnsafeMutablePointer<QUICStreamTransportState>, code: QUICApplicationErrorCode) {
        self.withEventContext { eventContext in
            transport.pointee.core.sendStopSending(code: code, in: &eventContext)
        }
    }
}

/// A slot's event manager, moved out of the slot to be unregistered: see
/// `QUICStreamTable.unregisterEventManager(of:in:)`.
@available(anyAppleOS 27, *)
private struct SlotRegistration: ProtocolInstance, ~Copyable {
    var identifier: InstanceIdentifier
    var eventManager: ProtocolEventManager

    // Not stored: unregistering never reads the context, and storing one would cost a retain and a release per stream.
    var context: NetworkContext {
        fatalError("A SlotRegistration has no context")
    }
}
