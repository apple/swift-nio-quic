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

import DequeModule
import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

@available(anyAppleOS 26, *)
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

    /// Whether there is any pending output that needs to be drained by the connection.
    @usableFromInline
    var _outputPending: Bool

    /// The role of the local peer.
    let role: Role

    /// Context for the `SwiftNetwork` stack.
    let context: NetworkContext

    init(role: Role, context: NetworkContext) {
        self._transportStates = QUICStreamSlots()
        self._consumerStates = QUICStreamConsumerStates()
        self._ready = []
        self._needsState = []
        self._byID = QUICStreamIDDictionary()
        self._outputPending = false
        self.role = role
        self.context = context
    }

    deinit {
        // Failing to close may lead to `Frame`'s not being finalized (which will trap).
        self.forceCloseAll(error: nil)
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
}

// MARK: - Bookkeeping

@available(anyAppleOS 26, *)
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

    /// The reference the `SwiftNetwork` stack routes the given stream's events through.
    func reference(for handle: QUICStreamHandle) -> ProtocolInstanceReference {
        ProtocolInstanceReference(custom: self, index: handle.protocolInstanceReferenceIndex)
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

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// The slot a handle addresses, or `nil` if it has been recycled since.
    @inlinable
    func transportState(
        for handle: QUICStreamHandle
    ) -> UnsafeMutablePointer<QUICStreamTransportState>? {
        self._transportStates.pointer(for: handle)
    }

    /// The stack assigned the stream its ID.
    func streamConnected(_ handle: QUICStreamHandle) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        if let rawID = transport.pointee.core.metadata()?.streamID {
            self.assignID(QUICStreamID(rawValue: rawID), to: handle)
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
    func peerStoppedSending(_ handle: QUICStreamHandle, code: QUICApplicationErrorCode) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        transport.pointee.core.receiveStopSending(code: code)
        transport.pointee.stopSendingCode = code

        self._markReady(handle: handle, transport: transport, events: .stopSending)
    }
}

// MARK: - Draining

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Hand each ready stream to the consumer.
    @inlinable
    func drain(into consumer: inout Consumer) {
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
    }

    /// Marks every stream as closed and visits each of them.
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
        self.forceCloseAll(error: disconnect)
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
    @inlinable
    func finishVisit(_ handle: QUICStreamHandle, presented: QUICStreamEvents) {
        if presented.contains(.closed) {
            self._remove(handle: handle)
        } else if let transport = self._transportStates.pointer(for: handle) {
            // Clear the events which were shown in the last visit.
            transport.pointee.events.subtract(presented)

            // Visit didn't consume all bytes: it should be readable in the next visit.
            if transport.pointee.core.needsReadVisit() {
                transport.pointee.events.insert(.readable)
            }

            if !transport.pointee.events.isEmpty {
                self._ready.append(handle)
            }
        }
    }
}

// MARK: - Slot teardown

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Closes and vacates a slot, without a visit.
    @inlinable
    func vacateSlot(_ handle: QUICStreamHandle) {
        // Note: this is helpful for a stream which never reached the stack, the consumer wasn't
        // told about it so there's nothing to report.
        self._remove(handle: handle)
    }

    /// Closes a slot's stream core and vacates the slot.
    ///
    /// - Parameter handle: The handle the visit was made with.
    @usableFromInline
    func _remove(handle: QUICStreamHandle) {
        guard let transport = self._transportStates.pointer(for: handle) else { return }

        if let id = transport.pointee.core.id {
            let removed = self._byID.removeValue(forID: id)
            assert(removed == handle)
        }

        // Close in case it isn't already (close is idempotent).
        let error = transport.pointee.disconnectError
        transport.pointee.core.close(error: error)

        self._transportStates.removeValue(for: handle)
        self._consumerStates.removeValue(at: handle.index)
    }

    /// Closes and vacates every remaining slot, without a visit.
    private func forceCloseAll(error: NetworkError?) {
        self._transportStates.removeAll { transport in
            var transport = consume transport
            transport.core.close(error: error)
        }
        self._consumerStates.removeAll()
        self._ready.removeAll(keepingCapacity: true)
        self._needsState.removeAll(keepingCapacity: true)
        self._byID.removeAll()
    }
}
