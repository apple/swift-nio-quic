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
import NIOCore
import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

/// The transport state for a stream.
@available(anyAppleOS 26, *)
@usableFromInline
struct QUICStreamCore: ~Copyable {
    /// The ID of the stream assigned by `SwiftNetwork`, `nil` if it hasn't been assigned yet
    /// (e.g. is locally opened).
    @usableFromInline
    private(set) var id: QUICStreamID?

    /// The state of a stream.
    private var state: QUICStreamStateMachine

    /// Set when a frame had the FIN flag set, cleared when the FIN is handed to the state machine.
    ///
    /// This is necessary so that the state machine doesn't receive the FIN before the consumer has
    /// received all inbound data (as each read must also go through the state machine).
    private var finPending: Bool

    /// Writes that haven't yet been emitted to the stack.
    private var pendingWrites: FrameArray

    /// Data received from `SwiftNetwork` which the application hasn't consumed yet.
    private var undeliveredReads: UniqueDeque<Frame>

    /// Whether a read left bytes in the stream that the consumer may still want.
    @usableFromInline
    var _needsReadVisit: Bool

    /// The handle used to talk to the SwiftNetwork stack.
    private var handle: SwiftNetworkStreamHandle

    /// The reference every linkage call is made "from".
    ///
    /// Holding it creates a retain cycle (the ref, holds the container, holds this stream) which
    /// is broken in ``close(error:)``.
    private var reference: ProtocolInstanceReference

    #if DEBUG
    /// Whether attach has been called yet.
    private var hasAttached: Bool = false
    #endif

    /// Temporary space for coalescing into when the consumer asks for reads to be coalesced and
    /// the leading frame doesn't contain enough bytes.
    private var coalescedBytes: [UInt8]

    init(id: QUICStreamID?, reference: ProtocolInstanceReference, linkage: OutboundStreamLinkage) {
        self.id = id
        self.state = QUICStreamStateMachine()
        self.handle = SwiftNetworkStreamHandle(linkage: linkage)
        self.reference = reference
        self.pendingWrites = FrameArray()
        self.undeliveredReads = UniqueDeque()
        self.finPending = false
        self.coalescedBytes = []
        self._needsReadVisit = false
    }

    /// Creates a stream core which is not yet wired to the stack.
    ///
    /// - Parameter id: The stream ID, if the stack has already assigned one.
    @usableFromInline
    init(id: QUICStreamID?) {
        self.id = id
        self.state = QUICStreamStateMachine()
        self.handle = SwiftNetworkStreamHandle()
        self.reference = ProtocolInstanceReference()
        self.pendingWrites = FrameArray()
        self.undeliveredReads = UniqueDeque()
        self.finPending = false
        self.coalescedBytes = []
        self._needsReadVisit = false
    }

    /// Attaches this stream core to the stack.
    ///
    /// - Parameters:
    ///   - reference: The reference the stack routes this stream's events back through, which is
    ///     also what every linkage call is made "from".
    ///   - linkage: The flow the stack attached for this stream.
    mutating func attach(reference: ProtocolInstanceReference, linkage: OutboundStreamLinkage) {
        #if DEBUG
        assert(!self.hasAttached, "\(#function) called more than once")
        self.hasAttached = true
        #endif
        self.reference = reference
        self.handle = SwiftNetworkStreamHandle(linkage: linkage)
    }

    /// The stack's metadata for this stream, or `nil` if it is detached.
    func metadata() -> ProtocolMetadata<QUICProtocol>? {
        switch self.handle.invokeGetMetadata() {
        case .proceed(let linkage):
            return linkage.invokeGetMetadata(self.reference) as ProtocolMetadata<QUICProtocol>?
        case .ignore:
            return nil
        }
    }

    /// Whether more data can be written to the stream.
    @usableFromInline
    var isSendOpen: Bool {
        !self.state.isSendFinished
    }

    /// Whether more data can be read from the stream.
    @usableFromInline
    var isReceiveOpen: Bool {
        !self.state.isReceiveClosed
    }

    /// Records the ID the stack assigned and resolves the stream's direction.
    ///
    /// - Parameters:
    ///   - id: The stream ID the stack assigned.
    ///   - direction: The direction implied by the ID and the local role, which only the
    ///     connection knows.
    /// - Returns: The state machine's action for the transition.
    mutating func connected(
        id: QUICStreamID,
        direction: QUICStreamDirection
    ) -> QUICStreamStateMachine.StreamConnectedAction {
        self.id = id
        return self.state.streamConnected(direction: direction)
    }
}

// MARK: - Peer events

@available(anyAppleOS 26, *)
extension QUICStreamCore {
    /// Records the peer's RESET\_STREAM.
    ///
    /// - Parameter code: The application error code the peer sent.
    mutating func receiveResetStream(code: QUICApplicationErrorCode) {
        let action: QUICStreamStateMachine.ReceiveResetStreamAction

        do {
            // The stack owns reassembly, so there is no final size to assert here.
            action = try self.state.receiveResetStream(applicationErrorCode: code, finalSize: 0)
        } catch {
            return  // Closed, or a direction with no receive side.
        }

        switch action {
        case .closeStream:
            ()
        case .surfaceReset:
            ()
        case .doNothing(.alreadyFullyReceived):
            ()  // The peer already finished cleanly; the reset has nothing left to abandon.
        case .doNothing(.alreadyReset):
            ()  // RESET_STREAM is not acted on twice.
        }
    }

    /// Records the peer's STOP\_SENDING and answers it with RESET\_STREAM.
    ///
    /// - Parameter code: The application error code the peer sent.
    mutating func receiveStopSending(code: QUICApplicationErrorCode) {
        let action: QUICStreamStateMachine.ReceiveStopSendingAction

        do {
            action = try self.state.receiveStopSending(applicationErrorCode: code)
        } catch {
            return  // Not connected, or a direction with no send side.
        }

        switch action {
        case .sendReset:
            self.abortSendSideForStopSending(code: code)
        case .sendResetAndCloseStream:
            self.abortSendSideForStopSending(code: code)
        case .ignore(.alreadyFinished):
            ()  // The send side finished cleanly; STOP_SENDING has nothing to stop.
        case .ignore(.alreadyReset):
            ()  // RESET_STREAM has already gone out.
        }
    }

    /// Answers a STOP\_SENDING with RESET\_STREAM, dropping what was queued.
    private mutating func abortSendSideForStopSending(code: QUICApplicationErrorCode) {
        self.pendingWrites.finalizeAllFramesAsFailed()
        self.abortOutbound(error: NetworkError(quicApplicationError: code.rawValue))
    }
}

// MARK: - Reading

@available(anyAppleOS 26, *)
extension QUICStreamCore {
    /// Hands the stream's inbound bytes to `body`.
    ///
    /// `body` returns the number of bytes that it consumed from the span. Bytes which weren't read
    /// are held until the next read. This function will return `endOfStream` if the stream is half
    /// closed.
    ///
    /// - Parameters:
    ///   - minContiguous: The shortest run of contiguous bytes to hand to `body`. Data is
    ///     coalesced to reach it; `1` never coalesces. If there aren't enough bytes available then
    ///     and the peer hasn't yet sent FIN then `body` won't be called. Note that the when the
    ///     peer sends FIN then `body` may be called with fewer bytes than `minContiguous`.
    ///   - body: Provided a contiguous block of bytes received from the remote peer, returning how
    ///     many of them it consumed. May be called more then once for each call to `read`.
    /// - Returns: The outcome of the read.
    @usableFromInline
    mutating func read(
        minContiguous: Int,
        _ body: (_ span: borrowing RawSpan) -> Int
    ) -> QUICStreamReadOutcome {
        let contiguous = max(minContiguous, 1)
        var totalDelivered = 0
        var totalRead = 0

        while true {
            let askedForBytes: Bool
            let bytesRead: Int

            switch self.state.attemptRead() {
            case .proceedWithRead:
                askedForBytes = true
                bytesRead = self.fillUndeliveredReads()
                totalRead &+= bytesRead

            case .doNotRead:
                // Receive side closed: only what is already held here can still be handed over.
                askedForBytes = false
                bytesRead = 0
            }

            // Deliver the bytes.
            totalDelivered &+= self.deliver(minContiguous: contiguous, body)

            if askedForBytes && bytesRead == 0 {
                self.confirmFin()
            }

            // If no bytes were read or there are undelivered reads then there's no point in asking
            // for more data: the consumer stopped reading.
            if bytesRead == 0 || !self.undeliveredReads.isEmpty {
                break
            }
        }

        let outcome: QUICStreamReadOutcome
        if self.undeliveredReads.isEmpty && self.state.hasReceivedFin {
            self.consumeEndOfStream()
            outcome = .endOfStream(totalDelivered)
        } else if totalDelivered > 0 {
            outcome = .read(totalDelivered)
        } else {
            outcome = .nothingAvailable
        }

        // Bytes are leftover: the stream should be marked as needing another visit.
        if totalDelivered > 0 && !self.undeliveredReads.isEmpty {
            self._needsReadVisit = true
        }

        return outcome
    }

    /// Whether the stream should be revisited in the next tick because it has unread data.
    @inlinable
    mutating func needsReadVisit() -> Bool {
        defer { self._needsReadVisit = false }
        return self._needsReadVisit
    }

    /// Hands a FIN to the state machine, but only once all data has been handed over to the
    /// consumer.
    private mutating func confirmFin() {
        guard self.finPending && self.undeliveredReads.isEmpty && self.state.isConnected else {
            return
        }

        self.finPending = false

        do {
            // The stack owns reassembly, so there is no final size to assert here.
            switch try self.state.receiveFin(finalSize: 0) {
            case .markAllDataReceived:
                ()
            case .ignore(.alreadyReceivedFin):
                ()
            case .ignore(.streamReset):
                ()
            }
        } catch {
            ()  // Closed, or a direction with no receive side.
        }
    }

    /// Pulls and stores up to `maxBytes` data.
    ///
    /// - Returns: The number of bytes the stack handed over.
    private mutating func fillUndeliveredReads() -> Int {
        var pulled = self.receiveFromStack()
        var bytesStored = 0

        while var frame = pulled.popFirst() {
            let keepFrame: Bool

            if frame.unclaimedLength > 0 {
                do {
                    switch try self.state.receiveData() {
                    case .bufferData:
                        keepFrame = true
                    case .doNotBuffer(.allDataReceived):
                        // Data past the final size the peer committed to. Only reachable once the
                        // FIN has been confirmed, so this shouldn't be possible.
                        assertionFailure("stream data arrived after all data was received")
                        keepFrame = false
                    case .doNotBuffer(.streamReset):
                        assertionFailure("stream data arrived after the stream was reset")
                        keepFrame = false
                    }
                } catch {
                    keepFrame = false  // Closed, or a direction with no receive side.
                }
            } else {
                keepFrame = false
            }

            if frame.connectionComplete {
                self.finPending = true
            }

            if keepFrame {
                bytesStored &+= frame.unclaimedLength
                self.undeliveredReads.append(frame)
            } else {
                // Drop empty frames.
                frame.finalize(success: true)
            }
        }

        return bytesStored
    }

    private func receiveFromStack() -> FrameArray {
        let received: FrameArray?

        switch self.handle.invokeReceiveStreamData() {
        case .proceed(let linkage):
            do throws(NetworkError) {
                received = try linkage.invokeReceiveStreamData(
                    self.reference,
                    minimumBytes: 1,
                    maximumBytes: .max
                )
            } catch {
                // Nothing readable: the stack reports what went wrong as a disconnect event
                // rather than through this call.
                received = nil
            }

        case .handleViolation:
            received = nil  // Detached: the stack has finished with this stream.
        }

        switch consume received {
        case .some(let frames):
            return frames
        case .none:
            return FrameArray()
        }
    }

    /// Hands the held frames to `body` until it stops consuming or they run out.
    private mutating func deliver(
        minContiguous: Int,
        _ body: (_ span: borrowing RawSpan) -> Int
    ) -> Int {
        var delivered = 0
        var available = self.undeliveredReads.unclaimedLength()

        while !self.undeliveredReads.isEmpty {
            if available < minContiguous && !(self.finPending || self.state.hasReceivedFin) {
                // Not enough bytes left.
                break
            }

            let effectiveReadLength = min(minContiguous, available)
            let firstLength = self.undeliveredReads[0].unclaimedLength
            let consumed: Int

            if firstLength >= effectiveReadLength {
                if let bytes = self.undeliveredReads[0].bytes {
                    consumed = body(bytes)
                } else {
                    consumed = 0
                }
            } else {
                consumed = self.coalesceAndDeliver(bytes: effectiveReadLength, body)
            }

            if consumed <= 0 {
                assert(consumed == 0, "body(_:) claimed to read negative bytes")
                // The consumer took nothing, hold on to the bytes.
                break
            }

            if consumed >= available {
                assert(consumed == available, "body(_:) claimed to read more bytes than available")
                self.undeliveredReads.claimAllBytes()
            } else {
                self.undeliveredReads.claimLeadingBytes(consumed)
            }

            delivered &+= consumed
            available &-= consumed
        }

        return delivered
    }

    /// Copies frames into a buffer until there are `bytes` contiguous bytes, then hands them
    /// to `body`.
    private mutating func coalesceAndDeliver(
        bytes: Int,
        _ body: (_ span: borrowing RawSpan) -> Int
    ) -> Int {
        // Move out of 'self' for the fill: Swift 6.3 and earlier fail with a compilation error
        // otherwise.
        var coalesced: [UInt8] = []
        swap(&coalesced, &self.coalescedBytes)
        coalesced.removeAll(keepingCapacity: true)
        coalesced.reserveCapacity(bytes)

        for index in self.undeliveredReads.indices {
            let wanted = bytes &- coalesced.count
            if wanted == 0 { break }

            if let source = self.undeliveredReads[index].span {
                let slice = source.extracting(0..<min(source.count, wanted))
                slice.withUnsafeBufferPointer { pointer in
                    coalesced.append(contentsOf: pointer)
                }
            }
        }

        // Put back the buffer.
        swap(&coalesced, &self.coalescedBytes)

        let consumed = body(self.coalescedBytes.span.bytes)
        assert(consumed >= 0, "body(_:) claimed to read negative bytes")
        assert(consumed <= self.coalescedBytes.count, "body(_:) claimed to read more bytes than available")
        return consumed
    }

    /// Moves the receive side to its terminal state.
    private mutating func consumeEndOfStream() {
        do {
            // Called for the transition; what the caller is told was decided by what the consumer
            // actually took.
            switch try self.state.applicationRead() {
            case .deliverData:
                ()
            case .deliverEndOfStream:
                ()
            case .deliverResetError:
                ()
            case .ignore(.noDataAvailable):
                ()
            case .ignore(.alreadyDelivered):
                ()
            }
        } catch {
            // Not connected: a FIN which arrived before the stack assigned an ID is replayed into
            // the receive side when it does, so there is nothing to consume yet.
        }
    }
}

// MARK: - Writing

@available(anyAppleOS 26, *)
extension QUICStreamCore {
    /// How many bytes the stack will take right now, which is advisory: a write past it is buffered
    /// rather than refused.
    ///
    /// Note that this returns `0` until the stack has assigned the stream its ID, which is not the
    /// same as no room: a write before then is legal and is buffered.
    @usableFromInline
    var writableBytes: Int {
        switch self.handle.invokeSendStreamData() {
        case .proceed(let linkage):
            do throws(NetworkError) {
                return try linkage.invokeGetOutboundStreamDataRoomAvailable(self.reference)
            } catch {
                return 0  // Not connected, or the stack has finished with the send side.
            }

        case .handleViolation:
            return 0
        }
    }

    /// Queues a frame filled in place by `body`.
    ///
    /// - Parameters:
    ///   - count: The number of writable bytes to hand to `body`.
    ///   - body: Handed `count` writable bytes, returning how many it wrote.
    @usableFromInline
    mutating func withWritableSpan(count: Int, _ body: (_ span: inout MutableRawSpan) -> Int) {
        var frame = Frame(allocatingCustomFinalizerBufferOfSize: count)
        var written = 0

        if var span = frame.mutableSpan {
            var bytes = span.mutableBytes
            let wrote = body(&bytes)
            assert(wrote <= count, "body(_:) claimed to write more bytes than there was space for")
            assert(wrote >= 0, "body(_:) claimed to write negative bytes")
            written = min(max(wrote, 0), count)
        }

        if written == 0 {
            frame.finalize(success: false)
        }

        if written < count {
            let sized = frame.collapse(to: written)
            precondition(sized, "frame of \(count) bytes cannot be sized to \(written)")
        }

        self.pendingWrites.add(frame: frame)
    }

    /// Queues a copy of `buffer`.
    ///
    /// - Parameter buffer: The bytes to queue.
    @usableFromInline
    mutating func write(_ buffer: ByteBuffer) {
        if buffer.readableBytes != 0 {
            var buffer = buffer
            buffer.withUnsafeMutableReadableBytesWithStorageManagement2 { buffer, owner in
                self.pendingWrites.add(frame: Frame(customBuffer: buffer, owner: owner))
            }
        }
    }

    /// Hands buffered writes queued to the `SwiftNetwork`.
    ///
    /// - Parameter fin: Whether to finish the send side.
    /// - Throws: If the send side can no longer carry what was queued, in which case the queue is
    ///   dropped; if the stack rejected the write; or if `fin` was asked for before the stack
    ///   assigned the stream its ID, in which case the queue is left as it was so the flush can be
    ///   repeated.
    @usableFromInline
    mutating func flush(fin: Bool) throws {
        if self.pendingWrites.isEmpty && !fin { return }

        var refusalReason: String?

        do {
            switch try self.state.writeData() {
            case .sendData:
                ()
            case .doNotWrite(.streamFinished):
                refusalReason = "a FIN has already been sent"
            case .doNotWrite(.streamReset):
                refusalReason = "RESET_STREAM has already been sent"
            }
        } catch {
            switch error {
            case .notConnected:
                // The stack hasn't assigned an ID yet, so nothing can have finished the send side:
                // data written before then is legal and the stack queues it.
                ()
            case .wrongDirection:
                refusalReason = "the stream has no send side"
            }
        }

        var carriesFin = false

        if fin && refusalReason == nil {
            do {
                switch try self.state.sendFin() {
                case .sendFin:
                    carriesFin = true
                case .ignore(.alreadyFinished):
                    refusalReason = "a FIN has already been sent"
                case .ignore(.streamReset):
                    refusalReason = "RESET_STREAM has already been sent"
                }
            } catch {
                assert(self.pendingWrites.isEmpty)
                throw NetworkError(streamStateViolation: "\(error)", operation: "sendFin")
            }
        }

        if let refusalReason {
            self.pendingWrites.finalizeAllFramesAsFailed()
            throw NetworkError(streamStateViolation: refusalReason, operation: "flush")
        }

        if self.pendingWrites.isEmpty && !carriesFin { return }

        if carriesFin {
            self.markFin()
        }

        switch self.handle.invokeSendStreamData() {
        case .proceed(let linkage):
            let outgoing = self.pendingWrites.drainArray()
            try linkage.invokeSendStreamData(self.reference, streamData: outgoing)

        case .handleViolation(let reason):
            self.pendingWrites.finalizeAllFramesAsFailed()
            throw SwiftNetworkStreamHandle.violationError(operation: "invokeSendStreamData", reason: reason)
        }
    }

    /// Sets the FIN flag on the last queued frame, or adds an empty frame.
    private mutating func markFin() {
        if self.pendingWrites.isEmpty {
            var frame = Frame(count: 0)
            frame.connectionComplete = true
            self.pendingWrites.add(frame: frame)
        } else {
            // This is dumb: FrameArray should make it possible to do this without iterating.
            let last = self.pendingWrites.count &- 1
            var index = 0
            self.pendingWrites.iterateMutableFrames { frame -> Bool in
                if index == last {
                    frame.connectionComplete = true
                }
                index &+= 1
                return true
            }
        }
    }
}

// MARK: - Closing

@available(anyAppleOS 26, *)
extension QUICStreamCore {
    /// Resets the send side of the stream.
    ///
    /// - Parameter code: The application error code to send in RESET\_STREAM.
    @usableFromInline
    mutating func sendResetStream(code: QUICApplicationErrorCode) {
        let action: QUICStreamStateMachine.LocalResetAction

        do {
            action = try self.state.localReset(applicationErrorCode: code)
        } catch {
            return  // Not connected: there is no send side on the wire to reset.
        }

        switch action {
        case .sendReset:
            // RESET_STREAM abandons the stream's data, so nothing still queued is going out.
            self.pendingWrites.finalizeAllFramesAsFailed()
            self.abortOutbound(error: NetworkError(quicApplicationError: code.rawValue))
        case .ignore(.alreadyFinished):
            ()  // Send side finished cleanly: RESET_STREAM would contradict the FIN.
        case .ignore(.alreadyReset):
            ()  // RESET_STREAM is not sent twice.
        }
    }

    /// Closes the receive side of the stream.
    ///
    /// - Parameter code: The application error code to send in STOP_SENDING.
    @usableFromInline
    mutating func sendStopSending(code: QUICApplicationErrorCode) {
        let action: QUICStreamStateMachine.CloseReadSideAction

        do {
            action = try self.state.closeReadSide()
        } catch {
            return  // Not connected: there is no receive side on the wire to stop.
        }

        switch action {
        case .markReadClosed:
            // Nothing will read what is held: the receive side is closed to the consumer as of
            // this call.
            self.undeliveredReads.finalizeAllAsFailed()
            self.finPending = false
            self.abortInbound(error: NetworkError(quicApplicationError: code.rawValue))
        case .ignoreAlreadyClosed:
            // The peer already finished; STOP_SENDING has nothing to stop.
            ()
        case .deliverPeerResetError:
            // The peer already reset the stream; it has stopped sending.
            ()
        }
    }

    /// Tears the stream down: closes the state machine, detaches from the stack, and finalizes
    /// everything still held in either direction.
    ///
    /// - Parameter error: The error to disconnect with, or `nil` for a clean close.
    mutating func close(error: NetworkError?) {
        switch self.state.close(reason: error == nil ? .clean : .error) {
        case .close:
            ()
        case .ignoreAlreadyClosed:
            ()
        }

        switch self.handle.invokeDisconnect() {
        case .proceed(let linkage):
            linkage.invokeDisconnect(self.reference, error: error)
        case .ignore:
            ()  // Already detached.
        }

        switch self.handle.invokeDetach() {
        case .proceed(let linkage):
            do throws(NetworkError) {
                try linkage.invokeDetach(self.reference)
            } catch {
                ()  // The stack has already let go of its side.
            }
        case .skipAlreadyDetached:
            ()
        }

        self.pendingWrites.finalizeAllFramesAsFailed()
        self.undeliveredReads.finalizeAllAsFailed()
        self.finPending = false
        self.coalescedBytes = []
        // Breaks the cycle through the container which owns this stream's slot.
        self.reference = ProtocolInstanceReference()
    }

    private func abortInbound(error: NetworkError?) {
        switch self.handle.invokeAbortInbound() {
        case .proceed(let linkage):
            do throws(NetworkError) {
                try linkage.invokeAbortInbound(self.reference, error: error)
            } catch {
                ()  // The stack has already torn the receive side down.
            }
        case .ignore:
            ()  // Detached: there is nothing to abort.
        }
    }

    private func abortOutbound(error: NetworkError?) {
        switch self.handle.invokeAbortOutbound() {
        case .proceed(let linkage):
            do throws(NetworkError) {
                try linkage.invokeAbortOutbound(self.reference, error: error)
            } catch {
                ()  // The stack has already torn the send side down.
            }
        case .ignore:
            ()  // Detached: there is nothing to abort.
        }
    }
}

@available(anyAppleOS 26, *)
extension NetworkError {
    fileprivate init(streamStateViolation reason: String, operation: String) {
        self.init(
            category: NetworkError.CommonCategory(
                identifier: "swift-nio-quic.streamStateViolation",
                description: "\(operation): \(reason)"
            )
        )
    }
}

@available(anyAppleOS 26, *)
extension UniqueDeque where Element == Frame {
    /// Compute the number of bytes which haven't been claimed.
    ///
    /// - Complexity: O(`count`)
    fileprivate func unclaimedLength() -> Int {
        var length = 0

        for index in self.indices {
            length += self[index].unclaimedLength
        }

        return length
    }

    /// Claims `bytes` from the leading frames, removing fully claimed frames.
    ///
    /// - Parameter bytes: The number of bytes to claim
    /// - Precondition: `bytes` must not exceed `unclaimedLength()`
    fileprivate mutating func claimLeadingBytes(_ bytes: Int) {
        assert(bytes <= self.unclaimedLength())
        var remaining = bytes

        while !self.isEmpty && remaining > 0 {
            let available = self[0].unclaimedLength

            if remaining >= available {
                var frame = self.removeFirst()
                frame.finalize(success: true)
                remaining &-= available
            } else {
                _ = self[0].claim(fromStart: remaining)
                remaining = 0
            }
        }
    }

    /// Claims all bytes, removing and finalizing each frame.
    fileprivate mutating func claimAllBytes() {
        while var frame = self.popFirst() {
            frame.finalize(success: true)
        }
    }

    /// Empties the deque, finalizing each frame as failed.
    fileprivate mutating func finalizeAllAsFailed() {
        while var frame = self.popFirst() {
            frame.finalize(success: false)
        }
    }
}
