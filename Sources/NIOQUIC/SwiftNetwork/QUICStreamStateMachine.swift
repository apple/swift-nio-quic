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

import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

enum WrappedStreamStateCriticalError {
    case stopSending(NIOQUICHelpers.QUICStopSendingError)
    case resetStream(QUICStreamResetError)
}

extension WrappedStreamStateCriticalError {
    /// The underlying error.
    var error: any Error {
        switch self {
        case .stopSending(let error):
            return error
        case .resetStream(let error):
            return error
        }
    }

    /// The application-level error code carried by this error.
    var errorCode: QUICApplicationErrorCode {
        switch self {
        case .stopSending(let error):
            return error.code
        case .resetStream(let error):
            return error.code
        }
    }
}

/// The direction to shutdown on a stream.
enum StreamShutdownDirection {
    case read
    case write
    case all
}

// ┌─────────────────────────────────────────────────────────────────────────────┐
// │                  Stream State Machine (see RFC 9000 §3)                     │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// Stream lifecycle:
//
//    init()
//      │
//      ▼
//   .pendingID ──── streamConnected(direction:) ────▶ .connected
//      │                                                  │
//      │                                             normal flow
//      │                                            (send/receive
//      │                                              sub-SMs)
//      │                                                  │
//      │              close()                         close()
//      └──────────────┬───────────────────────────────────┘
//                     ▼
//                  .closed
//
// When connected, the stream has independent send and receive sub-state-machines
// (see diagrams above). For unidirectional streams, only one direction is active:
//   - sendOnly:    send SM active, receive operations throw InvalidTransition.wrongDirection
//   - receiveOnly: receive SM active, send operations throw InvalidTransition.wrongDirection
//   - bidirectional: both active, stream is fully closed when both reach terminal
//

/// Combined state machine for a QUIC stream, handling both send and receive directions.
///
/// For bidirectional streams, both send and receive state machines are active.
/// For unidirectional streams, only one direction is relevant.
@available(anyAppleOS 26, *)
struct QUICStreamStateMachine: ~Copyable {

    /// Error thrown when a transition is called in an invalid state.
    enum InvalidTransition: Error, Hashable, CustomStringConvertible {
        /// The stream has not yet connected (still in `pendingID` or `closed` state).
        case notConnected
        /// The operation does not apply to this stream direction.
        case wrongDirection

        var description: String {
            switch self {
            case .notConnected: return "stream not connected"
            case .wrongDirection: return "wrong stream direction"
            }
        }
    }

    enum State: ~Copyable {
        /// The stream direction and associated sub-state-machines.
        enum StreamState: ~Copyable {
            struct Bidirectional: ~Copyable {
                var sendState: QUICStreamSendStateMachine
                var receiveState: QUICStreamReceiveStateMachine

                var isFullyClosed: Bool { sendState.isTerminal && receiveState.isTerminal }

                init() {
                    self.sendState = .init()
                    self.receiveState = .init()
                }
            }

            struct SendOnly: ~Copyable {
                var sendState: QUICStreamSendStateMachine

                var isFullyClosed: Bool { sendState.isTerminal }

                init() {
                    self.sendState = .init()
                }
            }

            struct ReceiveOnly: ~Copyable {
                var receiveState: QUICStreamReceiveStateMachine

                var isFullyClosed: Bool { receiveState.isTerminal }

                init() {
                    self.receiveState = .init()
                }
            }

            case bidirectional(Bidirectional)
            case sendOnly(SendOnly)
            case receiveOnly(ReceiveOnly)

            var isFullyClosed: Bool {
                switch self {
                case .bidirectional(let streamState): return streamState.isFullyClosed
                case .sendOnly(let streamState): return streamState.isFullyClosed
                case .receiveOnly(let streamState): return streamState.isFullyClosed
                }
            }
        }

        /// Stream is connected and active.
        struct Connected: ~Copyable {
            var streamState: StreamState
        }

        enum CloseReason: Sendable {
            case clean
            case localReset(applicationErrorCode: QUICApplicationErrorCode)
            case peerReset(applicationErrorCode: QUICApplicationErrorCode)
            case error
        }

        /// Stream is closed.
        struct Closed: ~Copyable {
            let reason: CloseReason
        }

        /// Stream created for an outbound flow; stream ID not yet assigned.
        struct PendingID: ~Copyable {
            /// The final size from an early FIN, if one was received before connected.
            var earlyFinFinalSize: UInt64? = nil
        }

        case pendingID(PendingID)
        case connected(Connected)
        case closed(Closed)
    }

    private var state: State

    /// Creates a state machine for a stream whose direction is not yet known.
    /// The direction will be resolved when ``streamConnected(direction:)`` is called.
    init() {
        self.state = .pendingID(.init())
    }

    // MARK: - Internal Queries

    /// Returns `true` if the stream is connected.
    var isConnected: Bool {
        switch self.state {
        case .connected:
            return true
        case .pendingID:
            return false
        case .closed:
            return false
        }
    }

    /// Returns `true` if the write side is closed (terminal, including both clean finish and reset).
    var isWriteClosed: Bool {
        switch self.state {
        case .connected(let connected):
            switch connected.streamState {
            case .bidirectional(let streamState): return streamState.sendState.isTerminal
            case .sendOnly(let streamState): return streamState.sendState.isTerminal
            case .receiveOnly: return true  // No write side
            }

        case .pendingID:
            return false

        case .closed:
            return true
        }
    }

    /// Returns `true` if the stream is fully closed (both directions terminal or closed).
    var isFullyClosed: Bool {
        switch self.state {
        case .connected(let connected):
            return connected.streamState.isFullyClosed
        case .pendingID:
            return false
        case .closed:
            return true
        }
    }

    /// Returns `true` if the read side has received FIN.
    var hasReceivedFin: Bool {
        switch self.state {
        case .connected(let connected):
            switch connected.streamState {
            case .bidirectional(let streamState): return streamState.receiveState.hasReceivedFin
            case .sendOnly: return false  // No read side — no FIN was received
            case .receiveOnly(let streamState): return streamState.receiveState.hasReceivedFin
            }

        case .pendingID(let pending):
            return pending.earlyFinFinalSize != nil

        case .closed(let closed):
            // Only report FIN for clean closes — error/reset closes
            // did not necessarily receive a FIN from the peer.
            switch closed.reason {
            case .clean:
                return true
            case .localReset, .peerReset, .error:
                return false
            }
        }
    }

    /// True if the stream has data or FIN ready for the application to read.
    /// `hasPendingData` must be passed by the caller because buffered bytes live outside the SM.
    func isReadable(hasPendingData: Bool) -> Bool {
        if self.isReceiveClosed {
            return false
        }

        if !hasPendingData && !self.hasReceivedFin {
            return false
        }

        switch self.state {
        case .connected(let connected):
            switch connected.streamState {
            case .bidirectional:
                return true
            case .sendOnly:
                return false
            case .receiveOnly:
                return true
            }

        case .pendingID:
            return false

        case .closed:
            return false
        }
    }

    /// Returns `true` if the stream is connected and the write side is open.
    var canWrite: Bool {
        self.isConnected && !self.isWriteClosed
    }

    // MARK: - Private Queries

    /// Returns `true` if the receive side is closed (terminal, reset, stream closed,
    /// or send-only direction where there is no receive side).
    private var isReceiveClosed: Bool {
        switch self.state {
        case .connected(let connected):
            switch connected.streamState {
            case .bidirectional(let streamState):
                return streamState.receiveState.isTerminal || streamState.receiveState.hasReceivedReset
            case .sendOnly:
                return true
            case .receiveOnly(let streamState):
                return streamState.receiveState.isTerminal || streamState.receiveState.hasReceivedReset
            }

        case .pendingID:
            return false

        case .closed:
            return true
        }
    }

    /// Returns `true` if the stream is disconnected and fully closed, indicating it needs cleanup.
    private var needsCleanup: Bool {
        !self.isConnected && self.isFullyClosed
    }

    /// True if the read direction is valid for this stream's direction.
    private var canShutdownRead: Bool {
        switch self.state {
        case .connected(let connected):
            switch connected.streamState {
            case .bidirectional:
                return true
            case .sendOnly:
                return false
            case .receiveOnly:
                return true
            }

        case .pendingID:
            return false

        case .closed:
            return false
        }
    }

    /// True if the write direction is valid for this stream's direction.
    private var canShutdownWrite: Bool {
        switch self.state {
        case .connected(let connected):
            switch connected.streamState {
            case .bidirectional:
                return true
            case .sendOnly:
                return true
            case .receiveOnly:
                return false
            }

        case .pendingID:
            return false

        case .closed:
            return false
        }
    }

    // MARK: - Transitions

    enum AttemptReadAction: ~Copyable {
        /// Receive side is open — caller should proceed with the read.
        case proceedWithRead
        /// Receive side is closed — caller should return without reading.
        case doNotRead
    }

    /// Returns whether a read attempt should proceed.
    func attemptRead() -> AttemptReadAction {
        self.isReceiveClosed ? .doNotRead : .proceedWithRead
    }

    enum InboundDataAvailableAction: ~Copyable {
        /// Stream is connected — caller should read data.
        case readData
        /// Stream is not connected — do not read.
        case doNotRead
    }

    /// Called when an inbound data available event is received from the lower protocol.
    mutating func inboundDataAvailable() -> InboundDataAvailableAction {
        // Allow reads even in pendingID: data can arrive before handleConnectedEvent fires
        // (e.g. optimistic reads from start(fromNewFlowHandler:)). Only block when closed.
        switch self.state {
        case .connected:
            return .readData
        case .pendingID:
            return .readData
        case .closed:
            return .doNotRead
        }
    }

    enum StreamConnectedAction: ~Copyable {
        /// Stream is now ready — activate it.
        case activateStream
        /// Already connected — ignore.
        case ignoreAlreadyConnected
        /// Already closed — ignore.
        case ignoreAlreadyClosed
    }

    /// Transition when the stream becomes connected and its direction is known.
    mutating func streamConnected(direction: QUICStreamDirection) -> StreamConnectedAction {
        // Capture early FIN final size before the state transition consumes pendingID.
        let earlyFinFinalSize: UInt64?
        switch self.state {
        case .pendingID(let pending):
            earlyFinFinalSize = pending.earlyFinFinalSize
        case .connected:
            earlyFinFinalSize = nil
        case .closed:
            earlyFinFinalSize = nil
        }
        let action = self.state.streamConnected(direction: direction)
        // If FIN was received before connected, replay it into the receive sub-SM.
        if let earlyFinFinalSize {
            self.state.replayEarlyFin(finalSize: earlyFinFinalSize)
        }
        return action
    }

    // MARK: - Send-side named transitions

    enum WriteDataAction: ~Copyable {
        case sendData
        enum DoNotWriteReason: ~Copyable {
            case streamFinished
            case streamReset
        }
        case doNotWrite(DoNotWriteReason)
    }

    mutating func writeData() throws(QUICStreamStateMachine.InvalidTransition) -> WriteDataAction {
        try self.state.writeData()
    }

    enum SendFinAction: ~Copyable {
        case sendFin(streamFullyClosed: Bool)
        enum IgnoreReason: ~Copyable {
            case alreadyFinished
            case streamReset(applicationErrorCode: QUICApplicationErrorCode)
        }
        case ignore(IgnoreReason)
    }

    mutating func sendFin() throws(QUICStreamStateMachine.InvalidTransition) -> SendFinAction {
        try self.state.sendFin()
    }

    enum LocalResetAction: ~Copyable {
        case sendReset(applicationErrorCode: QUICApplicationErrorCode, streamFullyClosed: Bool)
        enum IgnoreReason: ~Copyable {
            case alreadyFinished
            case alreadyReset(applicationErrorCode: QUICApplicationErrorCode)
        }
        case ignore(IgnoreReason)
    }

    mutating func localReset(
        applicationErrorCode: QUICApplicationErrorCode
    ) throws(QUICStreamStateMachine.InvalidTransition) -> LocalResetAction {
        try self.state.localReset(applicationErrorCode: applicationErrorCode)
    }

    enum ReceiveStopSendingAction: ~Copyable {
        /// Send RESET_STREAM to the peer; the stream remains open.
        case sendReset(WrappedStreamStateCriticalError)
        /// Send RESET_STREAM and clean up the stream — both sides are now terminal.
        case sendResetAndCloseStream(WrappedStreamStateCriticalError)
        enum IgnoreReason: ~Copyable {
            case alreadyFinished
            case alreadyReset
        }
        case ignore(IgnoreReason)
    }

    mutating func receiveStopSending(
        applicationErrorCode: QUICApplicationErrorCode
    ) throws(QUICStreamStateMachine.InvalidTransition) -> ReceiveStopSendingAction {
        let innerAction = try self.state.receiveStopSending(
            applicationErrorCode: applicationErrorCode
        )
        switch innerAction {
        case .sendReset:
            let error: WrappedStreamStateCriticalError = .stopSending(
                NIOQUICHelpers.QUICStopSendingError(code: applicationErrorCode)
            )
            return .sendReset(error)
        case .sendResetAndCloseStream:
            let error: WrappedStreamStateCriticalError = .stopSending(
                NIOQUICHelpers.QUICStopSendingError(code: applicationErrorCode)
            )
            return .sendResetAndCloseStream(error)
        case .ignore(.alreadyFinished):
            return .ignore(.alreadyFinished)
        case .ignore(.alreadyReset):
            return .ignore(.alreadyReset)
        }
    }

    enum AcknowledgeDataAction: ~Copyable {
        case completeSend
        case completeSendAfterReset
        case ignoreNotAwaitingAck
    }

    mutating func acknowledgeAllData() throws(QUICStreamStateMachine.InvalidTransition) -> AcknowledgeDataAction {
        try self.state.acknowledgeAllData()
    }

    // MARK: - Receive-side named transitions

    enum ReceiveDataAction: ~Copyable {
        case bufferData
        enum DoNotBufferReason: ~Copyable {
            case allDataReceived
            case streamReset
        }
        case doNotBuffer(DoNotBufferReason)
    }

    mutating func receiveData() throws(QUICStreamStateMachine.InvalidTransition) -> ReceiveDataAction {
        try self.state.receiveData()
    }

    enum ReceiveFinAction: ~Copyable {
        case markAllDataReceived(finalSize: UInt64)
        enum IgnoreReason: ~Copyable {
            case alreadyReceivedFin
            case streamReset
        }
        case ignore(IgnoreReason)
    }

    mutating func receiveFin(finalSize: UInt64) throws(QUICStreamStateMachine.InvalidTransition) -> ReceiveFinAction {
        try self.state.receiveFin(finalSize: finalSize)
    }

    enum ReceiveResetStreamAction: ~Copyable {
        /// The receive side is the only remaining live direction —
        /// tear down the channel with this error code.
        case closeStream(applicationErrorCode: QUICApplicationErrorCode)
        case surfaceReset(applicationErrorCode: QUICApplicationErrorCode)
        case doNothing(DoNothingReason)

        enum DoNothingReason: ~Copyable {
            /// Reset is moot — the receive side already saw FIN and all data.
            case alreadyFullyReceived
            case alreadyReset
        }
    }

    /// Transition when receiving RESET_STREAM from the peer.
    mutating func receiveResetStream(
        applicationErrorCode: QUICApplicationErrorCode,
        finalSize: UInt64
    ) throws(QUICStreamStateMachine.InvalidTransition) -> ReceiveResetStreamAction {
        let innerAction = try self.state.receiveResetStream(
            applicationErrorCode: applicationErrorCode,
            finalSize: finalSize
        )
        switch innerAction {
        case .closeStream:
            return .closeStream(applicationErrorCode: applicationErrorCode)
        case .doNotCloseStream:
            return .surfaceReset(applicationErrorCode: applicationErrorCode)
        case .ignore(.alreadyFullyReceived):
            return .doNothing(.alreadyFullyReceived)
        case .ignore(.alreadyReset):
            return .doNothing(.alreadyReset)
        }
    }

    enum ApplicationReadAction: ~Copyable {
        case deliverData
        case deliverEndOfStream
        case deliverResetError(applicationErrorCode: QUICApplicationErrorCode)
        enum IgnoreReason: ~Copyable {
            /// No data has been received yet.
            case noDataAvailable
            /// All data (or reset) already delivered.
            case alreadyDelivered
        }
        case ignore(IgnoreReason)
    }

    mutating func applicationRead() throws(QUICStreamStateMachine.InvalidTransition) -> ApplicationReadAction {
        try self.state.applicationRead()
    }

    // MARK: - Combined atomic transitions

    enum CloseReadSideAction: ~Copyable {
        case markReadClosed(streamFullyClosed: Bool)
        case ignoreAlreadyClosed
        case deliverPeerResetError(applicationErrorCode: QUICApplicationErrorCode)
    }

    /// Atomically force-close the read side: calls `receiveFin` then `applicationRead` on the
    /// inner receive SM in a single mutation. Use this instead of calling those two methods
    /// individually when you want to unconditionally close the receive direction.
    mutating func closeReadSide() throws(QUICStreamStateMachine.InvalidTransition) -> CloseReadSideAction {
        try self.state.closeReadSide()
    }

    enum CloseAction: ~Copyable {
        /// Close the stream.
        case close
        /// Already closed — ignore.
        case ignoreAlreadyClosed
    }

    /// Transition to close the stream.
    mutating func close(reason: State.CloseReason) -> CloseAction {
        self.state.close(reason: reason)
    }

    // MARK: - Compound transitions

    enum ConsumeFinOrResetAction: ~Copyable {
        /// The receive side has reached its FIN.
        case reportFin(streamFullyClosed: Bool)
        /// The receive side was reset by the peer.
        case reportPeerReset(applicationErrorCode: QUICApplicationErrorCode)
        /// No FIN or RESET was captured, or one has already been consumed.
        case nothingToReport
    }

    /// Advance the receive sub-SM past any captured FIN or peer RESET,
    /// and report what was consumed.
    mutating func consumeFinOrReset() -> ConsumeFinOrResetAction {
        self.state.consumeFinOrReset()
    }

    enum CompleteReadAction: ~Copyable {
        case nothingToReport
        case reportFin(streamFullyClosed: Bool)
        case reportPeerReset(applicationErrorCode: QUICApplicationErrorCode)
    }

    /// Called after draining read data. Checks whether the receive side has
    /// reached a terminal condition (FIN or reset) and atomically closes it.
    mutating func completeRead() -> CompleteReadAction {
        if self.isFullyClosed {
            // Stream was closed (e.g. connection teardown) — signal end-of-stream.
            return .reportFin(streamFullyClosed: true)
        }
        switch self.consumeFinOrReset() {
        case .reportFin(let streamFullyClosed):
            return .reportFin(streamFullyClosed: streamFullyClosed)
        case .reportPeerReset(let code):
            return .reportPeerReset(applicationErrorCode: code)
        case .nothingToReport:
            return .nothingToReport
        }
    }

    enum ShutdownStreamAction: ~Copyable {
        /// Stream already torn down — caller should just remove the handler.
        case cleanupOnly
        /// Shut down both directions — caller should stop and remove handler.
        case shutdownBoth
        /// Read side closed. If `streamFullyClosed`, stop and remove handler;
        /// otherwise send STOP_SENDING via `abortInbound`.
        case closeRead(streamFullyClosed: Bool)
        /// Read side was already closed — no action needed.
        case readAlreadyClosed
        /// Peer reset the stream — stop and remove handler.
        case readPeerReset
        /// Write side reset. Send RESET_STREAM via `abortOutbound` with the given code.
        /// If `streamFullyClosed`, also stop and remove handler.
        case sendReset(applicationErrorCode: QUICApplicationErrorCode, streamFullyClosed: Bool)
        /// Write side already finished cleanly — no action needed.
        case writeAlreadyFinished
        /// Write side already reset — no action needed.
        case writeAlreadyReset(applicationErrorCode: QUICApplicationErrorCode)
        /// Cannot shutdown this direction on this stream type.
        case cannotShutdown
    }

    /// Shuts down the stream in the specified direction. Encapsulates cleanup
    /// eligibility, direction checks, and the underlying SM transitions.
    #if compiler(<6.4)
    // TODO: Workaround compiler crash while evaluating request ExecuteSILPipelineRequest
    @_optimize(none)
    #endif
    mutating func shutdownStream(
        direction: StreamShutdownDirection,
        applicationErrorCode: QUICApplicationErrorCode?
    ) -> ShutdownStreamAction {
        if self.needsCleanup {
            return .cleanupOnly
        }
        switch direction {
        case .all:
            return .shutdownBoth
        case .read:
            if self.canShutdownRead {
                do {
                    switch try self.closeReadSide() {
                    case .markReadClosed(let streamFullyClosed):
                        return .closeRead(streamFullyClosed: streamFullyClosed)
                    case .ignoreAlreadyClosed:
                        return .readAlreadyClosed
                    case .deliverPeerResetError:
                        return .readPeerReset
                    }
                } catch {
                    return .cannotShutdown
                }
            }
            return .cannotShutdown
        case .write:
            if self.canShutdownWrite, let applicationErrorCode {
                do {
                    switch try self.localReset(applicationErrorCode: applicationErrorCode) {
                    case .sendReset(let code, let streamFullyClosed):
                        return .sendReset(applicationErrorCode: code, streamFullyClosed: streamFullyClosed)
                    case .ignore(.alreadyFinished):
                        return .writeAlreadyFinished
                    case .ignore(.alreadyReset(let code)):
                        return .writeAlreadyReset(applicationErrorCode: code)
                    }
                } catch {
                    return .cannotShutdown
                }
            }
            return .cannotShutdown
        }
    }
}

// MARK: - State Transitions

@available(anyAppleOS 26, *)
extension QUICStreamStateMachine.State {
    /// Replay the FIN with the original finalSize that was received before the stream connected.
    mutating func replayEarlyFin(finalSize: UInt64) {
        switch consume self {
        case .connected(var connected):
            // The action from receiveFin is not propagated because this is a
            // replay of a FIN already handled during pendingID. The caller
            // (streamConnected) already returned .activateStream.
            // withReceiveState returns nil if the stream connected as sendOnly —
            // the FIN is silently dropped since send-only streams have no receive side.
            connected.streamState.withReceiveState { recv in
                switch recv.receiveFin(finalSize: finalSize) {
                case .markAllDataReceived: break
                case .ignore: break
                }
            }
            self = .connected(connected)

        case .pendingID(let pending):
            self = .pendingID(pending)

        case .closed(let closed):
            self = .closed(closed)
        }
    }

    mutating func streamConnected(
        direction: QUICStreamDirection
    ) -> QUICStreamStateMachine.StreamConnectedAction {
        switch consume self {
        case .pendingID:
            let connected: Connected
            switch direction {
            case .bidirectional:
                connected = Connected(streamState: .bidirectional(StreamState.Bidirectional()))
            case .sendOnly:
                connected = Connected(streamState: .sendOnly(StreamState.SendOnly()))
            case .receiveOnly:
                connected = Connected(streamState: .receiveOnly(StreamState.ReceiveOnly()))
            }

            self = .connected(connected)
            return .activateStream

        case .connected(let connected):
            self = .connected(connected)
            return .ignoreAlreadyConnected

        case .closed(let closed):
            self = .closed(closed)
            return .ignoreAlreadyClosed
        }
    }

    mutating func writeData() throws(QUICStreamStateMachine.InvalidTransition) -> QUICStreamStateMachine.WriteDataAction
    {
        switch consume self {
        case .connected(var connected):
            let writeAction = connected.streamState.withSendState { $0.writeData() }
            self = .connected(connected)

            switch writeAction {
            case .sendData: return .sendData
            case .doNotWrite(.streamFinished): return .doNotWrite(.streamFinished)
            case .doNotWrite(.streamReset): return .doNotWrite(.streamReset)
            case nil: throw .wrongDirection
            }

        case .pendingID(let pending):
            self = .pendingID(pending)
            throw .notConnected

        case .closed(let closed):
            self = .closed(closed)
            throw .notConnected
        }
    }

    mutating func sendFin() throws(QUICStreamStateMachine.InvalidTransition) -> QUICStreamStateMachine.SendFinAction {
        switch consume self {
        case .connected(var connected):
            let sendAction = connected.streamState.withSendState { $0.sendFin() }

            let isFullyClosed = connected.streamState.isFullyClosed
            let resetCode = connected.streamState.withSendState { $0.resetErrorCode }.flatMap {
                $0  // missing reset code for either reason is handled the same
            }
            self = .connected(connected)

            switch sendAction {
            case .sendFin:
                return .sendFin(streamFullyClosed: isFullyClosed)
            case .ignore(.alreadyFinished):
                return .ignore(.alreadyFinished)
            case .ignore(.streamReset):
                if let resetCode {
                    return .ignore(.streamReset(applicationErrorCode: resetCode))
                } else {
                    // resetCode must be non-nil if the stream was reset
                    // Unreachable: resetErrorCode is always non-nil when the stream is in a reset state.
                    fatalError("stream is in reset state but has no error code")
                }
            case nil:
                throw .wrongDirection
            }

        case .pendingID(let pending):
            self = .pendingID(pending)
            throw .notConnected

        case .closed(let closed):
            self = .closed(closed)
            throw .notConnected
        }
    }

    mutating func localReset(
        applicationErrorCode: QUICApplicationErrorCode
    ) throws(QUICStreamStateMachine.InvalidTransition) -> QUICStreamStateMachine.LocalResetAction {
        switch consume self {
        case .connected(var connected):
            let resetAction = connected.streamState.withSendState {
                $0.localReset(applicationErrorCode: applicationErrorCode)
            }

            let isFullyClosed = connected.streamState.isFullyClosed
            let existingResetCode = connected.streamState.withSendState { $0.resetErrorCode }.flatMap {
                $0  // missing reset code for either reason is handled the same
            }
            self = .connected(connected)
            switch resetAction {
            case .sendReset(let code):
                return .sendReset(applicationErrorCode: code, streamFullyClosed: isFullyClosed)
            case .ignore(.alreadyFinished):
                return .ignore(.alreadyFinished)
            case .ignore(.alreadyReset):
                return .ignore(
                    .alreadyReset(
                        applicationErrorCode: existingResetCode ?? applicationErrorCode
                    )
                )
            case nil:
                throw .wrongDirection
            }

        case .pendingID(let pending):
            self = .pendingID(pending)
            throw .notConnected

        case .closed(let closed):
            self = .closed(closed)
            throw .notConnected
        }
    }

    /// Inner transition for STOP_SENDING; returns a simplified action without `ErrorToFire`.
    /// The outer `QUICStreamStateMachine.receiveStopSending` wraps this with error delivery logic.
    enum InnerReceiveStopSendingAction: ~Copyable {
        case sendReset
        case sendResetAndCloseStream
        enum IgnoreReason: ~Copyable {
            case alreadyFinished
            case alreadyReset
        }
        case ignore(IgnoreReason)
    }

    mutating func receiveStopSending(
        applicationErrorCode: QUICApplicationErrorCode
    ) throws(QUICStreamStateMachine.InvalidTransition) -> InnerReceiveStopSendingAction {
        switch consume self {
        case .connected(var connected):
            // Note: `stopSendingAction` must be computed before `isFullyClosed` because
            // `receiveStopSending` mutates `sendState`, affecting `isTerminal`.
            let stopSendingAction = connected.streamState.withSendState {
                $0.receiveStopSending(applicationErrorCode: applicationErrorCode)
            }

            let isFullyClosed = connected.streamState.isFullyClosed
            self = .connected(connected)

            switch stopSendingAction {
            case .sendReset:
                return isFullyClosed ? .sendResetAndCloseStream : .sendReset
            case .ignore(.alreadyFinished): return .ignore(.alreadyFinished)
            case .ignore(.alreadyReset): return .ignore(.alreadyReset)
            case nil: throw .wrongDirection
            }

        case .pendingID(let pending):
            self = .pendingID(pending)
            throw .notConnected

        case .closed(let closed):
            self = .closed(closed)
            throw .notConnected
        }
    }

    mutating func acknowledgeAllData() throws(QUICStreamStateMachine.InvalidTransition)
        -> QUICStreamStateMachine.AcknowledgeDataAction
    {
        switch consume self {
        case .connected(var connected):
            let ackAction = connected.streamState.withSendState { $0.acknowledgeAllData() }
            self = .connected(connected)

            switch ackAction {
            case .completeSend: return .completeSend
            case .completeSendAfterReset: return .completeSendAfterReset
            case .ignoreNotAwaitingAck: return .ignoreNotAwaitingAck
            case nil: throw .wrongDirection
            }

        case .pendingID(let pending):
            self = .pendingID(pending)
            throw .notConnected

        case .closed(let closed):
            self = .closed(closed)
            throw .notConnected
        }
    }

    // TODO: Workaround compiler crash while evaluating request ExecuteSILPipelineRequest
    @_optimize(none)
    mutating func receiveFin(
        finalSize: UInt64
    ) throws(QUICStreamStateMachine.InvalidTransition) -> QUICStreamStateMachine.ReceiveFinAction {
        switch consume self {
        case .connected(var connected):
            let finAction = connected.streamState.withReceiveState {
                $0.receiveFin(finalSize: finalSize)
            }
            self = .connected(connected)

            switch finAction {
            case .markAllDataReceived(let size): return .markAllDataReceived(finalSize: size)
            case .ignore(.alreadyReceivedFin): return .ignore(.alreadyReceivedFin)
            case .ignore(.streamReset): return .ignore(.streamReset)
            case nil: throw .wrongDirection
            }

        case .pendingID(var pending):
            // Direction isn't known yet in pendingID — SwiftNetwork can deliver
            // FIN before the connected event. Direction is validated when the
            // stream connects and replays the FIN into the receive sub-SM.
            pending.earlyFinFinalSize = finalSize
            self = .pendingID(pending)
            return .markAllDataReceived(finalSize: finalSize)

        case .closed(let closed):
            self = .closed(closed)
            throw .notConnected
        }
    }

    mutating func receiveData() throws(QUICStreamStateMachine.InvalidTransition)
        -> QUICStreamStateMachine.ReceiveDataAction
    {
        switch consume self {
        case .connected(var connected):
            let receiveAction = connected.streamState.withReceiveState { $0.receiveData() }
            self = .connected(connected)

            switch receiveAction {
            case .bufferData: return .bufferData
            case .doNotBuffer(.allDataReceived): return .doNotBuffer(.allDataReceived)
            case .doNotBuffer(.streamReset): return .doNotBuffer(.streamReset)
            case nil: throw .wrongDirection
            }

        case .pendingID(let pending):
            // SwiftNetwork can deliver data before the connected event fires
            // (e.g. optimistic reads from start(fromNewFlowHandler:)).
            // Allow buffering — the receive sub-SM will track state once connected.
            self = .pendingID(pending)
            return .bufferData

        case .closed(let closed):
            self = .closed(closed)
            throw .notConnected
        }
    }

    /// Inner transition for RESET_STREAM; returns a simplified action without `ErrorToFire`.
    /// The outer `QUICStreamStateMachine.receiveResetStream` wraps this with error delivery logic.
    enum InnerReceiveResetStreamAction: ~Copyable {
        case closeStream
        case doNotCloseStream
        enum IgnoreReason: ~Copyable {
            case alreadyFullyReceived
            case alreadyReset
        }
        case ignore(IgnoreReason)
    }

    mutating func receiveResetStream(
        applicationErrorCode: QUICApplicationErrorCode,
        finalSize: UInt64
    ) throws(QUICStreamStateMachine.InvalidTransition) -> InnerReceiveResetStreamAction {
        switch consume self {
        case .connected(var connected):
            let receiveAction = connected.streamState.withReceiveState {
                $0.receiveResetStream(
                    applicationErrorCode: applicationErrorCode,
                    finalSize: finalSize
                )
            }

            let isFullyClosed = connected.streamState.isFullyClosed
            self = .connected(connected)

            switch receiveAction {
            case .notifyApplication:
                return isFullyClosed ? .closeStream : .doNotCloseStream
            case .ignore(.alreadyFullyReceived): return .ignore(.alreadyFullyReceived)
            case .ignore(.alreadyReset): return .ignore(.alreadyReset)
            case nil: throw .wrongDirection
            }

        case .pendingID(let pending):
            self = .pendingID(pending)
            throw .notConnected

        case .closed(let closed):
            self = .closed(closed)
            throw .notConnected
        }
    }

    mutating func applicationRead() throws(QUICStreamStateMachine.InvalidTransition)
        -> QUICStreamStateMachine.ApplicationReadAction
    {
        switch consume self {
        case .connected(var connected):
            let readAction = connected.streamState.withReceiveState { $0.applicationRead() }
            self = .connected(connected)

            switch readAction {
            case .deliverData: return .deliverData
            case .deliverEndOfStream: return .deliverEndOfStream
            case .deliverResetError(let code): return .deliverResetError(applicationErrorCode: code)
            case .ignore(.noDataAvailable): return .ignore(.noDataAvailable)
            case .ignore(.alreadyDelivered): return .ignore(.alreadyDelivered)
            case nil: throw .wrongDirection
            }

        case .pendingID(let pending):
            self = .pendingID(pending)
            throw .notConnected

        case .closed(let closed):
            self = .closed(closed)
            throw .notConnected
        }
    }

    /// Consume any captured FIN or peer RESET on the receive side.
    /// Non-terminal or non-connected states return `.nothingToReport`.
    mutating func consumeFinOrReset()
        -> QUICStreamStateMachine.ConsumeFinOrResetAction
    {
        switch consume self {
        case .connected(var connected):
            let readAction = connected.streamState.withReceiveState { $0.applicationRead() }
            let isFullyClosed = connected.streamState.isFullyClosed
            self = .connected(connected)
            switch readAction {
            case .deliverEndOfStream:
                return .reportFin(streamFullyClosed: isFullyClosed)
            case .deliverResetError(let code):
                return .reportPeerReset(applicationErrorCode: code)
            case .deliverData:
                return .nothingToReport
            case .ignore:
                return .nothingToReport
            case nil:
                return .nothingToReport
            }

        case .pendingID(let pending):
            // FIN captured on the pending record is not consumable until
            // `streamConnected` replays it into the receive sub-SM.
            self = .pendingID(pending)
            return .nothingToReport

        case .closed(let closed):
            self = .closed(closed)
            return .nothingToReport
        }
    }

    mutating func closeReadSide() throws(QUICStreamStateMachine.InvalidTransition)
        -> QUICStreamStateMachine.CloseReadSideAction
    {
        switch consume self {
        case .connected(var connected):
            // finalSize is 0 because this is a local close, not a peer FIN —
            // no wire-level final size is being asserted.
            let finAction = connected.streamState.withReceiveState {
                $0.receiveFin(finalSize: 0)
            }
            // Transition receive SM to terminal state (dataRead or resetRead).
            // The return value is intentionally not propagated — closeReadSide
            // uses finAction (from receiveFin) to determine the outcome.
            connected.streamState.withReceiveState { recv in
                switch recv.applicationRead() {
                case .deliverData: break
                case .deliverEndOfStream: break
                case .deliverResetError: break
                case .ignore(.noDataAvailable): break
                case .ignore(.alreadyDelivered): break
                }
            }

            let resetCode = connected.streamState.withReceiveState { $0.resetErrorCode }.flatMap { $0 }
            let streamFullyClosed = connected.streamState.isFullyClosed
            self = .connected(connected)

            switch finAction {
            case .ignore(.alreadyReceivedFin):
                return .ignoreAlreadyClosed

            case .ignore(.streamReset):
                // resetCode must be non-nil if the stream was reset
                if let resetCode {
                    return .deliverPeerResetError(applicationErrorCode: resetCode)
                } else {
                    // Unreachable: resetErrorCode is always non-nil when the stream is in a reset state.
                    fatalError("stream is in reset state but has no error code")
                }

            case .markAllDataReceived:
                return .markReadClosed(streamFullyClosed: streamFullyClosed)

            case nil:
                throw .wrongDirection
            }

        case .pendingID(let pending):
            self = .pendingID(pending)
            throw .notConnected

        case .closed(let closed):
            self = .closed(closed)
            throw .notConnected
        }
    }

    mutating func close(
        reason: QUICStreamStateMachine.State.CloseReason
    ) -> QUICStreamStateMachine.CloseAction {
        switch consume self {
        case .pendingID:
            self = .closed(.init(reason: reason))
            return .close

        case .connected:
            self = .closed(.init(reason: reason))
            return .close

        case .closed(let closed):
            self = .closed(closed)
            return .ignoreAlreadyClosed
        }
    }
}

// MARK: - StreamState with*State Helpers

@available(anyAppleOS 26, *)
extension QUICStreamStateMachine.State.StreamState {
    /// Mutates the send sub-state-machine via `body`, if this stream has a send direction.
    ///
    /// Returns `nil` without calling `body` when the stream is receive-only,
    /// allowing callers to distinguish "wrong direction" from errors thrown by `body`.
    mutating func withSendState<T: ~Copyable, E: Error>(
        _ body: (inout QUICStreamSendStateMachine) throws(E) -> T
    ) throws(E) -> T? {
        switch consume self {
        case .bidirectional(var streamState):
            do {
                let result = try body(&streamState.sendState)
                self = .bidirectional(streamState)
                return result
            } catch {
                self = .bidirectional(streamState)
                throw error
            }

        case .sendOnly(var streamState):
            do {
                let result = try body(&streamState.sendState)
                self = .sendOnly(streamState)
                return result
            } catch {
                self = .sendOnly(streamState)
                throw error
            }

        case .receiveOnly(let streamState):
            self = .receiveOnly(streamState)
            return nil
        }
    }

    /// Mutates the receive sub-state-machine via `body`, if this stream has a receive direction.
    ///
    /// Returns `nil` without calling `body` when the stream is send-only,
    /// allowing callers to distinguish "wrong direction" from errors thrown by `body`.
    mutating func withReceiveState<T: ~Copyable, E: Error>(
        _ body: (inout QUICStreamReceiveStateMachine) throws(E) -> T
    ) throws(E) -> T? {
        switch consume self {
        case .bidirectional(var streamState):
            do {
                let result = try body(&streamState.receiveState)
                self = .bidirectional(streamState)
                return result
            } catch {
                self = .bidirectional(streamState)
                throw error
            }

        case .receiveOnly(var streamState):
            do {
                let result = try body(&streamState.receiveState)
                self = .receiveOnly(streamState)
                return result
            } catch {
                self = .receiveOnly(streamState)
                throw error
            }

        case .sendOnly(let streamState):
            self = .sendOnly(streamState)
            return nil
        }
    }
}

// ┌─────────────────────────────────────────────────────────────────────────────┐
// │              Send State Machine (see RFC 9000 §3.1)                         │
// └─────────────────────────────────────────────────────────────────────────────┘
//
//                       o
//                       │ Create Stream / Peer Creates Bidirectional Stream
//                       │ init()
//                       ▼
//                   ┌───────┐
//                   │ Ready │
//                   └───┬───┘
//                       │ Send STREAM / STREAM_DATA_BLOCKED
//                       │ writeData()
//                       ▼
//                   ┌───────┐
//                   │ Send  │
//                   └───┬───┘
//                       │ Send STREAM + FIN
//                       │ sendFin()
//                       ▼
//                ┌───────────┐
//                │ Data Sent │
//                └─────┬─────┘
//                      │ Recv All ACKs
//                      │ acknowledgeAllData()
//                      ▼
//                ┌───────────┐
//                │ Data Recvd│ (terminal)
//                └───────────┘
//
//   Reset branch (from Ready, Send, or Data Sent):
//     Recv STOP_SENDING / application abort
//     receiveStopSending() / localReset()
//                       │
//                       ▼
//                ┌────────────┐
//                │ Reset Sent │ ─── Send RESET_STREAM
//                └──────┬─────┘
//                       │ Recv ACK
//                       │ acknowledgeAllData()
//                       ▼
//                ┌────────────┐
//                │ Reset Recvd│ (terminal)
//                └────────────┘

/// State machine for the sending direction of a QUIC stream per RFC 9000 Section 3.1.
@available(anyAppleOS 26, *)
struct QUICStreamSendStateMachine: ~Copyable {
    enum State: ~Copyable {
        /// Stream created but no data sent yet.
        case ready

        /// Data is being sent, FIN not yet sent.
        case send

        /// FIN sent, waiting for all data to be acknowledged.
        case dataSent

        /// All data acknowledged by peer (terminal state).
        case dataRecvd

        /// RESET_STREAM sent (due to local abort or receiving STOP_SENDING).
        case resetSent(applicationErrorCode: QUICApplicationErrorCode)

        /// RESET_STREAM acknowledged (terminal state).
        case resetRecvd(applicationErrorCode: QUICApplicationErrorCode)
    }

    private var state: State

    init() {
        self.state = .ready
    }

    // MARK: - Capability Queries

    /// Returns `true` if data can be written to this stream.
    var canWrite: Bool {
        switch self.state {
        case .ready:
            return true
        case .send:
            return true
        case .dataSent:
            return false
        case .dataRecvd:
            return false
        case .resetSent:
            return false
        case .resetRecvd:
            return false
        }
    }

    /// Returns `true` if the stream is in a terminal state.
    var isTerminal: Bool {
        switch self.state {
        case .dataRecvd:
            return true
        case .resetRecvd:
            return true
        case .ready:
            return false
        case .send:
            return false
        case .dataSent:
            return false
        case .resetSent:
            return false
        }
    }

    /// Returns `true` if the FIN has been sent (or reset was sent).
    var isFinished: Bool {
        switch self.state {
        case .dataSent:
            return true
        case .dataRecvd:
            return true
        case .resetSent:
            return true
        case .resetRecvd:
            return true
        case .ready:
            return false
        case .send:
            return false
        }
    }

    /// Returns `true` if the stream was reset.
    var wasReset: Bool {
        switch self.state {
        case .resetSent:
            return true
        case .resetRecvd:
            return true
        case .ready:
            return false
        case .send:
            return false
        case .dataSent:
            return false
        case .dataRecvd:
            return false
        }
    }

    /// Returns the reset error code if the stream was reset.
    var resetErrorCode: QUICApplicationErrorCode? {
        switch self.state {
        case .resetSent(let applicationErrorCode):
            return applicationErrorCode
        case .resetRecvd(let applicationErrorCode):
            return applicationErrorCode
        case .ready:
            return nil
        case .send:
            return nil
        case .dataSent:
            return nil
        case .dataRecvd:
            return nil
        }
    }

    // MARK: - Transitions

    enum WriteDataAction: ~Copyable {
        /// Send the data.
        case sendData
        /// Do not write.
        case doNotWrite(DoNotWriteReason)

        enum DoNotWriteReason: ~Copyable {
            /// Stream already sent FIN.
            case streamFinished
            /// Stream was reset.
            case streamReset
        }
    }

    /// Transition when writing data to the stream.
    mutating func writeData() -> WriteDataAction {
        self.state.writeData()
    }

    enum SendFinAction: ~Copyable {
        /// Send the FIN.
        case sendFin
        /// Do not send FIN.
        case ignore(IgnoreReason)

        enum IgnoreReason: ~Copyable {
            /// FIN already sent or stream finished.
            case alreadyFinished
            /// Stream was reset.
            case streamReset
        }
    }

    /// Transition when sending FIN to close the write side.
    mutating func sendFin() -> SendFinAction {
        self.state.sendFin()
    }

    enum ReceiveStopSendingAction: ~Copyable {
        /// Respond with RESET_STREAM.
        case sendReset(applicationErrorCode: QUICApplicationErrorCode)
        /// STOP_SENDING ignored.
        case ignore(IgnoreReason)

        enum IgnoreReason: ~Copyable {
            /// Stream already finished.
            case alreadyFinished
            /// Stream already reset.
            case alreadyReset
        }
    }

    /// Transition when receiving STOP_SENDING from peer.
    /// Per RFC 9000 Section 3.5, receiving STOP_SENDING should cause us to send RESET_STREAM.
    mutating func receiveStopSending(applicationErrorCode: QUICApplicationErrorCode) -> ReceiveStopSendingAction {
        self.state.receiveStopSending(applicationErrorCode: applicationErrorCode)
    }

    enum LocalResetAction: ~Copyable {
        /// Send RESET_STREAM.
        case sendReset(applicationErrorCode: QUICApplicationErrorCode)
        /// Do not reset.
        case ignore(IgnoreReason)

        enum IgnoreReason: ~Copyable {
            /// Stream already finished.
            case alreadyFinished
            /// Stream already reset.
            case alreadyReset
        }
    }

    /// Transition when locally aborting the stream.
    mutating func localReset(applicationErrorCode: QUICApplicationErrorCode) -> LocalResetAction {
        self.state.localReset(applicationErrorCode: applicationErrorCode)
    }

    enum AcknowledgeDataAction: ~Copyable {
        /// All data fully acknowledged — complete the send side.
        case completeSend
        /// Reset acknowledged — complete the send side.
        case completeSendAfterReset
        /// Not in a state awaiting acknowledgment — do nothing.
        case ignoreNotAwaitingAck
    }

    /// Transition when all data (or reset) is acknowledged by peer.
    mutating func acknowledgeAllData() -> AcknowledgeDataAction {
        self.state.acknowledgeAllData()
    }
}

// MARK: - State Transitions

@available(anyAppleOS 26, *)
extension QUICStreamSendStateMachine.State {
    mutating func writeData() -> QUICStreamSendStateMachine.WriteDataAction {
        switch consume self {
        case .ready:
            self = .send
            return .sendData
        case .send:
            self = .send
            return .sendData
        case .dataSent:
            self = .dataSent
            return .doNotWrite(.streamFinished)
        case .dataRecvd:
            self = .dataRecvd
            return .doNotWrite(.streamFinished)
        case .resetSent(let applicationErrorCode):
            self = .resetSent(applicationErrorCode: applicationErrorCode)
            return .doNotWrite(.streamReset)
        case .resetRecvd(let applicationErrorCode):
            self = .resetRecvd(applicationErrorCode: applicationErrorCode)
            return .doNotWrite(.streamReset)
        }
    }

    mutating func sendFin() -> QUICStreamSendStateMachine.SendFinAction {
        switch consume self {
        case .ready:
            self = .dataSent
            return .sendFin
        case .send:
            self = .dataSent
            return .sendFin
        case .dataSent:
            self = .dataSent
            return .ignore(.alreadyFinished)
        case .dataRecvd:
            self = .dataRecvd
            return .ignore(.alreadyFinished)
        case .resetSent(let applicationErrorCode):
            self = .resetSent(applicationErrorCode: applicationErrorCode)
            return .ignore(.streamReset)
        case .resetRecvd(let applicationErrorCode):
            self = .resetRecvd(applicationErrorCode: applicationErrorCode)
            return .ignore(.streamReset)
        }
    }

    /// Per RFC 9000 Section 3.5, receiving STOP_SENDING should cause us to send RESET_STREAM.
    mutating func receiveStopSending(
        applicationErrorCode: QUICApplicationErrorCode
    ) -> QUICStreamSendStateMachine.ReceiveStopSendingAction {
        switch consume self {
        case .ready:
            self = .resetSent(applicationErrorCode: applicationErrorCode)
            return .sendReset(applicationErrorCode: applicationErrorCode)
        case .send:
            self = .resetSent(applicationErrorCode: applicationErrorCode)
            return .sendReset(applicationErrorCode: applicationErrorCode)
        case .dataSent:
            // Already sent FIN, but per RFC, should still respond with RESET_STREAM
            self = .resetSent(applicationErrorCode: applicationErrorCode)
            return .sendReset(applicationErrorCode: applicationErrorCode)
        case .dataRecvd:
            // All data was acknowledged, STOP_SENDING arrived too late
            self = .dataRecvd
            return .ignore(.alreadyFinished)
        case .resetSent(let applicationErrorCode):
            self = .resetSent(applicationErrorCode: applicationErrorCode)
            return .ignore(.alreadyReset)
        case .resetRecvd(let applicationErrorCode):
            self = .resetRecvd(applicationErrorCode: applicationErrorCode)
            return .ignore(.alreadyReset)
        }
    }

    mutating func localReset(
        applicationErrorCode: QUICApplicationErrorCode
    ) -> QUICStreamSendStateMachine.LocalResetAction {
        switch consume self {
        case .ready:
            self = .resetSent(applicationErrorCode: applicationErrorCode)
            return .sendReset(applicationErrorCode: applicationErrorCode)
        case .send:
            self = .resetSent(applicationErrorCode: applicationErrorCode)
            return .sendReset(applicationErrorCode: applicationErrorCode)
        case .dataSent:
            self = .resetSent(applicationErrorCode: applicationErrorCode)
            return .sendReset(applicationErrorCode: applicationErrorCode)
        case .dataRecvd:
            self = .dataRecvd
            return .ignore(.alreadyFinished)
        case .resetSent(let applicationErrorCode):
            self = .resetSent(applicationErrorCode: applicationErrorCode)
            return .ignore(.alreadyReset)
        case .resetRecvd(let applicationErrorCode):
            self = .resetRecvd(applicationErrorCode: applicationErrorCode)
            return .ignore(.alreadyReset)
        }
    }

    mutating func acknowledgeAllData() -> QUICStreamSendStateMachine.AcknowledgeDataAction {
        switch consume self {
        case .dataSent:
            self = .dataRecvd
            return .completeSend
        case .resetSent(let applicationErrorCode):
            self = .resetRecvd(applicationErrorCode: applicationErrorCode)
            return .completeSendAfterReset
        case .ready:
            self = .ready
            return .ignoreNotAwaitingAck
        case .send:
            self = .send
            return .ignoreNotAwaitingAck
        case .dataRecvd:
            self = .dataRecvd
            return .ignoreNotAwaitingAck
        case .resetRecvd(let applicationErrorCode):
            self = .resetRecvd(applicationErrorCode: applicationErrorCode)
            return .ignoreNotAwaitingAck
        }
    }
}

// ┌─────────────────────────────────────────────────────────────────────────────┐
// │            Receive State Machine (see RFC 9000 §3.2)                        │
// └─────────────────────────────────────────────────────────────────────────────┘
//
//                       o
//                       │ Recv STREAM / STREAM_DATA_BLOCKED /
//                       │ Create Bidirectional Stream (Sending)
//                       │ init()
//                       ▼
//                   ┌───────┐
//                   │ Recv  │
//                   └───┬───┘
//                       │ Recv STREAM + FIN
//                       │ receiveFin()
//                       ▼
//                ┌───────────┐
//                │ Data Recvd│
//                └─────┬─────┘
//                      │ Read All Data / App reads all buffered data
//                      │ applicationRead()
//                      ▼
//                ┌───────────┐
//                │ Data Read │ (terminal)
//                └───────────┘
//
//   Reset branch (from Recv):
//     Recv RESET_STREAM
//     receiveResetStream()
//                       │
//                       ▼
//                ┌────────────┐
//                │ Reset Recvd│ ─── optionally Send STOP_SENDING
//                └──────┬─────┘
//                       │ App reads reset error
//                       │ applicationRead()
//                       ▼
//                ┌───────────┐
//                │ Reset Read│ (terminal)
//                └───────────┘

/// State machine for QUIC stream receiving direction per RFC 9000 Section 3.2.
///
/// The RFC defines a `Size Known` state between `Recv` and `Data Recvd` for when
/// the FIN has been received but data gaps remain. Since the transport layer handles
/// reassembly, we skip `Size Known` and go directly from `Recv` to `Data Recvd`.
@available(anyAppleOS 26, *)
struct QUICStreamReceiveStateMachine: ~Copyable {
    enum State: ~Copyable {
        /// Receiving data, FIN not yet received.
        struct Recv: ~Copyable {
            /// Whether any data has been received on this stream.
            var hasReceivedData: Bool
        }

        /// All data received, waiting for application to read.
        struct DataRecvd: ~Copyable {
            /// The final size of the stream.
            var finalSize: UInt64
        }

        /// All data read by application (terminal state).
        case dataRead

        /// Application notified of reset (terminal state).
        struct ResetRead: ~Copyable {
            let applicationErrorCode: QUICApplicationErrorCode
        }

        case recv(Recv)
        case dataRecvd(DataRecvd)

        /// RESET_STREAM received from peer.
        struct ResetRecvd: ~Copyable {
            let applicationErrorCode: QUICApplicationErrorCode
            let finalSize: UInt64
        }

        case resetRecvd(ResetRecvd)
        case resetRead(ResetRead)
    }

    private var state: State

    init() {
        self.state = .recv(.init(hasReceivedData: false))
    }

    // MARK: - Capability Queries

    /// Returns `true` if data can be read from this stream.
    var canRead: Bool {
        switch self.state {
        case .recv:
            return true
        case .dataRecvd:
            return true
        case .dataRead:
            return false
        case .resetRecvd:
            return false
        case .resetRead:
            return false
        }
    }

    /// Returns `true` if the stream is in a terminal state.
    var isTerminal: Bool {
        switch self.state {
        case .dataRead:
            return true
        case .resetRead:
            return true
        case .recv:
            return false
        case .dataRecvd:
            return false
        case .resetRecvd:
            return false
        }
    }

    /// Returns `true` if FIN has been received.
    var hasReceivedFin: Bool {
        switch self.state {
        case .dataRecvd:
            return true
        case .dataRead:
            return true
        case .recv:
            return false
        case .resetRecvd:
            return false
        case .resetRead:
            return false
        }
    }

    /// Returns `true` if the peer has reset the receive side.
    var hasReceivedReset: Bool {
        switch self.state {
        case .resetRecvd:
            return true
        case .resetRead:
            return true
        case .recv:
            return false
        case .dataRecvd:
            return false
        case .dataRead:
            return false
        }
    }

    /// Returns the reset error code if the stream was reset.
    var resetErrorCode: QUICApplicationErrorCode? {
        switch self.state {
        case .resetRecvd(let resetRecvd):
            return resetRecvd.applicationErrorCode
        case .resetRead(let resetRead):
            return resetRead.applicationErrorCode
        case .recv:
            return nil
        case .dataRecvd:
            return nil
        case .dataRead:
            return nil
        }
    }

    /// Returns `true` if the stream is in a state where it can produce data for the application.
    /// This includes states where data has been received, FIN has been received, or a reset needs to be delivered.
    var canProduceData: Bool {
        switch self.state {
        case .recv(let recv):
            return recv.hasReceivedData
        case .dataRecvd:
            return true
        case .resetRecvd:
            return true  // Reset error needs to be delivered
        case .dataRead:
            return false
        case .resetRead:
            return false
        }
    }

    // MARK: - Transitions

    enum ReceiveDataAction: ~Copyable {
        /// Buffer the data.
        case bufferData
        /// Do not buffer.
        case doNotBuffer(DoNotBufferReason)

        enum DoNotBufferReason: ~Copyable {
            /// All data already received.
            case allDataReceived
            /// Stream was reset.
            case streamReset
        }
    }

    /// Transition when receiving data on the stream.
    mutating func receiveData() -> ReceiveDataAction {
        self.state.receiveData()
    }

    enum ReceiveFinAction: ~Copyable {
        /// FIN received — mark all data received.
        case markAllDataReceived(finalSize: UInt64)
        /// FIN ignored.
        case ignore(IgnoreReason)

        enum IgnoreReason: ~Copyable {
            /// FIN already received.
            case alreadyReceivedFin
            /// Stream was reset.
            case streamReset
        }
    }

    /// Transition when receiving FIN from peer.
    mutating func receiveFin(finalSize: UInt64) -> ReceiveFinAction {
        self.state.receiveFin(finalSize: finalSize)
    }

    enum ReceiveResetAction: ~Copyable {
        /// Notify application of reset.
        case notifyApplication(applicationErrorCode: QUICApplicationErrorCode)
        /// Reset ignored.
        case ignore(IgnoreReason)

        enum IgnoreReason: ~Copyable {
            /// All data already received.
            case alreadyFullyReceived
            /// Already reset.
            case alreadyReset
        }
    }

    /// Transition when receiving RESET_STREAM from peer.
    mutating func receiveResetStream(
        applicationErrorCode: QUICApplicationErrorCode,
        finalSize: UInt64
    ) -> ReceiveResetAction {
        self.state.receiveResetStream(applicationErrorCode: applicationErrorCode, finalSize: finalSize)
    }

    enum ApplicationReadAction: ~Copyable {
        /// Data is available — deliver to application.
        case deliverData
        /// All data has been read — deliver end of stream.
        case deliverEndOfStream
        /// Reset error — deliver to application.
        case deliverResetError(applicationErrorCode: QUICApplicationErrorCode)
        enum IgnoreReason: ~Copyable {
            /// No data has been received yet.
            case noDataAvailable
            /// All data (or reset) already delivered.
            case alreadyDelivered
        }
        case ignore(IgnoreReason)
    }

    /// Transition when application reads from the stream.
    mutating func applicationRead() -> ApplicationReadAction {
        self.state.applicationRead()
    }

    enum SendStopSendingAction: ~Copyable {
        /// Send STOP_SENDING to peer.
        case sendStopSending
        /// Do not send STOP_SENDING.
        case ignore(IgnoreReason)

        enum IgnoreReason: ~Copyable {
            /// All data already received.
            case alreadyReceivedAllData
            /// Already reset.
            case alreadyReset
        }
    }

    /// Transition when locally requesting peer to stop sending (via STOP_SENDING).
    mutating func sendStopSending() -> SendStopSendingAction {
        self.state.sendStopSending()
    }
}

// MARK: - Receive State Transitions

@available(anyAppleOS 26, *)
extension QUICStreamReceiveStateMachine.State {
    mutating func receiveData() -> QUICStreamReceiveStateMachine.ReceiveDataAction {
        switch consume self {
        case .recv:
            self = .recv(.init(hasReceivedData: true))
            return .bufferData
        case .dataRecvd(let dataRecvd):
            self = .dataRecvd(dataRecvd)
            return .doNotBuffer(.allDataReceived)
        case .dataRead:
            self = .dataRead
            return .doNotBuffer(.allDataReceived)
        case .resetRecvd(let resetRecvd):
            self = .resetRecvd(resetRecvd)
            return .doNotBuffer(.streamReset)
        case .resetRead(let resetRead):
            self = .resetRead(resetRead)
            return .doNotBuffer(.streamReset)
        }
    }

    mutating func receiveFin(finalSize: UInt64) -> QUICStreamReceiveStateMachine.ReceiveFinAction {
        switch consume self {
        case .recv:
            self = .dataRecvd(.init(finalSize: finalSize))
            return .markAllDataReceived(finalSize: finalSize)
        case .dataRecvd(let dataRecvd):
            self = .dataRecvd(dataRecvd)
            return .ignore(.alreadyReceivedFin)
        case .dataRead:
            self = .dataRead
            return .ignore(.alreadyReceivedFin)
        case .resetRecvd(let resetRecvd):
            self = .resetRecvd(resetRecvd)
            return .ignore(.streamReset)
        case .resetRead(let resetRead):
            self = .resetRead(resetRead)
            return .ignore(.streamReset)
        }
    }

    mutating func receiveResetStream(
        applicationErrorCode: QUICApplicationErrorCode,
        finalSize: UInt64
    ) -> QUICStreamReceiveStateMachine.ReceiveResetAction {
        switch consume self {
        case .recv:
            self = .resetRecvd(.init(applicationErrorCode: applicationErrorCode, finalSize: finalSize))
            return .notifyApplication(applicationErrorCode: applicationErrorCode)
        case .dataRecvd(let dataRecvd):
            self = .dataRecvd(dataRecvd)
            return .ignore(.alreadyFullyReceived)
        case .dataRead:
            self = .dataRead
            return .ignore(.alreadyFullyReceived)
        case .resetRecvd(let resetRecvd):
            self = .resetRecvd(resetRecvd)
            return .ignore(.alreadyReset)
        case .resetRead(let resetRead):
            self = .resetRead(resetRead)
            return .ignore(.alreadyReset)
        }
    }

    mutating func applicationRead() -> QUICStreamReceiveStateMachine.ApplicationReadAction {
        switch consume self {
        case .recv(let recv):
            let hasData = recv.hasReceivedData
            self = .recv(recv)
            return hasData ? .deliverData : .ignore(.noDataAvailable)
        case .dataRecvd:
            self = .dataRead
            return .deliverEndOfStream
        case .dataRead:
            self = .dataRead
            return .ignore(.alreadyDelivered)
        case .resetRecvd(let resetRecvd):
            let errorCode = resetRecvd.applicationErrorCode
            self = .resetRead(.init(applicationErrorCode: errorCode))
            return .deliverResetError(applicationErrorCode: errorCode)
        case .resetRead(let resetRead):
            self = .resetRead(resetRead)
            return .ignore(.alreadyDelivered)
        }
    }

    mutating func sendStopSending() -> QUICStreamReceiveStateMachine.SendStopSendingAction {
        switch consume self {
        case .recv(let recv):
            self = .recv(recv)
            return .sendStopSending
        case .dataRecvd(let dataRecvd):
            self = .dataRecvd(dataRecvd)
            return .ignore(.alreadyReceivedAllData)
        case .dataRead:
            self = .dataRead
            return .ignore(.alreadyReceivedAllData)
        case .resetRecvd(let resetRecvd):
            self = .resetRecvd(resetRecvd)
            return .ignore(.alreadyReset)
        case .resetRead(let resetRead):
            self = .resetRead(resetRead)
            return .ignore(.alreadyReset)
        }
    }
}

/// Tracks the channel pipeline initializer's lifecycle.
///
/// As opposed to the other state machines this one specifically
/// relates to the *channel pipeline* state.
struct QUICStreamPipelineStateMachine: ~Copyable {
    enum State: ~Copyable {
        /// No initializer has been started yet.
        case uninitialized
        /// The initializer's Future is in flight; the pipeline is not
        /// yet ready to dispatch inbound events.
        case initializing
        /// The initializer has completed; the pipeline is wired up and
        /// inbound events can be dispatched.
        case initialized
    }

    private var state: State

    init() {
        self.state = .uninitialized
    }

    // MARK: - Queries

    /// Returns `true` once the pipeline is ready to dispatch inbound events.
    /// `.initializing` does not count.
    var isInitialized: Bool {
        switch self.state {
        case .initialized:
            return true
        case .uninitialized:
            return false
        case .initializing:
            return false
        }
    }

    // MARK: - Transitions

    enum StartInitializerAction: ~Copyable {
        /// First caller; transition `.uninitialized → .initializing` and
        /// run the application's initializer. Caller must follow up with
        /// `markPipelineInitializerComplete()` once the Future succeeds.
        case runInitializer
        /// Don't run an initializer.
        case ignore(IgnoreReason)

        enum IgnoreReason: ~Copyable {
            /// The stream's channel is not active; nothing to dispatch into.
            case channelInactive
            /// The initializer's `Future` is already in flight.
            case initializerInProgress
            /// The initializer already completed.
            case initializerComplete
        }
    }

    /// Synchronous gate: call before kicking off the initializer.
    mutating func startInitializer(channelActive: Bool) -> StartInitializerAction {
        self.state.startInitializer(channelActive: channelActive)
    }

    enum MarkInitializerCompleteAction: ~Copyable {
        /// Newly transitioned `.initializing → .initialized`; surface the
        /// now-ready stream upward (yield to multiplexer or fire channel
        /// read) and trigger the initial `read`.
        case surfaceInitializedStream
        /// Idempotent re-call; the pipeline was already initialized.
        case ignoreAlreadyComplete
    }

    /// Call once the initializer's Future has succeeded.
    /// Traps if called before `startInitializer()` has run — that's a
    /// programmer error (completing before starting).
    mutating func markInitializerComplete() -> MarkInitializerCompleteAction {
        self.state.markInitializerComplete()
    }
}

// MARK: - Pipeline State Transitions

extension QUICStreamPipelineStateMachine.State {
    mutating func startInitializer(channelActive: Bool) -> QUICStreamPipelineStateMachine.StartInitializerAction {
        guard channelActive else {
            return .ignore(.channelInactive)
        }

        switch consume self {
        case .uninitialized:
            self = .initializing
            return .runInitializer
        case .initializing:
            self = .initializing
            return .ignore(.initializerInProgress)
        case .initialized:
            self = .initialized
            return .ignore(.initializerComplete)
        }
    }

    mutating func markInitializerComplete() -> QUICStreamPipelineStateMachine.MarkInitializerCompleteAction {
        switch consume self {
        case .uninitialized:
            preconditionFailure("markInitializerComplete called before startInitializer")
        case .initializing:
            self = .initialized
            return .surfaceInitializedStream
        case .initialized:
            self = .initialized
            return .ignoreAlreadyComplete
        }
    }
}

/// The handle a QUIC stream uses to talk to the SwiftNetwork transport stack.
@available(anyAppleOS 26, *)
struct SwiftNetworkStreamHandle: ~Copyable {
    /// Pure state machine: tracks attachment.
    ///
    /// As opposed to the other state machines this one specifically
    /// relates to the *SwiftNetwork reference/handle* state.
    struct StateMachine: ~Copyable {
        enum State: ~Copyable {
            case attached
            case detached
        }

        private var state: State

        init() {
            self.state = .attached
        }

        // MARK: - Queries

        var isAttached: Bool {
            switch self.state {
            case .attached: return true
            case .detached: return false
            }
        }

        // MARK: - Per-operation transitions

        enum ViolationReason: CustomStringConvertible {
            /// The handle has been detached; the linkage is gone.
            case detached

            var description: String {
                switch self {
                case .detached: return "linkage detached"
                }
            }
        }

        enum InvokeConnectAction: ~Copyable {
            case performConnect
            case handleViolation(ViolationReason)
        }
        func invokeConnect() -> InvokeConnectAction {
            switch self.state {
            case .attached: return .performConnect
            case .detached: return .handleViolation(.detached)
            }
        }

        enum InvokeDisconnectAction: ~Copyable {
            case performDisconnect
            case ignore
        }
        func invokeDisconnect() -> InvokeDisconnectAction {
            switch self.state {
            case .attached: return .performDisconnect
            case .detached: return .ignore
            }
        }

        enum InvokeAbortInboundAction: ~Copyable {
            case performAbortInbound
            case ignore
        }
        func invokeAbortInbound() -> InvokeAbortInboundAction {
            switch self.state {
            case .attached: return .performAbortInbound
            case .detached: return .ignore
            }
        }

        enum InvokeAbortOutboundAction: ~Copyable {
            case performAbortOutbound
            case ignore
        }
        func invokeAbortOutbound() -> InvokeAbortOutboundAction {
            switch self.state {
            case .attached: return .performAbortOutbound
            case .detached: return .ignore
            }
        }

        enum InvokeSendStreamDataAction: ~Copyable {
            case performSendStreamData
            case handleViolation(ViolationReason)
        }
        func invokeSendStreamData() -> InvokeSendStreamDataAction {
            switch self.state {
            case .attached: return .performSendStreamData
            case .detached: return .handleViolation(.detached)
            }
        }

        enum InvokeReceiveStreamDataAction: ~Copyable {
            case performReceiveStreamData
            case handleViolation(ViolationReason)
        }
        func invokeReceiveStreamData() -> InvokeReceiveStreamDataAction {
            switch self.state {
            case .attached: return .performReceiveStreamData
            case .detached: return .handleViolation(.detached)
            }
        }

        enum InvokeGetMetadataAction: ~Copyable {
            case performGetMetadata
            case ignore
        }
        func invokeGetMetadata() -> InvokeGetMetadataAction {
            switch self.state {
            case .attached: return .performGetMetadata
            case .detached: return .ignore
            }
        }

        // MARK: - Detach (mutating; transitions `.attached` → `.detached`)

        enum InvokeDetachAction: ~Copyable {
            case performDetach
            case skipAlreadyDetached
        }
        mutating func invokeDetach() -> InvokeDetachAction {
            self.state.invokeDetach()
        }
    }

    private var stateMachine: StateMachine
    private var linkage: OutboundStreamLinkage

    /// Initialise with a stub linkage (the outbound starting state, before
    /// `invokeConnect` wires up a real reference).
    init() {
        self.stateMachine = StateMachine()
        self.linkage = OutboundStreamLinkage(reference: .init())
    }

    /// Initialise with a specific linkage. Used by the inbound init path
    /// once `invokeAttachUpperStreamProtocolToNewFlow` has succeeded.
    init(linkage: OutboundStreamLinkage) {
        self.stateMachine = StateMachine()
        self.linkage = linkage
    }

    /// Returns `true` while the linkage is still attached.
    var isAttached: Bool {
        self.stateMachine.isAttached
    }

    // MARK: - Wrapper methods
    //
    // Every wrapper here returns an action that, on the success path, carries
    // a *value snapshot* of `self.linkage` (`OutboundStreamLinkage` is a
    // `struct` wrapping a struct `ProtocolInstanceReference`). Callers extract
    // the linkage from the action, which releases the borrow on
    // `self.swiftNetworkStreamHandle`, and then perform the SwiftNetwork side
    // effect on the local value. This is load-bearing: SwiftNetwork's
    // `handleCallFromUpperProtocol` drains queued peer events in a `defer`
    // after `body()`, so a peer disconnect can synchronously re-enter
    // `handleDisconnectedEvent` from inside an outer `invoke*` call. If the
    // outer call held a `read` access on `self.swiftNetworkStreamHandle` for
    // the duration of its linkage call, the inner `invokeDetach` (a `modify`)
    // would overlap and trap on Swift exclusivity. Releasing the borrow before
    // the side effect makes the re-entry safe at the language level.

    enum InvokeConnectAction: ~Copyable {
        case proceed(OutboundStreamLinkage)
        case handleViolation(StateMachine.ViolationReason)
    }

    func invokeConnect() -> InvokeConnectAction {
        switch self.stateMachine.invokeConnect() {
        case .performConnect:
            return .proceed(self.linkage)
        case .handleViolation(let reason):
            return .handleViolation(reason)
        }
    }

    enum InvokeDisconnectAction: ~Copyable {
        case proceed(OutboundStreamLinkage)
        case ignore
    }

    func invokeDisconnect() -> InvokeDisconnectAction {
        switch self.stateMachine.invokeDisconnect() {
        case .performDisconnect:
            return .proceed(self.linkage)
        case .ignore:
            return .ignore
        }
    }

    enum InvokeAbortInboundAction: ~Copyable {
        case proceed(OutboundStreamLinkage)
        case ignore
    }

    func invokeAbortInbound() -> InvokeAbortInboundAction {
        switch self.stateMachine.invokeAbortInbound() {
        case .performAbortInbound:
            return .proceed(self.linkage)
        case .ignore:
            return .ignore
        }
    }

    enum InvokeAbortOutboundAction: ~Copyable {
        case proceed(OutboundStreamLinkage)
        case ignore
    }

    func invokeAbortOutbound() -> InvokeAbortOutboundAction {
        switch self.stateMachine.invokeAbortOutbound() {
        case .performAbortOutbound:
            return .proceed(self.linkage)
        case .ignore:
            return .ignore
        }
    }

    enum InvokeSendStreamDataAction: ~Copyable {
        case proceed(OutboundStreamLinkage)
        case handleViolation(StateMachine.ViolationReason)
    }

    func invokeSendStreamData() -> InvokeSendStreamDataAction {
        switch self.stateMachine.invokeSendStreamData() {
        case .performSendStreamData:
            return .proceed(self.linkage)
        case .handleViolation(let reason):
            return .handleViolation(reason)
        }
    }

    enum InvokeReceiveStreamDataAction: ~Copyable {
        case proceed(OutboundStreamLinkage)
        case handleViolation(StateMachine.ViolationReason)
    }

    func invokeReceiveStreamData() -> InvokeReceiveStreamDataAction {
        switch self.stateMachine.invokeReceiveStreamData() {
        case .performReceiveStreamData:
            return .proceed(self.linkage)
        case .handleViolation(let reason):
            return .handleViolation(reason)
        }
    }

    enum InvokeGetMetadataAction: ~Copyable {
        case proceed(OutboundStreamLinkage)
        case ignore
    }

    func invokeGetMetadata() -> InvokeGetMetadataAction {
        switch self.stateMachine.invokeGetMetadata() {
        case .performGetMetadata:
            return .proceed(self.linkage)
        case .ignore:
            return .ignore
        }
    }

    enum InvokeDetachAction: ~Copyable {
        case proceed(OutboundStreamLinkage)
        case skipAlreadyDetached
    }

    /// Mutating: transitions the SM to `.detached` before returning. The
    /// modify borrow ends with the return; the caller performs `invokeDetach`
    /// on the returned linkage value without holding any borrow on the handle.
    mutating func invokeDetach() -> InvokeDetachAction {
        switch self.stateMachine.invokeDetach() {
        case .performDetach:
            return .proceed(self.linkage)
        case .skipAlreadyDetached:
            return .skipAlreadyDetached
        }
    }

    /// Builds the error raised by soft-violation operations
    /// (`invokeSendStreamData`, `invokeReceiveStreamData`). The reason is
    /// included in the description so callers and logs see exactly what
    /// went wrong.
    static func violationError(
        operation: String,
        reason: StateMachine.ViolationReason
    ) -> NetworkError {
        NetworkError(
            category: NetworkError.CommonCategory(
                identifier: "swift-nio-quic.streamHandleViolation",
                description: "\(operation): \(reason)"
            )
        )
    }
}

// MARK: - SwiftNetwork Stream Handle State Transitions

@available(anyAppleOS 26, *)
extension SwiftNetworkStreamHandle.StateMachine.State {
    mutating func invokeDetach() -> SwiftNetworkStreamHandle.StateMachine.InvokeDetachAction {
        switch consume self {
        case .attached:
            self = .detached
            return .performDetach
        case .detached:
            self = .detached
            return .skipAlreadyDetached
        }
    }
}
