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
import Testing

@testable import NIOQUIC

// MARK: - Test Helpers for ~Copyable Action Enums

@available(anyAppleOS 26, *)
extension QUICStreamStateMachine.StreamConnectedAction {
    consuming func requireActivateStream(sourceLocation: SourceLocation = #_sourceLocation) {
        guard case .activateStream = self else {
            Issue.record("Expected .activateStream", sourceLocation: sourceLocation)
            return
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamStateMachine.WriteDataAction {
    consuming func requireSendData(sourceLocation: SourceLocation = #_sourceLocation) {
        guard case .sendData = self else {
            Issue.record("Expected .sendData", sourceLocation: sourceLocation)
            return
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamStateMachine.ReceiveDataAction {
    consuming func requireBufferData(sourceLocation: SourceLocation = #_sourceLocation) {
        guard case .bufferData = self else {
            Issue.record("Expected .bufferData", sourceLocation: sourceLocation)
            return
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamStateMachine.SendFinAction {
    @discardableResult
    consuming func requireSendFin(sourceLocation: SourceLocation = #_sourceLocation) -> Bool {
        guard case .sendFin(let streamFullyClosed) = self else {
            Issue.record("Expected .sendFin", sourceLocation: sourceLocation)
            return false
        }
        return streamFullyClosed
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamStateMachine.AcknowledgeDataAction {
    consuming func requireCompleteSend(sourceLocation: SourceLocation = #_sourceLocation) {
        guard case .completeSend = self else {
            Issue.record("Expected .completeSend", sourceLocation: sourceLocation)
            return
        }
    }

    consuming func requireCompleteSendAfterReset(sourceLocation: SourceLocation = #_sourceLocation) {
        guard case .completeSendAfterReset = self else {
            Issue.record("Expected .completeSendAfterReset", sourceLocation: sourceLocation)
            return
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamStateMachine.ApplicationReadAction {
    consuming func requireDeliverEndOfStream(sourceLocation: SourceLocation = #_sourceLocation) {
        guard case .deliverEndOfStream = self else {
            Issue.record("Expected .deliverEndOfStream", sourceLocation: sourceLocation)
            return
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamStateMachine.CloseAction {
    consuming func requireClose(sourceLocation: SourceLocation = #_sourceLocation) {
        guard case .close = self else {
            Issue.record("Expected .close", sourceLocation: sourceLocation)
            return
        }
    }
}

struct QUICStreamSendStateMachineTests {
    @available(anyAppleOS 26, *)
    @Test("Initial state is ready")
    func initialStateIsReady() {
        let sm = QUICStreamSendStateMachine()
        let canWrite = sm.canWrite
        let isTerminal = sm.isTerminal
        let isFinished = sm.isFinished
        let wasReset = sm.wasReset
        let resetErrorCode = sm.resetErrorCode
        #expect(canWrite)
        #expect(!isTerminal)
        #expect(!isFinished)
        #expect(!wasReset)
        #expect(resetErrorCode == nil)
    }

    /// RFC 9000 §3.5: STOP_SENDING after FIN sent but before ack should still trigger RESET_STREAM.
    @available(anyAppleOS 26, *)
    @Test("Receive STOP_SENDING from dataSent triggers RESET_STREAM")
    func receiveStopSendingFromDataSent() throws {
        var sm = QUICStreamSendStateMachine()
        _ = sm.sendFin()
        let action = sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(99))
        guard case .sendReset(let code) = action else {
            Issue.record("Expected .sendReset")
            return
        }
        #expect(code == QUICApplicationErrorCode(99))
    }

    /// STOP_SENDING after all data acknowledged should be ignored.
    @available(anyAppleOS 26, *)
    @Test("Receive STOP_SENDING from dataRecvd is ignored")
    func receiveStopSendingFromDataRecvd() {
        var sm = QUICStreamSendStateMachine()
        _ = sm.sendFin()
        _ = sm.acknowledgeAllData()
        let action = sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(1))
        guard case .ignore(.alreadyFinished) = action else {
            Issue.record("Expected .ignore(.alreadyFinished)")
            return
        }
    }

    /// Duplicate STOP_SENDING should preserve the first error code.
    @available(anyAppleOS 26, *)
    @Test("Duplicate STOP_SENDING preserves first error code")
    func receiveStopSendingFromResetSentPreservesFirstErrorCode() {
        var sm = QUICStreamSendStateMachine()
        _ = sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(1))
        let action = sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(2))
        guard case .ignore(.alreadyReset) = action else {
            Issue.record("Expected .ignore(.alreadyReset)")
            return
        }
        let resetErrorCode = sm.resetErrorCode
        #expect(resetErrorCode == QUICApplicationErrorCode(1))
    }

    @available(anyAppleOS 26, *)
    @Test("Normal lifecycle: write, FIN, ack")
    func normalLifecycle() {
        var sm = QUICStreamSendStateMachine()
        let writeAction = sm.writeData()
        guard case .sendData = writeAction else {
            Issue.record("Expected .sendData")
            return
        }
        let finAction = sm.sendFin()
        guard case .sendFin = finAction else {
            Issue.record("Expected .sendFin")
            return
        }
        let ackAction = sm.acknowledgeAllData()
        guard case .completeSend = ackAction else {
            Issue.record("Expected .completeSend")
            return
        }
        let isTerminal = sm.isTerminal
        let isFinished = sm.isFinished
        let wasReset = sm.wasReset
        #expect(isTerminal)
        #expect(isFinished)
        #expect(!wasReset)
    }

    @available(anyAppleOS 26, *)
    @Test("Reset lifecycle via localReset")
    func resetLifecycleViaLocalReset() {
        var sm = QUICStreamSendStateMachine()
        let resetAction = sm.localReset(applicationErrorCode: QUICApplicationErrorCode(77))
        guard case .sendReset(let code) = resetAction else {
            Issue.record("Expected .sendReset")
            return
        }
        #expect(code == QUICApplicationErrorCode(77))
        let ackAction = sm.acknowledgeAllData()
        guard case .completeSendAfterReset = ackAction else {
            Issue.record("Expected .completeSendAfterReset")
            return
        }
        let isTerminal = sm.isTerminal
        let wasReset = sm.wasReset
        let resetErrorCode = sm.resetErrorCode
        #expect(isTerminal)
        #expect(wasReset)
        #expect(resetErrorCode == QUICApplicationErrorCode(77))
    }

    @available(anyAppleOS 26, *)
    @Test("Reset lifecycle via STOP_SENDING")
    func resetLifecycleViaStopSending() {
        var sm = QUICStreamSendStateMachine()
        _ = sm.writeData()
        let stopAction = sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(42))
        guard case .sendReset(let code) = stopAction else {
            Issue.record("Expected .sendReset")
            return
        }
        #expect(code == QUICApplicationErrorCode(42))
        let ackAction = sm.acknowledgeAllData()
        guard case .completeSendAfterReset = ackAction else {
            Issue.record("Expected .completeSendAfterReset")
            return
        }
        let isTerminal = sm.isTerminal
        let wasReset = sm.wasReset
        let resetErrorCode = sm.resetErrorCode
        #expect(isTerminal)
        #expect(wasReset)
        #expect(resetErrorCode == QUICApplicationErrorCode(42))
    }

    // MARK: - RFC 9000 §3.5: STOP_SENDING error code propagation

    /// RFC 9000 §3.5: "An endpoint SHOULD copy the error code from the STOP_SENDING frame
    /// to the RESET_STREAM frame it sends". Verify this from the ready state.
    @available(anyAppleOS 26, *)
    @Test("STOP_SENDING from ready copies error code to RESET_STREAM")
    func stopSendingFromReadyCopiesErrorCode() {
        var sm = QUICStreamSendStateMachine()
        let action = sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(12345))
        guard case .sendReset(let code) = action else {
            Issue.record("Expected .sendReset")
            return
        }
        #expect(code == QUICApplicationErrorCode(12345))
        let resetErrorCode = sm.resetErrorCode
        #expect(resetErrorCode == QUICApplicationErrorCode(12345))
    }

    /// RFC 9000 §3.5: Verify error code copy from the send state (data in flight).
    @available(anyAppleOS 26, *)
    @Test("STOP_SENDING from send copies error code to RESET_STREAM")
    func stopSendingFromSendCopiesErrorCode() {
        var sm = QUICStreamSendStateMachine()
        _ = sm.writeData()
        let action = sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(54321))
        guard case .sendReset(let code) = action else {
            Issue.record("Expected .sendReset")
            return
        }
        #expect(code == QUICApplicationErrorCode(54321))
        let resetErrorCode = sm.resetErrorCode
        #expect(resetErrorCode == QUICApplicationErrorCode(54321))
    }

    /// RFC 9000 §3.5: Verify error code copy from the dataSent state (FIN sent, awaiting ack).
    @available(anyAppleOS 26, *)
    @Test("STOP_SENDING from dataSent copies error code to RESET_STREAM")
    func stopSendingFromDataSentCopiesErrorCode() {
        var sm = QUICStreamSendStateMachine()
        _ = sm.writeData()
        _ = sm.sendFin()
        let action = sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(99999))
        guard case .sendReset(let code) = action else {
            Issue.record("Expected .sendReset")
            return
        }
        #expect(code == QUICApplicationErrorCode(99999))
        let resetErrorCode = sm.resetErrorCode
        #expect(resetErrorCode == QUICApplicationErrorCode(99999))
    }

    // MARK: - RFC 9000 §3.1: RESET_STREAM as first frame

    /// RFC 9000 §3.1: "An endpoint MAY send a RESET_STREAM as the first frame that
    /// mentions a stream". This opens and immediately resets the stream.
    @available(anyAppleOS 26, *)
    @Test("localReset from ready opens and immediately resets")
    func localResetFromReadyOpensAndResets() {
        var sm = QUICStreamSendStateMachine()
        let canWriteBefore = sm.canWrite
        #expect(canWriteBefore)

        let action = sm.localReset(applicationErrorCode: QUICApplicationErrorCode(0))
        guard case .sendReset(let code) = action else {
            Issue.record("Expected .sendReset")
            return
        }
        #expect(code == QUICApplicationErrorCode(0))

        let canWrite = sm.canWrite
        let wasReset = sm.wasReset
        let isFinished = sm.isFinished
        #expect(!canWrite)
        #expect(wasReset)
        #expect(isFinished)
    }

    // MARK: - RFC 9000 §3.3: MUST NOT send from terminal states

    /// RFC 9000 §3.3: "A sender MUST NOT send [STREAM, STREAM_DATA_BLOCKED, RESET_STREAM]
    /// from a terminal state". Verify writes are rejected after completion.
    @available(anyAppleOS 26, *)
    @Test("Write rejected after stream completed")
    func writeRejectedAfterCompletion() {
        var sm = QUICStreamSendStateMachine()
        _ = sm.sendFin()
        _ = sm.acknowledgeAllData()

        let action = sm.writeData()
        guard case .doNotWrite(.streamFinished) = action else {
            Issue.record("Expected .doNotWrite(.streamFinished)")
            return
        }
    }

    /// RFC 9000 §3.3: Verify writes are rejected after stream reset.
    @available(anyAppleOS 26, *)
    @Test("Write rejected after stream reset")
    func writeRejectedAfterReset() {
        var sm = QUICStreamSendStateMachine()
        _ = sm.localReset(applicationErrorCode: QUICApplicationErrorCode(1))

        let action = sm.writeData()
        guard case .doNotWrite(.streamReset) = action else {
            Issue.record("Expected .doNotWrite(.streamReset)")
            return
        }
    }

    /// RFC 9000 §3.3: localReset from a terminal state should be ignored.
    @available(anyAppleOS 26, *)
    @Test("localReset ignored after stream completed")
    func localResetIgnoredAfterCompletion() {
        var sm = QUICStreamSendStateMachine()
        _ = sm.sendFin()
        _ = sm.acknowledgeAllData()

        let action = sm.localReset(applicationErrorCode: QUICApplicationErrorCode(1))
        guard case .ignore(.alreadyFinished) = action else {
            Issue.record("Expected .ignore(.alreadyFinished)")
            return
        }
    }
}

struct QUICStreamReceiveStateMachineTests {
    @available(anyAppleOS 26, *)
    @Test("Initial state is recv")
    func initialStateIsRecv() {
        let sm = QUICStreamReceiveStateMachine()
        let canRead = sm.canRead
        let isTerminal = sm.isTerminal
        let hasReceivedFin = sm.hasReceivedFin
        let wasReset = sm.hasReceivedReset
        let resetErrorCode = sm.resetErrorCode
        let canProduceData = sm.canProduceData
        #expect(canRead)
        #expect(!isTerminal)
        #expect(!hasReceivedFin)
        #expect(!wasReset)
        #expect(resetErrorCode == nil)
        #expect(!canProduceData)
    }

    /// RESET_STREAM after all data received (FIN already processed) should be ignored.
    @available(anyAppleOS 26, *)
    @Test("Receive RESET_STREAM from dataRecvd is ignored")
    func receiveResetFromDataRecvd() {
        var sm = QUICStreamReceiveStateMachine()
        _ = sm.receiveFin(finalSize: 100)
        let action = sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(1), finalSize: 100)
        guard case .ignore(.alreadyFullyReceived) = action else {
            Issue.record("Expected .ignore(.alreadyFullyReceived)")
            return
        }
        let wasReset = sm.hasReceivedReset
        #expect(!wasReset)
    }

    /// Duplicate RESET_STREAM should preserve the first error code.
    @available(anyAppleOS 26, *)
    @Test("Duplicate RESET_STREAM preserves first error code")
    func receiveResetFromResetRecvdPreservesFirst() {
        var sm = QUICStreamReceiveStateMachine()
        _ = sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(1), finalSize: 0)
        let action = sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(2), finalSize: 0)
        guard case .ignore(.alreadyReset) = action else {
            Issue.record("Expected .ignore(.alreadyReset)")
            return
        }
        let resetErrorCode = sm.resetErrorCode
        #expect(resetErrorCode == QUICApplicationErrorCode(1))
    }

    /// FIN arriving after RESET_STREAM should be ignored.
    @available(anyAppleOS 26, *)
    @Test("Receive FIN from resetRecvd is ignored")
    func receiveFinFromResetRecvd() {
        var sm = QUICStreamReceiveStateMachine()
        _ = sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(1), finalSize: 0)
        let action = sm.receiveFin(finalSize: 100)
        guard case .ignore(.streamReset) = action else {
            Issue.record("Expected .ignore(.streamReset)")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Normal lifecycle: receive data, FIN, app read")
    func normalLifecycle() {
        var sm = QUICStreamReceiveStateMachine()
        let recvAction = sm.receiveData()
        guard case .bufferData = recvAction else {
            Issue.record("Expected .bufferData")
            return
        }
        let finAction = sm.receiveFin(finalSize: 1024)
        guard case .markAllDataReceived(let finalSize) = finAction else {
            Issue.record("Expected .markAllDataReceived")
            return
        }
        #expect(finalSize == 1024)
        let readAction = sm.applicationRead()
        guard case .deliverEndOfStream = readAction else {
            Issue.record("Expected .deliverEndOfStream")
            return
        }
        let isTerminal = sm.isTerminal
        let wasReset = sm.hasReceivedReset
        #expect(isTerminal)
        #expect(!wasReset)
    }

    @available(anyAppleOS 26, *)
    @Test("Reset lifecycle: receive data, RESET_STREAM, app read")
    func resetLifecycle() {
        var sm = QUICStreamReceiveStateMachine()
        _ = sm.receiveData()
        let resetAction = sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(77), finalSize: 50)
        guard case .notifyApplication(let code) = resetAction else {
            Issue.record("Expected .notifyApplication")
            return
        }
        #expect(code == QUICApplicationErrorCode(77))
        let readAction = sm.applicationRead()
        guard case .deliverResetError(let deliveredCode) = readAction else {
            Issue.record("Expected .deliverResetError")
            return
        }
        #expect(deliveredCode == QUICApplicationErrorCode(77))
        let isTerminal = sm.isTerminal
        let wasReset = sm.hasReceivedReset
        let resetErrorCode = sm.resetErrorCode
        #expect(isTerminal)
        #expect(wasReset)
        #expect(resetErrorCode == QUICApplicationErrorCode(77))
    }

    // MARK: - RFC 9000 §3.2: RESET_STREAM as first frame on receive side

    /// RFC 9000 §3.2: RESET_STREAM can arrive as the first frame on a stream,
    /// before any STREAM data. The stream transitions directly to resetRecvd.
    @available(anyAppleOS 26, *)
    @Test("RESET_STREAM as first frame with no prior data")
    func resetStreamAsFirstFrame() {
        var sm = QUICStreamReceiveStateMachine()
        let canProduceDataBefore = sm.canProduceData
        #expect(!canProduceDataBefore)

        let action = sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(42), finalSize: 0)
        guard case .notifyApplication(let code) = action else {
            Issue.record("Expected .notifyApplication")
            return
        }
        #expect(code == QUICApplicationErrorCode(42))

        let canRead = sm.canRead
        let wasReset = sm.hasReceivedReset
        let canProduceData = sm.canProduceData
        #expect(!canRead)
        #expect(wasReset)
        #expect(canProduceData)  // Reset error needs to be delivered
    }

    // MARK: - RFC 9000 §3.2: Data after FIN/RESET

    /// Receiving data after FIN (dataRecvd state) is rejected.
    @available(anyAppleOS 26, *)
    @Test("Data after FIN is rejected")
    func dataAfterFinRejected() {
        var sm = QUICStreamReceiveStateMachine()
        _ = sm.receiveFin(finalSize: 100)

        let action = sm.receiveData()
        guard case .doNotBuffer(.allDataReceived) = action else {
            Issue.record("Expected .doNotBuffer(.allDataReceived)")
            return
        }
    }

    /// Receiving data after RESET_STREAM is rejected.
    @available(anyAppleOS 26, *)
    @Test("Data after RESET_STREAM is rejected")
    func dataAfterResetRejected() {
        var sm = QUICStreamReceiveStateMachine()
        _ = sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(1), finalSize: 0)

        let action = sm.receiveData()
        guard case .doNotBuffer(.streamReset) = action else {
            Issue.record("Expected .doNotBuffer(.streamReset)")
            return
        }
    }

    // MARK: - RFC 9000 §3.5: STOP_SENDING from receive states

    /// STOP_SENDING can be sent while in recv state.
    @available(anyAppleOS 26, *)
    @Test("sendStopSending from recv is permitted")
    func sendStopSendingFromRecv() {
        var sm = QUICStreamReceiveStateMachine()
        let action = sm.sendStopSending()
        guard case .sendStopSending = action else {
            Issue.record("Expected .sendStopSending")
            return
        }
    }

    /// STOP_SENDING after all data received is pointless and ignored.
    @available(anyAppleOS 26, *)
    @Test("sendStopSending from dataRecvd is ignored")
    func sendStopSendingFromDataRecvd() {
        var sm = QUICStreamReceiveStateMachine()
        _ = sm.receiveFin(finalSize: 100)

        let action = sm.sendStopSending()
        guard case .ignore(.alreadyReceivedAllData) = action else {
            Issue.record("Expected .ignore(.alreadyReceivedAllData)")
            return
        }
    }

    /// STOP_SENDING after RESET_STREAM is unnecessary and ignored.
    @available(anyAppleOS 26, *)
    @Test("sendStopSending from resetRecvd is ignored")
    func sendStopSendingFromResetRecvd() {
        var sm = QUICStreamReceiveStateMachine()
        _ = sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(1), finalSize: 0)

        let action = sm.sendStopSending()
        guard case .ignore(.alreadyReset) = action else {
            Issue.record("Expected .ignore(.alreadyReset)")
            return
        }
    }

    /// Duplicate FIN is ignored.
    @available(anyAppleOS 26, *)
    @Test("Duplicate FIN is ignored")
    func duplicateFinIgnored() {
        var sm = QUICStreamReceiveStateMachine()
        _ = sm.receiveFin(finalSize: 100)

        let action = sm.receiveFin(finalSize: 100)
        guard case .ignore(.alreadyReceivedFin) = action else {
            Issue.record("Expected .ignore(.alreadyReceivedFin)")
            return
        }
    }

    /// applicationRead from dataRead (terminal) returns ignore(.alreadyDelivered).
    @available(anyAppleOS 26, *)
    @Test("applicationRead from terminal dataRead returns ignore(.alreadyDelivered)")
    func applicationReadFromDataRead() {
        var sm = QUICStreamReceiveStateMachine()
        _ = sm.receiveFin(finalSize: 100)
        _ = sm.applicationRead()  // → dataRead

        let action = sm.applicationRead()
        guard case .ignore(.alreadyDelivered) = action else {
            Issue.record("Expected .ignore(.alreadyDelivered)")
            return
        }
    }

    /// applicationRead from terminal resetRead returns ignore(.alreadyDelivered).
    @available(anyAppleOS 26, *)
    @Test("applicationRead from terminal resetRead returns ignore(.alreadyDelivered)")
    func applicationReadFromResetRead() {
        var sm = QUICStreamReceiveStateMachine()
        _ = sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(1), finalSize: 0)
        _ = sm.applicationRead()  // → resetRead

        let action = sm.applicationRead()
        guard case .ignore(.alreadyDelivered) = action else {
            Issue.record("Expected .ignore(.alreadyDelivered)")
            return
        }
    }
}

struct QUICStreamStateMachineTests {

    // MARK: - Initial State

    @available(anyAppleOS 26, *)
    @Test("Pending ID initial state")
    func pendingIDInitialState() {
        let sm = QUICStreamStateMachine()
        let isConnected = sm.isConnected
        let isFullyClosed = sm.isFullyClosed
        let isWriteClosed = sm.isWriteClosed
        let hasReceivedFin = sm.hasReceivedFin
        #expect(!isConnected)
        #expect(!isFullyClosed)
        #expect(!isWriteClosed)
        #expect(!hasReceivedFin)
    }

    // MARK: - Connection Lifecycle

    @available(anyAppleOS 26, *)
    @Test("Bidirectional stream connected from pendingID")
    func streamConnectedBidirectionalFromPendingID() throws {
        var sm = QUICStreamStateMachine()

        sm.streamConnected(direction: .bidirectional).requireActivateStream()

        let isConnected = sm.isConnected
        #expect(isConnected)

        // Both directions should work
        try sm.writeData().requireSendData()
        try sm.receiveData().requireBufferData()
    }

    @available(anyAppleOS 26, *)
    @Test("Send-only stream connected from pendingID")
    func streamConnectedSendOnlyFromPendingID() throws {
        var sm = QUICStreamStateMachine()

        sm.streamConnected(direction: .sendOnly).requireActivateStream()

        try sm.writeData().requireSendData()
    }

    @available(anyAppleOS 26, *)
    @Test("Receive-only stream connected from pendingID")
    func streamConnectedReceiveOnlyFromPendingID() throws {
        var sm = QUICStreamStateMachine()

        sm.streamConnected(direction: .receiveOnly).requireActivateStream()

        try sm.receiveData().requireBufferData()
    }

    @available(anyAppleOS 26, *)
    @Test("Stream connected from connected is ignored")
    func streamConnectedFromConnected() {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        let action = sm.streamConnected(direction: .bidirectional)
        guard case .ignoreAlreadyConnected = action else {
            Issue.record("Expected .ignoreAlreadyConnected")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Stream connected from closed is ignored")
    func streamConnectedFromClosed() {
        var sm = QUICStreamStateMachine()
        _ = sm.close(reason: .clean)

        let action = sm.streamConnected(direction: .bidirectional)
        guard case .ignoreAlreadyClosed = action else {
            Issue.record("Expected .ignoreAlreadyClosed")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("receiveData and receiveFin work in pendingID for optimistic reads")
    func optimisticReadsInPendingID() throws {
        // receiveData returns .bufferData in pendingID because SwiftNetwork
        // can deliver data before the connected event (optimistic reads).
        var sm1 = QUICStreamStateMachine()
        try sm1.receiveData().requireBufferData()

        // receiveFin returns .markAllDataReceived in pendingID because SwiftNetwork
        // can deliver FIN before the connected event (optimistic reads). The early
        // FIN is replayed into the receive sub-SM on streamConnected.
        var sm2 = QUICStreamStateMachine()
        guard case .markAllDataReceived = try sm2.receiveFin(finalSize: 0) else {
            Issue.record("Expected .markAllDataReceived from receiveFin in pendingID")
            return
        }
    }

    // MARK: - Bidirectional Stream Lifecycles

    @available(anyAppleOS 26, *)
    @Test("Typical bidirectional stream lifecycle")
    func typicalBidirectionalStreamLifecycle() throws {
        var sm = QUICStreamStateMachine()

        _ = sm.streamConnected(direction: .bidirectional)
        let isConnected = sm.isConnected
        #expect(isConnected)

        _ = try sm.writeData()
        _ = try sm.receiveData()

        _ = try sm.sendFin()
        var isWriteClosed = sm.isWriteClosed
        #expect(!isWriteClosed)

        _ = try sm.receiveFin(finalSize: 100)
        let hasReceivedFin = sm.hasReceivedFin
        #expect(hasReceivedFin)

        _ = try sm.acknowledgeAllData()
        isWriteClosed = sm.isWriteClosed
        #expect(isWriteClosed)

        _ = try sm.applicationRead()

        let isFullyClosed = sm.isFullyClosed
        #expect(isFullyClosed)
    }

    @available(anyAppleOS 26, *)
    @Test("Bidirectional fully closed requires both sides terminal")
    func combinedFullyClosedBidirectional() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        var isFullyClosed = sm.isFullyClosed
        #expect(!isFullyClosed)

        _ = try sm.sendFin()
        _ = try sm.acknowledgeAllData()

        isFullyClosed = sm.isFullyClosed
        #expect(!isFullyClosed)

        _ = try sm.receiveFin(finalSize: 0)
        _ = try sm.applicationRead()

        isFullyClosed = sm.isFullyClosed
        #expect(isFullyClosed)
    }

    @available(anyAppleOS 26, *)
    @Test("Bidirectional stream reset while sending")
    func bidirectionalStreamResetWhileSending() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        _ = try sm.writeData()

        let stopAction = try sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(42))
        guard case .sendReset(_) = stopAction else {
            Issue.record("Expected .sendReset")
            return
        }

        try sm.acknowledgeAllData().requireCompleteSendAfterReset()

        var isFullyClosed = sm.isFullyClosed
        #expect(!isFullyClosed)

        _ = try sm.receiveFin(finalSize: 0)
        _ = try sm.applicationRead()

        isFullyClosed = sm.isFullyClosed
        #expect(isFullyClosed)
    }

    // MARK: - Unidirectional Stream Lifecycles

    @available(anyAppleOS 26, *)
    @Test("Full outbound send-only lifecycle")
    func fullOutboundSendOnlyLifecycle() throws {
        var sm = QUICStreamStateMachine()

        let connectAction = sm.streamConnected(direction: .sendOnly)
        connectAction.requireActivateStream()

        try sm.writeData().requireSendData()
        try sm.sendFin().requireSendFin()

        var isFullyClosed = sm.isFullyClosed
        #expect(!isFullyClosed)

        try sm.acknowledgeAllData().requireCompleteSend()

        isFullyClosed = sm.isFullyClosed
        #expect(isFullyClosed)
    }

    @available(anyAppleOS 26, *)
    @Test("Full inbound receive-only lifecycle")
    func fullInboundReceiveOnlyLifecycle() throws {
        var sm = QUICStreamStateMachine()

        let connectAction = sm.streamConnected(direction: .receiveOnly)
        connectAction.requireActivateStream()

        try sm.receiveData().requireBufferData()

        guard case .markAllDataReceived = try sm.receiveFin(finalSize: 1024) else {
            Issue.record("Expected .markAllDataReceived")
            return
        }
        let hasReceivedFin = sm.hasReceivedFin
        #expect(hasReceivedFin)

        var isFullyClosed = sm.isFullyClosed
        #expect(!isFullyClosed)

        try sm.applicationRead().requireDeliverEndOfStream()

        isFullyClosed = sm.isFullyClosed
        #expect(isFullyClosed)
    }

    @available(anyAppleOS 26, *)
    @Test("Send-only stream has no read side")
    func sendOnlyDirectionSemantics() {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .sendOnly)

        let hasReceivedFin = sm.hasReceivedFin
        #expect(!hasReceivedFin)
    }

    @available(anyAppleOS 26, *)
    @Test("Receive-only stream has no write side")
    func receiveOnlyDirectionSemantics() {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .receiveOnly)

        let isWriteClosed = sm.isWriteClosed
        #expect(isWriteClosed)
    }

    // MARK: - Direction Errors

    @available(anyAppleOS 26, *)
    @Test("Send operations throw wrongDirection on receive-only stream")
    func sendOnReceiveOnlyThrows() {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .receiveOnly)

        #expect(throws: QUICStreamStateMachine.InvalidTransition.wrongDirection) {
            _ = try sm.writeData()
        }
        #expect(throws: QUICStreamStateMachine.InvalidTransition.wrongDirection) {
            _ = try sm.sendFin()
        }
        #expect(throws: QUICStreamStateMachine.InvalidTransition.wrongDirection) {
            _ = try sm.localReset(applicationErrorCode: QUICApplicationErrorCode(0))
        }
        #expect(throws: QUICStreamStateMachine.InvalidTransition.wrongDirection) {
            _ = try sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(0))
        }
        #expect(throws: QUICStreamStateMachine.InvalidTransition.wrongDirection) {
            _ = try sm.acknowledgeAllData()
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Receive operations throw wrongDirection on send-only stream")
    func receiveOnSendOnlyThrows() {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .sendOnly)

        #expect(throws: QUICStreamStateMachine.InvalidTransition.wrongDirection) {
            _ = try sm.receiveData()
        }
        #expect(throws: QUICStreamStateMachine.InvalidTransition.wrongDirection) {
            _ = try sm.receiveFin(finalSize: 0)
        }
        #expect(throws: QUICStreamStateMachine.InvalidTransition.wrongDirection) {
            _ = try sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(0), finalSize: 0)
        }
        #expect(throws: QUICStreamStateMachine.InvalidTransition.wrongDirection) {
            _ = try sm.applicationRead()
        }
        #expect(throws: QUICStreamStateMachine.InvalidTransition.wrongDirection) {
            _ = try sm.closeReadSide()
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Operations throw notConnected on closed stream")
    func operationsOnClosedStreamThrow() {
        var sm = QUICStreamStateMachine()
        _ = sm.close(reason: .clean)

        #expect(throws: QUICStreamStateMachine.InvalidTransition.notConnected) {
            _ = try sm.writeData()
        }
        #expect(throws: QUICStreamStateMachine.InvalidTransition.notConnected) {
            _ = try sm.receiveData()
        }
    }

    // MARK: - Reset Scenarios

    @available(anyAppleOS 26, *)
    @Test("Stream reset by peer via RESET_STREAM")
    func streamResetByPeer() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        let action = try sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(123), finalSize: 0)
        guard case .doNotCloseStream(let error) = action else {
            Issue.record("Expected .doNotCloseStream")
            return
        }
        guard case .resetStream = error else {
            Issue.record("Expected .resetStream")
            return
        }

        guard case .doNotBuffer(.streamReset) = try sm.receiveData() else {
            Issue.record("Expected .doNotBuffer(.streamReset)")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Stream reset by peer via STOP_SENDING")
    func streamResetByStopSending() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        let action = try sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(456))
        guard case .sendReset(let error) = action else {
            Issue.record("Expected .sendReset")
            return
        }
        guard case .stopSending = error else {
            Issue.record("Expected .stopSending")
            return
        }

        guard case .doNotWrite(.streamReset) = try sm.writeData() else {
            Issue.record("Expected .doNotWrite(.streamReset)")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Receive RESET_STREAM after data received delivers error on read")
    func receiveResetAfterDataReceived() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        _ = try sm.receiveData()
        let action = try sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(77), finalSize: 0)

        guard case .doNotCloseStream(_) = action else {
            Issue.record("Expected .doNotCloseStream")
            return
        }

        guard
            case .deliverResetError(
                applicationErrorCode: QUICApplicationErrorCode(77)
            ) = try sm.applicationRead()
        else {
            Issue.record("Expected .deliverResetError(77)")
            return
        }
    }

    // MARK: - RFC Edge Cases

    @available(anyAppleOS 26, *)
    @Test("STOP_SENDING after FIN before ack triggers RESET_STREAM per RFC 9000 §3.1")
    func stopSendingAfterFinBeforeAck() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        _ = try sm.writeData()
        _ = try sm.sendFin()

        let action = try sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(10))
        guard case .sendReset(_) = action else {
            Issue.record("Expected .sendReset per RFC 9000 §3.1")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("STOP_SENDING after all data acknowledged is ignored")
    func stopSendingAfterAllDataAcknowledged() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        _ = try sm.sendFin()
        _ = try sm.acknowledgeAllData()

        let action = try sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(10))
        guard case .ignore(.alreadyFinished) = action else {
            Issue.record("Expected .ignore(.alreadyFinished)")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("RESET_STREAM after all data received is ignored")
    func resetStreamAfterAllDataReceived() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        _ = try sm.receiveFin(finalSize: 1024)

        let action = try sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(55), finalSize: 1024)
        guard case .ignore(.alreadyFullyReceived) = action else {
            Issue.record("Expected .ignore(.alreadyFullyReceived)")
            return
        }

        guard case .doNotBuffer(.allDataReceived) = try sm.receiveData() else {
            Issue.record("Expected .doNotBuffer(.allDataReceived)")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Duplicate RESET_STREAM preserves first error code")
    func duplicateResetStreamPreservesFirstErrorCode() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        let first = try sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(42), finalSize: 0)
        guard case .doNotCloseStream(let firstError) = first else {
            Issue.record("Expected .doNotCloseStream")
            return
        }
        guard case .resetStream = firstError else {
            Issue.record("Expected .resetStream for first error")
            return
        }

        let second = try sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(99), finalSize: 0)
        guard case .ignore(.alreadyReset) = second else {
            Issue.record("Expected .ignore(.alreadyReset)")
            return
        }

        // Application read should deliver the FIRST error code
        guard
            case .deliverResetError(
                applicationErrorCode: QUICApplicationErrorCode(42)
            ) = try sm.applicationRead()
        else {
            Issue.record("Expected .deliverResetError(42), not 99")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Duplicate STOP_SENDING preserves first error code")
    func duplicateStopSendingPreservesFirstErrorCode() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        let first = try sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(42))
        guard case .sendReset(_) = first else {
            Issue.record("Expected .sendReset")
            return
        }

        let second = try sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(99))
        guard case .ignore(.alreadyReset) = second else {
            Issue.record("Expected .ignore(.alreadyReset)")
            return
        }
    }

    // MARK: - Post-Terminal Behavior

    @available(anyAppleOS 26, *)
    @Test("Write after FIN is rejected")
    func writeAfterFinRejected() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        _ = try sm.sendFin()

        guard case .doNotWrite(.streamFinished) = try sm.writeData() else {
            Issue.record("Expected .doNotWrite(.streamFinished)")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Receive data after FIN is rejected")
    func receiveDataAfterFinRejected() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        _ = try sm.receiveFin(finalSize: 0)

        guard case .doNotBuffer(.allDataReceived) = try sm.receiveData() else {
            Issue.record("Expected .doNotBuffer(.allDataReceived)")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Application read after all data read returns .ignore(.alreadyDelivered)")
    func applicationReadAfterAllDataRead() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        _ = try sm.receiveFin(finalSize: 1024)
        _ = try sm.applicationRead()

        guard case .ignore(.alreadyDelivered) = try sm.applicationRead() else {
            Issue.record("Expected .ignore(.alreadyDelivered) after terminal read")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Reset error delivered on application read")
    func resetErrorDeliveredOnApplicationRead() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        _ = try sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(77), finalSize: 0)

        guard
            case .deliverResetError(
                applicationErrorCode: QUICApplicationErrorCode(77)
            ) = try sm.applicationRead()
        else {
            Issue.record("Expected .deliverResetError(77)")
            return
        }
    }

    // MARK: - Close

    @available(anyAppleOS 26, *)
    @Test("Close from pendingID")
    func closeFromPendingID() {
        var sm = QUICStreamStateMachine()

        sm.close(reason: .clean).requireClose()

        let isFullyClosed = sm.isFullyClosed
        #expect(isFullyClosed)
    }

    @available(anyAppleOS 26, *)
    @Test("Close from connected")
    func closeFromConnected() {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        sm.close(reason: .clean).requireClose()

        let isConnected = sm.isConnected
        let isFullyClosed = sm.isFullyClosed
        #expect(!isConnected)
        #expect(isFullyClosed)
    }

    @available(anyAppleOS 26, *)
    @Test("Double close returns .ignoreAlreadyClosed")
    func doubleClose() {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)
        _ = sm.close(reason: .clean)

        guard case .ignoreAlreadyClosed = sm.close(reason: .error) else {
            Issue.record("Expected .ignoreAlreadyClosed")
            return
        }
    }

    // MARK: - Error Routing

    @available(anyAppleOS 26, *)
    @Test("RESET_STREAM produces correct error type and code")
    func resetStreamFiresCorrectError() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        let action = try sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(42), finalSize: 0)
        guard case .doNotCloseStream(let error) = action else {
            Issue.record("Expected .doNotCloseStream")
            return
        }
        guard case .resetStream(let resetError) = error else {
            Issue.record("Expected .resetStream")
            return
        }
        #expect(resetError.code.rawValue == 42)
    }

    @available(anyAppleOS 26, *)
    @Test("STOP_SENDING then RESET_STREAM both produce errors — no cross-direction dedup")
    func bothDirectionsRouteIndependently() throws {
        var sm = QUICStreamStateMachine()
        _ = sm.streamConnected(direction: .bidirectional)

        // First: STOP_SENDING
        let stopAction = try sm.receiveStopSending(applicationErrorCode: QUICApplicationErrorCode(10))
        guard case .sendReset(let stopError) = stopAction else {
            Issue.record("Expected .sendReset")
            return
        }
        guard case .stopSending(let stopSendingError) = stopError else {
            Issue.record("Expected .stopSending")
            return
        }
        #expect(stopSendingError.code.rawValue == 10)

        // Second: RESET_STREAM — should also produce an error (not deduplicated)
        let resetAction = try sm.receiveResetStream(applicationErrorCode: QUICApplicationErrorCode(20), finalSize: 0)
        guard case .doNotCloseStream(let resetError) = resetAction else {
            Issue.record("Expected .doNotCloseStream")
            return
        }
        guard case .resetStream(let streamResetError) = resetError else {
            Issue.record("Expected .resetStream")
            return
        }
        #expect(streamResetError.code.rawValue == 20)
    }
}

// MARK: - QUICStreamPipelineStateMachine

struct QUICStreamPipelineStateMachineTests {
    /// Only the first read on a fresh stream is allowed to kick off the initializer.
    @Test(
        "startInitializer from .uninitialized with active channel returns .runInitializer and transitions to .initializing"
    )
    func startInitializerFromUninitialized() {
        var sm = QUICStreamPipelineStateMachine()

        let action = sm.startInitializer(channelActive: true)
        guard case .runInitializer = action else {
            Issue.record("Expected .runInitializer")
            return
        }

        // .initializing does not count as initialized
        let isInitialized = sm.isInitialized
        #expect(!isInitialized)
    }

    /// Concurrent reads on the same event-loop tick must not double-initialize (race protection).
    @Test("startInitializer from .initializing returns .ignore(.initializerInProgress)")
    func startInitializerFromInitializing() {
        var sm = QUICStreamPipelineStateMachine()

        _ = sm.startInitializer(channelActive: true)

        let action = sm.startInitializer(channelActive: true)
        guard case .ignore(.initializerInProgress) = action else {
            Issue.record("Expected .ignore(.initializerInProgress)")
            return
        }
    }

    /// Reads after init completes must not restart the initializer.
    @Test("startInitializer from .initialized returns .ignore(.initializerComplete)")
    func startInitializerFromInitialized() {
        var sm = QUICStreamPipelineStateMachine()

        _ = sm.startInitializer(channelActive: true)
        _ = sm.markInitializerComplete()

        let action = sm.startInitializer(channelActive: true)
        guard case .ignore(.initializerComplete) = action else {
            Issue.record("Expected .ignore(.initializerComplete)")
            return
        }
    }

    /// An inactive channel has nothing to dispatch into; no transition happens.
    @Test("startInitializer with inactive channel returns .ignore(.channelInactive) and does not transition")
    func startInitializerInactiveChannel() {
        var sm = QUICStreamPipelineStateMachine()

        let action = sm.startInitializer(channelActive: false)
        guard case .ignore(.channelInactive) = action else {
            Issue.record("Expected .ignore(.channelInactive)")
            return
        }

        // No transition; a follow-up call with active channel should still be the first one.
        let secondAction = sm.startInitializer(channelActive: true)
        guard case .runInitializer = secondAction else {
            Issue.record("Expected .runInitializer on follow-up")
            return
        }
    }

    /// Successful init transitions to `.initialized` and instructs the caller to surface the stream.
    /// `.initializing` must NOT count as initialized — half-closure `receiveResetStream` defers based on this.
    @Test("markInitializerComplete from .initializing returns .surfaceInitializedStream and flips isInitialized")
    func markInitializerCompleteSurfaces() {
        var sm = QUICStreamPipelineStateMachine()

        _ = sm.startInitializer(channelActive: true)

        let isInitializedDuring = sm.isInitialized
        #expect(!isInitializedDuring)

        let action = sm.markInitializerComplete()
        guard case .surfaceInitializedStream = action else {
            Issue.record("Expected .surfaceInitializedStream")
            return
        }

        let isInitialized = sm.isInitialized
        #expect(isInitialized)
    }

    /// Idempotent: a duplicate completion (e.g. retry path) must not re-surface the stream.
    @Test("markInitializerComplete from .initialized returns .ignoreAlreadyComplete")
    func markInitializerCompleteIdempotent() {
        var sm = QUICStreamPipelineStateMachine()

        _ = sm.startInitializer(channelActive: true)
        _ = sm.markInitializerComplete()

        let action = sm.markInitializerComplete()
        guard case .ignoreAlreadyComplete = action else {
            Issue.record("Expected .ignoreAlreadyComplete")
            return
        }
    }
}

// MARK: - SwiftNetworkStreamHandle.StateMachine

struct SwiftNetworkStreamHandleStateMachineTests {
    /// Hard violation: starting a torn-down stream is a programmer bug. The wrapper
    /// `preconditionFailure`s on this — which we can't test directly — so we verify
    /// the SM signals it via the action enum here, with the reason that explains why.
    @available(anyAppleOS 26, *)
    @Test("invokeConnect returns .handleViolation(.detached) when detached")
    func invokeConnectReturnsHandleViolation() {
        var sm = SwiftNetworkStreamHandle.StateMachine()

        _ = sm.invokeDetach()

        let action = sm.invokeConnect()
        guard case .handleViolation(.detached) = action else {
            Issue.record("Expected .handleViolation(.detached)")
            return
        }
    }
}

// MARK: - SwiftNetworkStreamHandle (wrapper)

struct SwiftNetworkStreamHandleTests {
    /// Outbound streams start with a stub linkage; the handle is usable from creation.
    @available(anyAppleOS 26, *)
    @Test("Default init is attached")
    func defaultInitIsAttached() {
        let handle = SwiftNetworkStreamHandle()

        let isAttached = handle.isAttached
        #expect(isAttached)
    }

    /// Inbound init pathway: the listener-attached linkage is wrapped in an attached handle.
    @available(anyAppleOS 26, *)
    @Test("init(linkage:) is attached")
    func initWithLinkageIsAttached() {
        let handle = SwiftNetworkStreamHandle(linkage: OutboundStreamLinkage(reference: .init()))

        let isAttached = handle.isAttached
        #expect(isAttached)
    }

    /// End-to-end detach contract: wrapper consumes the linkage on the first call and
    /// tolerates duplicates (multiple call sites unconditionally try to tear down).
    @available(anyAppleOS 26, *)
    @Test("invokeDetach flips state and is idempotent")
    func invokeDetachIsIdempotent() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        let isAttachedAfter = handle.isAttached
        #expect(!isAttachedAfter)

        // Second call should report already detached, not crash.
        switch handle.invokeDetach() {
        case .proceed: Issue.record("Expected .skipAlreadyDetached on second detach")
        case .skipAlreadyDetached: break
        }
    }

    /// Stop sequencing: disconnect after detach is a normal teardown ordering and must not throw.
    @available(anyAppleOS 26, *)
    @Test("invokeDisconnect silently no-ops when detached")
    func invokeDisconnectNoopsWhenDetached() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        switch handle.invokeDisconnect() {
        case .proceed: Issue.record("Expected .ignore when detached")
        case .ignore: break
        }
    }

    /// Error/abort during teardown must not raise (called from the close-stream paths).
    @available(anyAppleOS 26, *)
    @Test("invokeAbortInbound silently no-ops when detached")
    func invokeAbortInboundNoopsWhenDetached() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        switch handle.invokeAbortInbound() {
        case .proceed: Issue.record("Expected .ignore when detached")
        case .ignore: break
        }
    }

    /// Error/abort during teardown must not raise (called from the close-stream paths).
    @available(anyAppleOS 26, *)
    @Test("invokeAbortOutbound silently no-ops when detached")
    func invokeAbortOutboundNoopsWhenDetached() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        switch handle.invokeAbortOutbound() {
        case .proceed: Issue.record("Expected .ignore when detached")
        case .ignore: break
        }
    }

    /// Soft-violation policy: write-after-detach surfaces as a violation the caller can
    /// surface as a throw (e.g. return false), not a silent no-op that drops bytes.
    @available(anyAppleOS 26, *)
    @Test("invokeSendStreamData reports violation when detached")
    func invokeSendStreamDataThrowsWhenDetached() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        switch handle.invokeSendStreamData() {
        case .proceed:
            Issue.record("Expected .handleViolation when detached")
        case .handleViolation(let reason):
            switch reason {
            case .detached: break
            }
        }
    }

    /// Soft-violation policy: read-after-detach surfaces as a violation, not a silent nil that
    /// the caller might confuse with "no data available".
    @available(anyAppleOS 26, *)
    @Test("invokeReceiveStreamData reports violation when detached")
    func invokeReceiveStreamDataThrowsWhenDetached() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        switch handle.invokeReceiveStreamData() {
        case .proceed:
            Issue.record("Expected .handleViolation when detached")
        case .handleViolation(let reason):
            switch reason {
            case .detached: break
            }
        }
    }

    /// Metadata is genuinely unavailable post-detach; the wrapper returns `.ignore`
    /// so the caller can surface nil to its own consumer.
    @available(anyAppleOS 26, *)
    @Test("invokeGetMetadata reports ignore when detached")
    func invokeGetMetadataReturnsNilWhenDetached() throws(NetworkError) {
        var handle = SwiftNetworkStreamHandle()

        switch handle.invokeDetach() {
        case .proceed: break
        case .skipAlreadyDetached: Issue.record("Expected .proceed on first detach")
        }

        switch handle.invokeGetMetadata() {
        case .proceed: Issue.record("Expected .ignore when detached")
        case .ignore: break
        }
    }
}
