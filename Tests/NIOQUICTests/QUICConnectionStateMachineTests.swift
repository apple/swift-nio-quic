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
import XCTest

@testable import NIOQUIC

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

@available(anyAppleOS 26, *)
final class QUICConnectionStateMachineTests: XCTestCase {

    // MARK: - Initial State Tests

    func testInitialState() {
        let stateMachine = QUICConnectionStateMachine()

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)
        XCTAssertFalse(stateMachine.isTerminating)
    }

    // MARK: - Connected Event Tests

    func testReceiveConnectedEventFromConnecting() {
        var stateMachine = QUICConnectionStateMachine()

        let action = stateMachine.receiveConnectedEvent()

        guard case .logConnectionEstablished = action else {
            return XCTFail("Expected .logConnectionEstablished")
        }

        XCTAssertTrue(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)
        XCTAssertFalse(stateMachine.isTerminating)
    }

    func testReceiveConnectedEventWhenAlreadyConnected() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()

        let action = stateMachine.receiveConnectedEvent()

        guard case .invalidTransition = action else {
            return XCTFail("Expected .invalidTransition")
        }

        XCTAssertTrue(stateMachine.canProcessData)
    }

    func testReceiveConnectedEventWhenDraining() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()
        let _ = stateMachine.receiveDisconnectedEvent(error: nil)

        let action = stateMachine.receiveConnectedEvent()

        guard case .invalidTransition = action else {
            return XCTFail("Expected .invalidTransition")
        }

        XCTAssertTrue(stateMachine.isDraining)
    }

    // MARK: - Disconnected Event Tests

    func testReceiveDisconnectedEventFromConnecting() {
        var stateMachine = QUICConnectionStateMachine()

        let (action, _) = stateMachine.receiveDisconnectedEvent(error: nil)

        guard case .beginDraining = action else {
            return XCTFail("Expected .beginDraining")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)  // Went straight to closed
        XCTAssertTrue(stateMachine.isTerminating)
    }

    func testReceiveDisconnectedEventFromConnected() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()

        let (action, _) = stateMachine.receiveDisconnectedEvent(error: nil)

        guard case .beginDraining = action else {
            return XCTFail("Expected .beginDraining")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertTrue(stateMachine.isDraining)
        XCTAssertTrue(stateMachine.isTerminating)
    }

    func testReceiveDisconnectedEventWhenAlreadyDraining() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()
        let _ = stateMachine.receiveDisconnectedEvent(error: nil)

        let (action, _) = stateMachine.receiveDisconnectedEvent(error: nil)

        guard case .alreadyClosing = action else {
            return XCTFail("Expected .alreadyClosing")
        }

        XCTAssertTrue(stateMachine.isDraining)
    }

    // MARK: - Idle Timeout Tests (via ETIMEDOUT in disconnected event)

    func testIdleTimeoutFromConnecting() {
        var stateMachine = QUICConnectionStateMachine()

        // SwiftNetwork delivers ETIMEDOUT via disconnected event
        let (action, _) = stateMachine.receiveDisconnectedEvent(error: .posix(ETIMEDOUT))

        guard case .beginDraining = action else {
            return XCTFail("Expected .beginDraining")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)  // Went straight to closed
        XCTAssertTrue(stateMachine.isTerminating)

        // After idle timeout, channel should close cleanly
        let outboundAction = stateMachine.outboundDataProcessed(isChannelInitializing: false)
        guard case .closeCleanly = outboundAction else {
            return XCTFail("Expected .closeCleanly")
        }
    }

    func testIdleTimeoutFromConnected() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()

        // SwiftNetwork delivers ETIMEDOUT via disconnected event
        let (action, _) = stateMachine.receiveDisconnectedEvent(error: .posix(ETIMEDOUT))

        guard case .beginDraining = action else {
            return XCTFail("Expected .beginDraining")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertTrue(stateMachine.isDraining)
        XCTAssertTrue(stateMachine.isTerminating)
    }

    func testIdleTimeoutWhenAlreadyClosed() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.abruptClose()

        // SwiftNetwork delivers ETIMEDOUT via disconnected event
        let (action, _) = stateMachine.receiveDisconnectedEvent(error: .posix(ETIMEDOUT))

        guard case .alreadyClosing = action else {
            return XCTFail("Expected .alreadyClosing")
        }

        XCTAssertTrue(stateMachine.isTerminating)
    }

    // MARK: - Force Close Tests

    func testAbruptCloseFromConnecting() {
        var stateMachine = QUICConnectionStateMachine()

        let action = stateMachine.abruptClose()

        guard case .closeImmediately = action else {
            return XCTFail("Expected .closeImmediately")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)
        XCTAssertTrue(stateMachine.isTerminating)
    }

    func testAbruptCloseFromConnected() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()

        let action = stateMachine.abruptClose()

        guard case .tearDownState = action else {
            return XCTFail("Expected .tearDownState")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)
        XCTAssertTrue(stateMachine.isTerminating)
    }

    func testAbruptCloseFromDraining() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()
        let _ = stateMachine.receiveDisconnectedEvent(error: nil)

        let action = stateMachine.abruptClose()

        guard case .tearDownState = action else {
            return XCTFail("Expected .tearDownState")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)  // Upgraded from draining to abrupt closed
        XCTAssertTrue(stateMachine.isTerminating)
    }

    func testAbruptCloseWhenAlreadyClosed() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.abruptClose()

        let action = stateMachine.abruptClose()

        guard case .alreadyClosed = action else {
            return XCTFail("Expected .alreadyClosed")
        }

        XCTAssertTrue(stateMachine.isTerminating)
    }

    // MARK: - Initiate Close Tests

    func testInitiateCloseFromConnected() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()

        let action = stateMachine.initiateClose(sendApplicationClose: true, errorCode: 0, reason: "Clean close")

        guard case .sendCloseFrame = action else {
            return XCTFail("Expected .sendCloseFrame")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)  // In closing state, not draining
        XCTAssertTrue(stateMachine.isTerminating)  // Closing includes both .closing and .draining states
    }

    func testInitiateCloseFromConnecting() {
        var stateMachine = QUICConnectionStateMachine()

        let action = stateMachine.initiateClose(sendApplicationClose: true, errorCode: 0, reason: "Clean close")

        guard case .sendCloseFrame = action else {
            return XCTFail("Expected .sendCloseFrame")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)  // Never connected, go straight to closed
        XCTAssertTrue(stateMachine.isTerminating)
    }

    func testInitiateCloseWhenAlreadyDraining() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()
        let _ = stateMachine.initiateClose(sendApplicationClose: true, errorCode: 0, reason: "Clean close")

        let action = stateMachine.initiateClose(sendApplicationClose: true, errorCode: 0, reason: "Clean close")

        guard case .alreadyClosing = action else {
            return XCTFail("Expected .alreadyClosing")
        }

        XCTAssertFalse(stateMachine.isDraining)  // In closing state
        XCTAssertTrue(stateMachine.isTerminating)
    }

    func testReceiveDisconnectedFromClosing() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()
        let _ = stateMachine.initiateClose(sendApplicationClose: true, errorCode: 0, reason: "Clean close")

        // Now in closing state, receive disconnected event
        let (action, _) = stateMachine.receiveDisconnectedEvent(error: nil)

        guard case .completeClosing = action else {
            return XCTFail("Expected .completeClosing")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)
        XCTAssertTrue(stateMachine.isTerminating)  // Now in closed state
    }

    func testAbruptCloseFromClosing() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()
        let _ = stateMachine.initiateClose(sendApplicationClose: true, errorCode: 0, reason: "Clean close")

        // Now in closing state, abrupt close
        let action = stateMachine.abruptClose()

        guard case .tearDownState = action else {
            return XCTFail("Expected .tearDownState")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)
        XCTAssertTrue(stateMachine.isTerminating)  // Now in closed state
    }

    func testPeerInitiatedCloseGoesToDraining() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()

        // Peer initiates close (we receive disconnected event)
        let (action, _) = stateMachine.receiveDisconnectedEvent(error: nil)

        guard case .beginDraining = action else {
            return XCTFail("Expected .beginDraining")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertTrue(stateMachine.isDraining)  // In draining state, not closing
        XCTAssertTrue(stateMachine.isTerminating)  // isClosing returns true for both draining and closing
    }

    // MARK: - Complete Draining Tests

    func testCompleteDrainingFromDraining() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()
        let _ = stateMachine.receiveDisconnectedEvent(error: nil)

        let action = stateMachine.completeDraining()

        guard case .finalizeClosure = action else {
            return XCTFail("Expected .finalizeClosure")
        }

        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)  // Completed draining, now closed
        XCTAssertTrue(stateMachine.isTerminating)
    }

    func testCompleteDrainingWhenNotDraining() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()

        let action = stateMachine.completeDraining()

        guard case .notDraining = action else {
            return XCTFail("Expected .notDraining")
        }

        XCTAssertTrue(stateMachine.canProcessData)
    }

    // MARK: - Outbound Data Processed Tests

    func testOutboundDataProcessedWhenConnecting() {
        let stateMachine = QUICConnectionStateMachine()

        // When connecting, no action needed regardless of channel state
        let actionInitializing = stateMachine.outboundDataProcessed(isChannelInitializing: true)
        guard case .noAction = actionInitializing else {
            return XCTFail("Expected .noAction")
        }

        let actionActivated = stateMachine.outboundDataProcessed(isChannelInitializing: false)
        guard case .noAction = actionActivated else {
            return XCTFail("Expected .noAction")
        }
    }

    func testOutboundDataProcessedWhenConnected() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()

        // When connected and channel is initializing, complete activation
        let actionInitializing = stateMachine.outboundDataProcessed(isChannelInitializing: true)
        guard case .completeActivation = actionInitializing else {
            return XCTFail("Expected .completeActivation")
        }

        // When connected and channel is already activated, no action
        let actionActivated = stateMachine.outboundDataProcessed(isChannelInitializing: false)
        guard case .noAction = actionActivated else {
            return XCTFail("Expected .noAction")
        }
    }

    func testOutboundDataProcessedWhenDraining() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()
        let _ = stateMachine.receiveDisconnectedEvent(error: nil)

        // When draining without error, no action needed
        let action = stateMachine.outboundDataProcessed(isChannelInitializing: false)
        guard case .noAction = action else {
            return XCTFail("Expected .noAction")
        }
    }

    func testOutboundDataProcessedWhenClosedCleanly() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.abruptClose()

        // When closed cleanly, channel should close
        let action = stateMachine.outboundDataProcessed(isChannelInitializing: false)
        guard case .closeCleanly = action else {
            return XCTFail("Expected .closeCleanly")
        }
    }

    // MARK: - State Transition Sequence Tests

    func testTypicalConnectionLifecycle() {
        var stateMachine = QUICConnectionStateMachine()

        // Start in connecting state
        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)
        XCTAssertFalse(stateMachine.isTerminating)

        // Transition to connected
        let _ = stateMachine.receiveConnectedEvent()
        XCTAssertTrue(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)
        XCTAssertFalse(stateMachine.isTerminating)

        // Initiate graceful close (go to closing, not draining)
        let _ = stateMachine.initiateClose(sendApplicationClose: true, errorCode: 0, reason: "Done")
        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)  // In closing state
        XCTAssertTrue(stateMachine.isTerminating)

        // Complete draining (works for both closing and draining states)
        let _ = stateMachine.completeDraining()
        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)
        XCTAssertTrue(stateMachine.isTerminating)  // Now in closed state
    }

    func testConnectionFailureBeforeEstablished() {
        var stateMachine = QUICConnectionStateMachine()

        // Start in connecting state
        XCTAssertFalse(stateMachine.canProcessData)

        // Receive disconnect before connected
        let _ = stateMachine.receiveDisconnectedEvent(error: nil)

        // Should go straight to closed (not draining)
        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertFalse(stateMachine.isDraining)
        XCTAssertTrue(stateMachine.isTerminating)
    }

    func testIdleTimeoutScenario() {
        var stateMachine = QUICConnectionStateMachine()
        let _ = stateMachine.receiveConnectedEvent()

        // Idle timeout delivered by SwiftNetwork as ETIMEDOUT
        let _ = stateMachine.receiveDisconnectedEvent(error: .posix(ETIMEDOUT))

        // Should be in draining state (timeout while connected goes to draining first)
        XCTAssertFalse(stateMachine.canProcessData)
        XCTAssertTrue(stateMachine.isDraining)
        XCTAssertTrue(stateMachine.isTerminating)

        // Complete draining
        let _ = stateMachine.completeDraining()

        // Now should be fully closed
        XCTAssertFalse(stateMachine.isDraining)
        XCTAssertTrue(stateMachine.isTerminating)

        // Channel should close cleanly
        let action = stateMachine.outboundDataProcessed(isChannelInitializing: false)
        guard case .closeCleanly = action else {
            return XCTFail("Expected .closeCleanly")
        }
    }
}

// MARK: - RFC 9000 §10.2 / §10.2.2: Immediate Close & Draining (Swift Testing)

@available(anyAppleOS 26, *)
extension QUICConnectionStateMachine {
    /// Assert the connection state machine's key state flags.
    /// Uses local variables to work around `~Copyable` + `#expect` limitation.
    func expectState(
        canProcessData expectedCanProcess: Bool,
        isDraining expectedIsDraining: Bool,
        isTerminating expectedIsTerminating: Bool,
        isTerminal expectedIsTerminal: Bool? = nil,
        canAcceptNewStreams expectedCanAccept: Bool? = nil,
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        let canProcess = self.canProcessData
        let isDraining = self.isDraining
        let isTerminating = self.isTerminating
        #expect(canProcess == expectedCanProcess, "canProcessData", sourceLocation: sourceLocation)
        #expect(isDraining == expectedIsDraining, "isDraining", sourceLocation: sourceLocation)
        #expect(isTerminating == expectedIsTerminating, "isTerminating", sourceLocation: sourceLocation)
        if let expectedIsTerminal {
            let isTerminal = self.isTerminal
            #expect(isTerminal == expectedIsTerminal, "isTerminal", sourceLocation: sourceLocation)
        }
        if let expectedCanAccept {
            let canAccept = self.canAcceptNewStreams
            #expect(canAccept == expectedCanAccept, "canAcceptNewStreams", sourceLocation: sourceLocation)
        }
    }
}

struct QUICConnectionImmediateCloseTests {

    @available(anyAppleOS 26, *)
    @Test("Immediate close full lifecycle: connected → closing → closed")
    func immediateCloseFullLifecycle() {
        var sm = QUICConnectionStateMachine()
        _ = sm.receiveConnectedEvent()
        sm.expectState(canProcessData: true, isDraining: false, isTerminating: false)

        let closeAction = sm.initiateClose(
            sendApplicationClose: true,
            errorCode: 0,
            reason: "Application done"
        )
        guard case .sendCloseFrame = closeAction else {
            Issue.record("Expected .sendCloseFrame")
            return
        }
        sm.expectState(canProcessData: false, isDraining: false, isTerminating: true)

        let (disconnectAction, _) = sm.receiveDisconnectedEvent(error: nil)
        guard case .completeClosing = disconnectAction else {
            Issue.record("Expected .completeClosing")
            return
        }
        sm.expectState(canProcessData: false, isDraining: false, isTerminating: true, isTerminal: true)
    }

    @available(anyAppleOS 26, *)
    @Test("Immediate close rejects new streams")
    func immediateCloseRejectsNewStreams() {
        var sm = QUICConnectionStateMachine()
        _ = sm.receiveConnectedEvent()
        sm.expectState(canProcessData: true, isDraining: false, isTerminating: false, canAcceptNewStreams: true)

        _ = sm.initiateClose(
            sendApplicationClose: true,
            errorCode: 0,
            reason: "Done"
        )
        sm.expectState(canProcessData: false, isDraining: false, isTerminating: true, canAcceptNewStreams: false)
    }

    @available(anyAppleOS 26, *)
    @Test("Duplicate initiateClose from closing returns alreadyClosing")
    func duplicateInitiateCloseFromClosing() {
        var sm = QUICConnectionStateMachine()
        _ = sm.receiveConnectedEvent()
        _ = sm.initiateClose(
            sendApplicationClose: true,
            errorCode: 0,
            reason: "First close"
        )

        let action = sm.initiateClose(
            sendApplicationClose: true,
            errorCode: 1,
            reason: "Second close"
        )
        guard case .alreadyClosing = action else {
            Issue.record("Expected .alreadyClosing")
            return
        }
    }

    /// RFC 9000 §10.2.1: When we initiated close and the peer responds with its
    /// own CONNECTION_CLOSE, we go straight to closed (not draining, since we
    /// initiated).
    @available(anyAppleOS 26, *)
    @Test("Closing completes to closed on peer response")
    func closingTransitionsToClosed() {
        var sm = QUICConnectionStateMachine()
        _ = sm.receiveConnectedEvent()
        _ = sm.initiateClose(
            sendApplicationClose: true,
            errorCode: 0,
            reason: "We close first"
        )
        sm.expectState(canProcessData: false, isDraining: false, isTerminating: true, isTerminal: false)

        let (action, _) = sm.receiveDisconnectedEvent(error: nil)
        guard case .completeClosing = action else {
            Issue.record("Expected .completeClosing")
            return
        }
        sm.expectState(canProcessData: false, isDraining: false, isTerminating: true, isTerminal: true)
    }
}

struct QUICConnectionDrainingStateTests {

    @available(anyAppleOS 26, *)
    @Test("Draining state: canProcessData is false, no outbound actions")
    func drainingStateMustNotSendPackets() {
        var sm = QUICConnectionStateMachine()
        _ = sm.receiveConnectedEvent()

        let (action, _) = sm.receiveDisconnectedEvent(error: nil)
        guard case .beginDraining = action else {
            Issue.record("Expected .beginDraining")
            return
        }

        sm.expectState(canProcessData: false, isDraining: true, isTerminating: true, canAcceptNewStreams: false)

        let outboundAction = sm.outboundDataProcessed(isChannelInitializing: false)
        guard case .noAction = outboundAction else {
            Issue.record("Expected .noAction in draining state")
            return
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Draining completes to closed")
    func drainingCompletesToClosed() {
        var sm = QUICConnectionStateMachine()
        _ = sm.receiveConnectedEvent()
        _ = sm.receiveDisconnectedEvent(error: nil)
        sm.expectState(canProcessData: false, isDraining: true, isTerminating: true, isTerminal: false)

        let action = sm.completeDraining()
        guard case .finalizeClosure = action else {
            Issue.record("Expected .finalizeClosure")
            return
        }

        sm.expectState(canProcessData: false, isDraining: false, isTerminating: true, isTerminal: true)
    }

    @available(anyAppleOS 26, *)
    @Test("Draining with peer error captures connectionError")
    func drainingWithPeerErrorCapturesError() {
        var sm = QUICConnectionStateMachine()
        _ = sm.receiveConnectedEvent()

        // connectionError should be nil before the disconnect
        let errorBefore = sm.connectionError
        #expect(errorBefore == nil)

        let (action, _) = sm.receiveDisconnectedEvent(
            error: .posix(ECONNRESET)
        )
        guard case .beginDraining = action else {
            Issue.record("Expected .beginDraining")
            return
        }
        sm.expectState(canProcessData: false, isDraining: true, isTerminating: true)
        let connError = sm.connectionError
        #expect(connError == .posix(ECONNRESET))
    }
}
