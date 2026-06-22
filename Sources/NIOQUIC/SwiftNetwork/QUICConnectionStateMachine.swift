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

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

//                                      ┌──────────────┐
//                                      │  CONNECTING  │ (Initial state, handshake in progress)
//                                      └──┬───────┬───┘
//                                         │       │
//                                         │       │
//                                         │       └─────────────────────────┐
//                 receiveConnectedEvent() │                                 │ receiveDisconnectedEvent()
//                                         │                                 │ [skip draining]
//                                         ▼                                 │
//                                  ┌──────────────┐                         │
//  (Established, can process data) │  CONNECTED   │                         │
//                                  └──┬───────┬───┘                         │
//                                     │       │                             │
//                                     │       │ initiateClose() [local]     │
//                                     │       │                             │
//                                     │       ▼                             │
//                                     │    ┌──────────┐                     │
//                                     │    │ CLOSING  │ (Sent CONN_CLOSE)   │
//                                     │    └────┬─────┘                     │
//                                     │         │                           │
//                                     │         │ receiveDisconnectedEvent()│
//                                     │         │ OR completeDraining()     │
//        receiveDisconnectedEvent()   │         └────────┐                  │
//        [peer close / idle timeout]  │                  │                  │
//                                     │                  │                  │
//                                     │                  │                  │
//                                     │                  │                  │
//                                     ▼                  │                  │
//                                  ┌──────────┐          │                  │
//                                  │ DRAINING │          │                  │
//                                  └────┬─────┘          │                  │
//                                       │                │                  │
//                    completeDraining() │                │                  │
//                                       │                │                  │
//         abruptClose()                 └────────┬───────┘                  │
//         from any state                         │                          │
//                                                ▼                          │
//                │                          ┌──────────┐                    │
//                └─────────────────────────▶│  CLOSED  │◄───────────────────┘
//                                           └──────────┘

/// A state machine managing the QUIC connection lifecycle.
@available(anyAppleOS 26, *)
struct QUICConnectionStateMachine: ~Copyable {
    /// The possible states of a QUIC connection.
    enum State: ~Copyable {
        /// Connection is establishing (handshake in progress).
        struct Connecting: ~Copyable {
            init() {}
        }

        /// Connection is established and ready for use.
        struct Connected: ~Copyable {
            init(from state: consuming Connecting) {}
        }

        /// Connection is actively closing (sent CONNECTION_CLOSE).
        /// Per RFC 9000 §10.2.1, endpoint can retransmit CONNECTION_CLOSE during this period.
        struct Closing: ~Copyable {
            init(from state: consuming Connected) {}
        }

        /// Connection is draining (received CONNECTION_CLOSE from peer).
        /// Per RFC 9000 §10.2.2, endpoint MUST NOT send packets during this period.
        struct Draining: ~Copyable {
            /// The reason for entering draining state
            let reason: DrainingReason
            /// The underlying network error, if any
            let connectionError: NetworkError?

            enum DrainingReason {
                /// Peer sent CONNECTION_CLOSE, optionally with an error
                case peerInitiated(NIOQUICHelpers.QUICConnectionError?)
                /// Idle timeout reached
                case idleTimeout
            }

            init(from state: consuming Connected, reason: DrainingReason, connectionError: NetworkError?) {
                self.reason = reason
                self.connectionError = connectionError
            }

            init(from state: consuming Closing, reason: DrainingReason, connectionError: NetworkError?) {
                self.reason = reason
                self.connectionError = connectionError
            }
        }

        /// Connection is closed (terminal state).
        struct Closed: ~Copyable {
            /// The reason the connection closed
            let closeReason: CloseReason
            /// The underlying network error, if any
            let connectionError: NetworkError?

            enum CloseReason {
                /// Clean shutdown with no errors
                case clean
                /// Idle timeout reached
                case idleTimeout
                /// Abrupt closed locally
                case abruptClose
                /// Peer sent a QUIC connection error
                case peerError(NIOQUICHelpers.QUICConnectionError)

                init(from drainingReason: State.Draining.DrainingReason) {
                    switch drainingReason {
                    case .idleTimeout:
                        self = .idleTimeout
                    case .peerInitiated(let error):
                        if let error {
                            self = .peerError(error)
                        } else {
                            self = .clean
                        }
                    }
                }
            }

            /// Closed from connecting state (connection failed before established)
            init(from state: consuming Connecting, closeReason: CloseReason, connectionError: NetworkError?) {
                self.closeReason = closeReason
                self.connectionError = connectionError
            }

            /// Closed from closing state (local close completed)
            init(from state: consuming Closing, closeReason: CloseReason, connectionError: NetworkError?) {
                self.closeReason = closeReason
                self.connectionError = connectionError
            }

            /// Closed from draining state (draining period completed)
            init(from state: consuming Draining) {
                self.closeReason = CloseReason(from: state.reason)
                self.connectionError = state.connectionError
            }

            /// Abrupt close from any state
            init(abruptCloseFrom state: consuming Connecting) {
                self.closeReason = .abruptClose
                self.connectionError = nil
            }

            init(abruptCloseFrom state: consuming Connected) {
                self.closeReason = .abruptClose
                self.connectionError = nil
            }

            init(abruptCloseFrom state: consuming Closing) {
                self.closeReason = .abruptClose
                self.connectionError = nil
            }

            init(abruptCloseFrom state: consuming Draining) {
                self.closeReason = .abruptClose
                self.connectionError = state.connectionError
            }
        }

        case connecting(Connecting)
        case connected(Connected)
        case closing(Closing)
        case draining(Draining)
        case closed(Closed)
    }

    /// Action to take after outbound data has been processed on a child channel.
    enum OutboundDataProcessedAction {
        /// Close the channel cleanly.
        case closeCleanly
        /// Close the channel with an error.
        case closeWithError(NIOQUICHelpers.QUICConnectionError)
        /// Complete activation of the channel (connection is now ready for data).
        case completeActivation
        /// No action needed.
        case noAction
    }

    /// Actions to execute when processing errors.
    /// The state machine returns these to tell the controller what side effects to perform.
    /// Note: ErrorAction is Copyable because it's collected into arrays.
    enum ErrorAction {
        /// Abrupt close the connection immediately
        case abruptClose
    }

    internal var state: State

    init() {
        self.state = .connecting(.init())
    }

    private init(state: consuming State) {
        self.state = state
    }

    // MARK: - State Queries

    var stateDescription: String {
        switch self.state {
        case .connecting:
            return "connecting"
        case .connected:
            return "connected"
        case .closing:
            return "closing"
        case .draining:
            return "disconnected"
        case .closed:
            return "disconnected"
        }
    }

    /// Determines the action a child channel should take after outbound data has been processed.
    ///
    /// This method encapsulates the decision logic for what happens after writing outbound data
    /// on the connection. The caller should invoke this method and act on the returned action.
    ///
    /// - Parameter isChannelInitializing: `true` if the channel is still in its initializing
    ///   state and waiting for activation.
    /// - Returns: The action the caller should take.
    func outboundDataProcessed(isChannelInitializing: Bool) -> OutboundDataProcessedAction {
        switch self.state {
        case .connecting:
            return .noAction
        case .connected:
            if isChannelInitializing {
                return .completeActivation
            }
            return .noAction
        case .closing:
            return .noAction
        case .draining(let draining):
            switch draining.reason {
            case .peerInitiated(let peerError):
                if let peerError {
                    return .closeWithError(peerError)
                }
                return .noAction
            case .idleTimeout:
                return .noAction
            }
        case .closed(let closed):
            switch closed.closeReason {
            case .clean, .idleTimeout, .abruptClose:
                return .closeCleanly
            case .peerError(let error):
                return .closeWithError(error)
            }
        }
    }

    // MARK: - Capability Queries

    /// Returns `true` if new streams can be created or accepted.
    /// Only valid in the connected state.
    var canAcceptNewStreams: Bool {
        switch self.state {
        case .connected:
            return true
        case .connecting:
            return false
        case .closing:
            return false
        case .draining:
            return false
        case .closed:
            return false
        }
    }

    /// Returns `true` if data can be read from or written to streams.
    /// Only valid in the connected state.
    var canProcessData: Bool {
        switch self.state {
        case .connected:
            return true
        case .connecting:
            return false
        case .closing:
            return false
        case .draining:
            return false
        case .closed:
            return false
        }
    }

    /// Returns `true` if the connection is in any termination state
    /// (closing, draining, or closed).
    var isTerminating: Bool {
        switch self.state {
        case .closing:
            return true
        case .draining:
            return true
        case .closed:
            return true
        case .connecting:
            return false
        case .connected:
            return false
        }
    }

    /// Returns `true` if the connection has reached the terminal closed state.
    var isTerminal: Bool {
        switch self.state {
        case .closed:
            return true
        case .connecting:
            return false
        case .connected:
            return false
        case .closing:
            return false
        case .draining:
            return false
        }
    }

    /// Returns `true` if the connection has completed the handshake.
    /// Used to check if the connection was ever fully established.
    var hasEstablishedConnection: Bool {
        switch self.state {
        case .connecting:
            return false
        case .connected:
            return true
        case .closing:
            return true
        case .draining:
            return true
        case .closed:
            return true
        }
    }

    // MARK: - RFC 9000 State Queries

    /// Returns `true` if the connection is in the draining state per RFC 9000 §10.2.2.
    /// In this state, the endpoint has received CONNECTION_CLOSE and MUST NOT send packets.
    var isDraining: Bool {
        switch self.state {
        case .draining:
            return true
        case .connecting:
            return false
        case .connected:
            return false
        case .closing:
            return false
        case .closed:
            return false
        }
    }

    /// Returns the underlying network error if the connection closed with one.
    var connectionError: NetworkError? {
        switch self.state {
        case .draining(let draining):
            return draining.connectionError
        case .closed(let closed):
            return closed.connectionError
        case .connecting:
            return nil
        case .connected:
            return nil
        case .closing:
            return nil
        }
    }

    // MARK: - Transitions

    enum ReceiveConnectedEventAction: ~Copyable {
        case logConnectionEstablished
        case invalidTransition
    }

    mutating func receiveConnectedEvent() -> ReceiveConnectedEventAction {
        switch consume self.state {
        case .connecting(let connecting):
            self = .init(state: .connected(.init(from: connecting)))
            return .logConnectionEstablished
        case .connected(let connected):
            self = .init(state: .connected(connected))
            return .invalidTransition
        case .closing(let closing):
            self = .init(state: .closing(closing))
            return .invalidTransition
        case .draining(let draining):
            self = .init(state: .draining(draining))
            return .invalidTransition
        case .closed(let closed):
            self = .init(state: .closed(closed))
            return .invalidTransition
        }
    }

    enum ReceiveDisconnectedEventAction {
        case beginDraining(NIOQUICHelpers.QUICConnectionError?)
        case completeClosing
        case alreadyClosing
        case invalidTransition
    }

    mutating func receiveDisconnectedEvent(
        error: NetworkError?
    ) -> (stateAction: ReceiveDisconnectedEventAction, errorAction: ErrorAction?) {
        var errorAction: ErrorAction? = nil
        var quicError: NIOQUICHelpers.QUICConnectionError? = nil
        var connectionError: NetworkError? = nil
        var isIdleTimeout = false

        // Inspect and convert the error
        if let error = error {
            // Idle timeout from SwiftNetwork
            if error == .posix(ETIMEDOUT) {
                isIdleTimeout = true
                // No error actions for timeout
            }
            // Abrupt close on connection abort
            else if error == .posix(ECONNABORTED) {
                connectionError = error
                errorAction = .abruptClose
            }
            // QUIC transport errors (0x00-0xFF)
            else if let transportErrorCode = error.quicTransportError, transportErrorCode != 0 {
                quicError = NIOQUICHelpers.QUICConnectionError(
                    reason: error.description,
                    isApplication: false,
                    code: UInt64(transportErrorCode)
                )
                connectionError = error
            }
            // QUIC application errors (0x100+)
            else if let applicationErrorCode = error.quicApplicationError, applicationErrorCode != 0 {
                quicError = NIOQUICHelpers.QUICConnectionError(
                    reason: error.description,
                    isApplication: true,
                    code: UInt64(applicationErrorCode)
                )
                connectionError = error
            }
            // Other errors (POSIX, etc.)
            else {
                connectionError = error
            }
        }

        // Determine state transition based on quicError and timeout
        let stateAction: ReceiveDisconnectedEventAction
        switch consume self.state {
        case .connecting(let connecting):
            // Disconnected before fully connecting
            if isIdleTimeout {
                self = .init(state: .closed(.init(from: connecting, closeReason: .idleTimeout, connectionError: nil)))
                stateAction = .beginDraining(nil)
            } else if let error = quicError {
                self = .init(
                    state: .closed(
                        .init(from: connecting, closeReason: .peerError(error), connectionError: connectionError)
                    )
                )
                stateAction = .beginDraining(error)
            } else {
                self = .init(
                    state: .closed(.init(from: connecting, closeReason: .clean, connectionError: connectionError))
                )
                stateAction = .beginDraining(nil)
            }
        case .connected(let connected):
            // Normal disconnection from connected state - peer initiated
            if isIdleTimeout {
                self = .init(state: .draining(.init(from: connected, reason: .idleTimeout, connectionError: nil)))
                stateAction = .beginDraining(nil)
            } else if let error = quicError {
                self = .init(
                    state: .draining(
                        .init(from: connected, reason: .peerInitiated(error), connectionError: connectionError)
                    )
                )
                stateAction = .beginDraining(error)
            } else {
                self = .init(
                    state: .draining(
                        .init(from: connected, reason: .peerInitiated(nil), connectionError: connectionError)
                    )
                )
                stateAction = .beginDraining(nil)
            }
        case .closing(let closing):
            // We were actively closing (sent CONNECTION_CLOSE), now complete the closure
            // No draining period needed since we initiated
            if isIdleTimeout {
                self = .init(state: .closed(.init(from: closing, closeReason: .idleTimeout, connectionError: nil)))
            } else {
                self = .init(
                    state: .closed(.init(from: closing, closeReason: .clean, connectionError: connectionError))
                )
            }
            stateAction = .completeClosing
        case .draining(let draining):
            // Already draining - duplicate event, ignore
            self = .init(state: .draining(draining))
            stateAction = .alreadyClosing
        case .closed(let closed):
            // Already closed - duplicate event, ignore
            self = .init(state: .closed(closed))
            stateAction = .alreadyClosing
        }

        return (stateAction, errorAction)
    }

    enum AbruptCloseAction: ~Copyable {
        case closeImmediately
        case tearDownState
        case alreadyClosed
    }

    mutating func abruptClose() -> AbruptCloseAction {
        switch consume self.state {
        case .connecting(let connecting):
            self = .init(state: .closed(.init(abruptCloseFrom: connecting)))
            return .closeImmediately
        case .connected(let connected):
            self = .init(state: .closed(.init(abruptCloseFrom: connected)))
            return .tearDownState
        case .closing(let closing):
            self = .init(state: .closed(.init(abruptCloseFrom: closing)))
            return .tearDownState
        case .draining(let draining):
            self = .init(state: .closed(.init(abruptCloseFrom: draining)))
            return .tearDownState
        case .closed(let closed):
            self = .init(state: .closed(closed))
            return .alreadyClosed
        }
    }

    enum InitiateCloseAction: ~Copyable {
        case sendCloseFrame
        case alreadyClosing
    }

    mutating func initiateClose(sendApplicationClose: Bool, errorCode: Int64, reason: String) -> InitiateCloseAction {
        switch consume self.state {
        case .connecting(let connecting):
            // Close before fully connected
            self = .init(state: .closed(.init(abruptCloseFrom: connecting)))
            return .sendCloseFrame
        case .connected(let connected):
            // Normal close from connected state - go to closing (actively sending CONNECTION_CLOSE)
            self = .init(state: .closing(.init(from: connected)))
            return .sendCloseFrame
        case .closing(let closing):
            self = .init(state: .closing(closing))
            return .alreadyClosing
        case .draining(let draining):
            self = .init(state: .draining(draining))
            return .alreadyClosing
        case .closed(let closed):
            self = .init(state: .closed(closed))
            return .alreadyClosing
        }
    }

    enum CompleteDrainingAction: ~Copyable {
        case finalizeClosure
        case alreadyClosed
        case notDraining
    }

    mutating func completeDraining() -> CompleteDrainingAction {
        switch consume self.state {
        case .connecting(let connecting):
            self = .init(state: .connecting(connecting))
            return .notDraining
        case .connected(let connected):
            self = .init(state: .connected(connected))
            return .notDraining
        case .closing(let closing):
            // Complete the closing process - local closes are always clean
            self = .init(state: .closed(.init(from: closing, closeReason: .clean, connectionError: nil)))
            return .finalizeClosure
        case .draining(let draining):
            self = .init(state: .closed(.init(from: draining)))
            return .finalizeClosure
        case .closed(let closed):
            self = .init(state: .closed(closed))
            return .alreadyClosed
        }
    }

    enum OutOfBandWriteRequestAction: ~Copyable {
        case unexpectedRequest
        case triggerEvent(on: any Channel)
        case ignoreRequest
    }

    mutating func receiveOutOfBandWriteRequest(connectionChannel: (any Channel)?) -> OutOfBandWriteRequestAction {
        switch consume self.state {
        case .connecting(let connecting):
            self = .init(state: .connecting(connecting))
            // It is expected that the channel is not always set at this time. Best-effort write.
            guard let connectionChannel else {
                return .ignoreRequest
            }
            return .triggerEvent(on: connectionChannel)
        case .connected(let connected):
            self = .init(state: .connected(connected))
            guard let connectionChannel else {
                return .unexpectedRequest
            }
            return .triggerEvent(on: connectionChannel)
        case .closing(let closing):
            self = .init(state: .closing(closing))
            guard let connectionChannel else {
                return .unexpectedRequest
            }
            return .triggerEvent(on: connectionChannel)
        case .draining(let draining):
            self = .init(state: .draining(draining))
            guard let connectionChannel else {
                return .unexpectedRequest
            }
            return .triggerEvent(on: connectionChannel)
        case .closed(let closed):
            self = .init(state: .closed(closed))
            return .unexpectedRequest
        }
    }
}
