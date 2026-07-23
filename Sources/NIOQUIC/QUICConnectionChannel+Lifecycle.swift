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

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    /// Lifecycle of a ``QUICConnectionChannel``.
    ///
    /// Tracks a few different things:
    /// - ``state``: the committed channel state (idle → ... → closed).
    /// - ``pending``: pending state that hasn't been applied yet (because the channel
    ///   is draining the connection.)
    /// - ``inFlightInitializers``: the number of stream initializers in flight, used
    ///   to (potentially) hold up channel inactive.
    struct Lifecycle {
        enum State: Equatable {
            case idle
            case initializing
            case initialized
            case activated
            case closing(inactiveFired: Bool)
            case closed
        }

        struct Pending {
            /// The connection activated.
            var activate: Bool
            /// The connection closed.
            var close: PendingClose?

            init() {
                self.activate = false
                self.close = nil
            }
        }

        enum PendingClose {
            case cleanly
            case withError(any Error)

            var error: (any Error)? {
                switch self {
                case .cleanly:
                    return nil
                case .withError(let error):
                    return error
                }
            }
        }

        /// The lifecycle state.
        private(set) var state: State
        // private(set) as tests read the state.

        /// Pending state to be applied by ``reconcile()``.
        private var pending: Pending

        /// The number of inbound stream initializers currently in progress. This is tracked so
        /// that channel inactive is fired after all streams are initialized.
        private var inFlightInitializers: Int

        init() {
            self.state = .idle
            self.pending = Pending()
            self.inFlightInitializers = 0
        }

        /// Move from `.idle` to `.initializing`. Returns whether the transition was valid.
        mutating func initialize() -> Bool {
            switch self.state {
            case .idle:
                self.state = .initializing
                return true
            case .initializing, .initialized, .activated, .closing, .closed:
                return false
            }
        }

        enum OnInitialized: Equatable {
            /// The initializer completed successfully; the channel is waiting to be activated.
            case awaitingActivation
            /// The connection closed while the initializer was running; activation won't fire.
            case closedDuringInit
        }

        /// The user's initializer finished.
        @discardableResult
        mutating func initialized() -> OnInitialized {
            switch self.state {
            case .initializing:
                self.state = .initialized
                return .awaitingActivation

            case .closing, .closed:
                // The channel was closed while the user's initializer was still in flight.
                // Stay on the close path; activation won't fire.
                return .closedDuringInit

            case .idle, .initialized, .activated:
                fatalError("Internal inconsistency")
            }
        }

        enum OnBeginClosing: Equatable {
            /// This call moved the channel onto the close path.
            case beganClosing
            /// A close was already in flight.
            case alreadyClosing
            /// The channel was already fully closed.
            case alreadyClosed
        }

        /// Commit the channel to closing.
        ///
        /// The channel must later call ``reconcile()`` to get the side effects of
        /// closing. The return value dictates whether the channel should proceed with
        /// closing the underlying connection.
        @discardableResult
        mutating func beginClosing(error: (any Error)?) -> OnBeginClosing {
            switch self.state {
            case .idle, .initializing, .initialized, .activated:
                self.pending.close = error.map { .withError($0) } ?? .cleanly
                self.state = .closing(inactiveFired: false)
                return .beganClosing
            case .closing:
                return .alreadyClosing
            case .closed:
                return .alreadyClosed
            }
        }

        /// Mark the channel as fully closed.
        mutating func closed() {
            switch self.state {
            case .idle, .initializing, .initialized, .activated, .closing:
                self.state = .closed
            case .closed:
                ()  // Already closed.
            }
        }

        enum OnForceClose: Equatable {
            /// No close committed to firing `channelInactive` yet; force teardown now.
            case forceThroughNow
            /// Another path already committed to firing `channelInactive` and may be
            /// mid-flight. Don't re-run the completion side effects.
            case alreadyCommitted
            /// The channel was already fully closed.
            case alreadyClosed
        }

        /// Force the channel onto the close path without waiting for a graceful drain.
        ///
        /// The escape hatch: deliberately bypasses ``reconcile()`` (it must not wait on the
        /// stream gate or QUIC ack). Sets `inactiveFired` on the force-through path so the
        /// `.closing` flag stays honest while `completeChannelInactive` runs.
        mutating func forceClosing() -> OnForceClose {
            let onForceClose: OnForceClose

            switch self.state {
            case .idle, .initializing, .initialized, .activated:
                self.state = .closing(inactiveFired: true)
                onForceClose = .forceThroughNow

            case .closing(let inactiveFired):
                if inactiveFired {
                    onForceClose = .alreadyCommitted
                } else {
                    self.state = .closing(inactiveFired: true)
                    onForceClose = .forceThroughNow
                }

            case .closed:
                onForceClose = .alreadyClosed
            }

            return onForceClose
        }

        // MARK: Signals

        /// The connection activated. Call ``reconcile()`` to apply the state change.
        mutating func connectionActivated() {
            self.pending.activate = true
        }

        /// The connection closed spontaneously. Call ``reconcile()`` to apply the state change.
        mutating func connectionClosed(error: (any Error)?) {
            self.pending.close = error.map { .withError($0) } ?? .cleanly
        }

        // MARK: Streams

        /// Record that an inbound stream initializer started.
        ///
        /// `channelInactive` is held until all in-flight initializers finish.
        mutating func willInitializeStream() {
            assert(self.canInitStream, "can't initialize a stream after channelInactive")
            self.inFlightInitializers &+= 1
        }

        private var canInitStream: Bool {
            switch self.state {
            case .idle, .initializing, .initialized, .activated:
                return true
            case .closing(let inactiveFired):
                return !inactiveFired
            case .closed:
                return false
            }
        }

        /// Record that an inbound stream initializer finished.
        ///
        /// Decrements the in-flight initializer count; this is used to hold up channel inactive
        /// on the connection channel while streams are being initialized. At the end of the
        /// outbound packet draining loop the channel should call ``reconcile()`` which may then
        /// lead to the connection becoming inactive.
        mutating func streamInitializerFinished() {
            assert(self.inFlightInitializers > 0)
            self.inFlightInitializers &-= 1
        }

        // MARK: Reconcile

        enum Action {
            /// Fire `channelActive`, complete the ready promise (if applicable), resume reads.
            case fireActive
            /// Fire `channelInactive` after firing the error (if present).
            case fireInactive(error: (any Error)?)
        }

        /// Reconcile the current lifecycle by applying a pending state change.
        ///
        /// When the channel has finished consuming outbound data from the connection it should
        /// call this in a loop and apply side effects until this returns `nil`.
        mutating func reconcile() -> Action? {
            switch self.state {
            case .initialized:
                if self.pending.activate {
                    self.state = .activated
                    self.pending.activate = false
                    return .fireActive
                }  // else: no pending signal

            case .idle, .initializing, .activated, .closing, .closed:
                ()
            }

            guard let close = self.pending.close else { return nil }

            let action: Action?

            switch self.state {
            case .idle, .initializing, .initialized, .activated, .closing(inactiveFired: false):
                if self.inFlightInitializers == 0 {
                    self.pending.close = nil
                    self.state = .closing(inactiveFired: true)
                    action = .fireInactive(error: close.error)
                } else {
                    // Delay the inactive until streams finish initializing.
                    self.state = .closing(inactiveFired: false)
                    action = nil
                }

            case .closing(inactiveFired: true), .closed:
                action = nil
            }

            return action
        }

    }
}
