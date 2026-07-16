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

import Testing

@testable import NIOQUIC

struct QUICConnectionChannelLifecycleTests {
    @available(anyAppleOS 26, *)
    typealias StateTag = QUICConnectionChannel.Lifecycle.StateTag

    @available(anyAppleOS 26, *)
    @Test
    func initializeFromIdle() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.idle)
        #expect(lifecycle.initialize() == true)
        #expect(lifecycle.state.tag == .initializing)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: [StateTag.initializing, .initialized, .activated, .closing, .closed])
    func initializeFromOtherStatesIsRejected(initialState: StateTag) {
        var lifecycle = QUICConnectionChannel.Lifecycle(initialState)
        #expect(lifecycle.initialize() == false)
        #expect(lifecycle.state.tag == initialState)
    }

    @available(anyAppleOS 26, *)
    @Test
    func initializedFromInitializing() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.initializing)
        #expect(lifecycle.initialized() == .awaitingActivation)
        #expect(lifecycle.state.tag == .initialized)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: [StateTag.closing, .closed])
    func initializedFromCloseStatesIsNoOp(initialState: StateTag) {
        var lifecycle = QUICConnectionChannel.Lifecycle(initialState)
        #expect(lifecycle.initialized() == .closedDuringInit)
        #expect(lifecycle.state.tag == initialState)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: [StateTag.idle, .initializing, .initialized, .activated])
    func beginClosingFromOpenStates(initialState: StateTag) {
        var lifecycle = QUICConnectionChannel.Lifecycle(initialState)
        #expect(lifecycle.beginClosing(error: nil) == .beganClosing)
        #expect(lifecycle.state.tag == .closing)
    }

    @available(anyAppleOS 26, *)
    @Test
    func beginClosingFromClosingIsAlreadyClosing() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.closing)
        #expect(lifecycle.beginClosing(error: nil) == .alreadyClosing)
        #expect(lifecycle.state.tag == .closing)
    }

    @available(anyAppleOS 26, *)
    @Test
    func beginClosingFromClosedIsAlreadyClosed() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.closed)
        #expect(lifecycle.beginClosing(error: nil) == .alreadyClosed)
        #expect(lifecycle.state.tag == .closed)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: [StateTag.idle, .initializing, .initialized, .activated, .closing])
    func closedFromAnyOpenState(initialState: StateTag) {
        var lifecycle = QUICConnectionChannel.Lifecycle(initialState)
        lifecycle.closed()
        #expect(lifecycle.state.tag == .closed)
    }

    @available(anyAppleOS 26, *)
    @Test
    func closedFromClosedIsIdempotent() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.closed)
        lifecycle.closed()
        #expect(lifecycle.state.tag == .closed)
    }

    @available(anyAppleOS 26, *)
    @Test
    func happyPath() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.idle)
        #expect(lifecycle.initialize() == true)
        #expect(lifecycle.initialized() == .awaitingActivation)
        lifecycle.connectionActivated()
        #expect(lifecycle.reconcile().isFireActive)
        #expect(lifecycle.state.tag == .activated)
        #expect(lifecycle.beginClosing(error: nil) == .beganClosing)
        lifecycle.closed()
        #expect(lifecycle.state.tag == .closed)
    }

    // MARK: - reconcile()

    @available(anyAppleOS 26, *)
    @Test
    func reconcileWithNothingPending() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.idle)
        #expect(lifecycle.reconcile() == nil)
    }

    @available(anyAppleOS 26, *)
    @Test
    func activationPendingUntilInitialized() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.idle)
        _ = lifecycle.initialize()
        lifecycle.connectionActivated()
        #expect(lifecycle.reconcile() == nil)
        #expect(lifecycle.state.tag == .initializing)

        // Reoncile causes the transition.
        lifecycle.initialized()
        #expect(lifecycle.reconcile().isFireActive)
        #expect(lifecycle.state.tag == .activated)
        // Consumed: no repeat activation.
        #expect(lifecycle.reconcile() == nil)
    }

    @available(anyAppleOS 26, *)
    @Test
    func activationThenCloseFireInOrder() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.idle)
        _ = lifecycle.initialize()
        lifecycle.initialized()
        lifecycle.connectionActivated()
        lifecycle.connectionClosed(error: nil)

        // Activation must happen before inactive.
        #expect(lifecycle.reconcile().isFireActive)
        #expect(lifecycle.reconcile().isFireInactive)
        #expect(lifecycle.reconcile() == nil)
    }

    @available(anyAppleOS 26, *)
    @Test
    func closeErrorIsCarriedThrough() {
        struct Boom: Error {}
        var lifecycle = QUICConnectionChannel.Lifecycle(.idle)
        lifecycle.connectionClosed(error: Boom())
        switch lifecycle.reconcile() {
        case .fireInactive(let error):
            #expect(error is Boom)
        case .fireActive, .none:
            Issue.record("expected .fireInactive")
        }
    }

    // MARK: - Inactive defer

    @available(anyAppleOS 26, *)
    @Test
    func driveFiresInactiveWhenNoInitializersInFlight() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.idle)
        lifecycle.connectionClosed(error: nil)
        #expect(lifecycle.reconcile().isFireInactive)
    }

    @available(anyAppleOS 26, *)
    @Test
    func driveInactiveIsIdempotent() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.idle)
        lifecycle.connectionClosed(error: nil)
        #expect(lifecycle.reconcile().isFireInactive)
        #expect(lifecycle.reconcile() == nil)
    }

    @available(anyAppleOS 26, *)
    @Test
    func inactiveDefersUntilInitializersDrain() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.idle)
        lifecycle.willInitializeStream()
        lifecycle.willInitializeStream()
        lifecycle.connectionClosed(error: nil)

        // Held while initializers are in flight.
        #expect(lifecycle.reconcile() == nil)
        lifecycle.streamInitializerFinished()
        #expect(lifecycle.reconcile() == nil)
        // Fires once the last one drains.
        lifecycle.streamInitializerFinished()
        #expect(lifecycle.reconcile().isFireInactive)
    }

    @available(anyAppleOS 26, *)
    @Test
    func deferredInactiveCarriesError() {
        struct Boom: Error {}
        var lifecycle = QUICConnectionChannel.Lifecycle(.idle)
        lifecycle.willInitializeStream()
        lifecycle.connectionClosed(error: Boom())
        #expect(lifecycle.reconcile() == nil)

        lifecycle.streamInitializerFinished()
        switch lifecycle.reconcile() {
        case .fireInactive(let error):
            #expect(error is Boom)
        case .fireActive, .none:
            Issue.record("expected .fireInactive")
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func beginClosingErrorIsCarriedThrough() {
        struct Boom: Error {}
        var lifecycle = QUICConnectionChannel.Lifecycle(.activated)
        lifecycle.beginClosing(error: Boom())
        switch lifecycle.reconcile() {
        case .fireInactive(let error):
            #expect(error is Boom)
        case .fireActive, .none:
            Issue.record("expected .fireInactive")
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func beginClosingMovesToClosingAndDriveFires() {
        // A close from an active channel lands on `.closing`, then reconcile fires inactive.
        var lifecycle = QUICConnectionChannel.Lifecycle(.activated)
        #expect(lifecycle.beginClosing(error: nil) == .beganClosing)
        #expect(lifecycle.state.tag == .closing)
        #expect(lifecycle.reconcile().isFireInactive)
    }

    @available(anyAppleOS 26, *)
    @Test
    func driveAfterClosedIsNoOp() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.closed)
        lifecycle.connectionClosed(error: nil)
        #expect(lifecycle.reconcile() == nil)
        #expect(lifecycle.state.tag == .closed)
    }

    // MARK: - forceClosing()

    @available(anyAppleOS 26, *)
    @Test(arguments: [StateTag.idle, .initializing, .initialized, .activated])
    func forceClosingFromOpenStatesForcesThroughNow(initialState: StateTag) {
        var lifecycle = QUICConnectionChannel.Lifecycle(initialState)
        #expect(lifecycle.forceClosing() == .forceThroughNow)
        #expect(lifecycle.state.tag == .closing)
    }

    @available(anyAppleOS 26, *)
    @Test
    func forceClosingFromClosingWithoutInactiveCommittedForcesThroughNow() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.closing)
        #expect(lifecycle.forceClosing() == .forceThroughNow)
        #expect(lifecycle.state.tag == .closing)
    }

    @available(anyAppleOS 26, *)
    @Test
    func forceClosingWhileInactiveDeferredForcesThroughNow() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.idle)
        lifecycle.willInitializeStream()
        lifecycle.connectionClosed(error: nil)
        #expect(lifecycle.reconcile() == nil)
        #expect(lifecycle.forceClosing() == .forceThroughNow)
    }

    @available(anyAppleOS 26, *)
    @Test
    func forceClosingAfterDeferredInactiveFiredDoesNotReCommit() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.idle)
        lifecycle.willInitializeStream()
        lifecycle.connectionClosed(error: nil)
        #expect(lifecycle.reconcile() == nil)
        lifecycle.streamInitializerFinished()
        #expect(lifecycle.reconcile().isFireInactive)

        #expect(lifecycle.forceClosing() == .alreadyCommitted)
    }

    @available(anyAppleOS 26, *)
    @Test
    func forceClosingAfterInactiveFiredDoesNotReCommit() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.activated)
        lifecycle.connectionClosed(error: nil)
        #expect(lifecycle.reconcile().isFireInactive)

        #expect(lifecycle.forceClosing() == .alreadyCommitted)
        #expect(lifecycle.state.tag == .closing)
    }

    @available(anyAppleOS 26, *)
    @Test
    func forceClosingFromClosedIsAlreadyClosed() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.closed)
        #expect(lifecycle.forceClosing() == .alreadyClosed)
        #expect(lifecycle.state.tag == .closed)
    }

    @available(anyAppleOS 26, *)
    @Test
    func forceClosingSetsInactiveFiredSoASecondForceIsAlreadyCommitted() {
        var lifecycle = QUICConnectionChannel.Lifecycle(.activated)
        #expect(lifecycle.forceClosing() == .forceThroughNow)
        #expect(lifecycle.forceClosing() == .alreadyCommitted)
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel.Lifecycle {
    // Like Lifecycle.State, but without associated data. Allows tests to do
    // equality checks on the tag (associated data only includes errors).
    enum StateTag {
        case idle
        case initializing
        case initialized
        case activated
        case closing
        case closed
    }

    init(_ desiredState: StateTag) {
        self = Self()

        switch desiredState {
        case .idle:
            ()
        case .initializing:
            _ = self.initialize()
        case .initialized:
            _ = self.initialize()
            self.initialized()
        case .activated:
            _ = self.initialize()
            self.initialized()
            self.connectionActivated()
            _ = self.reconcile()
        case .closing:
            _ = self.initialize()
            self.beginClosing(error: nil)
        case .closed:
            _ = self.initialize()
            self.beginClosing(error: nil)
            self.closed()
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel.Lifecycle.State {
    fileprivate var tag: QUICConnectionChannel.Lifecycle.StateTag {
        switch self {
        case .idle:
            return .idle
        case .initializing:
            return .initializing
        case .initialized:
            return .initialized
        case .activated:
            return .activated
        case .closing:
            return .closing
        case .closed:
            return .closed
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel.Lifecycle.Action? {
    fileprivate var isFireActive: Bool {
        switch self {
        case .fireActive:
            return true
        case .fireInactive, .none:
            return false
        }
    }

    fileprivate var isFireInactive: Bool {
        switch self {
        case .fireInactive:
            return true
        case .fireActive, .none:
            return false
        }
    }
}
