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
    typealias Lifecycle = QUICConnectionChannel.Lifecycle

    @available(anyAppleOS 26, *)
    @Test
    func initializeFromIdleSucceedsAndTransitions() {
        var lifecycle = Lifecycle.idle
        #expect(lifecycle.initialize() == true)
        #expect(lifecycle == .initializing)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: [Lifecycle.initializing, .initialized, .activated, .closing, .closed])
    func initializeFromOtherStatesIsRejected(start: Lifecycle) {
        var lifecycle = start
        #expect(lifecycle.initialize() == false)
        #expect(lifecycle == start)
    }

    @available(anyAppleOS 26, *)
    @Test
    func initializedFromInitializingTransitions() {
        var lifecycle = Lifecycle.initializing
        lifecycle.initialized()
        #expect(lifecycle == .initialized)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: [Lifecycle.closing, .closed])
    func initializedFromCloseStatesIsNoOp(start: Lifecycle) {
        // Channel closed while initializer in flight: stay on the close path.
        var lifecycle = start
        lifecycle.initialized()
        #expect(lifecycle == start)
    }

    @available(anyAppleOS 26, *)
    @Test
    func activatedFromInitializedTransitions() {
        var lifecycle = Lifecycle.initialized
        lifecycle.activated()
        #expect(lifecycle == .activated)
    }

    @available(anyAppleOS 26, *)
    @Test
    func activatedFromClosingIsNoOp() {
        var lifecycle = Lifecycle.closing
        lifecycle.activated()
        #expect(lifecycle == .closing)
    }

    // MARK: - closing()

    @available(anyAppleOS 26, *)
    @Test(arguments: [Lifecycle.idle, .initializing, .initialized, .activated])
    func closingFromOpenStatesTransitions(start: Lifecycle) {
        var lifecycle = start
        lifecycle.closing()
        #expect(lifecycle == .closing)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: [Lifecycle.closing, .closed])
    func closingFromCloseStatesIsNoOp(start: Lifecycle) {
        var lifecycle = start
        lifecycle.closing()
        #expect(lifecycle == start)
    }

    // MARK: - closed()

    @available(anyAppleOS 26, *)
    @Test(arguments: [Lifecycle.idle, .initializing, .initialized, .activated, .closing])
    func closedFromAnyOpenStateMovesToClosed(start: Lifecycle) {
        var lifecycle = start
        lifecycle.closed()
        #expect(lifecycle == .closed)
    }

    @available(anyAppleOS 26, *)
    @Test
    func closedFromClosedIsIdempotent() {
        var lifecycle = Lifecycle.closed
        lifecycle.closed()
        #expect(lifecycle == .closed)
    }

    // MARK: - outboundDataProcessed()

    @available(anyAppleOS 26, *)
    @Test(arguments: [Lifecycle.idle, .initializing, .initialized])
    func outboundDataProcessedReturnsTrueWhileInitializing(state: Lifecycle) {
        #expect(state.outboundDataProcessed() == true)
    }

    @available(anyAppleOS 26, *)
    @Test(arguments: [Lifecycle.activated, .closing, .closed])
    func outboundDataProcessedReturnsFalseAfterInitialization(state: Lifecycle) {
        #expect(state.outboundDataProcessed() == false)
    }

    // MARK: - End-to-end happy-path

    @available(anyAppleOS 26, *)
    @Test
    func happyPathTransitions() {
        var lifecycle = Lifecycle.idle
        let didInitialize = lifecycle.initialize()
        #expect(didInitialize)
        lifecycle.initialized()
        lifecycle.activated()
        lifecycle.closing()
        lifecycle.closed()
        #expect(lifecycle == .closed)
    }
}
