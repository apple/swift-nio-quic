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
import NIOEmbedded
import Testing

@testable import NIOQUIC

struct ConnectionAdmissionControllerTests {
    @Test
    func unboundedAlwaysAccepts() {
        var controller = ConnectionAdmissionController(
            activeLimit: nil,
            handshakeLimit: nil,
            newConnectionRateLimit: nil,
            eventLoop: EmbeddedEventLoop()
        )
        for _ in 0..<1_000 {
            #expect(controller.acceptNewConnection() == .accept)
        }
    }

    @Test
    func activeLimitDropsAtTheLimit() {
        var controller = ConnectionAdmissionController(
            activeLimit: 2,
            handshakeLimit: nil,
            newConnectionRateLimit: nil,
            eventLoop: EmbeddedEventLoop()
        )

        #expect(controller.acceptNewConnection() == .accept)
        #expect(controller.acceptNewConnection() == .accept)
        #expect(controller.acceptNewConnection() == .drop(.activeLimitReached))
    }

    @Test
    func handshakeLimitDropsEvenWhenActiveLimitHasRoom() {
        var controller = ConnectionAdmissionController(
            activeLimit: 10,
            handshakeLimit: 1,
            newConnectionRateLimit: nil,
            eventLoop: EmbeddedEventLoop()
        )

        #expect(controller.acceptNewConnection() == .accept)
        #expect(controller.acceptNewConnection() == .drop(.handshakeLimitReached))
    }

    @Test
    func handshakeLimitAcceptsAgainOnceAHandshakeFinishes() {
        var controller = ConnectionAdmissionController(
            activeLimit: 10,
            handshakeLimit: 1,
            newConnectionRateLimit: nil,
            eventLoop: EmbeddedEventLoop()
        )

        #expect(controller.acceptNewConnection() == .accept)
        #expect(controller.acceptNewConnection() == .drop(.handshakeLimitReached))

        // The first connection is still active, but no longer counts against the handshake limit.
        controller.finishedHandshake()
        #expect(controller.acceptNewConnection() == .accept)
    }

    @Test
    func activeLimitReachedDropsBeforeHandshakeLimitIsChecked() {
        var controller = ConnectionAdmissionController(
            activeLimit: 1,
            handshakeLimit: 5,
            newConnectionRateLimit: nil,
            eventLoop: EmbeddedEventLoop()
        )
        #expect(controller.acceptNewConnection() == .accept)
        controller.finishedHandshake()

        // Handshake count is 0 (well under the limit of 5), but the active limit still binds.
        #expect(controller.acceptNewConnection() == .drop(.activeLimitReached))
    }

    @Test
    func rateLimitDropsIndependentlyOfCounts() {
        var controller = ConnectionAdmissionController(
            activeLimit: nil,
            handshakeLimit: nil,
            newConnectionRateLimit: 1,
            eventLoop: EmbeddedEventLoop()
        )

        #expect(controller.acceptNewConnection() == .accept)
        #expect(controller.acceptNewConnection() == .drop(.rateLimited))
    }

    @Test
    func rateLimitNeverRefillsFasterThanConfigured() {
        let eventLoop = EmbeddedEventLoop()
        var controller = ConnectionAdmissionController(
            activeLimit: nil,
            handshakeLimit: nil,
            newConnectionRateLimit: 3,
            eventLoop: eventLoop
        )
        for _ in 0..<3 {
            #expect(controller.acceptNewConnection() == .accept)
        }

        // 1s / 3 is 333_333_333.3ns, so a token takes 333_333_334ns to come back.
        eventLoop.advanceTime(by: .nanoseconds(333_333_333))
        #expect(controller.acceptNewConnection() == .drop(.rateLimited))
        eventLoop.advanceTime(by: .nanoseconds(1))
        #expect(controller.acceptNewConnection() == .accept)
    }

    @Test
    func zeroRateLimitRejectsEveryConnection() {
        let eventLoop = EmbeddedEventLoop()
        var controller = ConnectionAdmissionController(
            activeLimit: nil,
            handshakeLimit: nil,
            newConnectionRateLimit: 0,
            eventLoop: eventLoop
        )
        #expect(controller.acceptNewConnection() == .drop(.rateLimited))

        // No amount of time refills a bucket without capacity.
        eventLoop.advanceTime(by: .hours(1))
        #expect(controller.acceptNewConnection() == .drop(.rateLimited))
    }

    @Test
    func countLimitsTakePriorityOverRateLimit() {
        var controller = ConnectionAdmissionController(
            activeLimit: 1,
            handshakeLimit: 1,
            newConnectionRateLimit: 1,
            eventLoop: EmbeddedEventLoop()
        )

        #expect(controller.acceptNewConnection() == .accept)

        // All limits are hit, the active limit will be checked first and reported.
        #expect(controller.acceptNewConnection() == .drop(.activeLimitReached))
    }

    @Test
    func countLimitRejectionDoesNotConsumeARateLimitToken() {
        var controller = ConnectionAdmissionController(
            activeLimit: 1,
            handshakeLimit: nil,
            newConnectionRateLimit: 2,
            eventLoop: EmbeddedEventLoop()
        )

        #expect(controller.acceptNewConnection() == .accept)
        // The active limit was reached.
        #expect(controller.acceptNewConnection() == .drop(.activeLimitReached))
        #expect(controller.acceptNewConnection() == .drop(.activeLimitReached))
        #expect(controller.acceptNewConnection() == .drop(.activeLimitReached))

        controller.finishedHandshake()
        controller.closingConnection()
        // The rate limiter's second token is still there: unaffected by the rejections above.
        #expect(controller.acceptNewConnection() == .accept)
    }

    @Test
    func closingConnectionAfterHandshakeFreesOnlyTheActiveSlot() {
        var controller = ConnectionAdmissionController(
            activeLimit: 1,
            handshakeLimit: 1,
            newConnectionRateLimit: nil,
            eventLoop: EmbeddedEventLoop()
        )

        #expect(controller.acceptNewConnection() == .accept)
        controller.finishedHandshake()
        #expect(controller.acceptNewConnection() == .drop(.activeLimitReached))

        controller.closingConnection()
        #expect(controller.acceptNewConnection() == .accept)
    }
}
