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

struct ConnectionAdmissionControllerTests {
    @Test
    func unboundedAlwaysAccepts() {
        var controller = ConnectionAdmissionController(activeLimit: 0, handshakeLimit: 0, newConnectionRateLimit: 0)
        for _ in 0..<1_000 {
            #expect(controller.acceptNewConnection() == .accept)
        }
    }

    @Test
    func activeLimitDropsAtTheLimit() {
        var controller = ConnectionAdmissionController(activeLimit: 2, handshakeLimit: 0, newConnectionRateLimit: 0)

        #expect(controller.acceptNewConnection() == .accept)
        #expect(controller.acceptNewConnection() == .accept)
        #expect(controller.acceptNewConnection() == .drop(.activeLimitReached))
    }

    @Test
    func handshakeLimitDropsEvenWhenActiveLimitHasRoom() {
        var controller = ConnectionAdmissionController(activeLimit: 10, handshakeLimit: 1, newConnectionRateLimit: 0)

        #expect(controller.acceptNewConnection() == .accept)
        #expect(controller.acceptNewConnection() == .drop(.handshakeLimitReached))
    }

    @Test
    func activeLimitReachedDropsBeforeHandshakeLimitIsChecked() {
        var controller = ConnectionAdmissionController(activeLimit: 1, handshakeLimit: 5, newConnectionRateLimit: 0)
        #expect(controller.acceptNewConnection() == .accept)
        controller.finishedHandshake()

        // Handshake count is 0 (well under the limit of 5), but the active limit still binds.
        #expect(controller.acceptNewConnection() == .drop(.activeLimitReached))
    }

    @Test
    func rateLimitDropsIndependentlyOfCounts() {
        var controller = ConnectionAdmissionController(activeLimit: 0, handshakeLimit: 0, newConnectionRateLimit: 1)

        #expect(controller.acceptNewConnection() == .accept)
        #expect(controller.acceptNewConnection() == .drop(.rateLimited))
    }

    @Test
    func countLimitsTakePriorityOverRateLimit() {
        var controller = ConnectionAdmissionController(activeLimit: 1, handshakeLimit: 1, newConnectionRateLimit: 1)

        #expect(controller.acceptNewConnection() == .accept)

        // All limits are hit, the active limit will be checked first and reported.
        #expect(controller.acceptNewConnection() == .drop(.activeLimitReached))
    }

    @Test
    func countLimitRejectionDoesNotConsumeARateLimitToken() {
        var controller = ConnectionAdmissionController(activeLimit: 1, handshakeLimit: 0, newConnectionRateLimit: 2)

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
        var controller = ConnectionAdmissionController(activeLimit: 1, handshakeLimit: 1, newConnectionRateLimit: 0)

        #expect(controller.acceptNewConnection() == .accept)
        controller.finishedHandshake()
        #expect(controller.acceptNewConnection() == .drop(.activeLimitReached))

        controller.closingConnection()
        #expect(controller.acceptNewConnection() == .accept)
    }
}
