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

import Metrics
import Testing

@testable import NIOQUIC

extension QUICMetrics {
    fileprivate static func makeTestMetrics() -> Self {
        QUICMetrics(
            connectionMetrics: .init(
                streamDuration: Timer(label: "streamDuration")
            ),
            connectionCloseMetrics: .init(
                receivedPackets: Recorder(label: "receivedPackets"),
                sentPackets: Recorder(label: "sentPackets"),
                lostPackets: Recorder(label: "lostPackets"),
                roundTripTimeInNanoseconds: Timer(label: "roundTripTimeInNanoseconds"),
                congestionWindowInBytes: Recorder(label: "congestionWindowInBytes"),
                deliveryRateInBytesPerSecond: Recorder(label: "deliveryRateInBytesPerSecond"),
                connectionDuration: Timer(label: "connectionDuration")
            ),
            quicConnectionHandlerMetrics: .init(
                openConnections: Meter(label: "openConnections")
            )
        )
    }
}

@Suite("QUICMetrics Copy-on-Write")
struct QUICMetricsCoWTests {
    @Test("Copy without mutation shares storage")
    func testCopyWithoutMutationSharesStorage() {
        let original = QUICMetrics.makeTestMetrics()
        let firstCopy = original
        let secondCopy = original

        #expect(original._storageID == firstCopy._storageID)
        #expect(original._storageID == secondCopy._storageID)
    }

    @Test("Copy then mutation results in separate storage")
    func testMutationWritesNewStorage() {
        let original = QUICMetrics.makeTestMetrics()
        var copy = original

        #expect(original._storageID == copy._storageID)

        copy.connectionMetrics = nil

        #expect(original._storageID != copy._storageID)
    }

    @Test("Mutation does not copy storage if uniquely referenced")
    func testMutationDoesNotCopyStorageWhenUniquelyReferenced() {
        var metrics = QUICMetrics.makeTestMetrics()
        let storageIDBeforeMutation = metrics._storageID

        metrics.connectionMetrics = nil

        #expect(metrics._storageID == storageIDBeforeMutation)
    }

    @Test("Original unchanged after copy")
    func testOriginalUnchangedAfterCopyMutation() {
        let original = QUICMetrics.makeTestMetrics()
        var copy = original

        copy.connectionCloseMetrics = nil
        copy.quicConnectionHandlerMetrics = nil

        #expect(original.connectionCloseMetrics != nil)
        #expect(original.quicConnectionHandlerMetrics != nil)

        #expect(copy.connectionCloseMetrics == nil)
        #expect(copy.quicConnectionHandlerMetrics == nil)
    }
}
