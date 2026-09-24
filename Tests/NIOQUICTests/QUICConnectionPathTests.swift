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

import Logging
import NIOCore
import NIOEmbedded
import Testing

@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
@testable import NIOQUIC

struct QUICConnectionPathTests {
    @available(anyAppleOS 26, *)
    private func makePath(
        isValidated: Bool = false,
        remoteAddress: SocketAddress = try! SocketAddress(ipAddress: "127.0.0.1", port: 9000)
    ) -> QUICConnectionPath<QUICStreamChannels> {
        let eventLoop = EmbeddedEventLoop()
        let context = NetworkContext(
            identifier: "test-context",
            externalScheduler: EventLoopBackedScheduler(eventLoop: eventLoop)
        )
        return QUICConnectionPath(
            role: .server,
            remoteAddress: remoteAddress,
            context: context,
            framePool: .makePool(forGSO: false),
            isValidated: isValidated,
            maxSegments: 1,
            bufferPoolCapacity: 8,
            logger: Logger(label: "test")
        )
    }

    @available(anyAppleOS 26, *)
    @Test
    func freshPathHasNoQueuedData() {
        let path = self.makePath()
        #expect(!path.hasQueuedInboundPackets)
        #expect(!path.hasQueuedOutboundData)
    }

    @available(anyAppleOS 26, *)
    @Test
    func enqueueInboundPacketTracksQueue() {
        let path = self.makePath()
        path.enqueueInboundPacket(ByteBuffer(repeating: 0xAA, count: 100))
        #expect(path.hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func drainInboundFramesEmptiesQueue() {
        let path = self.makePath()
        path.enqueueInboundPacket(ByteBuffer(repeating: 0xAA, count: 50))
        var drained = path.drainInboundFrames(maximumDatagramCount: 10)
        if drained == nil {
            Issue.record("Expected non-nil drained frames")
        } else {
            drained!.finalizeAllFramesAsFailed()
        }
        #expect(!path.hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func drainInboundFramesReturnsNilWhenEmpty() {
        let path = self.makePath()
        let result = path.drainInboundFrames(maximumDatagramCount: 10)
        if result != nil {
            Issue.record("Expected nil when draining empty queue")
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func idlePathDropsOutboundDatagrams() throws {
        let path = self.makePath()

        if let datagrams = try path.getDatagramsToSend(.init(), maximumDatagramCount: 2, minimumDatagramSize: 100) {
            try path.sendDatagrams(.init(), datagrams: datagrams)
        }
        #expect(!path.hasQueuedOutboundData)
    }

    @available(anyAppleOS 26, *)
    @Test
    func detachedPathDropsInboundPackets() {
        let path = self.makePath()
        path.detach()

        path.enqueueInboundPacket(ByteBuffer(repeating: 0xAA, count: 50))
        #expect(!path.hasQueuedInboundPackets)
    }
}
