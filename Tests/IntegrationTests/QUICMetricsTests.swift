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
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import XCTest

@testable import NIOQUIC

private final class MetricsEchoHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        context.writeAndFlush(self.wrapOutboundOut(self.unwrapInboundIn(data)), promise: nil)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let event = event as? ChannelEvent, case .inputClosed = event {
            context.close(promise: nil)
        } else {
            context.fireUserInboundEventTriggered(event)
        }
    }
}

private final class MetricsDropHandler: ChannelOutboundHandler {
    typealias OutboundIn = AddressedEnvelope<ByteBuffer>
    let state: NIOLockedValueBox<(armed: Bool, dropped: Int)>

    init(state: NIOLockedValueBox<(armed: Bool, dropped: Int)>) {
        self.state = state
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let shouldDrop = self.state.withLockedValue { state in
            guard state.armed else { return false }
            state.armed = false
            state.dropped += 1
            return true
        }
        if shouldDrop {
            // Simulate a successful UDP write lost in transit, without retaining the buffer.
            promise?.succeed(())
        } else {
            context.write(data, promise: promise)
        }
    }
}

@available(anyAppleOS 26, *)
final class QUICMetricsTests: XCTestCase {
    private typealias Connection = QUICConnectionChannel<QUICStreamChannels>
    private enum TestError: Error { case initializationFailed, deadlineExceeded }

    private func withEndpoints(
        _ body: (
            any Channel, any Channel, NIOLockedValueBox<[Connection]>, NIOLockedValueBox<(armed: Bool, dropped: Int)>
        ) async throws -> Void
    ) async throws {
        let peers = NIOLockedValueBox<[Connection]>([])
        let drop = NIOLockedValueBox((armed: false, dropped: 0))
        let server = try await createServerChannel(
            eventLoopGroup: MultiThreadedEventLoopGroup.singleton,
            host: "127.0.0.1",
            port: 0,
            logger: Logger(label: "metrics-server"),
            inboundConnectionInitializer: { channel, _ in
                channel.eventLoop.makeCompletedFuture {
                    let peer = try XCTUnwrap(channel as? Connection)
                    peers.withLockedValue { $0.append(peer) }
                }
            },
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(MetricsEchoHandler())
                }
            },
            noMoreConnections: {}
        ).get()
        defer { server.close(promise: nil) }
        let client = try await createClientChannel(
            eventLoopGroup: MultiThreadedEventLoopGroup.singleton,
            host: "127.0.0.1",
            port: 0,
            logger: Logger(label: "metrics-client"),
            udpChannelInitializer: { channel in
                try channel.pipeline.syncOperations.addHandler(MetricsDropHandler(state: drop))
            }
        ).get()
        defer { client.close(promise: nil) }
        let deadline = client.eventLoop.scheduleTask(in: .seconds(15)) {
            XCTFail("Metrics scenario exceeded its deadline")
            client.close(promise: nil)
            server.close(promise: nil)
        }
        defer { deadline.cancel() }
        try await body(server, client, peers, drop)
    }

    private func connect(
        client: any Channel,
        server: any Channel,
        initializer: @escaping @Sendable (any Channel, QUICStreamCreator) -> EventLoopFuture<Void> = { channel, _ in
            channel.eventLoop.makeSucceededVoidFuture()
        }
    ) async throws -> (Connection, QUICStreamCreator) {
        let address = try XCTUnwrap(server.localAddress)
        let result = try await client.pipeline.handler(type: QUICHandler<QUICStreamChannels>.self).flatMap { handler in
            handler.createOutboundConnection(
                serverName: "127.0.0.1",
                remoteAddress: address,
                connectionInitializer: initializer,
                inboundStreamInitializer: { $0.eventLoop.makeSucceededVoidFuture() }
            )
        }.get()
        return (try XCTUnwrap(result.0 as? Connection), result.1)
    }

    // Bound retained futures independently of socket cleanup, including lifecycle regressions.
    private func bounded<Value: Sendable>(_ future: EventLoopFuture<Value>) async throws -> Value {
        let promise = future.eventLoop.makePromise(of: Value.self)
        let completed = NIOLockedValueBox(false)
        let timeout = future.eventLoop.scheduleTask(in: .seconds(10)) {
            let first = completed.withLockedValue { done in
                defer { done = true }
                return !done
            }
            if first { promise.fail(TestError.deadlineExceeded) }
        }
        future.whenComplete { result in
            timeout.cancel()
            let first = completed.withLockedValue { done in
                defer { done = true }
                return !done
            }
            if first { promise.completeWith(result) }
        }
        return try await promise.futureResult.get()
    }

    private func assertUnavailable(_ connection: Connection) async throws {
        let transport = try await self.bounded(connection.currentMetrics())
        let establishment = try await self.bounded(connection.establishmentMetrics())
        XCTAssertNil(transport)
        XCTAssertNil(establishment)
    }

    private func exchange(
        _ creator: QUICStreamCreator,
        connection: Connection,
        label: String,
        chunks: Int = 1,
        armLoss: NIOLockedValueBox<(armed: Bool, dropped: Int)>? = nil
    ) async throws -> InternalQUICConnectionMetrics {
        let stream = try await self.bounded(
            creator.createBidirectionalStream { initializer in
                initializer.channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel(
                        wrappingChannelSynchronously: initializer.channel,
                        configuration: .init(inboundType: ByteBuffer.self, outboundType: ByteBuffer.self)
                    )
                }
            }
        )
        let initial = try await self.bounded(connection.establishmentMetrics())
        XCTAssertNotNil(initial)
        let before = try await self.bounded(connection.currentMetrics())
        var previous = try XCTUnwrap(before)
        try await stream.executeThenClose { inbound, outbound in
            var iterator = inbound.makeAsyncIterator()
            for index in 0..<chunks {
                let payload = ByteBuffer(string: String(repeating: "\(label)-\(index);", count: 1024))
                if index == 0 { armLoss?.withLockedValue { $0.armed = true } }
                try await outbound.write(payload)
                // Query while the stream is open and the response is still outstanding.
                let during = try await self.bounded(connection.currentMetrics())
                XCTAssertNotNil(during)
                var received = ByteBuffer()
                while received.readableBytes < payload.readableBytes {
                    guard let part = try await iterator.next() else {
                        return XCTFail("Echo ended before the complete payload")
                    }
                    received.writeImmutableBuffer(part)
                }
                XCTAssertEqual(received, payload)
                let result = try await self.bounded(connection.currentMetrics())
                let snapshot = try XCTUnwrap(result)
                XCTAssertGreaterThan(snapshot.receivedDatagrams, 0)
                XCTAssertGreaterThan(snapshot.sentPacketAttempts, 0)
                XCTAssertGreaterThanOrEqual(snapshot.receivedDatagrams, previous.receivedDatagrams)
                XCTAssertGreaterThanOrEqual(snapshot.sentPacketAttempts, previous.sentPacketAttempts)
                XCTAssertGreaterThanOrEqual(snapshot.lostPackets, previous.lostPackets)
                XCTAssertGreaterThan(snapshot.congestionWindowInBytes, 0)
                XCTAssertGreaterThanOrEqual(snapshot.currentRTT, .zero)
                XCTAssertLessThan(snapshot.minimumRTT, .seconds(Int64(UInt32.max)))
                XCTAssertGreaterThanOrEqual(snapshot.ecnCapablePacketsSent, previous.ecnCapablePacketsSent)
                XCTAssertGreaterThanOrEqual(
                    snapshot.ecnCapablePacketsAcknowledged,
                    previous.ecnCapablePacketsAcknowledged
                )
                XCTAssertGreaterThanOrEqual(snapshot.ecnMarkedPackets, previous.ecnMarkedPackets)
                XCTAssertGreaterThanOrEqual(snapshot.ecnCapablePacketsLost, previous.ecnCapablePacketsLost)
                let establishment = try await self.bounded(connection.establishmentMetrics())
                XCTAssertEqual(establishment, initial)
                previous = snapshot
            }
        }
        return previous
    }

    func testMetricsConnectionIsolation() async throws {
        try await self.withEndpoints { server, client, _, _ in
            let (first, firstStreams) = try await self.connect(client: client, server: server)
            let (second, secondStreams) = try await self.connect(client: client, server: server)
            XCTAssertFalse(first === second)
            let firstSaved = try await self.exchange(firstStreams, connection: first, label: "first")
            _ = try await self.exchange(secondStreams, connection: second, label: "second")
            let secondEstablishment = try await self.bounded(second.establishmentMetrics())
            let firstAfter = try await self.exchange(firstStreams, connection: first, label: "first-more", chunks: 3)
            XCTAssertGreaterThan(firstAfter.receivedDatagrams, firstSaved.receivedDatagrams)
            XCTAssertGreaterThan(firstAfter.sentPacketAttempts, firstSaved.sentPacketAttempts)
            if firstSaved.ecnCapablePacketsSent > 0 {
                XCTAssertGreaterThan(firstAfter.ecnCapablePacketsSent, firstSaved.ecnCapablePacketsSent)
            }
            try await self.bounded(first.close())
            try await self.assertUnavailable(first)
            _ = try await self.exchange(secondStreams, connection: second, label: "second-still-alive")
            let secondAfter = try await self.bounded(second.establishmentMetrics())
            XCTAssertEqual(secondAfter, secondEstablishment)
        }
    }

    func testMetricsOrderedAroundLocalClose() async throws {
        try await self.withEndpoints { server, client, _, _ in
            let (connection, _) = try await self.connect(client: client, server: server)
            let ordered = connection.eventLoop.flatSubmit {
                let before = connection.currentMetrics().and(connection.establishmentMetrics())
                connection.close(promise: nil)
                let after = connection.currentMetrics().and(connection.establishmentMetrics())
                return before.and(after)
            }
            let (before, after) = try await self.bounded(ordered)
            XCTAssertNotNil(before.0)
            XCTAssertNotNil(before.1)
            XCTAssertNil(after.0)
            XCTAssertNil(after.1)
            try await self.bounded(connection.closeFuture)
            try await self.assertUnavailable(connection)
        }
    }

    func testMetricsAfterPeerClose() async throws {
        try await self.withEndpoints { server, client, peers, _ in
            let (connection, creator) = try await self.connect(client: client, server: server)
            _ = try await self.exchange(creator, connection: connection, label: "peer-close")
            let peer = try XCTUnwrap(peers.withLockedValue { $0.first })
            try await self.bounded(peer.close())
            try await self.bounded(connection.closeFuture)
            try await self.assertUnavailable(connection)
        }
    }

    func testMetricsAfterParentShutdown() async throws {
        try await self.withEndpoints { server, client, _, _ in
            let (connection, _) = try await self.connect(client: client, server: server)
            try await self.bounded(client.close())
            try await self.bounded(connection.closeFuture)
            try await self.assertUnavailable(connection)
        }
    }

    func testMetricsAfterFailedInitialization() async throws {
        try await self.withEndpoints { server, client, _, _ in
            let captured = NIOLockedValueBox<Connection?>(nil)
            do {
                _ = try await self.connect(client: client, server: server) { channel, _ in
                    let connection = channel as! Connection
                    captured.withLockedValue { $0 = connection }
                    return connection.currentMetrics().and(connection.establishmentMetrics()).flatMap { metrics in
                        XCTAssertNil(metrics.0)
                        XCTAssertNil(metrics.1)
                        return channel.eventLoop.makeFailedFuture(TestError.initializationFailed)
                    }
                }
                XCTFail("Connection initialization unexpectedly succeeded")
            } catch TestError.initializationFailed {
                // The initializer failure must remain the reported connection error.
            }
            let connection = try XCTUnwrap(captured.withLockedValue { $0 })
            try await self.bounded(connection.closeFuture)
            try await self.assertUnavailable(connection)
        }
    }

    func testMetricsDuringTransferWithLoss() async throws {
        try await self.withEndpoints { server, client, _, drop in
            let (connection, creator) = try await self.connect(client: client, server: server)
            _ = try await self.exchange(creator, connection: connection, label: "loss", chunks: 3, armLoss: drop)
            XCTAssertEqual(drop.withLockedValue { $0.dropped }, 1)
            // A dropped UDP write is not necessarily ECN-capable; positive ECT-loss
            // accounting is covered separately by SwiftNetwork's controlled harness.
        }
    }
}
