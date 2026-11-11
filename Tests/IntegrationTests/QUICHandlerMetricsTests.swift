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

@available(anyAppleOS 26, *)
final class QUICHandlerMetricsTests: XCTestCase {
    private enum TestError: Error { case initializationFailed, deadlineExceeded }

    private func bounded<Value: Sendable>(_ future: EventLoopFuture<Value>) async throws -> Value {
        let promise = future.eventLoop.makePromise(of: Value.self)
        let completed = NIOLockedValueBox(false)
        let deadline = future.eventLoop.scheduleTask(in: .seconds(15)) {
            let first = completed.withLockedValue { completed in
                if completed { return false }
                completed = true
                return true
            }
            if first { promise.fail(TestError.deadlineExceeded) }
        }
        future.whenComplete { result in
            deadline.cancel()
            let first = completed.withLockedValue { completed in
                if completed { return false }
                completed = true
                return true
            }
            if first { promise.completeWith(result) }
        }
        return try await promise.futureResult.get()
    }

    private func handle(for channel: any Channel) async throws -> QUICHandlerHandle {
        try await self.bounded(
            channel.pipeline.handler(type: QUICHandler<QUICStreamChannels>.self).map { $0.makeHandle() }
        )
    }

    private func waitForMetrics(
        _ handle: QUICHandlerHandle,
        until predicate: (InternalQUICHandlerMetrics) -> Bool
    ) async throws -> InternalQUICHandlerMetrics {
        let deadline = ContinuousClock.now + .seconds(5)
        repeat {
            let metrics = try await self.bounded(handle.currentMetrics())
            XCTAssertEqual(
                metrics.establishmentAttempts,
                metrics.establishmentSuccesses + metrics.establishmentFailures + UInt64(metrics.handshakesInProgress)
            )
            XCTAssertGreaterThanOrEqual(metrics.handshakesInProgress, 0)
            XCTAssertLessThanOrEqual(metrics.handshakesInProgress, metrics.activeConnections)
            if predicate(metrics) { return metrics }
            try await Task.sleep(for: .milliseconds(5))
        } while ContinuousClock.now < deadline
        throw TestError.deadlineExceeded
    }

    private func withEndpoints(
        _ body: (any Channel, any Channel, QUICHandlerHandle, QUICHandlerHandle) async throws -> Void
    ) async throws {
        let server = try await self.bounded(
            createServerChannel(
                eventLoopGroup: MultiThreadedEventLoopGroup.singleton,
                host: "127.0.0.1",
                port: 0,
                logger: Logger(label: "handler-metrics-server"),
                inboundConnectionInitializer: { channel, _ in channel.eventLoop.makeSucceededVoidFuture() },
                noMoreConnections: {}
            )
        )
        defer { server.close(promise: nil) }
        let client = try await self.bounded(
            createClientChannel(
                eventLoopGroup: MultiThreadedEventLoopGroup.singleton,
                host: "127.0.0.1",
                port: 0,
                logger: Logger(label: "handler-metrics-client")
            )
        )
        defer { client.close(promise: nil) }
        let serverHandle = try await self.handle(for: server)
        let clientHandle = try await self.handle(for: client)
        do {
            try await body(server, client, serverHandle, clientHandle)
        } catch {
            _ = try? await self.bounded(client.close())
            _ = try? await self.bounded(server.close())
            throw error
        }
        _ = try? await self.bounded(client.close())
        _ = try? await self.bounded(server.close())
        try await self.bounded(client.closeFuture)
        try await self.bounded(server.closeFuture)
    }

    private func connect(
        client: any Channel,
        server: any Channel,
        initializer: @escaping @Sendable (any Channel, QUICStreamCreator) -> EventLoopFuture<Void> = { channel, _ in
            channel.eventLoop.makeSucceededVoidFuture()
        }
    ) throws -> EventLoopFuture<(any Channel, QUICStreamCreator)> {
        let address = try XCTUnwrap(server.localAddress)
        return client.pipeline.handler(type: QUICHandler<QUICStreamChannels>.self).flatMap { handler in
            handler.createOutboundConnection(
                serverName: "127.0.0.1",
                remoteAddress: address,
                connectionInitializer: initializer,
                inboundStreamInitializer: { $0.eventLoop.makeSucceededVoidFuture() }
            )
        }
    }

    func testLiveCallerSnapshotsAndCompletedCleanup() async throws {
        try await self.withEndpoints { server, client, serverHandle, clientHandle in
            XCTAssertFalse(client.eventLoop.inEventLoop)
            let initial = try await self.bounded(clientHandle.currentMetrics())
            XCTAssertEqual(initial.establishmentAttempts, 0)
            let (connection, _) = try await self.bounded(self.connect(client: client, server: server))
            let clientMetrics = try await self.waitForMetrics(clientHandle) { $0.establishmentSuccesses == 1 }
            let serverMetrics = try await self.waitForMetrics(serverHandle) { $0.establishmentSuccesses == 1 }
            XCTAssertEqual(clientMetrics.establishmentAttempts, 1)
            XCTAssertEqual(serverMetrics.establishmentAttempts, 1)
            XCTAssertEqual(clientMetrics.establishmentFailures, 0)
            XCTAssertEqual(serverMetrics.establishmentFailures, 0)
            XCTAssertEqual(clientMetrics.activeConnections, 1)
            XCTAssertEqual(serverMetrics.activeConnections, 1)
            XCTAssertEqual(clientMetrics.handshakesInProgress, 0)
            XCTAssertEqual(serverMetrics.handshakesInProgress, 0)
            let onLoop = try await self.bounded(client.eventLoop.flatSubmit { clientHandle.currentMetrics() })
            XCTAssertEqual(onLoop, clientMetrics)
            print("QUIC handler client metrics: \(clientMetrics)")
            print("QUIC handler server metrics: \(serverMetrics)")

            try await self.bounded(connection.close())
            let closed = try await self.waitForMetrics(clientHandle) { $0.activeConnections == 0 }
            XCTAssertEqual(closed.establishmentSuccesses, 1)
            XCTAssertEqual(closed.establishmentFailures, 0)
            try await self.bounded(client.close())
            let retained = try await self.bounded(clientHandle.currentMetrics())
            XCTAssertEqual(retained, closed)
            XCTAssertEqual(initial.establishmentAttempts, 0)
        }
    }

    func testMultipleConnectionsAndHandlerIsolation() async throws {
        try await self.withEndpoints { server, client, serverHandle, clientHandle in
            let otherServer = try await self.bounded(
                createServerChannel(
                    eventLoopGroup: MultiThreadedEventLoopGroup.singleton,
                    host: "127.0.0.1",
                    port: 0,
                    logger: Logger(label: "handler-metrics-other"),
                    inboundConnectionInitializer: { channel, _ in channel.eventLoop.makeSucceededVoidFuture() },
                    noMoreConnections: {}
                )
            )
            defer { otherServer.close(promise: nil) }
            let otherHandle = try await self.handle(for: otherServer)
            let (first, _) = try await self.bounded(self.connect(client: client, server: server))
            let (second, _) = try await self.bounded(self.connect(client: client, server: server))
            let serverMetrics = try await self.waitForMetrics(serverHandle) { $0.establishmentSuccesses == 2 }
            let clientMetrics = try await self.waitForMetrics(clientHandle) { $0.establishmentSuccesses == 2 }
            XCTAssertEqual(serverMetrics.establishmentAttempts, 2)
            XCTAssertEqual(serverMetrics.activeConnections, 2)
            XCTAssertEqual(clientMetrics.activeConnections, 2)
            try await self.bounded(first.close())
            let serverAfter = try await self.waitForMetrics(serverHandle) { $0.activeConnections == 1 }
            let clientAfter = try await self.waitForMetrics(clientHandle) { $0.activeConnections == 1 }
            XCTAssertEqual(serverAfter.establishmentSuccesses, 2)
            XCTAssertEqual(clientAfter.establishmentFailures, 0)
            let unrelated = try await self.bounded(otherHandle.currentMetrics())
            XCTAssertEqual(unrelated.establishmentAttempts, 0)
            XCTAssertEqual(unrelated.activeConnections, 0)
            try await self.bounded(second.close())
        }
    }

    func testLiveInitializerFailurePreservesCallerError() async throws {
        try await self.withEndpoints { server, client, _, clientHandle in
            do {
                _ = try await self.bounded(
                    self.connect(client: client, server: server) { channel, _ in
                        channel.eventLoop.makeFailedFuture(TestError.initializationFailed)
                    }
                )
                XCTFail("Initializer failure did not reach the caller")
            } catch TestError.initializationFailed {
                // Preserve the initializer's error rather than substituting a metrics error.
            }
            let metrics = try await self.waitForMetrics(clientHandle) { $0.activeConnections == 0 }
            XCTAssertEqual(metrics.establishmentAttempts, 1)
            XCTAssertEqual(metrics.establishmentFailures, 1)
            XCTAssertEqual(metrics.establishmentSuccesses, 0)
            XCTAssertEqual(metrics.handshakesInProgress, 0)
            print("QUIC handler failed initialization metrics: \(metrics)")
        }
    }

    func testParentShutdownDuringInitializationIgnoresLateResult() async throws {
        try await self.withEndpoints { server, client, _, clientHandle in
            let entered = client.eventLoop.makePromise(of: (any Channel).self)
            let initializer = client.eventLoop.makePromise(of: Void.self)
            let connecting = try self.connect(client: client, server: server) { channel, _ in
                entered.succeed(channel)
                return initializer.futureResult
            }
            let connection = try await self.bounded(entered.futureResult)
            let pending = try await self.bounded(clientHandle.currentMetrics())
            XCTAssertEqual(pending.handshakesInProgress, 1)
            try await self.bounded(client.close())
            try await self.bounded(connection.closeFuture)
            let closed = try await self.waitForMetrics(clientHandle) { $0.activeConnections == 0 }
            XCTAssertEqual(closed.establishmentFailures, 1)
            XCTAssertEqual(closed.handshakesInProgress, 0)
            initializer.succeed(())
            do {
                _ = try await self.bounded(connecting)
                XCTFail("A connection closed during initialization cannot become ready")
            } catch ChannelError.alreadyClosed {
                // The late initializer result must not create a second establishment outcome.
            }
            let late = try await self.bounded(clientHandle.currentMetrics())
            XCTAssertEqual(late, closed)
        }
    }
}
