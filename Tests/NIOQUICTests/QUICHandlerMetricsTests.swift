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

import Foundation
import Logging
import NIOCore
import NIOEmbedded
import XCTest

@testable import NIOQUIC

@available(anyAppleOS 26, *)
final class QUICHandlerMetricsTests: XCTestCase {
    private typealias Connection = QUICConnectionChannel<QUICStreamChannels>
    private enum TestError: Error { case initializationFailed, notCompleted }

    private static func serverConfiguration(
        publicKey: String = QUICHandlerMetricsTests.testPublicKeyPath,
        privateKey: String = QUICHandlerMetricsTests.testPrivateKeyPath
    ) -> QUICConfiguration {
        .server(
            serverName: "quic-test.local",
            authenticationConfiguration: .rawPublicKeys(publicKeyFilePath: publicKey, privateKeyFilePath: privateKey),
            applicationProtocols: []
        )
    }

    private final class Fixture {
        let loop = EmbeddedEventLoop()
        let channel: EmbeddedChannel
        let handler: QUICHandler<QUICStreamChannels>
        let connections: NIOLoopBoundBox<[Connection]>
        let handle: QUICHandlerHandle

        init(
            configuration: QUICConfiguration = QUICHandlerMetricsTests.serverConfiguration(),
            limits: QUICConnectionLimits = .unlimited,
            multiplexed: Bool = false,
            initializer: @escaping @Sendable (any Channel) -> EventLoopFuture<Void> = {
                $0.eventLoop.makeSucceededVoidFuture()
            }
        ) throws {
            let channel = EmbeddedChannel(loop: self.loop)
            channel.localAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 1234)
            let connections = NIOLoopBoundBox([Connection](), eventLoop: self.loop)
            self.channel = channel
            self.connections = connections
            if multiplexed {
                (self.handler, _) = try QUICHandler<QUICStreamChannels>.makeHandlerAndConnectionMultiplexer(
                    channel: channel,
                    quicConfiguration: configuration,
                    connectionLimits: limits,
                    logger: Logger(label: "handler-metrics"),
                    inboundStreamChannelInitializer: { $0.eventLoop.makeSucceededVoidFuture() }
                )
            } else {
                self.handler = QUICHandler(
                    channel: channel,
                    quicConfiguration: configuration,
                    connectionLimits: limits,
                    asyncVerifier: nil,
                    authenticator: nil,
                    logger: Logger(label: "handler-metrics"),
                    inboundConnectionInitializer: { channel, _ in
                        connections.value.append(channel as! Connection)
                        return initializer(channel)
                    },
                    inboundStreamInitializer: { $0.eventLoop.makeSucceededVoidFuture() },
                    noMoreConnections: {}
                )
            }
            try channel.pipeline.syncOperations.addHandler(self.handler)
            self.handle = self.handler.makeHandle()
        }

        @discardableResult
        func initial(_ destinationID: QUICConnectionID? = nil) throws -> QUICConnectionID {
            var rng: any RandomNumberGenerator = SystemRandomNumberGenerator()
            let id = destinationID ?? .random(using: &rng)
            let packet = QUICPackets.initial(
                destinationID: id,
                sourceID: .random(using: &rng),
                token: [],
                version: 1
            )
            self.channel.pipeline.fireChannelRead(
                AddressedEnvelope(
                    remoteAddress: try SocketAddress(ipAddress: "127.0.0.1", port: 443),
                    data: ByteBuffer(bytes: packet)
                )
            )
            self.channel.pipeline.fireChannelReadComplete()
            return id
        }

        func outbound(
            initializer: @escaping @Sendable (any Channel) -> EventLoopFuture<Void> = {
                $0.eventLoop.makeSucceededVoidFuture()
            }
        ) throws -> EventLoopFuture<(any Channel, QUICStreamCreator)> {
            let connections = self.connections
            return self.handler.createOutboundConnection(
                serverName: "quic-test.local",
                remoteAddress: try SocketAddress(ipAddress: "127.0.0.1", port: 443),
                connectionInitializer: { channel, _ in
                    connections.value.append(channel as! Connection)
                    return initializer(channel)
                },
                inboundStreamInitializer: { $0.eventLoop.makeSucceededVoidFuture() }
            )
        }

        func completeHandshake(_ index: Int = 0) {
            let connection = self.connections.value[index]
            connection.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 0)
            connection.connectionView.drainOutboundAndReconcileLifecycle()
        }

        func closeConnection(_ index: Int = 0) throws {
            let future = self.connections.value[index].close()
            try self.wait(future)
        }

        func wait<Value>(_ future: EventLoopFuture<Value>) throws -> Value {
            let result = NIOLoopBoundBox<Result<Value, any Error>?>(nil, eventLoop: self.loop)
            future.whenComplete { result.value = $0 }
            self.loop.run()
            guard let completed = result.value else {
                XCTFail("Future did not complete after draining the embedded loop")
                throw TestError.notCompleted
            }
            return try completed.get()
        }

        func finish() throws {
            _ = try self.channel.finish()
            self.loop.run()
        }
    }

    private func snapshot(_ fixture: Fixture) throws -> InternalQUICHandlerMetrics {
        let metrics = try fixture.wait(fixture.handle.currentMetrics())
        XCTAssertEqual(
            metrics.establishmentAttempts,
            metrics.establishmentSuccesses + metrics.establishmentFailures + UInt64(metrics.handshakesInProgress)
        )
        XCTAssertGreaterThanOrEqual(metrics.handshakesInProgress, 0)
        XCTAssertLessThanOrEqual(metrics.handshakesInProgress, metrics.activeConnections)
        return metrics
    }

    func testInitialSnapshot() throws {
        let fixture = try Fixture()
        defer { try! fixture.finish() }
        XCTAssertEqual(
            try self.snapshot(fixture),
            InternalQUICHandlerMetrics(
                establishmentAttempts: 0,
                establishmentSuccesses: 0,
                establishmentFailures: 0,
                activeConnections: 0,
                handshakesInProgress: 0,
                rejectedByActiveLimit: 0,
                rejectedByHandshakeLimit: 0,
                rejectedByRateLimit: 0
            )
        )
    }

    func testHandshakeProgressionAndOriginalConnectionIDAlias() throws {
        let fixture = try Fixture()
        defer { try! fixture.finish() }
        let originalID = try fixture.initial()
        let saved = try self.snapshot(fixture)
        XCTAssertEqual(saved.establishmentAttempts, 1)
        XCTAssertEqual(saved.handshakesInProgress, 1)
        XCTAssertEqual(saved.activeConnections, 1)
        XCTAssertEqual(saved.establishmentSuccesses, 0)
        // The original destination ID aliases the connection's generated local ID.
        try fixture.initial(originalID)
        XCTAssertEqual(try self.snapshot(fixture), saved)
        fixture.completeHandshake()
        let established = try self.snapshot(fixture)
        XCTAssertEqual(established.establishmentSuccesses, 1)
        XCTAssertEqual(established.handshakesInProgress, 0)
        XCTAssertEqual(established.activeConnections, 1)
        try fixture.closeConnection()
        let closed = try self.snapshot(fixture)
        XCTAssertEqual(closed.establishmentSuccesses, 1)
        XCTAssertEqual(closed.establishmentFailures, 0)
        XCTAssertEqual(closed.activeConnections, 0)
        XCTAssertEqual(saved.handshakesInProgress, 1)
    }

    func testRejectionReasonsAndPriority() throws {
        let limits: [QUICConnectionLimits] = [
            .perHandler(activeLimit: 1, handshakeLimit: 1, newConnectionRateLimit: 1),
            .perHandler(activeLimit: 10, handshakeLimit: 1, newConnectionRateLimit: 1),
            .perHandler(activeLimit: 10, handshakeLimit: 10, newConnectionRateLimit: 1),
        ]
        for (index, limit) in limits.enumerated() {
            let fixture = try Fixture(limits: limit)
            defer { try! fixture.finish() }
            try fixture.initial()
            try fixture.initial()
            let metrics = try self.snapshot(fixture)
            XCTAssertEqual(metrics.establishmentAttempts, 1)
            XCTAssertEqual(metrics.establishmentFailures, 0)
            XCTAssertEqual(metrics.activeConnections, 1)
            XCTAssertEqual(metrics.handshakesInProgress, 1)
            XCTAssertEqual(metrics.rejectedByActiveLimit, index == 0 ? 1 : 0)
            XCTAssertEqual(metrics.rejectedByHandshakeLimit, index == 1 ? 1 : 0)
            XCTAssertEqual(metrics.rejectedByRateLimit, index == 2 ? 1 : 0)
        }
    }

    func testRepeatedRejectionsAndRateRefill() throws {
        let fixture = try Fixture(limits: .perHandler(newConnectionRateLimit: 1))
        defer { try! fixture.finish() }
        try fixture.initial()
        let rejectedID = try fixture.initial()
        try fixture.initial(rejectedID)
        XCTAssertEqual(try self.snapshot(fixture).rejectedByRateLimit, 2)
        fixture.loop.advanceTime(by: .seconds(1))
        try fixture.initial(rejectedID)
        let metrics = try self.snapshot(fixture)
        XCTAssertEqual(metrics.establishmentAttempts, 2)
        XCTAssertEqual(metrics.rejectedByRateLimit, 2)
    }

    func testConstructorFailureReleasesMetricsAndAdmissionSlots() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let publicKey = directory.appendingPathComponent("publicKey.der").path
        let privateKey = directory.appendingPathComponent("privateKey.der").path
        let fixture = try Fixture(
            configuration: Self.serverConfiguration(publicKey: publicKey, privateKey: privateKey),
            limits: .perHandler(activeLimit: 1, handshakeLimit: 1)
        )
        defer { try! fixture.finish() }
        try fixture.initial()
        XCTAssertThrowsError(try fixture.channel.throwIfErrorCaught())
        let failed = try self.snapshot(fixture)
        XCTAssertEqual(failed.establishmentAttempts, 1)
        XCTAssertEqual(failed.establishmentFailures, 1)
        XCTAssertEqual(failed.activeConnections, 0)
        try FileManager.default.copyItem(atPath: Self.testPublicKeyPath, toPath: publicKey)
        try FileManager.default.copyItem(atPath: Self.testPrivateKeyPath, toPath: privateKey)
        try fixture.initial()
        let retried = try self.snapshot(fixture)
        XCTAssertEqual(retried.establishmentAttempts, 2)
        XCTAssertEqual(retried.establishmentFailures, 1)
        XCTAssertEqual(retried.handshakesInProgress, 1)
        XCTAssertEqual(retried.rejectedByActiveLimit, 0)
        XCTAssertEqual(retried.rejectedByHandshakeLimit, 0)
    }

    func testInitializerFailure() throws {
        let fixture = try Fixture(initializer: { $0.eventLoop.makeFailedFuture(TestError.initializationFailed) })
        defer { try! fixture.finish() }
        try fixture.initial()
        fixture.loop.run()
        let metrics = try self.snapshot(fixture)
        XCTAssertEqual(metrics.establishmentFailures, 1)
        XCTAssertEqual(metrics.activeConnections, 0)
    }

    func testCloseBeforeHandshakeAndRetainedHandleAfterRemoval() throws {
        let fixture = try Fixture()
        try fixture.initial()
        try fixture.closeConnection()
        let closed = try self.snapshot(fixture)
        XCTAssertEqual(closed.establishmentFailures, 1)
        XCTAssertEqual(closed.activeConnections, 0)
        try fixture.finish()
        XCTAssertEqual(try self.snapshot(fixture), closed)
    }

    func testCloseDuringPendingInitializationIgnoresLateResult() throws {
        for succeeds in [true, false] {
            let loop = EmbeddedEventLoop()
            let promise = loop.makePromise(of: Void.self)
            let fixture = try Fixture(initializer: { _ in promise.futureResult })
            defer { try! fixture.finish() }
            try fixture.initial()
            try fixture.closeConnection()
            let closed = try self.snapshot(fixture)
            XCTAssertEqual(closed.establishmentFailures, 1)
            XCTAssertEqual(closed.activeConnections, 0)
            if succeeds {
                promise.succeed(())
            } else {
                promise.fail(TestError.initializationFailed)
            }
            loop.run()
            fixture.loop.run()
            XCTAssertEqual(try self.snapshot(fixture), closed)
        }
    }

    private static func clientConfiguration(
        publicKey: String = QUICHandlerMetricsTests.testPublicKeyPath
    )
        -> QUICConfiguration
    {
        .client(verificationConfiguration: .rawPublicKeys(publicKeyFilePath: publicKey), applicationProtocols: [])
    }

    func testOutboundSuccessAndClose() throws {
        let fixture = try Fixture(configuration: Self.clientConfiguration())
        defer { try! fixture.finish() }
        let future = try fixture.outbound()
        XCTAssertEqual(try self.snapshot(fixture).handshakesInProgress, 1)
        fixture.completeHandshake()
        _ = try fixture.wait(future)
        XCTAssertEqual(try self.snapshot(fixture).establishmentSuccesses, 1)
        try fixture.closeConnection()
        let closed = try self.snapshot(fixture)
        XCTAssertEqual(closed.establishmentFailures, 0)
        XCTAssertEqual(closed.activeConnections, 0)
    }

    func testOutboundInitializerFailurePreservesError() throws {
        let fixture = try Fixture(configuration: Self.clientConfiguration())
        defer { try! fixture.finish() }
        let future = try fixture.outbound(initializer: { $0.eventLoop.makeFailedFuture(TestError.initializationFailed) }
        )
        fixture.loop.run()
        XCTAssertThrowsError(try fixture.wait(future)) { XCTAssertTrue($0 is TestError) }
        let metrics = try self.snapshot(fixture)
        XCTAssertEqual(metrics.establishmentFailures, 1)
        XCTAssertEqual(metrics.activeConnections, 0)
    }

    func testOutboundConstructorFailure() throws {
        let missingKey = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).path
        let fixture = try Fixture(configuration: Self.clientConfiguration(publicKey: missingKey))
        defer { try! fixture.finish() }
        XCTAssertThrowsError(try fixture.wait(fixture.outbound()))
        let metrics = try self.snapshot(fixture)
        XCTAssertEqual(metrics.establishmentAttempts, 1)
        XCTAssertEqual(metrics.establishmentFailures, 1)
        XCTAssertEqual(metrics.activeConnections, 0)
    }

    func testOutboundPreflightRejectionsDoNotStartAttempts() throws {
        let fixture = try Fixture(configuration: Self.clientConfiguration())
        defer { try! fixture.finish() }
        fixture.channel.localAddress = nil
        XCTAssertThrowsError(try fixture.wait(fixture.outbound()))
        XCTAssertEqual(try self.snapshot(fixture).establishmentAttempts, 0)
        try fixture.wait(fixture.handler.shutdownGracefully(deadline: .now()))
        XCTAssertThrowsError(try fixture.wait(fixture.outbound()))
        XCTAssertEqual(try self.snapshot(fixture).establishmentAttempts, 0)
    }

    func testHandshakeTimeoutFailure() throws {
        let fixture = try Fixture(configuration: Self.clientConfiguration())
        defer { try! fixture.finish() }
        let future = try fixture.outbound()
        let connection = try XCTUnwrap(fixture.connections.value.first)
        connection.connectionView.connectionClosed(error: ChannelError.connectTimeout(.seconds(1)))
        connection.connectionView.drainOutboundAndReconcileLifecycle()
        fixture.loop.run()
        XCTAssertThrowsError(try fixture.wait(future))
        let metrics = try self.snapshot(fixture)
        XCTAssertEqual(metrics.establishmentFailures, 1)
        XCTAssertEqual(metrics.activeConnections, 0)
    }

    func testMultiplexerCountsPendingConnectionAtParentClose() throws {
        let fixture = try Fixture(multiplexed: true)
        defer { try? fixture.finish() }
        try fixture.initial()
        let pending = try self.snapshot(fixture)
        XCTAssertEqual(pending.establishmentAttempts, 1)
        XCTAssertEqual(pending.handshakesInProgress, 1)
        try fixture.finish()
        let closed = try self.snapshot(fixture)
        XCTAssertEqual(closed.establishmentFailures, 1)
        XCTAssertEqual(closed.activeConnections, 0)
    }

    func testStreamConsumerEstablishmentAndClose() throws {
        let loop = EmbeddedEventLoop()
        let channel = EmbeddedChannel(loop: loop)
        defer {
            _ = try? channel.finish(acceptAlreadyClosed: true)
            loop.run()
        }
        channel.localAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 1234)
        let accepted = NIOLoopBoundBox<QUICConnectionChannel<MetricsConsumer>?>(nil, eventLoop: loop)
        let handler = QUICHandler<MetricsConsumer>(
            channel: channel,
            quicConfiguration: Self.serverConfiguration(),
            connectionLimits: .unlimited,
            asyncVerifier: nil,
            authenticator: nil,
            logger: Logger(label: "handler-metrics-consumer")
        ) { connection in
            accepted.value = connection.channel as? QUICConnectionChannel<MetricsConsumer>
            return MetricsConsumer()
        }
        try channel.pipeline.syncOperations.addHandler(handler)
        let handle = handler.makeHandle()
        func snapshot() throws -> InternalQUICHandlerMetrics {
            let result = NIOLoopBoundBox<Result<InternalQUICHandlerMetrics, any Error>?>(nil, eventLoop: loop)
            handle.currentMetrics().whenComplete { result.value = $0 }
            loop.run()
            return try XCTUnwrap(result.value).get()
        }
        var rng: any RandomNumberGenerator = SystemRandomNumberGenerator()
        let packet = QUICPackets.initial(
            destinationID: .random(using: &rng),
            sourceID: .random(using: &rng),
            token: [],
            version: 1
        )
        channel.pipeline.fireChannelRead(
            AddressedEnvelope(
                remoteAddress: try SocketAddress(ipAddress: "127.0.0.1", port: 443),
                data: ByteBuffer(bytes: packet)
            )
        )
        channel.pipeline.fireChannelReadComplete()
        XCTAssertEqual(try snapshot().handshakesInProgress, 1)
        let connection = try XCTUnwrap(accepted.value)
        connection.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 0)
        connection.connectionView.drainOutboundAndReconcileLifecycle()
        XCTAssertEqual(try snapshot().establishmentSuccesses, 1)
        _ = try channel.finish()
        loop.run()
        let closed = try snapshot()
        XCTAssertEqual(closed.activeConnections, 0)
        XCTAssertEqual(closed.handshakesInProgress, 0)
        XCTAssertEqual(closed.establishmentSuccesses, 1)
        XCTAssertEqual(closed.establishmentFailures, 0)
    }
}

@available(anyAppleOS 26, *)
private struct MetricsConsumer: QUICStreamConsumer {
    typealias StreamState = Void

    func makeStreamState(_ stream: inout QUICStream<Self>) {}

    func processStreams(_ streams: inout QUICStreamIterator<Self>) {}
}
