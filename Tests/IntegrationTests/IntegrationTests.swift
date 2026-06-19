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

import Crypto
import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOPosix
import NIOQUICHelpers
import Synchronization
import XCTest

@testable import NIOQUIC

final class IntegrationTests: XCTestCase {
    func testHTTP09Requests() async throws {
        // To open more streams you will need to plumb this through on the QUIC stream options. Max is 8.
        let requestCount = 6

        let (_, serverChannel, serverMultiplexer, clientMultiplexer) = try await makeClientAndServerPair()

        let connection = try await clientMultiplexer.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for await connection in serverMultiplexer.inboundConnections {
                    for await stream in connection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await buffer in inbound {
                                XCTAssertEqual(buffer, .init(string: "GET /foo"))
                                try await outbound.write(.init(string: "<b>Success</b>"))
                                outbound.finish()
                            }
                        }
                    }
                }
            }

            let responses = Atomic(0)
            for _ in 0..<requestCount {
                let stream = try await connection.createBidirectionalStream { streamInitializer in
                    streamInitializer.channel.eventLoop.makeCompletedFuture {
                        try NIOAsyncChannel(
                            wrappingChannelSynchronously: streamInitializer.channel,
                            configuration: .init(
                                isOutboundHalfClosureEnabled: true,
                                inboundType: ByteBuffer.self,
                                outboundType: ByteBuffer.self
                            )
                        )
                    }
                }
                try await stream.executeThenClose { inbound, outbound in
                    try await outbound.write(.init(string: "GET /foo"))
                    outbound.finish()

                    for try await buffer in inbound {
                        XCTAssertEqual(buffer, .init(string: "<b>Success</b>"))
                        responses.wrappingAdd(1, ordering: .sequentiallyConsistent)
                    }
                }
            }

            XCTAssertEqual(responses.load(ordering: .sequentiallyConsistent), requestCount)

            group.cancelAll()
        }
    }

    func testNewStreamAfterStreamIteratorCancelled() async throws {
        /// The server received an inbound stream on a connection after the task consuming inbound streams has been cancelled.
        /// This means the asyncchannel is dropped.
        /// This tests that the quic connection retains ownership and takes care of shutting down the asyncchannel
        /// If it did not, the asycnchannel would fatalError in its deinit
        let (_, serverChannel, serverMultiplexer, clientMultiplexer) = try await makeClientAndServerPair()

        let connection = try await clientMultiplexer.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )

        enum Event {
            case cancelServer
            case makeRequest
        }

        try await withThrowingTaskGroup(of: Void.self) { group in
            let (eventStream, eventContinutation) = AsyncStream.makeStream(of: Event.self)
            group.addTask {
                // Server waiting for incoming connection and expect one request on it
                var connectionIterator = serverMultiplexer.inboundConnections.makeAsyncIterator()
                let firstConnection = await connectionIterator.next()!

                var streamIterator = firstConnection.inboundStreams.makeAsyncIterator()

                eventContinutation.yield(.cancelServer)

                // stream iterator should now be cancelled. Now we send in a request
                eventContinutation.yield(.makeRequest)

                // Stream doesn't come because we're cancelled
                let firstStream = await streamIterator.next()
                XCTAssertNil(firstStream)

                // No more connections
                let nextConnection = await connectionIterator.next()
                XCTAssertNil(nextConnection)

                // test is finished
                eventContinutation.finish()
            }

            // The server will drive the event stream so we can send it requests when it wants
            for await event in eventStream {
                switch event {
                case .cancelServer:
                    group.cancelAll()
                case .makeRequest:
                    // Make one stream, and write a request on it.
                    let stream = try await connection.createBidirectionalStream { streamInitializer in
                        streamInitializer.channel.eventLoop.makeCompletedFuture {
                            try NIOAsyncChannel(
                                wrappingChannelSynchronously: streamInitializer.channel,
                                configuration: .init(
                                    isOutboundHalfClosureEnabled: true,
                                    inboundType: ByteBuffer.self,
                                    outboundType: ByteBuffer.self
                                )
                            )
                        }
                    }
                    try await stream.executeThenClose { _, outbound in
                        try await outbound.write(.init(string: "GET /foo"))
                        outbound.finish()
                    }
                }
            }
        }
    }

    func testShutdownForcefully() async throws {
        let (_, serverChannel, serverMultiplexer, clientMultiplexer) = try await makeClientAndServerPair()

        let connection = try await clientMultiplexer.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for await connection in serverMultiplexer.inboundConnections {
                    for await stream in connection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await buffer in inbound {
                                XCTAssertEqual(buffer, .init(string: "GET /foo"))
                                try await outbound.write(.init(string: "<b>Success</b>"))
                                outbound.finish()
                            }
                        }
                    }
                }
            }

            let stream = try await connection.createBidirectionalStream { streamInitializer in
                streamInitializer.channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel(
                        wrappingChannelSynchronously: streamInitializer.channel,
                        configuration: .init(
                            isOutboundHalfClosureEnabled: true,
                            inboundType: ByteBuffer.self,
                            outboundType: ByteBuffer.self
                        )
                    )
                }
            }
            try await stream.executeThenClose { inbound, outbound in
                try await outbound.write(.init(string: "GET /foo"))

                for try await buffer in inbound {
                    XCTAssertEqual(buffer, .init(string: "<b>Success</b>"))

                    let serverHandle = try await serverChannel.pipeline.handler(type: QUICHandler.self).map {
                        $0.makeHandle()
                    }.get()

                    try await serverHandle.shutdownGracefully(deadline: .now())

                    do {
                        try await serverHandle.shutdownGracefully(deadline: .now())
                    } catch {
                        XCTFail("Starting a graceful shutdown when already shutting down should not fail")
                    }
                }
            }

            group.cancelAll()
        }
    }

    func testCloseInput() async throws {
        let (_, serverChannel, serverMultiplexer, clientMultiplexer) = try await makeClientAndServerPair()

        let connection = try await clientMultiplexer.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for await connection in serverMultiplexer.inboundConnections {
                    for await stream in connection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await buffer in inbound {
                                XCTAssertEqual(buffer, .init(string: "GET /foo"))
                                try await outbound.write(.init(string: "<b>Success</b>"))
                            }
                            outbound.finish()
                        }
                    }
                }
            }

            let stream = try await connection.createBidirectionalStream { streamInitializer in
                streamInitializer.channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel(
                        wrappingChannelSynchronously: streamInitializer.channel,
                        configuration: .init(
                            isOutboundHalfClosureEnabled: true,
                            inboundType: ByteBuffer.self,
                            outboundType: ByteBuffer.self
                        )
                    )
                }
            }
            try await stream.executeThenClose { inbound, outbound in
                try await outbound.write(.init(string: "GET /foo"))

                for try await buffer in inbound {
                    XCTAssertEqual(buffer, .init(string: "<b>Success</b>"))
                    outbound.finish()
                }
            }

            group.cancelAll()
        }
    }

    func testPing() async throws {
        let testingEventLoop = NIOAsyncTestingEventLoop()

        let (clientChannel, serverChannel, serverMultiplexer, clientMultiplexer) =
            try await makeClientAndServerPair(
                testMode: .testing(eventLoop: testingEventLoop),
                maxIdleTimeout: .milliseconds(2000),
                clientKeepAliveTime: .seconds(1)
            )

        let connection = try await withThrowingTaskGroup { group in
            group.addTask {
                while !Task.isCancelled {
                    try await Self.deliverAllBytes(
                        from: clientChannel as! NIOAsyncTestingChannel,
                        to: serverChannel as! NIOAsyncTestingChannel
                    )
                    try await Self.deliverAllBytes(
                        from: serverChannel as! NIOAsyncTestingChannel,
                        to: clientChannel as! NIOAsyncTestingChannel
                    )
                }
            }
            let connection = try await clientMultiplexer.createNewConnection(
                serverName: "127.0.0.1",
                remoteAddress: serverChannel.localAddress!,
                inboundStreamInitializer: { channel in
                    channel.eventLoop.makeCompletedFuture { fatalError() }
                }
            )
            group.cancelAll()
            return connection
        }

        let clientGotResponse = Atomic(false)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                while !Task.isCancelled {
                    try await Self.deliverAllBytes(
                        from: clientChannel as! NIOAsyncTestingChannel,
                        to: serverChannel as! NIOAsyncTestingChannel
                    )
                    try await Self.deliverAllBytes(
                        from: serverChannel as! NIOAsyncTestingChannel,
                        to: clientChannel as! NIOAsyncTestingChannel
                    )
                }
            }

            group.addTask {
                for await connection in serverMultiplexer.inboundConnections {
                    for await stream in connection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await buffer in inbound {
                                XCTAssertEqual(buffer, .init(string: "GET /foo"))
                                // Respond after 3 seconds without blocking the server thread
                                await testingEventLoop.advanceTime(by: .seconds(3))
                                try await outbound.write(.init(string: "<b>Success</b>"))
                                outbound.finish()
                            }
                        }
                    }
                }
            }

            let stream = try await connection.createBidirectionalStream { streamInitializer in
                streamInitializer.channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel(
                        wrappingChannelSynchronously: streamInitializer.channel,
                        configuration: .init(
                            isOutboundHalfClosureEnabled: true,
                            inboundType: ByteBuffer.self,
                            outboundType: ByteBuffer.self
                        )
                    )
                }
            }

            try await stream.executeThenClose { inbound, outbound in
                try await outbound.write(.init(string: "GET /foo"))
                outbound.finish()

                for try await buffer in inbound {
                    XCTAssertEqual(buffer, .init(string: "<b>Success</b>"))
                    _ = clientGotResponse.exchange(true, ordering: .sequentiallyConsistent)
                }
            }

            await testingEventLoop.run()
            group.cancelAll()
        }
    }

    func testHandleVeryLargeInitialPackets() async throws {
        // This is like the HTTP/0.9 test, but we send a massive ALPN payload. This is done
        // to cause the initial packet to be spread across multiple datagrams.
        let requestCount = 10

        let recorder = PacketHeaderRecorderHandler()

        let (_, serverChannel, serverMultiplexer, clientMultiplexer) = try await makeClientAndServerPair(
            extraClientALPNs: Array(repeating: "ignored", count: 1000),
            clientDebugChannelInitializer: { channel in
                try channel.pipeline.syncOperations.addHandler(recorder, position: .first)
            }
        )

        let connection = try await clientMultiplexer.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for await connection in serverMultiplexer.inboundConnections {
                    for await stream in connection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await buffer in inbound {
                                XCTAssertEqual(buffer, .init(string: "GET /foo"))
                                try await outbound.write(.init(string: "<b>Success</b>"))
                                outbound.finish()
                            }
                        }
                    }
                }
            }

            let responses = Atomic(0)
            for _ in 0..<requestCount {
                let stream = try await connection.createBidirectionalStream {
                    let channel = $0.channel
                    return channel.eventLoop.makeCompletedFuture {
                        try NIOAsyncChannel(
                            wrappingChannelSynchronously: channel,
                            configuration: .init(
                                isOutboundHalfClosureEnabled: true,
                                inboundType: ByteBuffer.self,
                                outboundType: ByteBuffer.self
                            )
                        )
                    }
                }
                try await stream.executeThenClose { inbound, outbound in
                    try await outbound.write(.init(string: "GET /foo"))
                    outbound.finish()

                    for try await buffer in inbound {
                        XCTAssertEqual(buffer, .init(string: "<b>Success</b>"))
                        responses.wrappingAdd(1, ordering: .sequentiallyConsistent)
                    }
                }
            }

            XCTAssertEqual(responses.load(ordering: .sequentiallyConsistent), 10)

            group.cancelAll()
        }

        let headers = recorder.headers.withLockedValue { $0 }

        // We expect to see all the initial headers (there may be more than one!) using the same SCID.

        let initialHeaders = headers.prefix(while: { $0.type == .initial })
        let initialSCIDs = Set(initialHeaders.map { $0.sourceConnectionID })
        XCTAssertEqual(initialSCIDs.count, 1)
    }

    // This test verifies that when we send an application close error, we don't
    // mistakenly treat our own sent error as a peer error.
    func testApplicationCloseErrorDirection() async throws {
        let (_, serverChannel, serverMultiplexer, clientMultiplexer) = try await makeClientAndServerPair()

        let serverReceivedStreamPromise = serverChannel.eventLoop.makePromise(of: Void.self)
        let clientErrorHandler = ClientApplicationCloseHandler()
        let connectionChannelBox: NIOLockedValueBox<(any Channel)?> = NIOLockedValueBox(nil)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for await connection in serverMultiplexer.inboundConnections {
                    for await stream in connection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await buffer in inbound {
                                XCTAssertEqual(buffer, .init(string: "GET /foo"))
                                serverReceivedStreamPromise.succeed()
                                // Don't respond - client will close the connection
                            }
                        }
                    }
                }
            }

            let connection = try await clientMultiplexer.createNewConnection(
                serverName: serverChannel.localAddress!.ipAddress!,
                remoteAddress: serverChannel.localAddress!,
                inboundStreamInitializer: { channel in
                    channel.eventLoop.makeCompletedFuture { fatalError() }
                }
            )

            // Create a stream and send a request
            let stream = try await connection.createBidirectionalStream { streamInitializer in
                streamInitializer.channel.eventLoop.makeCompletedFuture {
                    // Capture the connection channel (parent of stream channel) and install error handler
                    let connectionChannel = streamInitializer.channel.parent!
                    connectionChannelBox.withLockedValue { $0 = connectionChannel }
                    try connectionChannel.pipeline.syncOperations.addHandler(clientErrorHandler)

                    return try NIOAsyncChannel(
                        wrappingChannelSynchronously: streamInitializer.channel,
                        configuration: .init(
                            isOutboundHalfClosureEnabled: true,
                            inboundType: ByteBuffer.self,
                            outboundType: ByteBuffer.self
                        )
                    )
                }
            }

            try await stream.executeThenClose { inbound, outbound in
                try await outbound.write(.init(string: "GET /foo"))
                outbound.finish()

                // Wait for server to receive the stream
                try await serverReceivedStreamPromise.futureResult.get()

                // Get the connection channel and trigger an application close FROM the client
                connectionChannelBox.withLockedValue { connectionChannel in
                    connectionChannel?.triggerUserOutboundEvent(
                        NIOQUICHelpers.QUICCloseConnectionEvent(
                            code: NIOQUICHelpers.QUICApplicationErrorCode(42)!,
                            reasonPhrase: "client initiated close"
                        ),
                        promise: nil
                    )
                }
            }

            group.cancelAll()
        }

        // Get the errors caught by the client
        let thrownErrors = await clientErrorHandler.getErrors()

        // Verify that we don't see errors, specifically we don't want to see the outbound error
        // (code 42, reason "client initiated close") being reported as if it came from the peer.
        XCTAssertEqual(thrownErrors.count, 0, "Unexpected error(s): \(thrownErrors)")
    }

    func testIdleTimeout() async throws {
        // Test that idle timeout closes the connection when there's no activity.
        // SwiftNetwork handles idle timeout internally and delivers ETIMEDOUT via disconnected event.
        let idleTimeout: Duration = .seconds(2)

        let (_, serverChannel, serverMultiplexer, clientMultiplexer) =
            try await makeClientAndServerPair(
                maxIdleTimeout: idleTimeout,
                clientKeepAliveTime: nil  // No keepalives - we want idle timeout to fire
            )

        let connectionClosed = Counter()

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Server task - process one request then wait
            group.addTask {
                for await serverConnection in serverMultiplexer.inboundConnections {
                    for await stream in serverConnection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await buffer in inbound {
                                XCTAssertEqual(buffer, .init(string: "GET /foo"))
                                try await outbound.write(.init(string: "<b>Success</b>"))
                                outbound.finish()
                            }
                        }
                        // After first stream, wait for idle timeout then exit
                        try await Task.sleep(for: .seconds(3))
                        connectionClosed.increment()
                        return
                    }
                }
            }

            // Create connection and make one request
            let connection = try await clientMultiplexer.createNewConnection(
                serverName: serverChannel.localAddress!.ipAddress!,
                remoteAddress: serverChannel.localAddress!,
                inboundStreamInitializer: { channel in
                    channel.eventLoop.makeCompletedFuture { fatalError() }
                }
            )

            let stream = try await connection.createBidirectionalStream { streamInitializer in
                streamInitializer.channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel(
                        wrappingChannelSynchronously: streamInitializer.channel,
                        configuration: .init(
                            isOutboundHalfClosureEnabled: true,
                            inboundType: ByteBuffer.self,
                            outboundType: ByteBuffer.self
                        )
                    )
                }
            }

            try await stream.executeThenClose { inbound, outbound in
                try await outbound.write(.init(string: "GET /foo"))
                outbound.finish()

                for try await buffer in inbound {
                    XCTAssertEqual(buffer, .init(string: "<b>Success</b>"))
                }
            }

            // Wait for server task to complete (which waits for idle timeout)
            try await group.waitForAll()
        }

        // After idle timeout, the connection should be closed
        XCTAssertEqual(connectionClosed.load(), 1)
    }

    // MARK: - Zero-length connection ID tests (RFC 9000 Section 5.1)

    func testZeroLengthClientSCID() async throws {
        try await self.verifyPingPongWithCIDLengths(
            clientConnectionIDGenerator: RandomQUICConnectionIDGenerator(connectionIDLength: 0)
        )
    }

    func testZeroLengthServerSCID() async throws {
        try await self.verifyPingPongWithCIDLengths(
            serverConnectionIDGenerator: RandomQUICConnectionIDGenerator(connectionIDLength: 0)
        )
    }

    func testZeroLengthBothSCIDs() async throws {
        try await self.verifyPingPongWithCIDLengths(
            clientConnectionIDGenerator: RandomQUICConnectionIDGenerator(connectionIDLength: 0),
            serverConnectionIDGenerator: RandomQUICConnectionIDGenerator(connectionIDLength: 0)
        )
    }

    func testCustomConnectionIDGenerator() async throws {
        struct PrefixedGenerator: QUICConnectionIDGenerator {
            var connectionIDLength: Int = 8
            var counter: UInt8 = 0
            let prefix: UInt8

            init(prefix: UInt8) {
                self.prefix = prefix
            }

            mutating func next() -> QUICConnectionID {
                defer { self.counter += 1 }
                var bytes = InlineArray<20, UInt8>(repeating: 0)
                bytes[0] = self.prefix
                bytes[1] = self.counter
                return QUICConnectionID(bytes: bytes, length: UInt8(self.connectionIDLength))
            }

            mutating func next(
                sourceConnectionID: QUICConnectionID,
                destinationConnectionID: QUICConnectionID
            ) -> QUICConnectionID {
                self.next()
            }
        }

        try await self.verifyPingPongWithCIDLengths(
            clientConnectionIDGenerator: PrefixedGenerator(prefix: 0xCC),
            serverConnectionIDGenerator: PrefixedGenerator(prefix: 0x55)
        )
    }

    func testServerAcceptsDCIDAsOwnSCID() async throws {
        // A generator where the server adopts the client's chosen DCID as its SCID.
        // This exercises the next(sourceConnectionID:destinationConnectionID:) path
        // with a generator that actually uses the parameters.
        struct EchoDCIDGenerator: QUICConnectionIDGenerator {
            var connectionIDLength: Int = 8
            var counter: UInt8 = 0

            mutating func next() -> QUICConnectionID {
                // Fallback for post-handshake batch generation.
                defer { self.counter += 1 }
                var bytes = InlineArray<20, UInt8>(repeating: 0)
                bytes[0] = 0xEE
                bytes[1] = self.counter
                return QUICConnectionID(bytes: bytes, length: UInt8(self.connectionIDLength))
            }

            mutating func next(
                sourceConnectionID: QUICConnectionID,
                destinationConnectionID: QUICConnectionID
            ) -> QUICConnectionID {
                destinationConnectionID
            }
        }

        try await self.verifyPingPongWithCIDLengths(
            serverConnectionIDGenerator: EchoDCIDGenerator()
        )
    }

    private func verifyPingPongWithCIDLengths(
        clientConnectionIDGenerator: any QUICConnectionIDGenerator = RandomQUICConnectionIDGenerator(),
        serverConnectionIDGenerator: any QUICConnectionIDGenerator = RandomQUICConnectionIDGenerator()
    ) async throws {
        let (_, serverChannel, serverMultiplexer, clientMultiplexer) = try await makeClientAndServerPair(
            clientConnectionIDGenerator: clientConnectionIDGenerator,
            serverConnectionIDGenerator: serverConnectionIDGenerator
        )

        let connection = try await clientMultiplexer.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for await connection in serverMultiplexer.inboundConnections {
                    for await stream in connection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await buffer in inbound {
                                XCTAssertEqual(buffer, .init(string: "ping"))
                                try await outbound.write(.init(string: "pong"))
                                outbound.finish()
                            }
                        }
                    }
                }
            }

            let stream = try await connection.createBidirectionalStream { streamInitializer in
                streamInitializer.channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel(
                        wrappingChannelSynchronously: streamInitializer.channel,
                        configuration: .init(
                            isOutboundHalfClosureEnabled: true,
                            inboundType: ByteBuffer.self,
                            outboundType: ByteBuffer.self
                        )
                    )
                }
            }
            try await stream.executeThenClose { inbound, outbound in
                try await outbound.write(.init(string: "ping"))
                outbound.finish()

                for try await buffer in inbound {
                    XCTAssertEqual(buffer, .init(string: "pong"))
                }
            }

            group.cancelAll()
        }
    }
}

final class PacketHeaderRecorderHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>
    typealias InboundOut = AddressedEnvelope<ByteBuffer>

    let headers: NIOLockedValueBox<[QUICPacketHeader]>

    init() {
        self.headers = .init([])
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let message = self.unwrapInboundIn(data)
        let header = try? message.data.getQUICPacketHeader(
            destinationIDLength: 16  // this is the random ID length
        )
        if let header {
            self.headers.withLockedValue { $0.append(header) }
        }
        context.fireChannelRead(data)
    }
}

final class ClientApplicationCloseHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn = Never

    private let errors: NIOLockedValueBox<[any Error]>

    init() {
        self.errors = .init([])
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        self.errors.withLockedValue { $0.append(error) }
        context.fireErrorCaught(error)
    }

    func getErrors() async -> [any Error] {
        self.errors.withLockedValue { $0 }
    }
}
