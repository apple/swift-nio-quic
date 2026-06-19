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
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOPosix
import NIOQUICHelpers
import Synchronization
import Testing
import XCTest

@testable import ChildChannelMultiplexer
@testable import NIOQUIC

final class QUICProtocolStackTests: XCTestCase {

    private func buildClientChannel(
        logger: Logger,
        host: String = "127.0.0.1",
        bindPort: Int = 0,
        maxIdleTimeout: Duration = .milliseconds(30000),
        forceVersionNegotiation: Bool = false,
        eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton
    ) async throws -> (any Channel, QUICHandler.ConnectionMultiplexer<Never>) {
        let (channel, multiplexer) = try await DatagramBootstrap(group: eventLoopGroup)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelOption(ChannelOptions.maxMessagesPerRead, value: 32)
            .bind(host: host, port: bindPort) { channel in
                channel.eventLoop.makeCompletedFuture {
                    let (quicHandler, connectionMultiplexer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
                        channel: channel,
                        quicConfiguration: .client(
                            verificationConfiguration: VerificationConfiguration.rawPublicKeys(
                                publicKeyFilePath: Self.testPublicKeyPath
                            ),
                            applicationProtocols: ["swift_nio_quic"],
                            maxIdleTimeout: maxIdleTimeout,
                            forceVersionNegotiation: forceVersionNegotiation
                        ),
                        logger: logger,
                        metrics: nil,
                        inboundStreamChannelInitializer: { streamChannel in
                            channel.eventLoop.makeCompletedFuture { fatalError() }
                        }
                    )
                    try channel.pipeline.syncOperations.addHandler(quicHandler)
                    return (channel, connectionMultiplexer)
                }
            }
        return (channel, multiplexer)
    }

    private func buildServerChannel(
        logger: Logger,
        host: String = "127.0.0.1",
        maxIdleTimeout: Duration = .milliseconds(30000),
        initialMaxBidirectionalStreams: Int = 8,
        initialMaxUnidirectionalStreams: Int = 8,
        qlogConfiguration: QUICConfiguration.QLogConfiguration? = nil,
        sendRetry: Bool = false,
        eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton
    ) async throws -> (any Channel, QUICHandler.ConnectionMultiplexer<NIOAsyncChannel<ByteBuffer, ByteBuffer>>) {
        let (channel, multiplexer) = try await DatagramBootstrap(group: eventLoopGroup)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelOption(ChannelOptions.maxMessagesPerRead, value: 32)
            .bind(host: host, port: 0) { channel in
                channel.eventLoop.makeCompletedFuture {
                    let (quicHandler, connectionMultiplexer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
                        channel: channel,
                        quicConfiguration: .server(
                            serverName: host,
                            authenticationConfiguration: AuthenticationConfiguration.rawPublicKeys(
                                publicKeyFilePath: Self.testPublicKeyPath,
                                privateKeyFilePath: Self.testPrivateKeyPath,
                            ),
                            applicationProtocols: ["swift_nio_quic"],
                            maxIdleTimeout: maxIdleTimeout,
                            initialMaxStreamsBidi: initialMaxBidirectionalStreams,
                            initialMaxStreamsUni: initialMaxUnidirectionalStreams,
                            sendRetry: sendRetry,
                            qLogConfiguration: qlogConfiguration
                        ),
                        logger: logger,
                        metrics: nil,
                        inboundStreamChannelInitializer: { streamChannel in
                            streamChannel.eventLoop.makeCompletedFuture {
                                let asyncChannel = try NIOAsyncChannel(
                                    wrappingChannelSynchronously: streamChannel,
                                    configuration: .init(
                                        isOutboundHalfClosureEnabled: true,
                                        inboundType: ByteBuffer.self,
                                        outboundType: ByteBuffer.self
                                    )
                                )
                                return asyncChannel
                            }
                        }
                    )
                    try channel.pipeline.syncOperations.addHandler(quicHandler)
                    return (channel, connectionMultiplexer)
                }
            }
        return (channel, multiplexer)
    }

    func getChannelLoggers() -> (serverLogger: Logger, clientLogger: Logger) {
        var clientLogger = Logger(label: "Client")
        clientLogger.logLevel = .info
        var serverLogger = Logger(label: "Server")
        serverLogger.logLevel = .info
        return (serverLogger, clientLogger)
    }

    func testSingleDataTransfer() async throws {

        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(logger: loggers.serverLogger)
        let (_, clientMultiplexer) = try await buildClientChannel(logger: loggers.clientLogger)

        let clientConnection = try await clientMultiplexer.createNewConnection(
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
            let stream = try await clientConnection.createBidirectionalStream { streamInitializer in
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

            XCTAssertEqual(responses.load(ordering: .sequentiallyConsistent), 1)

            group.cancelAll()
        }
    }

    func testSingleDataTransferOverTwoUnidirectionalStreams() async throws {

        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(logger: loggers.serverLogger)
        let (_, clientMultiplexer) = try await buildClientChannel(logger: loggers.clientLogger)

        let clientConnection = try await clientMultiplexer.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture {
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
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for await connection in serverMultiplexer.inboundConnections {
                    for await stream in connection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await buffer in inbound {
                                XCTAssertEqual(buffer, .init(string: "Message to server"))
                                // Send a unidirectional stream back to client
                                let responseStream = try await connection.createUnidirectionalStream {
                                    streamInitializer in
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
                                try await responseStream.executeThenClose { inbound, outbound in
                                    try await outbound.write(.init(string: "Response to client"))
                                    outbound.finish()
                                }
                            }
                        }
                    }
                }
            }
            let clientResponses = Atomic(0)
            group.addTask {
                for await stream in clientConnection.inboundStreams {
                    try await stream.executeThenClose { inbound, outbound in
                        for try await buffer in inbound {
                            XCTAssertEqual(buffer, .init(string: "Response to client"))
                            clientResponses.wrappingAdd(1, ordering: .sequentiallyConsistent)
                            try clientMultiplexer.eventLoop.close()
                        }
                    }
                }
            }
            let clientStream = try await clientConnection.createUnidirectionalStream { streamInitializer in
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
            try await clientStream.executeThenClose { inbound, outbound in
                try await outbound.write(.init(string: "Message to server"))
                outbound.finish()
                try await Task.sleep(for: .milliseconds(300))
            }
            XCTAssertEqual(clientResponses.load(ordering: .sequentiallyConsistent), 1)
            group.cancelAll()
        }
    }

    func testClientIdleTimeout() async throws {

        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(logger: loggers.serverLogger)
        let (_, clientMultiplexer) = try await buildClientChannel(
            logger: loggers.clientLogger,
            maxIdleTimeout: .milliseconds(400)
        )

        let clientConnection = try await clientMultiplexer.createNewConnection(
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
                            for try await _ in inbound {
                                XCTFail("Should not reach this path")
                            }
                        }
                    }
                }
            }
            let stream = try await clientConnection.createBidirectionalStream { streamInitializer in
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
                // The server should not get the client write here due to inactivity on the client and the connection going down.
                try await Task.sleep(for: .milliseconds(800))
                do {
                    try await outbound.write(.init(string: "GET /foo"))
                    outbound.finish()
                } catch {
                    XCTAssertNotNil(error, "Channel should be closed due to timeout")
                }
            }
            group.cancelAll()
        }
    }

    func testServerIdleTimeout() async throws {

        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(
            logger: loggers.serverLogger,
            maxIdleTimeout: .milliseconds(400)
        )
        let (_, clientMultiplexer) = try await buildClientChannel(
            logger: loggers.clientLogger
        )

        let clientConnection = try await clientMultiplexer.createNewConnection(
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
                            for try await _ in inbound {
                                XCTFail("Should not reach this path")
                            }
                        }
                    }
                }
            }
            let stream = try await clientConnection.createBidirectionalStream { streamInitializer in
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
                try await Task.sleep(for: .milliseconds(800))
                do {
                    // The server should not get the client write here due to inactivity on the server and the connection going down.
                    try await outbound.write(.init(string: "GET /foo"))
                    outbound.finish()
                } catch {
                    XCTAssertNotNil(error, "Channel should be closed due to timeout")
                }
            }
            group.cancelAll()
        }
    }

    func testMultipleConnections() async throws {
        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(logger: loggers.serverLogger)
        defer {
            let _ = serverChannel.close()
        }
        // Adding explicit unwrapping here is fine because if the local ip and port for the server are not obtained then
        // this unit test should not continue because there are larger issues taking place.
        let serverPort = serverChannel.localAddress!.port!
        XCTAssertNotNil(serverPort, "Server port code not be determined")
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                let handledStreams = Counter()
                // Handle multiple connections
                await withThrowingTaskGroup(of: Void.self) { connectionGroup in
                    for await connection in serverMultiplexer.inboundConnections {
                        // Run a new connection on a new task?
                        connectionGroup.addTask {
                            for await stream in connection.inboundStreams {
                                try await stream.executeThenClose { inbound, outbound in
                                    for try await buffer in inbound {
                                        // This is not very scientific because we are essentially just incrementing the stream
                                        // count on the connection and then sending it back so it doesnt exactly match the incoming read.
                                        // There may be a better way to do this but for now we are just trying to test that the project
                                        // handles multiple connections with individual reads.
                                        let streamNumber = handledStreams.increment()
                                        XCTAssertTrue(String(buffer: buffer).hasPrefix("GET /foo/"))
                                        let responseMessage = "<b>Success \(streamNumber)</b>"
                                        try await outbound.write(.init(string: responseMessage))
                                        outbound.finish()
                                    }
                                }
                            }
                        }
                    }
                }
            }
            let connectionCount = 6
            let responses = Atomic(0)
            // Runs a client side connections one at a time (server is handling connections concurrently)
            // NOTE: For this to work the client side needs to increment the local port used so packets do not get mixed up on connections.
            for connectionIndex in 0..<connectionCount {
                let (clientChannel, clientMultiplexer) = try await self.buildClientChannel(logger: loggers.clientLogger)
                // Adding explicit unwrapping here is fine because if the local ip and port for the client are not obtained then
                // this unit test should not continue because there are larger issues taking place.
                XCTAssertNotNil(clientChannel.localAddress!.port!, "Client local port should not be nil")
                defer {
                    // Tear it down after the response is received
                    let _ = clientChannel.close()
                }
                let clientConnection = try await clientMultiplexer.createNewConnection(
                    serverName: serverChannel.localAddress!.ipAddress!,
                    remoteAddress: serverChannel.localAddress!,
                    inboundStreamInitializer: { channel in
                        channel.eventLoop.makeCompletedFuture { fatalError() }
                    }
                )
                let stream = try await clientConnection.createBidirectionalStream { streamInitializer in
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
                    try await outbound.write(.init(string: "GET /foo/\(connectionIndex + 1)"))
                    outbound.finish()
                    for try await buffer in inbound {
                        // There may be a better way here, but for now just check for the prefix on the client side.
                        XCTAssertTrue(String(buffer: buffer).hasPrefix("<b>Success"))
                        responses.wrappingAdd(1, ordering: .sequentiallyConsistent)
                    }
                }
                try await Task.sleep(for: .milliseconds(5))
            }
            XCTAssertEqual(responses.load(ordering: .sequentiallyConsistent), connectionCount)
            group.cancelAll()
        }
    }

    func testMultipleConnectionsMultipleStreams() async throws {
        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(logger: loggers.serverLogger)
        defer {
            let _ = serverChannel.close()
        }
        // Adding explicit unwrapping here is fine because if the local ip and port for the server are not obtained then
        // this unit test should not continue because there are larger issues taking place.
        let serverPort = serverChannel.localAddress!.port!
        XCTAssertNotNil(serverPort, "Server port code not be determined")
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                let handledStreams = Counter()
                // Handle multiple connections
                await withThrowingTaskGroup(of: Void.self) { connectionGroup in
                    for await connection in serverMultiplexer.inboundConnections {
                        // Run a new connection on a new task?
                        connectionGroup.addTask {
                            for await stream in connection.inboundStreams {
                                try await stream.executeThenClose { inbound, outbound in
                                    for try await buffer in inbound {
                                        // Just incrementing the stream count on the connection.
                                        let streamNumber = handledStreams.increment()
                                        let msg = String(buffer: buffer)
                                        XCTAssertTrue(msg.hasPrefix("GET /connection/"))
                                        let parts = msg.split(separator: "/")
                                        XCTAssertTrue(parts.count == 5, "Unexpected message: \(msg)")
                                        let clientConnectionNumber = try XCTUnwrap(Int(parts[2]))
                                        let clientStreamNumber = try XCTUnwrap(Int(parts[4]))
                                        let responseMessage =
                                            "<b>Success \(streamNumber):\(clientConnectionNumber):\(clientStreamNumber)</b>"
                                        try await outbound.write(.init(string: responseMessage))
                                        outbound.finish()
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Run 6 connections with 6 streams inside each connection
            let connectionCount = 6
            let streamCountPerConnection = 6
            let streamResponses = Atomic(0)
            // Runs a client side connections one at a time (server is handling connections concurrently)
            // NOTE: For this to work the client side needs to increment the local port used so packets do not get mixed up on connections.
            for connectionIndex in 0..<connectionCount {
                let (clientChannel, clientMultiplexer) = try await self.buildClientChannel(logger: loggers.clientLogger)
                // Adding explicit unwrapping here is fine because if the local ip and port for the client are not obtained then
                // this unit test should not continue because there are larger issues taking place.
                XCTAssertNotNil(clientChannel.localAddress!.port!, "Client local port should not be nil")
                defer {
                    // Tear it down after the response is received
                    let _ = clientChannel.close()
                }
                let clientConnection = try await clientMultiplexer.createNewConnection(
                    serverName: serverChannel.localAddress!.ipAddress!,
                    remoteAddress: serverChannel.localAddress!,
                    inboundStreamInitializer: { channel in
                        channel.eventLoop.makeCompletedFuture { fatalError() }
                    }
                )
                for streamIndex in 0..<streamCountPerConnection {
                    let stream = try await clientConnection.createBidirectionalStream { streamInitializer in
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
                        try await outbound.write(
                            .init(string: "GET /connection/\(connectionIndex + 1)/stream/\(streamIndex + 1)")
                        )
                        outbound.finish()
                        for try await buffer in inbound {
                            var msg = String(buffer: buffer)
                            XCTAssertTrue(msg.hasPrefix("<b>Success"))
                            msg.removeLast(4)  // Remove '</b>'
                            let parts = msg.split(separator: ":")
                            XCTAssertTrue(parts.count == 3, "Unexpected message: \(msg)")
                            let clientConnectionNumber = try XCTUnwrap(Int(parts[1]), "Unexpected part: \(parts[1])")
                            let clientStreamNumber = try XCTUnwrap(Int(parts[2]), "Unexpected part: \(parts[2])")
                            XCTAssertEqual(connectionIndex + 1, clientConnectionNumber)
                            XCTAssertEqual(streamIndex + 1, clientStreamNumber)
                            streamResponses.wrappingAdd(1, ordering: .sequentiallyConsistent)
                        }
                    }
                }
            }
            XCTAssertEqual(
                streamResponses.load(ordering: .sequentiallyConsistent),
                (connectionCount * streamCountPerConnection)
            )
            group.cancelAll()
        }
    }

    func test100BidirectionalStreamsOnOneConnection() async throws {
        // Advertise that the server will support opening 100 bidirectional streams on one connection
        let requestCount = 100
        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(
            logger: loggers.serverLogger,
            initialMaxBidirectionalStreams: requestCount
        )
        let (clientChannel, clientMultiplexer) = try await buildClientChannel(logger: loggers.clientLogger)

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

        try await clientChannel.close().get()
        try await serverChannel.close().get()
    }

    func test100UnidirectionalStreamsOnOneConnection() async throws {
        // Advertise that the server will support opening 100 unidirectional streams on one connection
        let requestCount = 100
        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(
            logger: loggers.serverLogger,
            initialMaxUnidirectionalStreams: requestCount
        )
        let (clientChannel, clientMultiplexer) = try await buildClientChannel(logger: loggers.clientLogger)

        let connection = try await clientMultiplexer.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )

        let processedStreams = Counter()
        let allStreamsSent = Atomic<Bool>(false)
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for await connection in serverMultiplexer.inboundConnections {
                    for await stream in connection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await buffer in inbound {
                                let str = String(decoding: Array(buffer: buffer), as: Unicode.UTF8.self)
                                XCTAssert(str.starts(with: "GET /foo/"))
                                processedStreams.increment()
                            }
                        }
                    }
                }
            }
            for i in 0..<requestCount {
                let stream = try await connection.createUnidirectionalStream { streamInitializer in
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
                    try await outbound.write(.init(string: "GET /foo/\(i)"))
                    outbound.finish()
                }
            }
            allStreamsSent.store(true, ordering: .sequentiallyConsistent)

            // Give it 4 seconds for all of the streams to be processed
            let startTime = ContinuousClock.now
            while processedStreams.load() < requestCount {
                let elapsed = ContinuousClock.now - startTime
                if elapsed > .seconds(4) {
                    XCTFail(
                        "Test timed out, only \(processedStreams.load())/\(requestCount) streams processed"
                    )
                    break
                }
                try await Task.sleep(for: .milliseconds(10))
            }
            XCTAssertEqual(processedStreams.load(), requestCount)
            group.cancelAll()
        }

        try await clientChannel.close().get()
        try await serverChannel.close().get()
    }

    func testMAXStreamsLimitForBidirectionalStreams() async throws {
        let requestCount = 1000
        // Start by making sure the initial advertised MAX_STREAMS is the default of 8 bidirectional streams.
        // The server should detect that its about to overflow the remote max streams and raise the limit to server 1000 streams.
        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(logger: loggers.serverLogger)
        let (clientChannel, clientMultiplexer) = try await buildClientChannel(logger: loggers.clientLogger)

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

        try await clientChannel.close().get()
        try await serverChannel.close().get()
    }

    func testMAXStreamsLimitForUnidirectionalStreams() async throws {
        let requestCount = 1000
        // Start by making sure the initial advertised MAX_STREAMS is the default of 8 unidirectional streams.
        // The server should detect that its about to overflow the remote max streams and raise the limit to server 1000 streams.
        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(logger: loggers.serverLogger)
        let (clientChannel, clientMultiplexer) = try await buildClientChannel(logger: loggers.clientLogger)

        let connection = try await clientMultiplexer.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )
        let processedStreams = Counter()
        let allStreamsSent = Atomic<Bool>(false)
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for await connection in serverMultiplexer.inboundConnections {
                    for await stream in connection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await buffer in inbound {
                                let str = String(decoding: Array(buffer: buffer), as: Unicode.UTF8.self)
                                XCTAssert(str.starts(with: "GET /foo/"))
                                processedStreams.increment()
                            }
                        }
                    }
                }
            }
            for i in 0..<requestCount {
                let stream = try await connection.createUnidirectionalStream { streamInitializer in
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
                    try await outbound.write(.init(string: "GET /foo/\(i)"))
                    outbound.finish()
                    // Add a small delay here to make sure the stream reaches the other side
                    try await Task.sleep(for: .milliseconds(5))
                }
            }
            allStreamsSent.store(true, ordering: .sequentiallyConsistent)

            // Give it 4 seconds for all of the streams to be processed
            let startTime = ContinuousClock.now
            while processedStreams.load() < requestCount {
                let elapsed = ContinuousClock.now - startTime
                if elapsed > .seconds(4) {
                    XCTFail(
                        "Test timed out, only \(processedStreams.load())/\(requestCount) streams processed"
                    )
                    break
                }
                try await Task.sleep(for: .milliseconds(10))
            }
            XCTAssertEqual(processedStreams.load(), requestCount)
            group.cancelAll()
        }

        try await clientChannel.close().get()
        try await serverChannel.close().get()
    }

    func testClientForceVersionNegotiation() async throws {

        // This test forces version negotiation by making the client send the negotiation pattern.
        // The server will see that this is an unknown version and force the version negotiation exchange.
        // By the fact that a stream can be opened and data can be sent on it means that a successful version was negotiated.

        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(logger: loggers.serverLogger)
        let (_, clientMultiplexer) = try await buildClientChannel(
            logger: loggers.clientLogger,
            forceVersionNegotiation: true
        )

        let clientConnection = try await clientMultiplexer.createNewConnection(
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
            let stream = try await clientConnection.createBidirectionalStream { streamInitializer in
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

            XCTAssertEqual(responses.load(ordering: .sequentiallyConsistent), 1)

            group.cancelAll()
        }
    }

    func testServerSendRetry() async throws {
        // This test directy the server to send a retry in response to the client initial.

        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(
            logger: loggers.serverLogger,
            sendRetry: true
        )

        let (_, clientMultiplexer) = try await buildClientChannel(
            logger: loggers.clientLogger
        )

        let clientConnection = try await clientMultiplexer.createNewConnection(
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
            let stream = try await clientConnection.createBidirectionalStream { streamInitializer in
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

            XCTAssertEqual(responses.load(ordering: .sequentiallyConsistent), 1)

            group.cancelAll()
        }
    }

    func testServerUsingQlog() async throws {
        let path = FileManager.default.temporaryDirectory
        let tag = UUID().uuidString
        let title = "ServerTestQlog_\(tag)"
        let clientConnectionCount = 2
        let qlogConfiguration = QUICConfiguration.QLogConfiguration(
            path: "\(path.path)/",
            topic: title,
            description: "TestLogDescription"
        )
        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(
            logger: loggers.serverLogger,
            maxIdleTimeout: .milliseconds(400),
            qlogConfiguration: qlogConfiguration
        )
        let (_, clientMultiplexer) = try await buildClientChannel(logger: loggers.clientLogger)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for await connection in serverMultiplexer.inboundConnections {
                    for await stream in connection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await _ in inbound {
                                XCTFail("Should not reach this path")
                            }
                        }
                    }
                }
            }
            for _ in 0..<clientConnectionCount {
                let clientConnection = try await clientMultiplexer.createNewConnection(
                    serverName: serverChannel.localAddress!.ipAddress!,
                    remoteAddress: serverChannel.localAddress!,
                    inboundStreamInitializer: { channel in
                        channel.eventLoop.makeCompletedFuture { fatalError() }
                    }
                )
                let stream = try await clientConnection.createBidirectionalStream { streamInitializer in
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
                    try await Task.sleep(for: .milliseconds(800))
                    do {
                        // The server should not get the client write here due to inactivity on the server and the connection going down.
                        try await outbound.write(.init(string: "GET /foo"))
                        outbound.finish()
                    } catch {
                        XCTAssertNotNil(error, "Channel should be closed due to timeout")
                    }
                }
            }
            group.cancelAll()

            // Wait for the files to be written.
            try await Task.sleep(for: .milliseconds(1000))

            // Validate that each connection wrote a qlog file. The connection ID in
            // the filename depends on process-global state. Look for files with our
            // UUID tag instead of relying on the specific IDs.
            let filePrefix = "qlog_server_\(title)_"
            let allFiles = try FileManager.default.contentsOfDirectory(atPath: path.path)
            let qlogFiles = allFiles.filter { $0.hasPrefix(filePrefix) && $0.hasSuffix(".qlog") }
            XCTAssertEqual(
                qlogFiles.count,
                clientConnectionCount,
                "Expected \(clientConnectionCount) qlog files matching prefix \(filePrefix), found \(qlogFiles.count)"
            )
            for filename in qlogFiles {
                let filePath = path.appending(component: filename)
                let resultData = try Data(contentsOf: filePath)
                XCTAssert(!resultData.isEmpty, "qlog file is unexpectedly empty")
                try FileManager.default.removeItem(at: filePath)
            }
        }
    }

    /// Verifies that when the server sends RESET_STREAM on a bidirectional
    /// stream, the client sees a `QUICStreamResetError` rather than a clean EOF.
    ///
    /// This reproduces the race condition where `handleInboundAbortedEvent` sets
    /// `readClosed = true` (hiding the stream from `readableStreamIDs`) while the
    /// error delivery via the multiplexer is deferred to the next event-loop tick.
    /// Without the synchronous read-path fix, the client would see a clean EOF
    /// because the deferred error arrives after the stream has already been
    /// closed.
    func testResetStreamErrorDeliveredToClient() async throws {
        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(logger: loggers.serverLogger)
        let (_, clientMultiplexer) = try await buildClientChannel(logger: loggers.clientLogger)

        let clientConnection = try await clientMultiplexer.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Server: receive the stream, read data, then send RESET_STREAM.
            group.addTask {
                for await connection in serverMultiplexer.inboundConnections {
                    for await stream in connection.inboundStreams {
                        let channel = stream.channel
                        try await stream.executeThenClose { inbound, outbound in
                            // Wait for the client's data to arrive
                            for try await buffer in inbound {
                                XCTAssertEqual(buffer, ByteBuffer(string: "request"))
                            }
                            // Send RESET_STREAM to abort our write side.
                            // The client should see this as an error on its read side.
                            try await channel.triggerUserOutboundEvent(
                                NIOQUICHelpers.QUICResetStreamEvent(code: NIOQUICHelpers.QUICApplicationErrorCode(42)!)
                            )
                        }
                    }
                }
            }

            // Client: open a stream, write data, then read — expecting an error.
            let stream = try await clientConnection.createBidirectionalStream { streamInitializer in
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

            var receivedResetError = false
            do {
                try await stream.executeThenClose { inbound, outbound in
                    try await outbound.write(ByteBuffer(string: "request"))
                    outbound.finish()

                    for try await _ in inbound {
                        XCTFail("Should not receive data — expected RESET_STREAM error")
                    }
                    // If we reach here, the inbound stream ended with nil (clean EOF).
                    // That means the RESET_STREAM error was not delivered — the bug.
                    XCTFail("Expected QUICStreamResetError but got clean EOF")
                }
            } catch {
                // The error should be a QUICStreamResetError with code 42.
                // Accept any error here — the important thing is that an error
                // was thrown rather than a clean EOF.
                receivedResetError = true
            }
            XCTAssertTrue(receivedResetError, "Expected an error from RESET_STREAM")
            group.cancelAll()
        }
    }

    // MARK: Stream concurrency limits

    /// Per-stream lifecycle for the two-stream concurrency helper.
    private struct TwoStreamState {
        enum StreamState {
            case unknown
            case open
            case closed
        }
        private(set) var stream1: StreamState = .unknown
        private(set) var stream2: StreamState = .unknown

        mutating func updateStream1(_ new: StreamState) {
            switch (stream1, new) {
            case (.unknown, .open), (.open, .closed):
                stream1 = new
            default:
                XCTFail("Invalid stream1 transition: \(stream1) -> \(new)")
            }
        }

        mutating func updateStream2(_ new: StreamState) {
            switch (stream2, new) {
            case (.unknown, .open), (.open, .closed):
                stream2 = new
            default:
                XCTFail("Invalid stream2 transition: \(stream2) -> \(new)")
            }
        }
    }

    /// Helper: open two streams concurrently on a connection where the server
    /// advertises `maxBidiStreams` as the initial bidirectional stream limit.
    /// Returns `true` if stream 2 opened while stream 1 was still open (limit
    /// allowed concurrent streams); `false` if stream 2's open was blocked
    /// until stream 1 closed.
    private func openTwoStreamsConcurrently(maxBidiStreams: Int) async throws -> Bool {
        let eventLoopGroup = MultiThreadedEventLoopGroup.singleton
        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(
            logger: loggers.serverLogger,
            initialMaxBidirectionalStreams: maxBidiStreams,
            eventLoopGroup: eventLoopGroup
        )
        let (clientChannel, clientMultiplexer) = try await buildClientChannel(
            logger: loggers.clientLogger,
            eventLoopGroup: eventLoopGroup
        )

        let connection = try await clientMultiplexer.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )

        let state = NIOLockedValueBox(TwoStreamState())
        let stream1AtStream2Open = NIOLockedValueBox<TwoStreamState.StreamState?>(nil)

        // Promises that the work tasks complete when they finish — the same
        // synchronization primitive used by `ChannelHierarchyTests`.
        let stream1Closed = eventLoopGroup.any().makePromise(of: Void.self)
        let stream2Done = eventLoopGroup.any().makePromise(of: Void.self)

        let stream2OpenedConcurrently = try await withThrowingTaskGroup(of: Void.self, returning: Bool.self) { group in
            group.addTask {
                for await serverConn in serverMultiplexer.inboundConnections {
                    for await stream in serverConn.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await buffer in inbound {
                                try await outbound.write(buffer)
                            }
                            outbound.finish()
                        }
                    }
                }
            }

            // Open stream 1 but don't close it yet.
            let stream1 = try await connection.createBidirectionalStream { si in
                si.channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel(
                        wrappingChannelSynchronously: si.channel,
                        configuration: .init(
                            isOutboundHalfClosureEnabled: true,
                            inboundType: ByteBuffer.self,
                            outboundType: ByteBuffer.self
                        )
                    )
                }
            }
            state.withLockedValue { $0.updateStream1(.open) }

            // Stream 2 and stream 1's close run in parallel.
            group.addTask {
                do {
                    let stream2 = try await connection.createBidirectionalStream { si in
                        si.channel.eventLoop.makeCompletedFuture {
                            try NIOAsyncChannel(
                                wrappingChannelSynchronously: si.channel,
                                configuration: .init(
                                    isOutboundHalfClosureEnabled: true,
                                    inboundType: ByteBuffer.self,
                                    outboundType: ByteBuffer.self
                                )
                            )
                        }
                    }
                    let observed = state.withLockedValue { state -> TwoStreamState.StreamState in
                        state.updateStream2(.open)
                        return state.stream1
                    }
                    stream1AtStream2Open.withLockedValue { $0 = observed }
                    try await stream2.executeThenClose { inbound, outbound in
                        try await outbound.write(ByteBuffer(string: "stream2"))
                        outbound.finish()
                        for try await _ in inbound {}
                    }
                    state.withLockedValue { $0.updateStream2(.closed) }
                    stream2Done.succeed(())
                } catch {
                    stream2Done.fail(error)
                    throw error
                }
            }

            group.addTask {
                do {
                    try await stream1.executeThenClose { inbound, outbound in
                        try await outbound.write(ByteBuffer(string: "stream1"))
                        outbound.finish()
                        for try await _ in inbound {}
                    }
                    state.withLockedValue { $0.updateStream1(.closed) }
                    stream1Closed.succeed(())
                } catch {
                    stream1Closed.fail(error)
                    throw error
                }
            }

            try await stream1Closed.futureResult.get()
            try await stream2Done.futureResult.get()

            group.cancelAll()
            return stream1AtStream2Open.withLockedValue { $0 } == .open
        }

        try await clientChannel.close().get()
        try await serverChannel.close().get()
        return stream2OpenedConcurrently
    }

    /// Tests that insufficient stream credit blocks new streams.
    /// With `initialMaxStreamsBidi=1`, the second stream MUST block
    /// until the first stream closes - the peer's limit prevents concurrent streams.
    func testStreamConcurrencyLimitOf1BlocksSecondStream() async throws {
        let stream2OpenedConcurrently = try await openTwoStreamsConcurrently(maxBidiStreams: 1)
        XCTAssertFalse(
            stream2OpenedConcurrently,
            "With initialMaxStreamsBidi=1, stream 2 must block while stream 1 is open"
        )
    }

    /// Control test: with `initialMaxStreamsBidi=2`, both streams open concurrently.
    /// This compliments the assertion in `testStreamConcurrencyLimitOf1BlocksSecondStream`.
    func testStreamConcurrencyLimitOf2AllowsBothStreams() async throws {
        let stream2OpenedConcurrently = try await openTwoStreamsConcurrently(maxBidiStreams: 2)
        XCTAssertTrue(
            stream2OpenedConcurrently,
            "With initialMaxStreamsBidi=2, stream 2 should open immediately alongside stream 1"
        )
    }

    // MARK: Subsequent streams after RESET_STREAM

    /// Verifies that the connection remains usable for subsequent streams
    /// after one has been reset.
    func testFlowControlAfterResetStream() async throws {
        let loggers = getChannelLoggers()
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(logger: loggers.serverLogger)
        let (clientChannel, clientMultiplexer) = try await buildClientChannel(logger: loggers.clientLogger)

        let connection = try await clientMultiplexer.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for await serverConn in serverMultiplexer.inboundConnections {
                    for await stream in serverConn.inboundStreams {
                        do {
                            try await stream.executeThenClose { inbound, outbound in
                                for try await buffer in inbound {
                                    try await outbound.write(buffer)
                                }
                                outbound.finish()
                            }
                        } catch {
                            // Stream 1 was reset — expect an error here.
                            // Continue processing subsequent streams.
                        }
                    }
                }
            }

            // Stream 1: reset it immediately.
            let stream1 = try await connection.createBidirectionalStream { si in
                si.channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel(
                        wrappingChannelSynchronously: si.channel,
                        configuration: .init(
                            isOutboundHalfClosureEnabled: true,
                            inboundType: ByteBuffer.self,
                            outboundType: ByteBuffer.self
                        )
                    )
                }
            }
            try await stream1.channel.triggerUserOutboundEvent(
                NIOQUICHelpers.QUICResetStreamEvent(code: NIOQUICHelpers.QUICApplicationErrorCode(0)!)
            )

            // Stream 2: after the reset, a new stream should work normally.
            let stream2 = try await connection.createBidirectionalStream { si in
                si.channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel(
                        wrappingChannelSynchronously: si.channel,
                        configuration: .init(
                            isOutboundHalfClosureEnabled: true,
                            inboundType: ByteBuffer.self,
                            outboundType: ByteBuffer.self
                        )
                    )
                }
            }
            try await stream2.executeThenClose { inbound, outbound in
                try await outbound.write(ByteBuffer(string: "after-reset"))
                outbound.finish()
                var response = ""
                for try await buffer in inbound {
                    response += String(buffer: buffer)
                }
                XCTAssertEqual(response, "after-reset", "Stream after RESET_STREAM should transfer data normally")
            }

            group.cancelAll()
        }

        try await clientChannel.close().get()
        try await serverChannel.close().get()
    }
}
