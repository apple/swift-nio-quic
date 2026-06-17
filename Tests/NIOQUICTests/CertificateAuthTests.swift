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
import NIOEmbedded
import NIOPosix
import NIOQUICHelpers
import Synchronization
import XCTest

@testable import ChildChannelMultiplexer
@testable import NIOQUIC

final class CertificateAuthTests: XCTestCase {

    private func buildClientChannel(
        address: String = "127.0.0.1",
        bindPort: Int = 0,
        maxIdleTimeout: Duration = .milliseconds(30000),
        trustStoreFilePath: String
    ) async throws -> (any Channel, QUICHandler.ConnectionMultiplexer<Never>) {
        let eventLoopGroup = MultiThreadedEventLoopGroup.singleton
        let (channel, multiplexer) = try await DatagramBootstrap(group: eventLoopGroup)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .bind(host: address, port: bindPort) { channel in
                channel.eventLoop.makeCompletedFuture {
                    let (quicHandler, connectionMultiplexer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
                        channel: channel,
                        quicConfiguration: .client(
                            verificationConfiguration: .x509Certificates(trustRootsFilePath: trustStoreFilePath),
                            applicationProtocols: ["swift_nio_quic"],
                            maxIdleTimeout: maxIdleTimeout
                        ),
                        logger: Logger(label: "test"),
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
        address: String = "127.0.0.1",
        name: String,
        certificateChainFilePath: String,
        privateKeyFilePath: String
    ) async throws -> (any Channel, QUICHandler.ConnectionMultiplexer<NIOAsyncChannel<ByteBuffer, ByteBuffer>>) {
        let eventLoopGroup = MultiThreadedEventLoopGroup.singleton
        let (channel, multiplexer) = try await DatagramBootstrap(group: eventLoopGroup)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .bind(host: address, port: 0) { channel in
                channel.eventLoop.makeCompletedFuture {
                    let (quicHandler, connectionMultiplexer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
                        channel: channel,
                        quicConfiguration: .server(
                            serverName: name,
                            authenticationConfiguration: .x509Certificates(
                                certificateChainFilePath: certificateChainFilePath,
                                privateKeyFilePath: privateKeyFilePath
                            ),
                            applicationProtocols: ["swift_nio_quic"]
                        ),
                        logger: Logger(label: "test"),
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

    func testSingleDataTransfer() async throws {
        let certs = try TestCertificates()
        let certificateFilePaths = try certs.writeToTemp(fileTag: "testSingleDataTransfer")

        let (serverChannel, serverMultiplexer) = try await buildServerChannel(
            name: certs.leafName,
            certificateChainFilePath: certificateFilePaths.serverCertFilePath,
            privateKeyFilePath: certificateFilePaths.serverPrivateKeyFilePath
        )
        let (_, clientMultiplexer) = try await buildClientChannel(
            trustStoreFilePath: certificateFilePaths.trustStoreFilePath
        )

        let clientConnection = try await clientMultiplexer.createNewConnection(
            serverName: certs.leafName,
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

    func testClientIdleTimeout() async throws {
        let certs = try TestCertificates()
        let certificateFilePaths = try certs.writeToTemp(fileTag: "testClientIdleTimeout")

        let (serverChannel, serverMultiplexer) = try await buildServerChannel(
            name: certs.leafName,
            certificateChainFilePath: certificateFilePaths.serverCertFilePath,
            privateKeyFilePath: certificateFilePaths.serverPrivateKeyFilePath
        )
        let (_, clientMultiplexer) = try await buildClientChannel(
            maxIdleTimeout: .milliseconds(2000),
            trustStoreFilePath: certificateFilePaths.trustStoreFilePath
        )

        let clientConnection = try await clientMultiplexer.createNewConnection(
            serverName: certs.leafName,
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
                try await Task.sleep(for: .seconds(4))
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

    func testMultipleConnections() async throws {
        let certs = try TestCertificates()
        let certificateFilePaths = try certs.writeToTemp(fileTag: "testMultipleConnections")
        let (serverChannel, serverMultiplexer) = try await buildServerChannel(
            name: certs.leafName,
            certificateChainFilePath: certificateFilePaths.serverCertFilePath,
            privateKeyFilePath: certificateFilePaths.serverPrivateKeyFilePath
        )
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
                let (clientChannel, clientMultiplexer) = try await self.buildClientChannel(
                    trustStoreFilePath: certificateFilePaths.trustStoreFilePath
                )
                // Adding explicit unwrapping here is fine because if the local ip and port for the client are not obtained then
                // this unit test should not continue because there are larger issues taking place.
                XCTAssertNotNil(clientChannel.localAddress!.port!, "Client local port should not be nil")
                defer {
                    // Tear it down after the response is received
                    let _ = clientChannel.close()
                }
                let clientConnection = try await clientMultiplexer.createNewConnection(
                    serverName: certs.leafName,
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
        let certs = try TestCertificates()
        let certificateFilePaths = try certs.writeToTemp(fileTag: "testMultipleConnectionsMultipleStreams")
        let (serverChannel, serverMultiplexer) = try await self.buildServerChannel(
            name: certs.leafName,
            certificateChainFilePath: certificateFilePaths.serverCertFilePath,
            privateKeyFilePath: certificateFilePaths.serverPrivateKeyFilePath
        )
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
                let (clientChannel, clientMultiplexer) = try await self.buildClientChannel(
                    trustStoreFilePath: certificateFilePaths.trustStoreFilePath
                )
                // Adding explicit unwrapping here is fine because if the local ip and port for the client are not obtained then
                // this unit test should not continue because there are larger issues taking place.
                XCTAssertNotNil(clientChannel.localAddress!.port!, "Client local port should not be nil")
                defer {
                    // Tear it down after the response is received
                    let _ = clientChannel.close()
                }
                let clientConnection = try await clientMultiplexer.createNewConnection(
                    serverName: certs.leafName,
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

    func testUntrustedServerCertsFailConnection() async throws {
        let serverCerts = try TestCertificates()
        let certificateFilePathsServer = try serverCerts.writeToTemp(fileTag: "testDifferentKeysShouldFailServer")
        let (serverChannel, _) = try await buildServerChannel(
            name: serverCerts.leafName,
            certificateChainFilePath: certificateFilePathsServer.serverCertFilePath,
            privateKeyFilePath: certificateFilePathsServer.serverPrivateKeyFilePath
        )

        // Generate new certs for the client. The server certs will not be trusted by the client.
        let certificateFilePathsClient = try TestCertificates().writeToTemp(
            fileTag: "testDifferentKeysShouldFailClient"
        )
        let (_, clientMultiplexer) = try await buildClientChannel(
            maxIdleTimeout: .milliseconds(2000),
            trustStoreFilePath: certificateFilePathsClient.trustStoreFilePath
        )

        do {
            _ = try await clientMultiplexer.createNewConnection(
                serverName: serverChannel.localAddress!.ipAddress!,
                remoteAddress: serverChannel.localAddress!,
                inboundStreamInitializer: { channel in
                    channel.eventLoop.makeCompletedFuture { fatalError() }
                }
            )
            XCTFail("Client should not connect to untrusted server.")
        } catch let error as QUICConnectionError {
            XCTAssert(error.code == 256)
            XCTAssert(error.reason == "TLS error")
            XCTAssert(error.isApplication == false)
        }
    }
}
