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
import NIOPosix
import NIOQUIC

@main
struct Example {
    static func main() async throws {
        let logger = Logger(label: "quic.example")

        // Generate a cert chain.
        let chain = try CertificateChain(dnsNames: ["localhost"], ipAddresses: ["127.0.0.1"])
        let paths = try chain.writeToDirectory(directory: FileManager.default.temporaryDirectory)

        // Create a QUIC server channel with a stream multiplexer.
        let (channel, multiplexer) = try await makeQUICServer(
            host: "127.0.0.1",
            certificateChainPath: paths.serverCert,
            privateKeyPath: paths.serverPrivateKey,
            logger: logger,
        )
        logger.info("Started QUIC server listening on \(channel.localAddress!)")

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                // Run the server, and echo back the input for each stream.
                try await self.runQUICServer(multiplexer: multiplexer, logger: logger) { stream in
                    try await stream.executeThenClose { inbound, outbound in
                        // Echo back the input.
                        try await outbound.write(contentsOf: inbound)
                        outbound.finish()
                    }
                }
            }

            // Create a client and open a single stream to the server.
            try await self.sendRequest(
                to: channel.localAddress!,
                trustStorePath: paths.trustStore,
                logger: logger
            ) { stream in
                // Send a message to the server.
                try await stream.executeThenClose { inbound, outbound in
                    let request = "echo"
                    try await outbound.write(ByteBuffer(string: request))
                    logger.info("Client sent: '\(request)'")
                    outbound.finish()

                    for try await buffer in inbound {
                        let reply = String(buffer: buffer)
                        logger.info("Client received: '\(reply)'")
                    }
                }
            }

            group.cancelAll()
        }
    }

    private static func makeQUICServer(
        host: String,
        certificateChainPath: String,
        privateKeyPath: String,
        logger: Logger
    ) async throws -> (
        any Channel, QUICHandler.ConnectionMultiplexer<NIOAsyncChannel<ByteBuffer, ByteBuffer>>
    ) {
        try await DatagramBootstrap(group: .singletonMultiThreadedEventLoopGroup)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .bind(host: host, port: 0) { channel in
                channel.eventLoop.makeCompletedFuture {
                    let quicConfiguration = QUICConfiguration.server(
                        serverName: "quic-test.local",
                        authenticationConfiguration: .x509Certificates(
                            certificateChainFilePath: certificateChainPath,
                            privateKeyFilePath: privateKeyPath
                        ),
                        applicationProtocols: ["echo-example"]
                    )
                    let (quicHandler, connectionMultiplexer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
                        channel: channel,
                        quicConfiguration: quicConfiguration,
                        logger: logger,
                        metrics: nil,
                        inboundStreamChannelInitializer: { streamChannel in
                            streamChannel.eventLoop.makeCompletedFuture {
                                try NIOAsyncChannel(
                                    wrappingChannelSynchronously: streamChannel,
                                    configuration: .init(
                                        isOutboundHalfClosureEnabled: true,
                                        inboundType: ByteBuffer.self,
                                        outboundType: ByteBuffer.self
                                    )
                                )
                            }
                        }
                    )
                    try channel.pipeline.syncOperations.addHandler(quicHandler)
                    return (channel, connectionMultiplexer)
                }
            }
    }

    private static func runQUICServer(
        multiplexer: QUICHandler.ConnectionMultiplexer<NIOAsyncChannel<ByteBuffer, ByteBuffer>>,
        logger: Logger,
        streamHandler: @Sendable @escaping (NIOAsyncChannel<ByteBuffer, ByteBuffer>) async throws -> Void
    ) async throws {
        try await withThrowingDiscardingTaskGroup { group in
            for await connection in multiplexer.inboundConnections {
                logger.info("Server handling QUIC connection")
                group.addTask {
                    try await withThrowingDiscardingTaskGroup { group in
                        for await streamChannel in connection.inboundStreams {
                            group.addTask {
                                logger.info("Server handling QUIC stream")
                                try await streamHandler(streamChannel)
                            }
                        }
                    }
                }
            }
        }
    }

    private static func sendRequest(
        to serverAddress: SocketAddress,
        trustStorePath: String,
        logger: Logger,
        streamHandler: @Sendable @escaping (NIOAsyncChannel<ByteBuffer, ByteBuffer>) async throws -> Void
    ) async throws {
        let (channel, clientConnectionMultiplexer) = try await DatagramBootstrap(
            group: .singletonMultiThreadedEventLoopGroup
        )
        .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
        .bind(
            host: "127.0.0.1",
            port: 0
        ) { channel -> EventLoopFuture<(any Channel, QUICHandler.ConnectionMultiplexer<Never>)> in
            channel.eventLoop.makeCompletedFuture {
                let quicConfiguration = QUICConfiguration.client(
                    verificationConfiguration: .x509Certificates(trustRootsFilePath: trustStorePath),
                    applicationProtocols: ["echo-example"]
                )
                let (quicHandler, connectionMultiplexer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
                    channel: channel,
                    quicConfiguration: quicConfiguration,
                    logger: logger,
                    metrics: nil,
                    inboundStreamChannelInitializer: { channel -> EventLoopFuture<Never> in
                        channel.eventLoop.makeCompletedFuture { fatalError() }
                    }
                )
                try channel.pipeline.syncOperations.addHandler(quicHandler)
                return (channel, connectionMultiplexer)
            }
        }

        let connection = try await clientConnectionMultiplexer.createNewConnection(
            serverName: serverAddress.ipAddress!,
            remoteAddress: serverAddress,
        ) { _ -> EventLoopFuture<Never> in
            fatalError()  // No inbound streams expected.
        }

        logger.info("Client created QUIC connection")

        let quicStream = try await connection.createBidirectionalStream { streamInitializer in
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

        logger.info("Client created QUIC stream")
        try await streamHandler(quicStream)
        try await channel.close().get()
    }
}
