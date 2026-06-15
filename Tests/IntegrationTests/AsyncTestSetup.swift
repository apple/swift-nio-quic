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
import NIOCore
import NIOEmbedded
import NIOPosix
import XCTest

@testable import NIOQUIC

enum TestingMode {
    case network(eventLoopGroup: any EventLoopGroup, host: String)
    case testing(eventLoop: NIOAsyncTestingEventLoop)
}

func makeClientAndServerPair(
    testMode: TestingMode = .network(
        eventLoopGroup: MultiThreadedEventLoopGroup.singleton,
        host: "127.0.0.1"
    ),
    maxIdleTimeout: Duration = .milliseconds(5000),
    clientKeepAliveTime: Duration? = nil,
    extraClientALPNs: [String] = [],
    clientDebugChannelInitializer: (@Sendable (any Channel) throws -> Void)? = nil,
    clientConnectionIDGenerator: any QUICConnectionIDGenerator = RandomQUICConnectionIDGenerator(),
    serverConnectionIDGenerator: any QUICConnectionIDGenerator = RandomQUICConnectionIDGenerator(),
    initialMaxData: Int = 16_777_216,
    initialMaxStreamDataBidi: Int = 2_097_152,
    initialMaxStreamsBidi: Int = 8
) async throws -> (
    clientChannel: any Channel,
    serverChannel: any Channel,
    serverMultiplexer: QUICHandler.ConnectionMultiplexer<NIOAsyncChannel<ByteBuffer, ByteBuffer>>,
    clientMultiplexer: QUICHandler.ConnectionMultiplexer<Never>
) {
    let certUID = UUID().uuidString
    let publicKeyPath = FileManager.default.temporaryDirectory.appendingPathComponent(
        "test-publickey-\(certUID).der"
    )
    let privateKeyPath = FileManager.default.temporaryDirectory.appendingPathComponent(
        "test-privatekey-\(certUID).der"
    )

    let privateKey = P256.Signing.PrivateKey()
    let publicKey = privateKey.publicKey

    try privateKey.derRepresentation.write(to: privateKeyPath)
    try publicKey.derRepresentation.write(to: publicKeyPath)

    switch testMode {
    case .network(let eventLoopGroup, let host):
        return try await makeNetworkClientAndServerPair(
            eventLoopGroup: eventLoopGroup,
            host: host,
            publicKeyPath: publicKeyPath.path,
            privateKeyPath: privateKeyPath.path,
            maxIdleTimeout: maxIdleTimeout,
            clientKeepAliveTime: clientKeepAliveTime,
            extraClientALPNs: extraClientALPNs,
            clientDebugChannelInitializer: clientDebugChannelInitializer,
            clientConnectionIDGenerator: clientConnectionIDGenerator,
            serverConnectionIDGenerator: serverConnectionIDGenerator,
            initialMaxData: initialMaxData,
            initialMaxStreamDataBidi: initialMaxStreamDataBidi,
            initialMaxStreamsBidi: initialMaxStreamsBidi
        )

    case .testing(let testingEventLoop):
        return try await makeMockClientAndServerPair(
            testingEventLoop: testingEventLoop,
            publicKeyPath: publicKeyPath.path,
            privateKeyPath: privateKeyPath.path,
            maxIdleTimeout: maxIdleTimeout,
            clientKeepAliveTime: clientKeepAliveTime,
        )
    }
}

func makeMockClientAndServerPair(
    testingEventLoop: NIOAsyncTestingEventLoop,
    publicKeyPath: String,
    privateKeyPath: String,
    maxIdleTimeout: Duration = .milliseconds(5000),
    clientKeepAliveTime: Duration? = nil,
    extraClientALPNs: [String] = [],
    clientDebugChannelInitializer: (@Sendable (any Channel) throws -> Void)? = nil
) async throws -> (
    clientChannel: any Channel,
    serverChannel: any Channel,
    serverMultiplexer: QUICHandler.ConnectionMultiplexer<NIOAsyncChannel<ByteBuffer, ByteBuffer>>,
    clientMultiplexer: QUICHandler.ConnectionMultiplexer<Never>
) {
    // Create testing channels for client and server communication
    let serverChannel = NIOAsyncTestingChannel(loop: testingEventLoop)
    let clientChannel = NIOAsyncTestingChannel(loop: testingEventLoop)

    try await testingEventLoop.executeInContext {
        serverChannel.localAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 8080)
        serverChannel.remoteAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 9090)
        clientChannel.localAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 9090)
        clientChannel.remoteAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 8080)
    }

    try await serverChannel.connect(to: serverChannel.remoteAddress!)
    try await clientChannel.connect(to: clientChannel.remoteAddress!)

    // Set up mock QUIC handlers and multiplexers
    let serverMultiplexer = try await testingEventLoop.executeInContext {
        // Mock server multiplexer setup
        let (quicHandler, connectionMultiplexer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
            channel: serverChannel,
            quicConfiguration: QUICConfiguration.server(
                serverName: "quic-test.local",
                authenticationConfiguration: AuthenticationConfiguration.rawPublicKeys(
                    publicKeyFilePath: publicKeyPath,
                    privateKeyFilePath: privateKeyPath
                ),
                applicationProtocols: ["http/0.9"]
            ),
            logger: Logger(label: "Testing Server"),
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
        try serverChannel.pipeline.syncOperations.addHandler(quicHandler)
        serverChannel.pipeline.fireChannelActive()
        return connectionMultiplexer
    }

    let clientMultiplexer = try await testingEventLoop.executeInContext {
        // Mock client multiplexer setup
        let clientConfiguration = QUICConfiguration.client(
            verificationConfiguration: VerificationConfiguration.rawPublicKeys(publicKeyFilePath: publicKeyPath),
            applicationProtocols: ["http/0.9"] + extraClientALPNs,
            maxIdleTimeout: maxIdleTimeout,
            keepAliveInterval: clientKeepAliveTime
        )

        let (quicHandler, connectionMultiplexer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
            channel: clientChannel,
            quicConfiguration: clientConfiguration,
            logger: Logger(label: "Testing Client"),
            metrics: nil,
            inboundStreamChannelInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )
        try clientChannel.pipeline.syncOperations.addHandler(quicHandler)
        clientChannel.pipeline.fireChannelActive()
        if let clientDebugChannelInitializer {
            try clientDebugChannelInitializer(clientChannel)
        }
        return connectionMultiplexer
    }
    return (
        clientChannel: clientChannel,
        serverChannel: serverChannel,
        serverMultiplexer: serverMultiplexer,
        clientMultiplexer: clientMultiplexer
    )
}

func makeNetworkClientAndServerPair(
    eventLoopGroup: any EventLoopGroup,
    host: String,
    publicKeyPath: String,
    privateKeyPath: String,
    maxIdleTimeout: Duration = .milliseconds(5000),
    clientKeepAliveTime: Duration? = nil,
    extraClientALPNs: [String] = [],
    clientDebugChannelInitializer: (@Sendable (any Channel) throws -> Void)? = nil,
    clientConnectionIDGenerator: any QUICConnectionIDGenerator = RandomQUICConnectionIDGenerator(),
    serverConnectionIDGenerator: any QUICConnectionIDGenerator = RandomQUICConnectionIDGenerator(),
    initialMaxData: Int = 16_777_216,
    initialMaxStreamDataBidi: Int = 2_097_152,
    initialMaxStreamsBidi: Int = 8
) async throws -> (
    clientChannel: any Channel,
    serverChannel: any Channel,
    serverMultiplexer: QUICHandler.ConnectionMultiplexer<NIOAsyncChannel<ByteBuffer, ByteBuffer>>,
    clientMultiplexer: QUICHandler.ConnectionMultiplexer<Never>
) {
    let (serverChannel, serverMultiplexer) = try await setUpServerChannelAndConnectionMultiplexer(
        eventLoopGroup: eventLoopGroup,
        host: host,
        publicKeyFilePath: publicKeyPath,
        privateKeyFilePath: privateKeyPath,
        connectionIDGenerator: serverConnectionIDGenerator,
        initialMaxData: initialMaxData,
        initialMaxStreamDataBidi: initialMaxStreamDataBidi,
        initialMaxStreamsBidi: initialMaxStreamsBidi
    )
    let (clientChannel, clientMultiplexer) = try await setUpClientConnectionMultiplexer(
        eventLoopGroup: eventLoopGroup,
        host: host,
        maxIdleTimeout: maxIdleTimeout,
        publicKeyFilePath: publicKeyPath,
        clientKeepAliveTime: clientKeepAliveTime,
        extraClientALPNs: extraClientALPNs,
        debugChannelInitializer: clientDebugChannelInitializer,
        connectionIDGenerator: clientConnectionIDGenerator,
        initialMaxData: initialMaxData,
        initialMaxStreamDataBidi: initialMaxStreamDataBidi,
        initialMaxStreamsBidi: initialMaxStreamsBidi
    )
    return (
        clientChannel: clientChannel,
        serverChannel: serverChannel,
        serverMultiplexer: serverMultiplexer,
        clientMultiplexer: clientMultiplexer
    )
}

private func setUpClientConnectionMultiplexer(
    eventLoopGroup: any EventLoopGroup,
    host: String,
    maxIdleTimeout: Duration = .milliseconds(30000),  // time in ms
    publicKeyFilePath: String,
    clientKeepAliveTime: Duration? = nil,
    extraClientALPNs: [String] = [],
    debugChannelInitializer: (@Sendable (any Channel) throws -> Void)? = nil,
    connectionIDGenerator: any QUICConnectionIDGenerator = RandomQUICConnectionIDGenerator(),
    initialMaxData: Int = 16_777_216,
    initialMaxStreamDataBidi: Int = 2_097_152,
    initialMaxStreamsBidi: Int = 8
) async throws -> (
    serverChannel: any Channel,
    QUICHandler.ConnectionMultiplexer<Never>
) {
    let (channel, multiplexer) = try await DatagramBootstrap(group: eventLoopGroup)
        .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
        .channelOption(ChannelOptions.maxMessagesPerRead, value: 32)
        .bind(host: host, port: 0) { channel in
            channel.eventLoop.makeCompletedFuture {
                var logger = Logger(label: "Testing Client")
                logger.logLevel = .debug
                let clientConfiguration = QUICConfiguration.client(
                    verificationConfiguration: .rawPublicKeys(
                        publicKeyFilePath: publicKeyFilePath
                    ),
                    applicationProtocols: ["http/0.9"] + extraClientALPNs,
                    maxIdleTimeout: maxIdleTimeout,
                    initialMaxData: initialMaxData,
                    initialMaxStreamDataBidiLocal: initialMaxStreamDataBidi,
                    initialMaxStreamDataBidiRemote: initialMaxStreamDataBidi,
                    initialMaxStreamsBidi: initialMaxStreamsBidi,
                    keyLogPath: "/tmp/quic-integration-tests-keylogs"
                )
                let (quicHandler, connectionMultiplexer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
                    channel: channel,
                    quicConfiguration: clientConfiguration,
                    logger: logger,
                    metrics: nil,
                    inboundStreamChannelInitializer: { channel in
                        channel.eventLoop.makeCompletedFuture { fatalError() }
                    },
                    quicConnectionIDGenerator: connectionIDGenerator
                )
                try channel.pipeline.syncOperations.addHandler(quicHandler)
                if let debugChannelInitializer {
                    try debugChannelInitializer(channel)
                }
                return (channel, connectionMultiplexer)
            }
        }

    return (channel, multiplexer)
}

private func setUpServerChannelAndConnectionMultiplexer(
    eventLoopGroup: any EventLoopGroup,
    host: String,
    publicKeyFilePath: String,
    privateKeyFilePath: String,
    connectionIDGenerator: any QUICConnectionIDGenerator = RandomQUICConnectionIDGenerator(),
    initialMaxData: Int = 16_777_216,
    initialMaxStreamDataBidi: Int = 2_097_152,
    initialMaxStreamsBidi: Int = 8
) async throws -> (any Channel, QUICHandler.ConnectionMultiplexer<NIOAsyncChannel<ByteBuffer, ByteBuffer>>) {
    var mutableLogger = Logger(label: "Testing Server")
    mutableLogger.logLevel = .debug
    let logger = mutableLogger
    let (channel, multiplexer) = try await DatagramBootstrap(group: eventLoopGroup)
        .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
        .channelOption(ChannelOptions.maxMessagesPerRead, value: 32)
        .bind(host: host, port: 0) { channel in
            channel.eventLoop.makeCompletedFuture {
                let quicConfiguration = QUICConfiguration.server(
                    serverName: "quic-test.local",
                    authenticationConfiguration: AuthenticationConfiguration.rawPublicKeys(
                        publicKeyFilePath: publicKeyFilePath,
                        privateKeyFilePath: privateKeyFilePath
                    ),
                    applicationProtocols: ["http/0.9"],
                    initialMaxData: initialMaxData,
                    initialMaxStreamDataBidiLocal: initialMaxStreamDataBidi,
                    initialMaxStreamDataBidiRemote: initialMaxStreamDataBidi,
                    initialMaxStreamsBidi: initialMaxStreamsBidi,
                )
                let (quicHandler, connectionMultiplexer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
                    channel: channel,
                    quicConfiguration: quicConfiguration,
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
                    },
                    quicConnectionIDGenerator: connectionIDGenerator
                )
                try channel.pipeline.syncOperations.addHandler(quicHandler)
                return (channel, connectionMultiplexer)
            }
        }

    logger.info("Started on port \(channel.localAddress!.port!)")

    return (channel, multiplexer)
}
