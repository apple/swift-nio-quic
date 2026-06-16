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
import NIOPosix
import XCTest

@testable import NIOQUIC

/// Bind a UDP datagram channel and add a QUIC server `QUICHandler` to its pipeline.
///
/// - Parameters:
///   - udpChannelInitializer: Called once the UDP channel is bound, before
///     the `QUICHandler` is added. Use this to install handlers on the
///     UDP channel pipeline.
///   - inboundConnectionInitializer: Called for each accepted QUIC
///     connection. Use this to install handlers on the connection channel
///     pipeline.
///   - inboundStreamInitializer: Called for each inbound QUIC stream on
///     accepted connections. Defaults to a no-op.
///   - noMoreConnections: Called when the handler becomes inactive and no
///     further connections will be accepted.
func createServerChannel(
    eventLoopGroup: any EventLoopGroup,
    host: String,
    port: Int,
    logger: Logger,
    udpChannelInitializer: @Sendable @escaping (any Channel) throws -> Void = { _ in },
    inboundConnectionInitializer:
        @Sendable @escaping (any Channel, NIOQUIC.QUICStreamCreator) -> EventLoopFuture<Void>,
    inboundStreamInitializer:
        @Sendable @escaping (any Channel) -> EventLoopFuture<Void> = { $0.eventLoop.makeSucceededVoidFuture() },
    noMoreConnections: @Sendable @escaping () -> Void
) -> EventLoopFuture<any Channel> {
    let quicConfiguration = QUICConfiguration.server(
        serverName: "quic-test.local",
        authenticationConfiguration: .rawPublicKeys(
            publicKeyFilePath: Bundle.module.url(forResource: "publicKey", withExtension: "der")!.path,
            privateKeyFilePath: Bundle.module.url(forResource: "privateKey", withExtension: "der")!.path
        ),
        applicationProtocols: ["http/0.9"],
        keyLogPath: "/tmp/quic-sync-integration-tests-keylogs"
    )
    return createQUICChannel(
        eventLoopGroup: eventLoopGroup,
        host: host,
        port: port,
        quicConfiguration: quicConfiguration,
        logger: logger,
        udpChannelInitializer: udpChannelInitializer,
        inboundConnectionInitializer: inboundConnectionInitializer,
        inboundStreamInitializer: inboundStreamInitializer,
        noMoreConnections: noMoreConnections
    )
}

/// Bind a UDP datagram channel and add a QUIC client `QUICHandler` to its pipeline.
///
/// - Parameter udpChannelInitializer: Called once the UDP channel is bound,
///   before the `QUICHandler` is added. Use this to install handlers on
///   the UDP channel pipeline.
func createClientChannel(
    eventLoopGroup: any EventLoopGroup,
    host: String,
    port: Int,
    logger: Logger,
    udpChannelInitializer: @Sendable @escaping (any Channel) throws -> Void = { _ in }
) -> EventLoopFuture<any Channel> {
    let quicConfiguration = QUICConfiguration.client(
        verificationConfiguration: .rawPublicKeys(
            publicKeyFilePath: Bundle.module.url(forResource: "publicKey", withExtension: "der")!.path
        ),
        applicationProtocols: ["http/0.9"]
    )

    return createQUICChannel(
        eventLoopGroup: eventLoopGroup,
        host: host,
        port: port,
        quicConfiguration: quicConfiguration,
        logger: logger,
        udpChannelInitializer: udpChannelInitializer,
        inboundConnectionInitializer: { _, _ in fatalError() },
        inboundStreamInitializer: { $0.eventLoop.makeSucceededVoidFuture() },
        noMoreConnections: {}
    )
}

private func createQUICChannel(
    eventLoopGroup: any EventLoopGroup,
    host: String,
    port: Int,
    quicConfiguration: QUICConfiguration,
    logger: Logger,
    udpChannelInitializer: @Sendable @escaping (any Channel) throws -> Void,
    inboundConnectionInitializer:
        @Sendable @escaping (any Channel, NIOQUIC.QUICStreamCreator) -> EventLoopFuture<Void>,
    inboundStreamInitializer: @Sendable @escaping (any Channel) -> EventLoopFuture<Void>,
    noMoreConnections: @Sendable @escaping () -> Void
) -> EventLoopFuture<any Channel> {
    DatagramBootstrap(group: eventLoopGroup)
        .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
        .channelOption(ChannelOptions.maxMessagesPerRead, value: 32)
        .bind(host: host, port: port)
        .flatMapThrowing { channel in
            try udpChannelInitializer(channel)
            let quicHandler = QUICHandler(
                channel: channel,
                quicConfiguration: quicConfiguration,
                asyncVerifier: nil,
                authenticator: nil,
                logger: logger,
                metrics: nil,
                inboundConnectionInitializer: inboundConnectionInitializer,
                inboundStreamInitializer: inboundStreamInitializer,
                noMoreConnections: noMoreConnections
            )
            try channel.pipeline.syncOperations.addHandler(quicHandler)
            return channel
        }
}
