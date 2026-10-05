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
import Testing

@testable import NIOQUIC

struct ConnectionMigrationTests {
    @available(anyAppleOS 26, *)
    @Test(.timeLimit(.minutes(1)))
    func clientContinuesConnectionFromNewAddress() async throws {
        // One thread: the relay hands datagrams from one channel's pipeline to the other's synchronously.
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let host = "127.0.0.1"
        let serverConnectionChannel = NIOLockedValueBox<(any Channel)?>(nil)

        let serverChannel = try await createServerChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: Logger(label: "Server"),
            inboundConnectionInitializer: { connectionChannel, _ in
                serverConnectionChannel.withLockedValue { $0 = connectionChannel }
                return connectionChannel.eventLoop.makeSucceededVoidFuture()
            },
            inboundStreamInitializer: { streamChannel in
                streamChannel.eventLoop.makeCompletedFuture {
                    try streamChannel.pipeline.syncOperations.addHandler(TestServerHandler())
                }
            },
            noMoreConnections: {}
        ).get()
        let serverPort = serverChannel.localAddress!.port!

        let clientChannel = try await createClientChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: Logger(label: "Client"),
            udpChannelInitializer: { channel in
                try channel.pipeline.syncOperations.addHandler(AddressSwitchHandler())
            }
        ).get()
        let (_, streamCreator) = try await clientChannel.pipeline.handler(type: QUICHandler<QUICStreamChannels>.self)
            .flatMap { quicHandler in
                quicHandler.createOutboundConnection(
                    serverName: "\(host):\(serverPort)",
                    remoteAddress: try! .init(ipAddress: host, port: serverPort),
                    connectionInitializer: { channel, _ in channel.eventLoop.makeSucceededVoidFuture() },
                    inboundStreamInitializer: { channel in channel.eventLoop.makeSucceededVoidFuture() }
                )
            }.get()
        #expect(try await Self.request(on: streamCreator) == ByteBuffer(string: "<b>Success</b>"))

        // The client's socket "rebinds": its packets now come from a new port and its old address is gone.
        let newSocket = try await DatagramBootstrap(group: eventLoopGroup)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(RelayHandler(clientChannel: clientChannel))
                }
            }
            .bind(host: host, port: 0)
            .get()
        try await clientChannel.eventLoop.submit {
            try clientChannel.pipeline.syncOperations.handler(type: AddressSwitchHandler.self).newSocket = newSocket
        }.get()

        // Only succeeds if the server moves the connection over to the client's new address.
        #expect(try await Self.request(on: streamCreator) == ByteBuffer(string: "<b>Success</b>"))
        #expect(serverConnectionChannel.withLockedValue { $0 }?.remoteAddress == newSocket.localAddress)

        try await newSocket.close()
        try await clientChannel.close()
        try await serverChannel.close()
        try await eventLoopGroup.shutdownGracefully()
    }

    /// Sends a request on a new stream and returns the full response.
    @available(anyAppleOS 26, *)
    private static func request(on streamCreator: QUICStreamCreator) async throws -> ByteBuffer {
        let stream = try await streamCreator.createBidirectionalStream { initializer in
            initializer.channel.eventLoop.makeCompletedFuture {
                try NIOAsyncChannel(
                    wrappingChannelSynchronously: initializer.channel,
                    configuration: .init(
                        isOutboundHalfClosureEnabled: true,
                        inboundType: ByteBuffer.self,
                        outboundType: ByteBuffer.self
                    )
                )
            }
        }.get()
        return try await stream.executeThenClose { inbound, outbound in
            try await outbound.write(ByteBuffer(string: "GET /foo"))
            outbound.finish()
            var response = ByteBuffer()
            for try await chunk in inbound {
                response.writeImmutableBuffer(chunk)
            }
            return response
        }
    }
}

/// Sits in front of the client's `QUICHandler` and simulates its socket rebinding to a new port: once
/// `newSocket` is set, datagrams leave through it, and datagrams still arriving on the old socket are
/// dropped because the old address is gone.
@available(anyAppleOS 26, *)
private final class AddressSwitchHandler: ChannelDuplexHandler {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>
    typealias OutboundIn = AddressedEnvelope<ByteBuffer>

    var newSocket: (any Channel)?

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if self.newSocket == nil {
            context.fireChannelRead(data)
        }
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        if let newSocket = self.newSocket {
            newSocket.writeAndFlush(self.unwrapOutboundIn(data), promise: promise)
        } else {
            context.write(data, promise: promise)
        }
    }
}

/// Installed on the client's new socket. Feeds what arrives there into the client's pipeline just past
/// the `AddressSwitchHandler`, so the client's `QUICHandler` receives it as before.
@available(anyAppleOS 26, *)
private final class RelayHandler: ChannelInboundHandler {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>

    private let clientChannel: any Channel

    init(clientChannel: any Channel) {
        self.clientChannel = clientChannel
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.switchContext()?.fireChannelRead(data)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        self.switchContext()?.fireChannelReadComplete()
    }

    // Both channels share the test's event loop, so the client's pipeline can be used synchronously.
    private func switchContext() -> ChannelHandlerContext? {
        try? self.clientChannel.pipeline.syncOperations.context(handlerType: AddressSwitchHandler.self)
    }
}
