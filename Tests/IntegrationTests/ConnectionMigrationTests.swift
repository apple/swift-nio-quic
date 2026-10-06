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
        let peers = try await ConnectedPeers.connect()

        // The client's socket "rebinds": its packets now come from a new port and its old address is gone.
        let newSocket = try await peers.makeClientSocket()
        try await peers.rebindClient(to: newSocket)

        // Only succeeds if the server moves the connection over to the client's new address.
        #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
        #expect(peers.serverConnectionChannel.remoteAddress == newSocket.localAddress)

        try await newSocket.close()
        try await peers.close()
    }

    @available(anyAppleOS 26, *)
    @Test(.timeLimit(.minutes(1)))
    func clientMigratesToTwoNewAddresses() async throws {
        let peers = try await ConnectedPeers.connect()

        var newSockets: [any Channel] = []
        for _ in 0..<2 {
            let newSocket = try await peers.makeClientSocket()
            newSockets.append(newSocket)
            try await peers.rebindClient(to: newSocket)

            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
            #expect(peers.serverConnectionChannel.remoteAddress == newSocket.localAddress)
        }

        for newSocket in newSockets {
            try await newSocket.close()
        }
        try await peers.close()
    }

    @available(anyAppleOS 26, *)
    @Test(.timeLimit(.minutes(1)))
    func clientMigratesBackToItsFirstAddress() async throws {
        let peers = try await ConnectedPeers.connect()
        let newSocket = try await peers.makeClientSocket()
        try await peers.rebindClient(to: newSocket)
        #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
        #expect(peers.serverConnectionChannel.remoteAddress == newSocket.localAddress)

        try await peers.rebindClient(to: nil)
        #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
        #expect(peers.serverConnectionChannel.remoteAddress == peers.clientChannel.localAddress)

        try await newSocket.close()
        try await peers.close()
    }

    /// Anyone who sees the connection's traffic can send packets to it from any address.
    @available(anyAppleOS 26, *)
    @Test(.timeLimit(.minutes(1)), arguments: [1, 2, 100])
    func serverIgnoresUnauthenticatedPacketsFromNewAddresses(addressCount: Int) async throws {
        let peers = try await ConnectedPeers.connect()
        let packet = try await peers.unauthenticatedPacket()

        // Every new address makes the server set up a path, which SwiftNetwork then probes.
        var spoofers: [any Channel] = []
        for _ in 0..<addressCount {
            let spoofer = try await DatagramBootstrap(group: peers.eventLoopGroup).bind(host: Self.host, port: 0).get()
            try await spoofer.writeAndFlush(AddressedEnvelope(remoteAddress: peers.serverAddress, data: packet))
            spoofers.append(spoofer)
        }
        // Let SwiftNetwork's migration timer fire: two paths probing at once used to crash the process.
        try await Task.sleep(for: .seconds(1))

        #expect(peers.serverConnectionChannel.remoteAddress == peers.clientChannel.localAddress)
        #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))

        for spoofer in spoofers {
            try await spoofer.close()
        }
        try await peers.close()
    }

    @available(anyAppleOS 26, *)
    @Test(.timeLimit(.minutes(1)))
    func serverCloseReachesClientAfterUnauthenticatedPacket() async throws {
        let peers = try await ConnectedPeers.connect()
        let spoofer = try await DatagramBootstrap(group: peers.eventLoopGroup).bind(host: Self.host, port: 0).get()
        try await spoofer.writeAndFlush(
            AddressedEnvelope(remoteAddress: peers.serverAddress, data: try await peers.unauthenticatedPacket())
        )
        try await Task.sleep(for: .milliseconds(100))

        let start = ContinuousClock.now
        try await peers.serverConnectionChannel.close()
        try await peers.clientConnectionChannel.closeFuture.get()
        // Without the server's CONNECTION_CLOSE the client only notices once its idle timeout fires.
        #expect(ContinuousClock.now - start < .seconds(5))

        try await spoofer.close()
        try await peers.close()
    }

    private static let host = "127.0.0.1"

    /// A client connected to a server over loopback, after one successful request.
    @available(anyAppleOS 26, *)
    private struct ConnectedPeers {
        var eventLoopGroup: MultiThreadedEventLoopGroup
        var serverChannel: any Channel
        var serverAddress: SocketAddress
        var serverConnectionChannel: any Channel
        var clientChannel: any Channel
        var clientConnectionChannel: any Channel
        var streamCreator: QUICStreamCreator

        static func connect() async throws -> ConnectedPeers {
            // One thread: the relay hands datagrams from one channel's pipeline to the other's synchronously.
            let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
            let serverConnectionChannel = NIOLockedValueBox<(any Channel)?>(nil)

            let serverChannel = try await createServerChannel(
                eventLoopGroup: eventLoopGroup,
                host: ConnectionMigrationTests.host,
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
            let serverAddress = try SocketAddress(
                ipAddress: ConnectionMigrationTests.host,
                port: serverChannel.localAddress!.port!
            )

            let clientChannel = try await createClientChannel(
                eventLoopGroup: eventLoopGroup,
                host: ConnectionMigrationTests.host,
                port: 0,
                logger: Logger(label: "Client"),
                udpChannelInitializer: { channel in
                    try channel.pipeline.syncOperations.addHandler(AddressSwitchHandler())
                    try channel.pipeline.syncOperations.addHandler(ShortHeaderRecorder())
                }
            ).get()
            let (clientConnectionChannel, streamCreator) = try await clientChannel.pipeline.handler(
                type: QUICHandler<QUICStreamChannels>.self
            )
            .flatMap { quicHandler in
                quicHandler.createOutboundConnection(
                    serverName: "\(ConnectionMigrationTests.host):\(serverAddress.port!)",
                    remoteAddress: serverAddress,
                    connectionInitializer: { channel, _ in channel.eventLoop.makeSucceededVoidFuture() },
                    inboundStreamInitializer: { channel in channel.eventLoop.makeSucceededVoidFuture() }
                )
            }.get()
            #expect(
                try await ConnectionMigrationTests.request(on: streamCreator) == ByteBuffer(string: "<b>Success</b>")
            )

            return ConnectedPeers(
                eventLoopGroup: eventLoopGroup,
                serverChannel: serverChannel,
                serverAddress: serverAddress,
                serverConnectionChannel: try #require(serverConnectionChannel.withLockedValue { $0 }),
                clientChannel: clientChannel,
                clientConnectionChannel: clientConnectionChannel,
                streamCreator: streamCreator
            )
        }

        /// A packet the server routes to the client's connection but can't decrypt: what anyone who
        /// sees the connection's traffic can send from any address.
        func unauthenticatedPacket() async throws -> ByteBuffer {
            let lastPacket = try await self.clientChannel.eventLoop.submit {
                try self.clientChannel.pipeline.syncOperations.handler(type: ShortHeaderRecorder.self).lastPacket
            }.get()
            let clientPacket = try #require(lastPacket)
            // A short header starts with one byte of flags, followed by the server's connection ID.
            let connectionID = try #require(
                clientPacket.getSlice(at: clientPacket.readerIndex + 1, length: Int(QUICConnectionID.randomIDLength))
            )
            var packet = ByteBuffer()
            packet.writeInteger(UInt8(0x40))  // Short header with the fixed bit set.
            packet.writeImmutableBuffer(connectionID)
            packet.writeRepeatingByte(0xAA, count: 41)
            return packet
        }

        /// A new socket for the client: what it receives goes into the client's pipeline while the client uses it.
        func makeClientSocket() async throws -> any Channel {
            let clientChannel = self.clientChannel
            return try await DatagramBootstrap(group: self.eventLoopGroup)
                .channelInitializer { channel in
                    channel.eventLoop.makeCompletedFuture {
                        try channel.pipeline.syncOperations.addHandler(RelayHandler(clientChannel: clientChannel))
                    }
                }
                .bind(host: ConnectionMigrationTests.host, port: 0)
                .get()
        }

        /// Makes the client use `socket`, or its own socket for `nil`, as if its socket rebound to that port.
        func rebindClient(to socket: (any Channel)?) async throws {
            let clientChannel = self.clientChannel
            try await clientChannel.eventLoop.submit {
                let switchHandler = try clientChannel.pipeline.syncOperations.handler(type: AddressSwitchHandler.self)
                switchHandler.currentSocket = socket
            }.get()
        }

        func close() async throws {
            try await self.clientChannel.close()
            try await self.serverChannel.close()
            try await self.eventLoopGroup.shutdownGracefully()
        }
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

/// Sits in front of the client's `QUICHandler` and simulates its socket rebinding to other ports: datagrams
/// leave through `currentSocket`, or the client's own socket while it is `nil`. Datagrams arriving on any
/// other socket are dropped because that address is gone.
@available(anyAppleOS 26, *)
private final class AddressSwitchHandler: ChannelDuplexHandler {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>
    typealias OutboundIn = AddressedEnvelope<ByteBuffer>

    var currentSocket: (any Channel)?

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if self.currentSocket == nil {
            context.fireChannelRead(data)
        }
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        if let currentSocket = self.currentSocket {
            currentSocket.writeAndFlush(self.unwrapOutboundIn(data), promise: promise)
        } else {
            context.write(data, promise: promise)
        }
    }
}

/// Installed on a new socket of the client. While the client uses that socket, feeds what arrives there into
/// the client's pipeline just past the `AddressSwitchHandler`, so the client's `QUICHandler` receives it as before.
@available(anyAppleOS 26, *)
private final class RelayHandler: ChannelInboundHandler {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>

    private let clientChannel: any Channel

    init(clientChannel: any Channel) {
        self.clientChannel = clientChannel
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.switchContext(for: context.channel)?.fireChannelRead(data)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        self.switchContext(for: context.channel)?.fireChannelReadComplete()
    }

    // Both channels share the test's event loop, so the client's pipeline can be used synchronously.
    private func switchContext(for socket: any Channel) -> ChannelHandlerContext? {
        let operations = self.clientChannel.pipeline.syncOperations
        guard let switchHandler = try? operations.handler(type: AddressSwitchHandler.self),
            switchHandler.currentSocket === socket
        else {
            return nil
        }
        return try? operations.context(handlerType: AddressSwitchHandler.self)
    }
}

/// Sits in the client's pipeline and remembers the last short-header packet the client sent, which
/// carries the server's connection ID.
@available(anyAppleOS 26, *)
private final class ShortHeaderRecorder: ChannelOutboundHandler {
    typealias OutboundIn = AddressedEnvelope<ByteBuffer>

    private(set) var lastPacket: ByteBuffer?

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let envelope = self.unwrapOutboundIn(data)
        // The most significant bit of the first byte is 0 for short headers.
        if let firstByte = envelope.data.getInteger(at: envelope.data.readerIndex, as: UInt8.self),
            firstByte & 0x80 == 0
        {
            self.lastPacket = envelope.data
        }
        context.write(data, promise: promise)
    }
}
