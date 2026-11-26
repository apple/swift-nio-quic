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
import NIOQUICHelpers
import Testing

@testable import NIOQUIC

@Suite(.timeLimit(.minutes(1)))
struct ConnectionMigrationTests {
    @available(anyAppleOS 27, *)
    @Test
    func clientContinuesConnectionFromNewAddress() async throws {
        try await Self.withConnectedPeers { peers in
            // The client's socket "rebinds": its packets now come from a new port and its old address is gone.
            let newSocket = try await peers.makeClientSocket()
            try await peers.rebindClient(to: newSocket)

            // Only succeeds if the server moves the connection over to the client's new address.
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
            #expect(peers.serverConnectionChannel.remoteAddress == newSocket.localAddress)
            // The server set up the request's stream before switching to the new path.
            #expect(peers.latestServerStreamChannel.withLockedValue { $0 }?.remoteAddress == newSocket.localAddress)
        }
    }

    @available(anyAppleOS 27, *)
    @Test
    func clientMigratesToTwoNewAddresses() async throws {
        try await Self.withConnectedPeers { peers in
            for _ in 0..<2 {
                let newSocket = try await peers.makeClientSocket()
                try await peers.rebindClient(to: newSocket)

                #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
                #expect(peers.serverConnectionChannel.remoteAddress == newSocket.localAddress)
            }
        }
    }

    @available(anyAppleOS 27, *)
    @Test(.enabled { await Self.isDualStackAvailable() })
    func dualStackServerFollowsClientToTwoNewAddresses() async throws {
        try await Self.withConnectedPeers(serverHost: "::") { peers in
            for _ in 0..<2 {
                let newSocket = try await peers.makeClientSocket()
                try await peers.rebindClient(to: newSocket)

                #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
                let mappedAddress = try SocketAddress(
                    ipAddress: "::ffff:\(Self.host)",
                    port: newSocket.localAddress!.port!
                )
                #expect(peers.serverConnectionChannel.remoteAddress == mappedAddress)
            }
        }
    }

    @available(anyAppleOS 27, *)
    @Test
    func clientMigratesBackToItsFirstAddress() async throws {
        try await Self.withConnectedPeers { peers in
            let newSocket = try await peers.makeClientSocket()
            try await peers.rebindClient(to: newSocket)
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
            #expect(peers.serverConnectionChannel.remoteAddress == newSocket.localAddress)

            try await peers.rebindClient(to: nil)
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
            #expect(peers.serverConnectionChannel.remoteAddress == peers.clientChannel.localAddress)
        }
    }

    @available(anyAppleOS 27, *)
    @Test func clientReturnsBeforeItsNewAddressIsValidated() async throws {
        try await Self.withConnectedPeers { peers in
            // The client never receives on the new socket, so it never answers the server's PATH_CHALLENGE there.
            let newSocket = try await peers.makeClientSocket(receives: false)
            try await peers.rebindClient(to: newSocket)
            let previousStream = peers.latestServerStreamChannel.withLockedValue { $0 }

            let response = try await Self.request(on: peers.streamCreator) {
                // Go back once the request reached the server from the new address.
                try await peers.waitForServerStream(after: previousStream)
                try await peers.rebindClient(to: nil)
            }
            #expect(response == ByteBuffer(string: "<b>Success</b>"))
            #expect(peers.serverConnectionChannel.remoteAddress == peers.clientChannel.localAddress)
        }
    }

    @available(anyAppleOS 27, *)
    @Test func clientHopsThroughAddressesItNeverReceivesOn() async throws {
        try await Self.withConnectedPeers { peers in
            // Rotate a few addresses, but don't use them.
            for _ in 0..<5 {
                try await peers.rebindClient(to: try await peers.makeClientSocket(receives: false))
                try await Self.sendRequest(on: peers)
            }
            // Switch back to our original path and expect the server to follow.
            try await peers.rebindClient(to: nil)
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
            #expect(peers.serverConnectionChannel.remoteAddress == peers.clientChannel.localAddress)
        }
    }

    @available(anyAppleOS 27, *)
    @Test func serverLimitsWhatItSendsToAnUnvalidatedAddress() async throws {
        try await Self.withConnectedPeers { peers in
            // The client never receives on the new socket, so the server never validates its address.
            let newSocket = try await peers.makeClientSocket(receives: false)
            try await peers.rebindClient(to: newSocket)
            try await Self.sendRequest(on: peers)
            // Let SwiftNetwork's migration timer fire, and the client's retransmissions arrive.
            try await Task.sleep(for: .seconds(1))
            try await peers.rebindClient(to: nil)

            // Until the server validated an address, it sends there at most three
            // times what it received from there (RFC 9000, Section 8.1).
            let counts = try await Self.byteCounts(of: newSocket)
            #expect(counts.received > 0)
            #expect(counts.received <= 3 * counts.sent)
        }
    }

    @available(anyAppleOS 27, *)
    @Test func clientCloseReachesServerAfterMigration() async throws {
        try await Self.withConnectedPeers { peers in
            let newSocket = try await peers.makeClientSocket()
            try await peers.rebindClient(to: newSocket)
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))

            try await peers.clientConnectionChannel.close()
            try await peers.serverConnectionChannel.closeFuture.get()
        }
    }

    /// Anyone who sees the connection's traffic can send packets to it from any address.
    @available(anyAppleOS 27, *)
    @Test(arguments: [1, 2, 100])
    func serverIgnoresUnauthenticatedPacketsFromNewAddresses(addressCount: Int) async throws {
        try await Self.withConnectedPeers { peers in
            let packet = try await peers.unauthenticatedPacket()

            // Every new address makes the server set up a path, which SwiftNetwork may probe.
            var spoofers: [any Channel] = []
            for _ in 0..<addressCount {
                let spoofer = try await DatagramBootstrap(group: peers.eventLoopGroup)
                    .channelInitializer { channel in
                        channel.eventLoop.makeCompletedFuture {
                            try channel.pipeline.syncOperations.addHandler(ByteCounter())
                        }
                    }
                    .bind(host: Self.host, port: 0)
                    .get()
                try await spoofer.writeAndFlush(AddressedEnvelope(remoteAddress: peers.serverAddress, data: packet))
                spoofers.append(spoofer)
            }
            // Let SwiftNetwork's migration timer fire.
            try await Task.sleep(for: .seconds(1))

            // The server must not amplify a spoofer's traffic, not even by probing its address (RFC 9000, Section 8.1).
            for spoofer in spoofers {
                let counts = try await Self.byteCounts(of: spoofer)
                #expect(counts.received <= 3 * counts.sent)
            }
            #expect(peers.serverConnectionChannel.remoteAddress == peers.clientChannel.localAddress)
            #expect(try await peers.serverPathUpdates().isEmpty)
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
        }
    }

    /// The unauthenticated packet must not interrupt .
    @available(anyAppleOS 27, *)
    @Test
    func serverCloseReachesClientAfterUnauthenticatedPacket() async throws {
        try await Self.withConnectedPeers { peers in
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
        }
    }

    @available(anyAppleOS 27, *)
    @Test func serverReportsTheClientsNewAddressAndItsValidation() async throws {
        try await Self.withConnectedPeers { peers in
            let newSocket = try await peers.makeClientSocket()
            try await peers.rebindClient(to: newSocket)
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))

            // The server moves to the new address before validating it (RFC 9000, Section 9.3).
            let newAddress = try #require(newSocket.localAddress)
            let updates = try await peers.serverPathUpdates(atLeast: 2)
            #expect(
                updates == [
                    QUICPathUpdateEvent(remoteAddress: newAddress, isValidated: false),
                    QUICPathUpdateEvent(remoteAddress: newAddress, isValidated: true),
                ]
            )
        }
    }

    @available(anyAppleOS 27, *)
    @Test func serverRevalidatesTheClientsFirstAddressOnReturn() async throws {
        try await Self.withConnectedPeers { peers in
            let newSocket = try await peers.makeClientSocket()
            try await peers.rebindClient(to: newSocket)
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))
            // Go back only once the server validated the new address.
            #expect(try await peers.serverPathUpdates(atLeast: 2).count == 2)

            try await peers.rebindClient(to: nil)
            #expect(try await Self.request(on: peers.streamCreator) == ByteBuffer(string: "<b>Success</b>"))

            // SwiftNetwork dropped the path to the first address after validating the new one, so the
            // server validates the first address again.
            let firstAddress = try #require(peers.clientChannel.localAddress)
            let newAddress = try #require(newSocket.localAddress)
            let updates = try await peers.serverPathUpdates(atLeast: 4)
            #expect(
                updates == [
                    QUICPathUpdateEvent(remoteAddress: newAddress, isValidated: false),
                    QUICPathUpdateEvent(remoteAddress: newAddress, isValidated: true),
                    QUICPathUpdateEvent(remoteAddress: firstAddress, isValidated: false),
                    QUICPathUpdateEvent(remoteAddress: firstAddress, isValidated: true),
                ]
            )
        }
    }

    @available(anyAppleOS 27, *)
    @Test func serverReportsTheClientsReturnBeforeItsNewAddressIsValidated() async throws {
        try await Self.withConnectedPeers { peers in
            // The client never receives on the new socket, so it never answers the server's PATH_CHALLENGE there.
            let newSocket = try await peers.makeClientSocket(receives: false)
            try await peers.rebindClient(to: newSocket)
            let previousStream = peers.latestServerStreamChannel.withLockedValue { $0 }

            let response = try await Self.request(on: peers.streamCreator) {
                // Go back once the request reached the server from the new address.
                try await peers.waitForServerStream(after: previousStream)
                try await peers.rebindClient(to: nil)
            }
            #expect(response == ByteBuffer(string: "<b>Success</b>"))

            let firstAddress = try #require(peers.clientChannel.localAddress)
            let newAddress = try #require(newSocket.localAddress)
            let updates = try await peers.serverPathUpdates(atLeast: 2)
            #expect(
                updates == [
                    QUICPathUpdateEvent(remoteAddress: newAddress, isValidated: false),
                    QUICPathUpdateEvent(remoteAddress: firstAddress, isValidated: true),
                ]
            )
        }
    }

    private static let host = "127.0.0.1"

    /// Whether a socket can bind `::`, which NIO makes dual-stack.
    static func isDualStackAvailable() async -> Bool {
        let channel = try? await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "::", port: 0)
            .get()
        try? await channel?.close()
        return channel != nil
    }

    /// Connects a client to a server bound to `serverHost`, runs `body`, and tears both down.
    @available(anyAppleOS 27, *)
    private static func withConnectedPeers(
        serverHost: String = ConnectionMigrationTests.host,
        _ body: (ConnectedPeers) async throws -> Void
    ) async throws {
        let peers = try await ConnectedPeers.connect(serverHost: serverHost)
        do {
            try await body(peers)
        } catch {
            try? await peers.close()
            throw error
        }
        try await peers.close()
    }

    /// A client connected to a server over loopback, after one successful request.
    @available(anyAppleOS 27, *)
    private struct ConnectedPeers {
        var eventLoopGroup: MultiThreadedEventLoopGroup
        var serverChannel: any Channel
        var serverAddress: SocketAddress
        var serverConnectionChannel: any Channel
        var latestServerStreamChannel: NIOLockedValueBox<(any Channel)?>
        var clientChannel: any Channel
        var clientConnectionChannel: any Channel
        var streamCreator: NIOQUIC.QUICStreamCreator

        static func connect(serverHost: String) async throws -> ConnectedPeers {
            // One thread: the relay hands datagrams from one channel's pipeline to the other's synchronously.
            let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
            do {
                return try await Self.connect(on: eventLoopGroup, serverHost: serverHost)
            } catch {
                // Shutting down the event loop group also closes the channels set up so far.
                try? await eventLoopGroup.shutdownGracefully()
                throw error
            }
        }

        private static func connect(
            on eventLoopGroup: MultiThreadedEventLoopGroup,
            serverHost: String
        ) async throws -> ConnectedPeers {
            let serverConnectionChannel = NIOLockedValueBox<(any Channel)?>(nil)
            let latestServerStreamChannel = NIOLockedValueBox<(any Channel)?>(nil)

            let serverChannel = try await createServerChannel(
                eventLoopGroup: eventLoopGroup,
                host: serverHost,
                port: 0,
                logger: Logger(label: "Server"),
                inboundConnectionInitializer: { connectionChannel, _ in
                    serverConnectionChannel.withLockedValue { $0 = connectionChannel }
                    return connectionChannel.eventLoop.makeCompletedFuture {
                        try connectionChannel.pipeline.syncOperations.addHandler(PathUpdateRecorder())
                    }
                },
                inboundStreamInitializer: { streamChannel in
                    latestServerStreamChannel.withLockedValue { $0 = streamChannel }
                    return streamChannel.eventLoop.makeCompletedFuture {
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
                latestServerStreamChannel: latestServerStreamChannel,
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

        /// A new socket for the client: what it receives goes into the client's pipeline while the client uses it,
        /// unless `receives` is `false`.
        func makeClientSocket(receives: Bool = true) async throws -> any Channel {
            let clientChannel = self.clientChannel
            return try await DatagramBootstrap(group: self.eventLoopGroup)
                .channelInitializer { channel in
                    channel.eventLoop.makeCompletedFuture {
                        try channel.pipeline.syncOperations.addHandler(ByteCounter())
                        if receives {
                            try channel.pipeline.syncOperations.addHandler(RelayHandler(clientChannel: clientChannel))
                        }
                    }
                }
                .bind(host: ConnectionMigrationTests.host, port: 0)
                .get()
        }

        /// Waits until the server has a stream newer than `previousStream`.
        func waitForServerStream(after previousStream: (any Channel)?) async throws {
            while self.latestServerStreamChannel.withLockedValue({ $0 }) === previousStream {
                try await Task.sleep(for: .milliseconds(10))
            }
        }

        /// The path updates the server's connection reported, once there are at least `count` or after five seconds.
        func serverPathUpdates(atLeast count: Int = 0) async throws -> [QUICPathUpdateEvent] {
            let connectionChannel = self.serverConnectionChannel
            let deadline = ContinuousClock.now + .seconds(5)
            while true {
                let updates = try await connectionChannel.eventLoop.submit {
                    try connectionChannel.pipeline.syncOperations.handler(type: PathUpdateRecorder.self).updates
                }.get()
                if updates.count >= count || ContinuousClock.now >= deadline {
                    return updates
                }
                try await Task.sleep(for: .milliseconds(10))
            }
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

    /// The bytes `socket` sent and received so far, as its `ByteCounter` counted them.
    @available(anyAppleOS 27, *)
    private static func byteCounts(of socket: any Channel) async throws -> (sent: Int, received: Int) {
        try await socket.eventLoop.submit {
            let counter = try socket.pipeline.syncOperations.handler(type: ByteCounter.self)
            return (counter.sentBytes, counter.receivedBytes)
        }.get()
    }

    /// Sends a request on a new stream without waiting for the response. Returns once the server has the stream.
    @available(anyAppleOS 27, *)
    private static func sendRequest(on peers: ConnectedPeers) async throws {
        let previousStream = peers.latestServerStreamChannel.withLockedValue { $0 }
        let stream = try await peers.streamCreator.createBidirectionalStream { initializer in
            initializer.channel.eventLoop.makeSucceededFuture(initializer.channel)
        }.get()
        try await stream.writeAndFlush(ByteBuffer(string: "GET /foo"))
        try await peers.waitForServerStream(after: previousStream)
    }

    /// Sends a request on a new stream and returns the full response. `afterSending` runs once the request is out.
    @available(anyAppleOS 27, *)
    private static func request(
        on streamCreator: NIOQUIC.QUICStreamCreator,
        afterSending: () async throws -> Void = {}
    ) async throws -> ByteBuffer {
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
            try await afterSending()
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
@available(anyAppleOS 27, *)
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
@available(anyAppleOS 27, *)
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
@available(anyAppleOS 27, *)
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

/// Sits in a connection channel's pipeline and records the `QUICPathUpdateEvent`s it receives.
@available(anyAppleOS 27, *)
private final class PathUpdateRecorder: ChannelInboundHandler {
    typealias InboundIn = Any

    private(set) var updates: [QUICPathUpdateEvent] = []

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let event = event as? QUICPathUpdateEvent {
            self.updates.append(event)
        }
        context.fireUserInboundEventTriggered(event)
    }
}

/// Counts the bytes of the datagrams a socket sends and receives.
@available(anyAppleOS 27, *)
private final class ByteCounter: ChannelDuplexHandler {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>
    typealias OutboundIn = AddressedEnvelope<ByteBuffer>

    private(set) var sentBytes = 0
    private(set) var receivedBytes = 0

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.receivedBytes += self.unwrapInboundIn(data).data.readableBytes
        context.fireChannelRead(data)
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        self.sentBytes += self.unwrapOutboundIn(data).data.readableBytes
        context.write(data, promise: promise)
    }
}
