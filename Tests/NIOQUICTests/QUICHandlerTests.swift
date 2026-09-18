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
import NIOTestUtils
import XCTest

@testable import NIOQUIC

@available(anyAppleOS 26, *)
final class QUICHandlerTests: XCTestCase {
    private static let statelessResetKey = [UInt8](repeating: 0x5A, count: 32)

    private var eventLoop: EmbeddedEventLoop!
    private var channel: EmbeddedChannel!
    private var serverHandler: QUICHandler<QUICStreamChannels>!
    private var channelHandler: MockChannelHandler!
    private var randomNumberGenerator: (any RandomNumberGenerator)!

    override func setUp() {
        super.setUp()

        self.eventLoop = EmbeddedEventLoop()
        self.channel = EmbeddedChannel(loop: self.eventLoop)
        self.channel.localAddress = try! SocketAddress(ipAddress: "127.0.0.0", port: 1234)
        let channelHandler = NIOLoopBound(MockChannelHandler(), eventLoop: self.eventLoop)
        self.channelHandler = channelHandler.value
        self.randomNumberGenerator = SystemRandomNumberGenerator()
        self.serverHandler = try! Self.makeHandler(
            channel: self.channel,
            channelHandler: channelHandler,
            connectionIDLength: Int(QUICConnectionID.randomIDLength)
        )
        try! self.channel.pipeline.syncOperations.addHandler(self.serverHandler)
    }

    /// Creates a server handler which derives its stateless reset tokens from ``statelessResetKey``.
    private static func makeHandler(
        channel: EmbeddedChannel,
        channelHandler: NIOLoopBound<MockChannelHandler>,
        connectionIDLength: Int,
        connectionLimit: Int = 0,
        handshakeConnectionLimit: Int = 0,
        newConnectionRateLimit: Int = 0
    ) throws -> QUICHandler<QUICStreamChannels> {
        let (handler, _) = try QUICHandler<QUICStreamChannels>.makeHandlerAndConnectionMultiplexer(
            channel: channel,
            quicConfiguration: .server(
                serverName: "quic-test.local",
                authenticationConfiguration: .rawPublicKeys(
                    publicKeyFilePath: Self.testPublicKeyPath,
                    privateKeyFilePath: Self.testPrivateKeyPath
                ),
                applicationProtocols: [],
                connectionLimit: connectionLimit,
                handshakeConnectionLimit: handshakeConnectionLimit,
                newConnectionRateLimit: newConnectionRateLimit
            ),
            logger: Logger(label: "Test"),
            inboundStreamChannelInitializer: { channel in
                do {
                    try channel.pipeline.syncOperations.addHandler(channelHandler.value)
                    return channel.eventLoop.makeSucceededVoidFuture()
                } catch {
                    return channel.eventLoop.makeFailedFuture(error)
                }
            },
            connectionIDGenerator: QUICConnectionID.RandomGenerator(
                connectionIDLength: connectionIDLength
            ),
            statelessResetTokenGenerator: .defaultWithUserProvidedKey(Self.statelessResetKey)
        )
        return handler
    }

    override func tearDown() {
        super.tearDown()

        try! self.channel.close().wait()
        self.eventLoop = nil
        self.channel = nil
        self.serverHandler = nil
        self.channelHandler = nil
        self.randomNumberGenerator = nil
    }

    func testShutdownGracefully_whenNoOpenConnection() throws {
        let future = self.serverHandler.shutdownGracefully(deadline: .now())

        XCTAssertNoThrow(try future.wait())
    }

    func testShutdownGracefully_whenAlreadyShutDown() throws {
        let future = self.serverHandler.shutdownGracefully(deadline: .now())
        try future.wait()

        let future2 = self.serverHandler.shutdownGracefully(deadline: .now())
        try future2.wait()
    }

    func testCreateOutboundConnection_whenHandlerHasNoConsumer_fails() throws {
        let future = self.serverHandler.createOutboundConnection(
            serverName: "quic-test.local",
            remoteAddress: try SocketAddress(ipAddress: "127.0.0.1", port: 4433)
        )

        XCTAssertThrowsError(try future.wait()) { error in
            XCTAssertEqual(error as? QUICError, .noStreamConsumer)
        }
    }

    func testChannelRead_whenVersionNegotiation() throws {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 0, 0,
                0, 0, 0, 0, 0,
                0, 0, 0, 0, 0,
            ],
            length: 8
        )
        let packet = QUICPackets.versionNegotiation(destinationID: connectionID, sourceID: connectionID)
        let buffer = ByteBuffer(bytes: packet)
        let outboundHeader = buffer.parseQUICPacketHeader(
            destinationIDLength: 8
        )
        XCTAssertEqual(outboundHeader?.sourceConnectionID, connectionID)
        XCTAssertEqual(outboundHeader?.destinationConnectionID, connectionID)
        XCTAssertEqual(outboundHeader?.type, .versionNegotiation)
    }

    func testChannelRead_whenVersionNegotiation_andEmptySCID() throws {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 0, 0,
                0, 0, 0, 0, 0,
                0, 0, 0, 0, 0,
            ],
            length: 8
        )
        let packet = QUICPackets.versionNegotiation(destinationID: connectionID, sourceID: nil, payloadLength: 6)
        let buffer = ByteBuffer(bytes: packet)
        let outboundHeader = buffer.parseQUICPacketHeader(
            destinationIDLength: 8
        )

        XCTAssertEqual(outboundHeader?.destinationConnectionID, connectionID)
        XCTAssertEqual(outboundHeader?.sourceConnectionID?.length, 0)
        XCTAssertEqual(outboundHeader?.type, .versionNegotiation)
    }

    func testChannelRead_whenVersionNegotiation_andEmptyDCID() throws {
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 0, 0,
                0, 0, 0, 0, 0,
                0, 0, 0, 0, 0,
            ],
            length: 8
        )
        let packet = QUICPackets.versionNegotiation(destinationID: nil, sourceID: connectionID, payloadLength: 6)
        let buffer = ByteBuffer(bytes: packet)
        let outboundHeader = buffer.parseQUICPacketHeader(
            destinationIDLength: 8
        )
        XCTAssertEqual(outboundHeader?.destinationConnectionID.length, 0)
        XCTAssertEqual(outboundHeader?.sourceConnectionID, connectionID)
        XCTAssertEqual(outboundHeader?.type, .versionNegotiation)
    }

    func testChannelRead_whenVersionNegotiation_andEmptyDCID_andEmptySCID() throws {
        let packet = QUICPackets.versionNegotiation(destinationID: nil, sourceID: nil, payloadLength: 14)
        let buffer = ByteBuffer(bytes: packet)
        let outboundHeader = buffer.parseQUICPacketHeader(
            destinationIDLength: 1
        )
        XCTAssertEqual(outboundHeader?.sourceConnectionID?.length, 0)
        XCTAssertEqual(outboundHeader?.destinationConnectionID.length, 0)
        XCTAssertEqual(outboundHeader?.type, .versionNegotiation)
    }

    func testChannelRead_whenForcedVersionNegotiationPattern() throws {
        // RFC 9000 §15: versions matching 0x?a?a?a?a are reserved to force a version
        // negotiation exchange and must be treated as an unsupported version.
        let connectionID = QUICConnectionID(
            bytes: [
                1, 1, 1, 1, 1,
                1, 1, 1, 0, 0,
                0, 0, 0, 0, 0,
                0, 0, 0, 0, 0,
            ],
            length: 8
        )
        let packet = QUICPackets.versionNegotiation(
            destinationID: connectionID,
            sourceID: connectionID,
            version: [0x1a, 0x2a, 0x3a, 0x4a]
        )
        let buffer = ByteBuffer(bytes: packet)
        let outboundHeader = buffer.parseQUICPacketHeader(
            destinationIDLength: 8
        )
        XCTAssertEqual(outboundHeader?.sourceConnectionID, connectionID)
        XCTAssertEqual(outboundHeader?.destinationConnectionID, connectionID)
        XCTAssertEqual(outboundHeader?.type, .versionNegotiation)
    }

    func testChannelReadComplete_whenNoWrite() throws {
        self.channel.pipeline.fireChannelReadComplete()

        let outbound = try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self)
        XCTAssertNil(outbound)
    }

    func testChannelReadComplete_whenSingleWrite() throws {
        let address = try SocketAddress(ipAddress: "127.0.0.0", port: 443)
        let message = AddressedEnvelope<ByteBuffer>(
            remoteAddress: address,
            data: .init()
        )
        self.serverHandler.writeDatagram(message, promise: nil)

        self.channel.pipeline.fireChannelReadComplete()

        let outbound = try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self)
        XCTAssertEqual(outbound, message)
    }

    func testChannelReadComplete_whenSingleWriteWhichIsFlushed() throws {
        let address = try SocketAddress(ipAddress: "127.0.0.0", port: 443)
        let message = AddressedEnvelope<ByteBuffer>(
            remoteAddress: address,
            data: .init()
        )
        self.serverHandler.writeDatagram(message, promise: nil)
        self.serverHandler.flush()

        var outbound = try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self)
        XCTAssertEqual(outbound, message)

        self.channel.pipeline.fireChannelReadComplete()

        outbound = try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self)
        XCTAssertNil(outbound)
    }

    func testWriteFromChildChannel() throws {
        let address = try SocketAddress(ipAddress: "127.0.0.0", port: 443)
        let message = AddressedEnvelope<ByteBuffer>(
            remoteAddress: address,
            data: .init()
        )

        self.serverHandler.writeDatagram(message, promise: nil)
        self.channel.flush()

        let outbound = try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self)
        XCTAssertEqual(outbound, message)
    }

    func testFlushFromChildChannel() throws {
        let address = try SocketAddress(ipAddress: "127.0.0.0", port: 443)
        let message = AddressedEnvelope<ByteBuffer>(
            remoteAddress: address,
            data: .init()
        )
        self.serverHandler.writeDatagram(message, promise: nil)

        self.serverHandler.flush()

        let outbound = try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self)
        XCTAssertEqual(outbound, message)
    }

    func testFlushFromChildChannel_whenReading() throws {
        let packet = QUICPackets.initial(
            destinationID: .random(using: &self.randomNumberGenerator),
            sourceID: .random(using: &self.randomNumberGenerator),
            token: [],
            version: 1
        )
        let buffer = ByteBuffer(bytes: packet)
        let address = try SocketAddress(ipAddress: "127.0.0.0", port: 443)
        let data = AddressedEnvelope<ByteBuffer>(
            remoteAddress: address,
            data: buffer
        )
        self.channel.pipeline.fireChannelRead(data)

        self.serverHandler.flush()

        let outbound = try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self)
        XCTAssertNil(outbound)
    }

    // MARK: - Stateless reset

    func testChannelRead_whenUnroutableShortHeaderPacket_sendsStatelessReset() throws {
        let connectionID = QUICConnectionID.random(using: &self.randomNumberGenerator)
        let packet = QUICPackets.shortHeader(destinationID: connectionID, payloadLength: 31)
        let address = try SocketAddress(ipAddress: "127.0.0.0", port: 443)
        self.channel.pipeline.fireChannelRead(
            AddressedEnvelope<ByteBuffer>(remoteAddress: address, data: ByteBuffer(bytes: packet))
        )
        self.channel.pipeline.fireChannelReadComplete()

        let outbound = try XCTUnwrap(try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self))
        XCTAssertEqual(outbound.remoteAddress, address)
        // The reset must be smaller than its trigger (RFC 9000 § 10.3.3) and carry the token the
        // peer would have received for this connection ID.
        XCTAssertLessThan(outbound.data.readableBytes, packet.count)
        let tokenBytes = Array(outbound.data.readableBytesView.suffix(16))
        XCTAssertEqual(
            QUICStatelessResetToken(tokenBytes.span),
            QUICStatelessResetToken.HMACSHA256Generator(key: Self.statelessResetKey).token(for: connectionID)
        )
        XCTAssertNil(try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self))
    }

    func testChannelRead_whenUnroutableLongHeaderPacket_sendsNoStatelessReset() throws {
        // Handshake packets are only used before the peer has a token to compare a reset against.
        let packet = QUICPackets.handshake(
            destinationID: .random(using: &self.randomNumberGenerator),
            sourceID: .random(using: &self.randomNumberGenerator),
            version: 1
        )
        let address = try SocketAddress(ipAddress: "127.0.0.0", port: 443)
        self.channel.pipeline.fireChannelRead(
            AddressedEnvelope<ByteBuffer>(remoteAddress: address, data: ByteBuffer(bytes: packet))
        )
        self.channel.pipeline.fireChannelReadComplete()

        XCTAssertNil(try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self))
    }

    func testChannelRead_whenUnroutableShortHeaderPacketIsTooSmall_sendsNoStatelessReset() throws {
        // No valid reset fits below 21 bytes, so this packet cannot be answered.
        // Header is 9 bytes + 12 bytes payload = 21 and the response needs to be smaller.
        let packet = QUICPackets.shortHeader(
            destinationID: .random(using: &self.randomNumberGenerator),
            payloadLength: 12
        )

        let address = try SocketAddress(ipAddress: "127.0.0.0", port: 443)
        self.channel.pipeline.fireChannelRead(
            AddressedEnvelope<ByteBuffer>(remoteAddress: address, data: ByteBuffer(bytes: packet))
        )
        self.channel.pipeline.fireChannelReadComplete()

        XCTAssertNil(try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self))
    }

    func testChannelRead_whenConnectionIDsAreZeroLength_sendsNoStatelessReset() throws {
        // With zero-length connection IDs there is nothing to derive a token from
        // (RFC 9000 § 10.3.2).
        let eventLoop = EmbeddedEventLoop()
        let channel = EmbeddedChannel(loop: eventLoop)
        channel.localAddress = try SocketAddress(ipAddress: "127.0.0.0", port: 1234)
        let handler = try Self.makeHandler(
            channel: channel,
            channelHandler: NIOLoopBound(MockChannelHandler(), eventLoop: eventLoop),
            connectionIDLength: 0
        )
        try channel.pipeline.syncOperations.addHandler(handler)
        defer { _ = try? channel.finish() }

        let zeroLengthCID = QUICConnectionID(bytes: InlineArray(repeating: 0), length: 0)
        let packet = QUICPackets.shortHeader(destinationID: zeroLengthCID, payloadLength: 39)
        let address = try SocketAddress(ipAddress: "127.0.0.0", port: 443)
        channel.pipeline.fireChannelRead(
            AddressedEnvelope<ByteBuffer>(remoteAddress: address, data: ByteBuffer(bytes: packet))
        )
        channel.pipeline.fireChannelReadComplete()

        XCTAssertNil(try channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self))
    }

    // MARK: - Parsing failures

    func testChannelRead_whenPacketFailsToParse_doesNotFireError() throws {
        let recorder = ErrorRecordingHandler()
        try self.channel.pipeline.syncOperations.addHandler(recorder)

        let address = try SocketAddress(ipAddress: "127.0.0.0", port: 443)
        self.channel.pipeline.fireChannelRead(
            AddressedEnvelope<ByteBuffer>(remoteAddress: address, data: ByteBuffer())
        )
        self.channel.pipeline.fireChannelReadComplete()

        XCTAssertEqual(recorder.errors.count, 0)
        XCTAssertNil(try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self))
    }

    // MARK: - Connection limits

    /// Fires an INITIAL packet for `destinationID` and returns whether it is now routable: a
    /// probing short-header packet for the same ID either gets no reply (routed to a real
    /// connection) or a stateless reset (never registered, i.e. dropped).
    private func isRoutable(_ destinationID: QUICConnectionID, on channel: EmbeddedChannel) throws -> Bool {
        let address = try SocketAddress(ipAddress: "127.0.0.0", port: 443)
        let probe = QUICPackets.shortHeader(destinationID: destinationID, payloadLength: 31)
        channel.pipeline.fireChannelRead(
            AddressedEnvelope<ByteBuffer>(remoteAddress: address, data: ByteBuffer(bytes: probe))
        )
        channel.pipeline.fireChannelReadComplete()
        return try channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self) == nil
    }

    private func fireInitial(_ destinationID: QUICConnectionID, on channel: EmbeddedChannel) throws {
        let address = try SocketAddress(ipAddress: "127.0.0.0", port: 443)
        let packet = QUICPackets.initial(
            destinationID: destinationID,
            sourceID: .random(using: &self.randomNumberGenerator),
            token: [],
            version: 1
        )
        channel.pipeline.fireChannelRead(
            AddressedEnvelope<ByteBuffer>(remoteAddress: address, data: ByteBuffer(bytes: packet))
        )
        channel.pipeline.fireChannelReadComplete()
    }

    func testChannelRead_whenActiveConnectionLimitReached_dropsFurtherConnections() throws {
        let eventLoop = EmbeddedEventLoop()
        let channel = EmbeddedChannel(loop: eventLoop)
        channel.localAddress = try SocketAddress(ipAddress: "127.0.0.0", port: 1234)
        let handler = try Self.makeHandler(
            channel: channel,
            channelHandler: NIOLoopBound(MockChannelHandler(), eventLoop: eventLoop),
            connectionIDLength: Int(QUICConnectionID.randomIDLength),
            connectionLimit: 2
        )
        try channel.pipeline.syncOperations.addHandler(handler)
        defer { _ = try? channel.finish() }

        let accepted1 = QUICConnectionID.random(using: &self.randomNumberGenerator)
        let accepted2 = QUICConnectionID.random(using: &self.randomNumberGenerator)
        let dropped = QUICConnectionID.random(using: &self.randomNumberGenerator)

        try self.fireInitial(accepted1, on: channel)
        try self.fireInitial(accepted2, on: channel)
        try self.fireInitial(dropped, on: channel)

        XCTAssertTrue(try self.isRoutable(accepted1, on: channel))
        XCTAssertTrue(try self.isRoutable(accepted2, on: channel))
        XCTAssertFalse(try self.isRoutable(dropped, on: channel))
    }

    func testChannelRead_whenHandshakeLimitReached_dropsFurtherConnectionsEvenUnderActiveLimit() throws {
        let eventLoop = EmbeddedEventLoop()
        let channel = EmbeddedChannel(loop: eventLoop)
        channel.localAddress = try SocketAddress(ipAddress: "127.0.0.0", port: 1234)
        let handler = try Self.makeHandler(
            channel: channel,
            channelHandler: NIOLoopBound(MockChannelHandler(), eventLoop: eventLoop),
            connectionIDLength: Int(QUICConnectionID.randomIDLength),
            connectionLimit: 10,
            handshakeConnectionLimit: 1
        )
        try channel.pipeline.syncOperations.addHandler(handler)
        defer { _ = try? channel.finish() }

        let accepted = QUICConnectionID.random(using: &self.randomNumberGenerator)
        let dropped = QUICConnectionID.random(using: &self.randomNumberGenerator)

        try self.fireInitial(accepted, on: channel)
        try self.fireInitial(dropped, on: channel)

        // Dropped for the handshake limit alone: the active limit (10) has plenty of room.
        XCTAssertTrue(try self.isRoutable(accepted, on: channel))
        XCTAssertFalse(try self.isRoutable(dropped, on: channel))
    }

    func testChannelRead_whenNewConnectionRateLimitReached_dropsFurtherConnections() throws {
        let eventLoop = EmbeddedEventLoop()
        let channel = EmbeddedChannel(loop: eventLoop)
        channel.localAddress = try SocketAddress(ipAddress: "127.0.0.0", port: 1234)
        let handler = try Self.makeHandler(
            channel: channel,
            channelHandler: NIOLoopBound(MockChannelHandler(), eventLoop: eventLoop),
            connectionIDLength: Int(QUICConnectionID.randomIDLength),
            newConnectionRateLimit: 2
        )
        try channel.pipeline.syncOperations.addHandler(handler)
        defer { _ = try? channel.finish() }

        let accepted1 = QUICConnectionID.random(using: &self.randomNumberGenerator)
        let accepted2 = QUICConnectionID.random(using: &self.randomNumberGenerator)
        let dropped = QUICConnectionID.random(using: &self.randomNumberGenerator)

        // Fired back-to-back, well within the same second: the burst capacity (2, equal to the
        // configured rate) covers the first two, the third exceeds it.
        try self.fireInitial(accepted1, on: channel)
        try self.fireInitial(accepted2, on: channel)
        try self.fireInitial(dropped, on: channel)

        XCTAssertTrue(try self.isRoutable(accepted1, on: channel))
        XCTAssertTrue(try self.isRoutable(accepted2, on: channel))
        XCTAssertFalse(try self.isRoutable(dropped, on: channel))
    }

    /// Like `makeHandler`, but exposes each accepted connection's channel via
    /// `onConnectionAccepted`, for tests that need to close a connection or complete its
    /// handshake to observe a freed admission slot.
    private static func makeHandlerWithConnectionAccess(
        channel: EmbeddedChannel,
        channelHandler: NIOLoopBound<MockChannelHandler>,
        connectionIDLength: Int,
        connectionLimit: Int = 0,
        handshakeConnectionLimit: Int = 0,
        newConnectionRateLimit: Int = 0,
        onConnectionAccepted: @escaping @Sendable (any Channel) -> Void
    ) -> QUICHandler<QUICStreamChannels> {
        QUICHandler(
            channel: channel,
            quicConfiguration: .server(
                serverName: "quic-test.local",
                authenticationConfiguration: .rawPublicKeys(
                    publicKeyFilePath: Self.testPublicKeyPath,
                    privateKeyFilePath: Self.testPrivateKeyPath
                ),
                applicationProtocols: [],
                connectionLimit: connectionLimit,
                handshakeConnectionLimit: handshakeConnectionLimit,
                newConnectionRateLimit: newConnectionRateLimit
            ),
            asyncVerifier: nil,
            authenticator: nil,
            logger: Logger(label: "Test"),
            inboundConnectionInitializer: { channel, _ in
                onConnectionAccepted(channel)
                return channel.eventLoop.makeSucceededVoidFuture()
            },
            inboundStreamInitializer: { channel in
                do {
                    try channel.pipeline.syncOperations.addHandler(channelHandler.value)
                    return channel.eventLoop.makeSucceededVoidFuture()
                } catch {
                    return channel.eventLoop.makeFailedFuture(error)
                }
            },
            noMoreConnections: {},
            connectionIDGenerator: QUICConnectionID.RandomGenerator(
                connectionIDLength: connectionIDLength
            ),
            statelessResetTokenGenerator: .defaultWithUserProvidedKey(Self.statelessResetKey)
        )
    }

    func testChannelRead_whenActiveConnectionLimitReached_acceptsNewConnectionAfterOneCloses() throws {
        let eventLoop = EmbeddedEventLoop()
        let channel = EmbeddedChannel(loop: eventLoop)
        channel.localAddress = try SocketAddress(ipAddress: "127.0.0.0", port: 1234)

        let acceptedChannels = NIOLockedValueBox<[any Channel]>([])
        let handler = Self.makeHandlerWithConnectionAccess(
            channel: channel,
            channelHandler: NIOLoopBound(MockChannelHandler(), eventLoop: eventLoop),
            connectionIDLength: Int(QUICConnectionID.randomIDLength),
            connectionLimit: 1,
            onConnectionAccepted: { channel in acceptedChannels.withLockedValue { $0.append(channel) } }
        )
        try channel.pipeline.syncOperations.addHandler(handler)
        defer { _ = try? channel.finish() }

        let accepted1 = QUICConnectionID.random(using: &self.randomNumberGenerator)
        let dropped = QUICConnectionID.random(using: &self.randomNumberGenerator)
        let accepted2 = QUICConnectionID.random(using: &self.randomNumberGenerator)

        try self.fireInitial(accepted1, on: channel)
        try self.fireInitial(dropped, on: channel)
        XCTAssertTrue(try self.isRoutable(accepted1, on: channel))
        XCTAssertFalse(try self.isRoutable(dropped, on: channel))

        // Close the first connection, freeing its active slot.
        let firstAcceptedChannel = try XCTUnwrap(acceptedChannels.withLockedValue { $0 }.first)
        let closeFuture = firstAcceptedChannel.close()
        // Teardown (including releasing the admission slot) completes on the next loop tick.
        eventLoop.run()
        try closeFuture.wait()

        try self.fireInitial(accepted2, on: channel)
        XCTAssertTrue(try self.isRoutable(accepted2, on: channel))
    }

    func testChannelRead_whenHandshakeLimitReached_acceptsNewConnectionAfterHandshakeCompletes() throws {
        let eventLoop = EmbeddedEventLoop()
        let channel = EmbeddedChannel(loop: eventLoop)
        channel.localAddress = try SocketAddress(ipAddress: "127.0.0.0", port: 1234)

        let acceptedChannels = NIOLockedValueBox<[any Channel]>([])
        let handler = Self.makeHandlerWithConnectionAccess(
            channel: channel,
            channelHandler: NIOLoopBound(MockChannelHandler(), eventLoop: eventLoop),
            connectionIDLength: Int(QUICConnectionID.randomIDLength),
            connectionLimit: 10,
            handshakeConnectionLimit: 1,
            onConnectionAccepted: { channel in acceptedChannels.withLockedValue { $0.append(channel) } }
        )
        try channel.pipeline.syncOperations.addHandler(handler)
        defer { _ = try? channel.finish() }

        let accepted1 = QUICConnectionID.random(using: &self.randomNumberGenerator)
        let dropped = QUICConnectionID.random(using: &self.randomNumberGenerator)
        let accepted2 = QUICConnectionID.random(using: &self.randomNumberGenerator)

        try self.fireInitial(accepted1, on: channel)
        try self.fireInitial(dropped, on: channel)
        XCTAssertTrue(try self.isRoutable(accepted1, on: channel))
        XCTAssertFalse(try self.isRoutable(dropped, on: channel))

        // Complete the first connection's handshake: it stays active, but frees its handshake
        // slot (the active limit of 10 has plenty of room regardless).
        let firstAcceptedChannel = try XCTUnwrap(acceptedChannels.withLockedValue { $0 }.first)
        let connectionChannel = try XCTUnwrap(firstAcceptedChannel as? QUICConnectionChannel<QUICStreamChannels>)
        connectionChannel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 0)
        connectionChannel.connectionView.drainOutboundAndReconcileLifecycle()

        try self.fireInitial(accepted2, on: channel)
        XCTAssertTrue(try self.isRoutable(accepted2, on: channel))
    }

    func testChannelRead_whenNewConnectionRateLimitReached_acceptsNewConnectionAfterRateLimitRefills() throws {
        let eventLoop = EmbeddedEventLoop()
        let channel = EmbeddedChannel(loop: eventLoop)
        channel.localAddress = try SocketAddress(ipAddress: "127.0.0.0", port: 1234)
        let handler = try Self.makeHandler(
            channel: channel,
            channelHandler: NIOLoopBound(MockChannelHandler(), eventLoop: eventLoop),
            connectionIDLength: Int(QUICConnectionID.randomIDLength),
            newConnectionRateLimit: 5
        )
        try channel.pipeline.syncOperations.addHandler(handler)
        defer { _ = try? channel.finish() }

        // Exhaust the burst capacity (5, equal to the configured rate).
        var accepted: [QUICConnectionID] = []
        for _ in 0..<5 {
            let id = QUICConnectionID.random(using: &self.randomNumberGenerator)
            try self.fireInitial(id, on: channel)
            accepted.append(id)
        }
        let dropped = QUICConnectionID.random(using: &self.randomNumberGenerator)
        try self.fireInitial(dropped, on: channel)

        for id in accepted {
            XCTAssertTrue(try self.isRoutable(id, on: channel))
        }
        XCTAssertFalse(try self.isRoutable(dropped, on: channel))

        // One refill interval (1s / 5 = 200ms) later, a token is available again.
        Thread.sleep(forTimeInterval: 0.4)

        let acceptedAfterRefill = QUICConnectionID.random(using: &self.randomNumberGenerator)
        try self.fireInitial(acceptedAfterRefill, on: channel)
        XCTAssertTrue(try self.isRoutable(acceptedAfterRefill, on: channel))
    }
}

/// Records errors fired down the pipeline.
private final class ErrorRecordingHandler: ChannelInboundHandler {
    typealias InboundIn = Any

    var errors: [any Error] = []

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        self.errors.append(error)
    }
}
