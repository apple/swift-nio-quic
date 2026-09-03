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
import NIOTestUtils
import XCTest

@testable import NIOQUIC

@available(anyAppleOS 26, *)
final class QUICHandlerTests: XCTestCase {
    private static let statelessResetKey = [UInt8](repeating: 0x5A, count: 32)

    private var eventLoop: EmbeddedEventLoop!
    private var channel: EmbeddedChannel!
    private var serverHandler: QUICHandler!
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
        connectionIDLength: Int
    ) throws -> QUICHandler {
        let (handler, _) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
            channel: channel,
            quicConfiguration: .server(
                serverName: "quic-test.local",
                authenticationConfiguration: .rawPublicKeys(
                    publicKeyFilePath: Self.testPublicKeyPath,
                    privateKeyFilePath: Self.testPrivateKeyPath
                ),
                applicationProtocols: []
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
        let packet = QUICPackets.versionNegotiation(destinationID: connectionID, sourceID: nil)
        let buffer = ByteBuffer(bytes: packet)
        let outboundHeader = buffer.getQUICPacketHeader(
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
        let packet = QUICPackets.versionNegotiation(destinationID: nil, sourceID: connectionID)
        let buffer = ByteBuffer(bytes: packet)
        let outboundHeader = buffer.getQUICPacketHeader(
            destinationIDLength: 8
        )
        XCTAssertEqual(outboundHeader?.destinationConnectionID.length, 0)
        XCTAssertEqual(outboundHeader?.sourceConnectionID, connectionID)
        XCTAssertEqual(outboundHeader?.type, .versionNegotiation)
    }

    func testChannelRead_whenVersionNegotiation_andEmptyDCID_andEmptySCID() throws {
        let packet = QUICPackets.versionNegotiation(destinationID: nil, sourceID: nil)
        let buffer = ByteBuffer(bytes: packet)
        let outboundHeader = buffer.getQUICPacketHeader(
            destinationIDLength: 1
        )
        XCTAssertEqual(outboundHeader?.sourceConnectionID?.length, 0)
        XCTAssertEqual(outboundHeader?.destinationConnectionID.length, 0)
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
        // No valid reset fits below 22 bytes, so this packet cannot be answered.
        let packet = QUICPackets.shortHeader(
            destinationID: .random(using: &self.randomNumberGenerator)
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
}

/// Records errors fired down the pipeline.
private final class ErrorRecordingHandler: ChannelInboundHandler {
    typealias InboundIn = Any

    var errors: [any Error] = []

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        self.errors.append(error)
    }
}
