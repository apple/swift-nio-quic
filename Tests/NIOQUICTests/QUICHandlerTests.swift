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

final class QUICHandlerTests: XCTestCase {
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
        let (handler, _) = try! QUICHandler.makeHandlerAndConnectionMultiplexer(
            channel: self.channel,
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
            }
        )
        self.serverHandler = handler
        try! self.channel.pipeline.syncOperations.addHandler(self.serverHandler)
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
        let outboundHeader = try buffer.parseQUICPacketHeader(
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
        let outboundHeader = try buffer.getQUICPacketHeader(
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
        let outboundHeader = try buffer.getQUICPacketHeader(
            destinationIDLength: 8
        )
        XCTAssertEqual(outboundHeader?.destinationConnectionID.length, 0)
        XCTAssertEqual(outboundHeader?.sourceConnectionID, connectionID)
        XCTAssertEqual(outboundHeader?.type, .versionNegotiation)
    }

    func testChannelRead_whenVersionNegotiation_andEmptyDCID_andEmptySCID() throws {
        let packet = QUICPackets.versionNegotiation(destinationID: nil, sourceID: nil)
        let buffer = ByteBuffer(bytes: packet)
        let outboundHeader = try buffer.getQUICPacketHeader(
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
        self.serverHandler.writeFromChildChannel(childChannelID: .zero, message: message, promise: nil)

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
        self.serverHandler.writeFromChildChannel(childChannelID: .zero, message: message, promise: nil)
        self.serverHandler.flushFromChildChannel(childChannelID: .zero)

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

        self.serverHandler.writeFromChildChannel(childChannelID: .zero, message: message, promise: nil)
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
        self.serverHandler.writeFromChildChannel(childChannelID: .zero, message: message, promise: nil)

        self.serverHandler.flushFromChildChannel(childChannelID: .zero)

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

        self.serverHandler.flushFromChildChannel(childChannelID: .zero)

        let outbound = try self.channel.readOutbound(as: AddressedEnvelope<ByteBuffer>.self)
        XCTAssertNil(outbound)
    }
}
