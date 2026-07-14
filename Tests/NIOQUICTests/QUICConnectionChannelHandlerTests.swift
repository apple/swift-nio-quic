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
import XCTest

@testable import NIOQUIC

@available(anyAppleOS 26, *)
final class QUICConnectionChannelHandlerTests: XCTestCase {
    private var eventLoop: EmbeddedEventLoop!
    private var channel: EmbeddedChannel!
    private var serverHandler: QUICConnectionChannelHandler!
    private var quicConnection: SwiftNetworkQUICConnection!

    override func setUp() {
        super.setUp()

        self.eventLoop = EmbeddedEventLoop()
        self.channel = EmbeddedChannel(loop: self.eventLoop)
        let logger = Logger(label: "test")
        var randomNumberGenerator: any RandomNumberGenerator = SystemRandomNumberGenerator()
        let quicConnection = try! SwiftNetworkQUICConnection.server(
            configuration: .server(
                serverName: "quic-test.local",
                authenticationConfiguration: .rawPublicKeys(
                    publicKeyFilePath: Self.testPublicKeyPath,
                    privateKeyFilePath: Self.testPrivateKeyPath
                ),
                applicationProtocols: []
            ),
            sourceConnectionID: .random(using: &randomNumberGenerator),
            authenticator: nil,
            localAddress: try! SocketAddress(ipAddress: "127.0.0.1", port: 1234),
            remoteAddress: try! SocketAddress(ipAddress: "127.0.0.1", port: 1234),
            logger: logger,
            eventLoop: self.eventLoop,
        )
        self.quicConnection = quicConnection
        let (handler, _) = QUICConnectionChannelHandler.makeHandlerAndQUICConnection(
            quicConnection: quicConnection,
            role: .server,
            channel: self.channel,
            logger: logger,
            metrics: nil,
            inboundStreamInitializer: { channel in channel.eventLoop.makeSucceededVoidFuture() }
        )
        self.serverHandler = handler
        XCTAssertNoThrow(try self.channel.pipeline.syncOperations.addHandler(self.serverHandler))
    }

    override func tearDown() {
        super.tearDown()

        if self.channel.isActive {
            XCTAssertNoThrow(try self.channel.close().wait())
        }
        self.eventLoop = nil
        self.channel = nil
        self.serverHandler = nil
        self.quicConnection = nil
    }

    func testClosingConnectionAllowsDeinit() throws {
        weak let weakConnection = self.quicConnection

        _ = self.quicConnection.close(sendApplicationClose: false, errorCode: 0, reason: "test")
        self.quicConnection = nil
        self.serverHandler = nil
        try self.channel.close().wait()
        // EmbeddedChannel.close0 defers pipeline.removeHandlers() to the event loop's task
        // queue; run() flushes it so the pipeline actually releases its handlers.
        self.eventLoop.run()

        XCTAssertNil(weakConnection, "SwiftNetworkQUICConnection should deinit once closed and released")
    }

    func testChannelReadComplete_whenNoWrite() throws {
        self.channel.pipeline.fireChannelReadComplete()

        let outbound = try self.channel.readOutbound(as: QUICConnectionChannelOutboundMessage.self)
        XCTAssertNil(outbound)
    }

    func testForwardsEvents() throws {
        class TestHandler: ChannelInboundHandler {
            typealias InboundIn = Any
            var events: [Any] = []

            init() {}

            func channelRead(context: ChannelHandlerContext, data: NIOAny) {
                context.fireChannelRead(data)
            }

            func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
                self.events.append(event)
                context.fireUserInboundEventTriggered(event)
            }
        }
        let testHandler = TestHandler()
        try self.channel.pipeline.syncOperations.addHandler(testHandler)
        self.channel.pipeline.syncOperations.fireUserInboundEventTriggered(123)
        XCTAssertEqual(testHandler.events.count, 1)
        XCTAssertEqual(testHandler.events.first as? Int, 123)
    }

    func testShutdownWhenNoStreams() {
        var state = QUICConnectionChannelHandler.StateMachine()
        let action = state.channelInactive()
        XCTAssertEqual(action, .fireInactive)
    }

    func testShutdownWhenStreams() {
        var state = QUICConnectionChannelHandler.StateMachine()

        // Start 3
        state.startingInitializingAStream()
        state.startingInitializingAStream()
        state.startingInitializingAStream()

        // Finish one. There are now 2
        XCTAssertEqual(state.finishedInitializingAStream(), .none)

        // Go into stopping state, with 2 streams in flight
        XCTAssertEqual(state.channelInactive(), .none)

        // Now 1 in flight
        XCTAssertEqual(state.finishedInitializingAStream(), .none)

        // Again 2 in flight
        state.startingInitializingAStream()

        // Back to 1 in flight
        XCTAssertEqual(state.finishedInitializingAStream(), .none)

        // 0 in flight. Can shut down
        XCTAssertEqual(state.finishedInitializingAStream(), .fireInactive)
    }

    // MARK: - writeDataForStream integration tests

    /// After sendFin(), writeData() should return .doNotWrite(.streamFinished)
    /// so no further data frames are sent.
    func testWriteDataAfterSendFinIsGated() throws {
        var sm = QUICStreamStateMachine()
        switch sm.streamConnected(direction: .bidirectional) {
        case .activateStream: break
        default: XCTFail("Expected .activateStream")
        }

        // Write some data — should be allowed
        switch try sm.writeData() {
        case .sendData: break
        default: XCTFail("Expected .sendData")
        }

        // Send FIN
        switch try sm.sendFin() {
        case .sendFin: break
        default: XCTFail("Expected .sendFin")
        }

        // Subsequent writeData should be gated
        switch try sm.writeData() {
        case .doNotWrite(.streamFinished): break
        default: XCTFail("Expected .doNotWrite(.streamFinished)")
        }
    }

    /// sendFin() after sendFin() should return .ignore(.alreadyFinished).
    func testDuplicateSendFinIsIgnored() throws {
        var sm = QUICStreamStateMachine()
        switch sm.streamConnected(direction: .bidirectional) {
        case .activateStream: break
        default: XCTFail("Expected .activateStream")
        }

        // First FIN succeeds
        switch try sm.sendFin() {
        case .sendFin: break
        default: XCTFail("Expected .sendFin")
        }

        // Second FIN is ignored
        switch try sm.sendFin() {
        case .ignore(.alreadyFinished): break
        default: XCTFail("Expected .ignore(.alreadyFinished)")
        }
    }

    /// writeData() on a data-only (non-eof) path should keep the stream open for more writes.
    func testWriteDataWithoutFinKeepsStreamOpen() throws {
        var sm = QUICStreamStateMachine()
        switch sm.streamConnected(direction: .bidirectional) {
        case .activateStream: break
        default: XCTFail("Expected .activateStream")
        }

        // Multiple writes should all succeed
        for _ in 0..<3 {
            switch try sm.writeData() {
            case .sendData: break
            default: XCTFail("Expected .sendData")
            }
        }

        // Stream should still be writable
        XCTAssertTrue(sm.canWrite)
    }
}
