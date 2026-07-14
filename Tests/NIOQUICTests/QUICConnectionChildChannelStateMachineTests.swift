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

@testable import ChildChannelMultiplexer
@testable import NIOQUIC

@available(anyAppleOS 26, *)
final class QUICConnectionChildChannelStateMachineTests: XCTestCase {
    private var eventLoop: EmbeddedEventLoop!
    private var channel: EmbeddedChannel!
    private var swiftNetworkQUICConnection: SwiftNetworkQUICConnection!
    private var stateMachine: QUICConnectionChildChannelStateMachine!

    override func setUp() {
        super.setUp()

        self.eventLoop = EmbeddedEventLoop()
        self.channel = EmbeddedChannel(loop: self.eventLoop)
        var randomNumberGenerator: any RandomNumberGenerator = SystemRandomNumberGenerator()
        let localAddress = try! SocketAddress(ipAddress: "127.0.0.1", port: 4321)
        let remoteAddress = try! SocketAddress(ipAddress: "127.0.0.1", port: 1234)
        let logger = Logger(label: "QUICConnectionChildChannelStateMachineTests")
        self.swiftNetworkQUICConnection = try! SwiftNetworkQUICConnection.server(
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
            localAddress: localAddress,
            remoteAddress: remoteAddress,
            logger: logger,
            eventLoop: self.eventLoop
        )
        self.stateMachine = QUICConnectionChildChannelStateMachine(
            quicConnection: self.swiftNetworkQUICConnection,
            localAddress: localAddress,
            remoteAddress: remoteAddress,
            allocator: ByteBufferAllocator(),
            logger: logger
        )
    }

    override func tearDown() {
        super.tearDown()

        self.eventLoop = nil
        self.swiftNetworkQUICConnection = nil
        self.stateMachine = nil
    }

    func testChildChannelInitializationSucceeded_whenOpen() throws {
        // When the connection is open and hasn't established a QUIC handshake yet,
        // childChannelInitializationSucceeded should return no actions
        // (timeouts are handled internally by SwiftNetwork)
        let actions = self.stateMachine.childChannelInitializationSucceeded()

        XCTAssertEqual(actions.count, 0)
    }

    func testChildChannelInitializationSucceeded_whenClosed() throws {
        _ = self.swiftNetworkQUICConnection.close(
            sendApplicationClose: false,
            errorCode: QUICTransportErrorCode.noError.rawValue,
            reason: ""
        )
        let actions = self.stateMachine.childChannelInitializationSucceeded()

        XCTAssertEqual(actions.count, 1)
        actions[0].action.assertIsChildChannelClosedCleanly()
    }

    func testChildChildChannelInitializationFailed() throws {
        let actions = self.stateMachine.childChannelInitializationFailed(error: ChannelError.eof)

        // Initialization failure closes the QUIC connection cleanly (sends CONNECTION_CLOSE)
        // but also reports an error to the child channel
        XCTAssertEqual(actions.count, 3)
        guard actions.count == 3 else { return }
        actions[0].action.assertIsChildChannelFlush()
        actions[1].action.assertIsChildChannelClosedCleanly()
        actions[2].action.assertIsChildChannelEncounterError()
    }

    func testParentChannelInactive() {
        let actions = self.stateMachine.parentChannelInactive()

        XCTAssertEqual(actions.count, 1)
        actions[0].action.assertIsChildChannelClosedCleanly()
    }

    func testChildChannelClosed() throws {
        let actions = self.stateMachine.childChannelClose(error: ChannelError.inputClosed, mode: .all, promise: nil)

        XCTAssertEqual(actions.count, 2)
        guard actions.count == 2 else { return }
        actions[0].action.assertIsChildChannelFlush()
        actions[1].action.assertIsChildChannelClosedCleanly()
    }

    func testParentChannelUserInboundEventTriggered_whenOpen() {
        let actions = self.stateMachine.parentChannelUserInboundEventTriggered(ChannelShouldQuiesceEvent())

        XCTAssertEqual(actions.count, 1)
        actions[0].action.assertIsChildChannelFireUserInboundEventTriggered()
    }

    func testParentChannelUserInboundEventTriggered_whenClosed() throws {
        _ = self.stateMachine.childChannelClose(error: ChannelError.inputClosed, mode: .all, promise: nil)

        let actions = self.stateMachine.parentChannelUserInboundEventTriggered(ChannelShouldQuiesceEvent())

        XCTAssertEqual(actions.count, 1)
        actions[0].action.assertIsChildChannelClosedCleanly()
    }

    func testChildChannelTriggerUserOutboundEvent() {
        let promise = self.eventLoop.makePromise(of: Void.self)
        let actions = self.stateMachine.childChannelTriggerUserOutboundEvent("event", promise: promise)

        XCTAssertEqual(actions.count, 1)
        actions[0].action.assertIsFailPromise()
        promise.succeed(())
    }

    func testChildChannelReadMessage() {
        let readableStreamMessage = QUICConnectionChannelInboundMessage(streamID: .init(rawValue: 1))

        let actions = self.stateMachine.childChannelReadMessage(readableStreamMessage)

        XCTAssertEqual(actions.count, 1)
        actions[0].action.assertIsChildChannelFireChannelRead()
    }

    func testExtraChannelIDAssigned() {
        var randomNumberGenerator: any RandomNumberGenerator = SystemRandomNumberGenerator()
        let connectionID = QUICConnectionID.random(using: &randomNumberGenerator)

        let actions = self.stateMachine.extraChannelIDAssigned(connectionID)

        XCTAssertEqual(actions.count, 1)
        let event = actions[0].action.assertIsChildChannelFireUserInboundEventTriggered(QUICSCIDAssociatedEvent.self)
        XCTAssertEqual(event?.scid, connectionID)
    }

    func testChannelIDRetired() {
        var randomNumberGenerator: any RandomNumberGenerator = SystemRandomNumberGenerator()
        let connectionID = QUICConnectionID.random(using: &randomNumberGenerator)

        let actions = self.stateMachine.channelIDRetired(connectionID)

        XCTAssertEqual(actions.count, 1)
        let event = actions[0].action.assertIsChildChannelFireUserInboundEventTriggered(QUICSCIDRetiredEvent.self)
        XCTAssertEqual(event?.scid, connectionID)
    }
}
