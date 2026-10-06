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

import Foundation
import Logging
import NIOCore
import NIOEmbedded
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
import Testing

@testable import NIOQUIC

struct QUICConnectionPathTests {
    @available(anyAppleOS 26, *)
    private func makePath(
        isValidated: Bool = false,
        remoteAddress: SocketAddress = try! SocketAddress(ipAddress: "127.0.0.1", port: 9000)
    ) -> QUICConnectionPath<QUICStreamChannels> {
        let eventLoop = EmbeddedEventLoop()
        let context = NetworkContext(
            identifier: "test-context",
            externalScheduler: EventLoopBackedScheduler(eventLoop: eventLoop)
        )
        return QUICConnectionPath(
            role: .server,
            remoteAddress: remoteAddress,
            context: context,
            framePool: .makePool(forGSO: false),
            isValidated: isValidated,
            maxSegments: 1,
            bufferPoolCapacity: 8,
            logger: Logger(label: "test")
        )
    }

    @available(anyAppleOS 26, *)
    @Test
    func freshPathHasNoQueuedData() {
        let path = self.makePath()
        #expect(!path.hasQueuedInboundPackets)
        #expect(!path.hasQueuedOutboundData)
    }

    @available(anyAppleOS 26, *)
    @Test
    func enqueueInboundPacketTracksQueue() {
        let path = self.makePath()
        path.enqueueInboundPacket(ByteBuffer(repeating: 0xAA, count: 100))
        #expect(path.hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func drainInboundFramesEmptiesQueue() {
        let path = self.makePath()
        path.enqueueInboundPacket(ByteBuffer(repeating: 0xAA, count: 50))
        var drained = path.drainInboundFrames(maximumDatagramCount: 10)
        if drained == nil {
            Issue.record("Expected non-nil drained frames")
        } else {
            drained!.finalizeAllFramesAsFailed()
        }
        #expect(!path.hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func drainInboundFramesReturnsNilWhenEmpty() {
        let path = self.makePath()
        let result = path.drainInboundFrames(maximumDatagramCount: 10)
        if result != nil {
            Issue.record("Expected nil when draining empty queue")
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func idlePathDropsOutboundDatagrams() throws {
        let path = self.makePath()

        if let datagrams = try path.getDatagramsToSend(.init(), maximumDatagramCount: 2, minimumDatagramSize: 100) {
            try path.sendDatagrams(.init(), datagrams: datagrams)
        }
        #expect(!path.hasQueuedOutboundData)
    }

    @available(anyAppleOS 26, *)
    @Test
    func detachedPathDropsInboundPackets() {
        let path = self.makePath()
        path.detach()

        path.enqueueInboundPacket(ByteBuffer(repeating: 0xAA, count: 50))
        #expect(!path.hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func unconnectedServerDropsPacketsFromNewAddresses() throws {
        let connection = try self.makeConnection(role: .server, isConnected: false)

        #expect(connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload)) == 0)
        #expect(connection.receivePacket(AddressedEnvelope(remoteAddress: Self.pathAddress, data: Self.payload)) == 50)
        #expect(connection.otherPaths.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func clientDropsPacketsFromNewAddresses() throws {
        let connection = try self.makeConnection(role: .client)

        #expect(connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload)) == 0)
        #expect(connection.otherPaths.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func serverSetsUpPathForNewAddress() throws {
        let connection = try self.makeConnection(role: .server)

        #expect(connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload)) == 50)
        connection.handlePathChanged(remote: Self.pathAddress.toAddressEndpoint())
        try #require(connection.otherPaths.map(\.remoteAddress) == [Self.newAddress])
        #expect(!connection.otherPaths[0].isValidated)
        #expect(connection.otherPaths[0].hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func newPathIsPromotedWhenSwiftNetworkAnnouncesIt() throws {
        let connection = try self.makeConnection(role: .server)

        // SwiftNetwork announces the new path with 'pathChanged' while it is being attached.
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        #expect(connection.activePath.remoteAddress == Self.newAddress)
        #expect(connection.otherPaths.map(\.remoteAddress) == [Self.pathAddress])
    }

    @available(anyAppleOS 26, *)
    @Test
    func pathValidatedMarksPathValidated() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        connection.handlePathChanged(remote: Self.pathAddress.toAddressEndpoint())
        try #require(connection.otherPaths.map(\.isValidated) == [false])

        connection.handlePathValidated(remote: Self.newAddress.toAddressEndpoint())
        #expect(connection.otherPaths.map(\.isValidated) == [true])
    }

    @available(anyAppleOS 26, *)
    @Test
    func pathChangedPromotesPath() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))

        connection.handlePathChanged(remote: Self.pathAddress.toAddressEndpoint())
        #expect(connection.activePath.remoteAddress == Self.pathAddress)
        #expect(connection.otherPaths.map(\.remoteAddress) == [Self.newAddress])

        connection.handlePathChanged(remote: Self.newAddress.toAddressEndpoint())
        #expect(connection.activePath.remoteAddress == Self.newAddress)
        #expect(connection.otherPaths.map(\.remoteAddress) == [Self.pathAddress])
    }

    @available(anyAppleOS 26, *)
    @Test
    func pathUnreachableRemovesPath() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        connection.handlePathChanged(remote: Self.pathAddress.toAddressEndpoint())
        try #require(connection.otherPaths.count == 1)
        let removedPath = connection.otherPaths[0]

        connection.handlePathUnreachable(remote: Self.newAddress.toAddressEndpoint())
        #expect(connection.otherPaths.isEmpty)
        #expect(connection.activePath.remoteAddress == Self.pathAddress)
        #expect(!removedPath.hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func removingActivePathPromotesNewestValidatedPath() throws {
        let connection = try self.makeConnection(role: .server)
        let validated = try SocketAddress(ipAddress: "127.0.0.1", port: 9001)
        let unvalidated = try SocketAddress(ipAddress: "127.0.0.1", port: 9002)
        let active = try SocketAddress(ipAddress: "127.0.0.1", port: 9003)
        for address in [validated, unvalidated, active] {
            connection.receivePacket(AddressedEnvelope(remoteAddress: address, data: Self.payload))
        }
        // Move through the paths in order: every demoted path becomes the newest of the other paths.
        for address in [validated, unvalidated, active] {
            connection.handlePathChanged(remote: address.toAddressEndpoint())
        }
        connection.handlePathValidated(remote: validated.toAddressEndpoint())
        try #require(connection.otherPaths.map(\.remoteAddress) == [Self.pathAddress, validated, unvalidated])

        connection.handlePathUnreachable(remote: active.toAddressEndpoint())
        #expect(connection.activePath.remoteAddress == validated)
        #expect(connection.otherPaths.map(\.remoteAddress) == [Self.pathAddress, unvalidated])
    }

    @available(anyAppleOS 26, *)
    @Test
    func lastPathSurvivesPathUnreachable() throws {
        let connection = try self.makeConnection(role: .server)

        connection.handlePathUnreachable(remote: Self.pathAddress.toAddressEndpoint())
        #expect(connection.activePath.remoteAddress == Self.pathAddress)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.pathAddress, data: Self.payload))
        #expect(connection.activePath.hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func swiftNetworkDetachingRemovesOnlyNonActivePaths() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        connection.handlePathChanged(remote: Self.pathAddress.toAddressEndpoint())
        try #require(connection.otherPaths.count == 1)

        // The active path keeps flushing its last packets.
        try connection.activePath.detach(.init())
        try #require(connection.otherPaths.count == 1)
        #expect(connection.activePath.remoteAddress == Self.pathAddress)

        try connection.otherPaths[0].detach(.init())
        #expect(connection.otherPaths.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func receivePacketsCompleteFeedsEveryPath() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        connection.handlePathChanged(remote: Self.pathAddress.toAddressEndpoint())
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.pathAddress, data: Self.payload))
        try #require(connection.otherPaths.count == 1)
        try #require(connection.activePath.hasQueuedInboundPackets)
        try #require(connection.otherPaths[0].hasQueuedInboundPackets)

        connection.receivePacketsComplete()
        #expect(!connection.activePath.hasQueuedInboundPackets)
        #expect(!connection.otherPaths[0].hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func drainPacketsToSendDrainsEveryPathActiveFirst() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        connection.handlePathChanged(remote: Self.pathAddress.toAddressEndpoint())
        try #require(connection.otherPaths.count == 1)
        for path in [connection.otherPaths[0], connection.activePath] {
            if let datagrams = try path.getDatagramsToSend(.init(), maximumDatagramCount: 1, minimumDatagramSize: 100) {
                try path.sendDatagrams(.init(), datagrams: datagrams)
            }
        }

        let transport = RecordingTransport()
        connection.drainPacketsToSend(to: transport)
        let remoteAddresses = transport.events.map { event -> SocketAddress? in
            switch event {
            case .wrote(let envelope):
                envelope.remoteAddress
            case .flushed, .read:
                nil
            }
        }
        #expect(remoteAddresses == [Self.pathAddress, Self.newAddress])
    }

    private static let pathAddress = try! SocketAddress(ipAddress: "127.0.0.1", port: 9000)
    private static let newAddress = try! SocketAddress(ipAddress: "127.0.0.1", port: 9001)
    private static let payload = ByteBuffer(repeating: 0xAA, count: 50)

    /// A connection whose only path leads to `pathAddress`.
    @available(anyAppleOS 26, *)
    private func makeConnection(
        role: Role,
        isConnected: Bool = true
    ) throws -> SwiftNetworkQUICConnection<QUICStreamChannels> {
        var rng: any RandomNumberGenerator = SystemRandomNumberGenerator()
        let publicKeyPath = Bundle.module.url(forResource: "publicKey", withExtension: "der")!.path
        let localAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 4433)
        let connection: SwiftNetworkQUICConnection<QUICStreamChannels>
        switch role {
        case .client:
            connection = try .client(
                configuration: .client(
                    verificationConfiguration: .rawPublicKeys(publicKeyFilePath: publicKeyPath),
                    applicationProtocols: []
                ),
                sourceConnectionID: .random(using: &rng),
                statelessResetTokenGenerator: .defaultWithAutoGeneratedKey,
                serverName: "quic-test.local",
                asyncVerifier: nil,
                localAddress: localAddress,
                remoteAddress: Self.pathAddress,
                eventLoop: EmbeddedEventLoop(),
                logger: Logger(label: "test")
            )
        case .server:
            connection = try .server(
                configuration: .server(
                    serverName: "quic-test.local",
                    authenticationConfiguration: .rawPublicKeys(
                        publicKeyFilePath: publicKeyPath,
                        privateKeyFilePath: Bundle.module.url(forResource: "privateKey", withExtension: "der")!.path
                    ),
                    applicationProtocols: []
                ),
                sourceConnectionID: .random(using: &rng),
                statelessResetTokenGenerator: .defaultWithAutoGeneratedKey,
                authenticator: nil,
                localAddress: localAddress,
                remoteAddress: Self.pathAddress,
                logger: Logger(label: "test"),
                eventLoop: EmbeddedEventLoop()
            )
        }
        if isConnected {
            connection._forTesting_markConnected()
        }
        return connection
    }
}
