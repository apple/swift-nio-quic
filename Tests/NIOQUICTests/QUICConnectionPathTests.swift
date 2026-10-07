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
        #expect(connection._forTesting_getOtherPaths().isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func clientDropsPacketsFromNewAddresses() throws {
        let connection = try self.makeConnection(role: .client)

        #expect(connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload)) == 0)
        #expect(connection._forTesting_getOtherPaths().isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func serverSetsUpPathForNewAddress() throws {
        let connection = try self.makeConnection(role: .server)

        #expect(connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload)) == 50)
        try #require(connection._forTesting_getOtherPaths().map(\.remoteAddress) == [Self.newAddress])
        #expect(!connection._forTesting_getOtherPaths()[0].isValidated)
        #expect(connection._forTesting_getOtherPaths()[0].hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func addressesDifferingOnlyInIPv6FlowInfoShareAPath() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.ipv6Address(flowInfo: 1), data: Self.payload))
        let path = try #require(connection._forTesting_getOtherPaths().first)

        // SwiftNetwork tells paths apart by address and port only, so this is the same path.
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.ipv6Address(flowInfo: 2), data: Self.payload))
        #expect(connection._forTesting_getOtherPaths().count == 1)
        #expect(connection._forTesting_getOtherPaths().first === path)
    }

    @available(anyAppleOS 26, *)
    @Test
    func newPathIsNotPromotedBeforeValidation() throws {
        let connection = try self.makeConnection(role: .server)

        // SwiftNetwork announces the new path with 'pathChanged' while it is being attached, before the peer
        // proved that it receives packets at the new address.
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        #expect(connection._forTesting_getActivePath().remoteAddress == Self.pathAddress)
        #expect(connection._forTesting_getOtherPaths().map(\.remoteAddress) == [Self.newAddress])
    }

    @available(anyAppleOS 26, *)
    @Test
    func pathValidatedPromotesPath() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        try #require(connection._forTesting_getActivePath().remoteAddress == Self.pathAddress)

        connection._forTesting_handlePathValidated(remote: Self.newAddress.toAddressEndpoint())
        #expect(connection._forTesting_getActivePath().remoteAddress == Self.newAddress)
        #expect(connection._forTesting_getActivePath().isValidated)
        #expect(connection._forTesting_getOtherPaths().map(\.remoteAddress) == [Self.pathAddress])
    }

    @available(anyAppleOS 26, *)
    @Test
    func pathUnreachableRemovesPath() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        try #require(connection._forTesting_getOtherPaths().count == 1)
        let removedPath = connection._forTesting_getOtherPaths()[0]

        connection._forTesting_handlePathUnreachable(remote: Self.newAddress.toAddressEndpoint())
        #expect(connection._forTesting_getOtherPaths().isEmpty)
        #expect(connection._forTesting_getActivePath().remoteAddress == Self.pathAddress)
        #expect(!removedPath.hasQueuedInboundPackets)
        // SwiftNetwork keeps unreachable paths until it is told to drop them.
        #expect(connection._forTesting_getSwiftNetworkPathCount() == 1)
    }

    @available(anyAppleOS 26, *)
    @Test
    func newAddressReplacesUnvalidatedPath() throws {
        let connection = try self.makeConnection(role: .server)
        let replacedAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 9002)
        connection.receivePacket(AddressedEnvelope(remoteAddress: replacedAddress, data: Self.payload))
        let replacedPath = try #require(self.trackedPaths(connection).first { $0.remoteAddress == replacedAddress })

        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        #expect(Set(self.trackedPaths(connection).map(\.remoteAddress)) == [Self.pathAddress, Self.newAddress])
        #expect(connection._forTesting_getSwiftNetworkPathCount() == 2)
        replacedPath.enqueueInboundPacket(Self.payload)
        #expect(!replacedPath.hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func newAddressKeepsValidatedPaths() throws {
        let connection = try self.makeConnection(role: .server)
        let validatedAddress = try SocketAddress(ipAddress: "127.0.0.1", port: 9002)
        connection.receivePacket(AddressedEnvelope(remoteAddress: validatedAddress, data: Self.payload))
        connection._forTesting_handlePathValidated(remote: validatedAddress.toAddressEndpoint())

        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        #expect(
            Set(self.trackedPaths(connection).map(\.remoteAddress))
                == [Self.pathAddress, validatedAddress, Self.newAddress]
        )
    }

    @available(anyAppleOS 26, *)
    @Test
    func removingActivePathPromotesNewestValidatedPath() throws {
        let connection = try self.makeConnection(role: .server)
        let validated = try SocketAddress(ipAddress: "127.0.0.1", port: 9001)
        let unvalidated = try SocketAddress(ipAddress: "127.0.0.1", port: 9002)
        let active = try SocketAddress(ipAddress: "127.0.0.1", port: 9003)
        // Validating a path promotes it, so the path validated last ends up active.
        for address in [validated, active] {
            connection.receivePacket(AddressedEnvelope(remoteAddress: address, data: Self.payload))
            connection._forTesting_handlePathValidated(remote: address.toAddressEndpoint())
        }
        connection.receivePacket(AddressedEnvelope(remoteAddress: unvalidated, data: Self.payload))
        try #require(connection._forTesting_getActivePath().remoteAddress == active)
        try #require(
            connection._forTesting_getOtherPaths().map(\.remoteAddress) == [Self.pathAddress, validated, unvalidated]
        )

        connection._forTesting_handlePathUnreachable(remote: active.toAddressEndpoint())
        #expect(connection._forTesting_getActivePath().remoteAddress == validated)
        #expect(connection._forTesting_getOtherPaths().map(\.remoteAddress) == [Self.pathAddress, unvalidated])
    }

    @available(anyAppleOS 26, *)
    @Test
    func removingActivePathKeepsItWithoutValidatedReplacement() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))

        connection._forTesting_handlePathUnreachable(remote: Self.pathAddress.toAddressEndpoint())
        #expect(connection._forTesting_getActivePath().remoteAddress == Self.pathAddress)
        #expect(connection._forTesting_getOtherPaths().map(\.remoteAddress) == [Self.newAddress])
    }

    @available(anyAppleOS 26, *)
    @Test
    func lastPathSurvivesPathUnreachable() throws {
        let connection = try self.makeConnection(role: .server)

        connection._forTesting_handlePathUnreachable(remote: Self.pathAddress.toAddressEndpoint())
        #expect(connection._forTesting_getActivePath().remoteAddress == Self.pathAddress)
        #expect(connection._forTesting_getSwiftNetworkPathCount() == 1)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.pathAddress, data: Self.payload))
        #expect(connection._forTesting_getActivePath().hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func swiftNetworkDetachingRemovesOnlyNonActivePaths() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        try #require(connection._forTesting_getOtherPaths().count == 1)

        // The active path keeps flushing its last packets.
        try connection._forTesting_getActivePath().detach(.init())
        try #require(connection._forTesting_getOtherPaths().count == 1)
        #expect(connection._forTesting_getActivePath().remoteAddress == Self.pathAddress)

        try connection._forTesting_getOtherPaths()[0].detach(.init())
        #expect(connection._forTesting_getOtherPaths().isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func swiftNetworkDetachingFlushesQueuedPacketsBeforeRemovingPath() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        let path = try #require(connection._forTesting_getOtherPaths().first)
        // The last packet SwiftNetwork sends on the path, e.g. a CONNECTION_CLOSE.
        if let datagrams = try path.getDatagramsToSend(.init(), maximumDatagramCount: 1, minimumDatagramSize: 100) {
            try path.sendDatagrams(.init(), datagrams: datagrams)
        }

        try path.detach(.init())
        #expect(connection._forTesting_getOtherPaths().map(\.remoteAddress) == [Self.newAddress])

        let transport = RecordingTransport()
        connection.drainPacketsToSend(to: transport)
        #expect(self.writtenAddresses(transport) == [Self.newAddress])
        #expect(connection._forTesting_getOtherPaths().isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func receivePacketsCompleteFeedsEveryPath() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.pathAddress, data: Self.payload))
        try #require(connection._forTesting_getOtherPaths().count == 1)
        try #require(connection._forTesting_getActivePath().hasQueuedInboundPackets)
        try #require(connection._forTesting_getOtherPaths()[0].hasQueuedInboundPackets)

        connection.receivePacketsComplete()
        #expect(!connection._forTesting_getActivePath().hasQueuedInboundPackets)
        #expect(!connection._forTesting_getOtherPaths()[0].hasQueuedInboundPackets)
    }

    @available(anyAppleOS 26, *)
    @Test
    func drainPacketsToSendDrainsEveryPathActiveFirst() throws {
        let connection = try self.makeConnection(role: .server)
        connection.receivePacket(AddressedEnvelope(remoteAddress: Self.newAddress, data: Self.payload))
        try #require(connection._forTesting_getOtherPaths().count == 1)
        for path in [connection._forTesting_getOtherPaths()[0], connection._forTesting_getActivePath()] {
            if let datagrams = try path.getDatagramsToSend(.init(), maximumDatagramCount: 1, minimumDatagramSize: 100) {
                try path.sendDatagrams(.init(), datagrams: datagrams)
            }
        }

        let transport = RecordingTransport()
        connection.drainPacketsToSend(to: transport)
        #expect(self.writtenAddresses(transport) == [Self.pathAddress, Self.newAddress])
    }

    /// Every path `connection` tracks, the active one first.
    @available(anyAppleOS 26, *)
    private func trackedPaths(
        _ connection: SwiftNetworkQUICConnection<QUICStreamChannels>
    ) -> [QUICConnectionPath<QUICStreamChannels>] {
        [connection._forTesting_getActivePath()] + connection._forTesting_getOtherPaths()
    }

    /// Where the datagrams written to `transport` went, in order. Flushes and reads show up as `nil`.
    private func writtenAddresses(_ transport: RecordingTransport) -> [SocketAddress?] {
        transport.events.map { event in
            switch event {
            case .wrote(let envelope):
                envelope.remoteAddress
            case .flushed, .read:
                nil
            }
        }
    }

    private static let pathAddress = try! SocketAddress(ipAddress: "127.0.0.1", port: 9000)
    private static let newAddress = try! SocketAddress(ipAddress: "127.0.0.1", port: 9001)
    private static let payload = ByteBuffer(repeating: 0xAA, count: 50)

    /// `[::1]:9001` with the given IPv6 flow information.
    private static func ipv6Address(flowInfo: UInt32) -> SocketAddress {
        var address = sockaddr_in6()
        address.sin6_family = sa_family_t(AF_INET6)
        address.sin6_port = in_port_t(9001).bigEndian
        address.sin6_addr = in6addr_loopback
        address.sin6_flowinfo = flowInfo
        return SocketAddress(address, host: "::1")
    }

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
