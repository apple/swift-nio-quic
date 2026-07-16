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

import NIOCore
import NIOEmbedded
import Testing

@testable import NIOQUIC

struct QUICConnectionChannelTests {
    @available(anyAppleOS 26, *)
    private func makeChannel(
        localAddress: SocketAddress = try! SocketAddress(ipAddress: "127.0.0.1", port: 8080),
        remoteAddress: SocketAddress = try! SocketAddress(ipAddress: "127.0.0.1", port: 8081),
        isSever: Bool = true
    ) throws -> QUICConnectionChannel {
        let parent = EmbeddedChannel()
        let connection = NoOpConnection(localAddress: localAddress, remoteAddress: remoteAddress)

        return QUICConnectionChannel(
            udpChannel: parent,
            connection: .test(connection),
            registrar: .test(NoOpRegistrar()),
            transport: .test(NoOpTransport()),
            isServer: isSever
        )
    }

    @available(anyAppleOS 26, *)
    @Test
    func getAutoRead() throws {
        let channel = try self.makeChannel()

        let autoRead = try channel.getOption(.autoRead).wait()
        #expect(autoRead)

        let autoReadSync = try channel.syncOptions!.getOption(.autoRead)
        #expect(autoReadSync)
    }

    @available(anyAppleOS 26, *)
    @Test
    func setAutoRead() throws {
        let channel = try self.makeChannel()

        try channel.setOption(.autoRead, value: false).wait()
        let autoRead = try channel.getOption(.autoRead).wait()
        #expect(!autoRead)

        try channel.syncOptions!.setOption(.autoRead, value: true)
        let autoReadSync = try channel.syncOptions!.getOption(.autoRead)
        #expect(autoReadSync)
    }

    @available(anyAppleOS 26, *)
    @Test
    func getUnsupportedOption() throws {
        let channel = try self.makeChannel()

        #expect(throws: ChannelError.self) {
            try channel.getOption(.backlog).wait()
        }

        #expect(throws: ChannelError.self) {
            try channel.syncOptions!.getOption(.backlog)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func setUnsupportedOption() throws {
        let channel = try self.makeChannel()

        #expect(throws: ChannelError.self) {
            try channel.setOption(.backlog, value: 42).wait()
        }

        #expect(throws: ChannelError.self) {
            try channel.syncOptions!.setOption(.backlog, value: 43)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func address() throws {
        let local = try SocketAddress(ipAddress: "127.0.0.1", port: 1234)
        let remote = try SocketAddress(ipAddress: "127.0.0.1", port: 5678)

        let channel = try self.makeChannel(localAddress: local, remoteAddress: remote)
        #expect(channel.localAddress == local)
        #expect(channel.remoteAddress == remote)
    }
}

struct NoOpTransport: QUICTransport {
    func writeDatagram(_ envelope: AddressedEnvelope<ByteBuffer>, promise: EventLoopPromise<Void>?) {
        promise?.succeed()
    }

    func flush() {
    }

    func read() {
    }
}

@available(anyAppleOS 26, *)
struct NoOpRegistrar: QUICConnectionIDRegistrar {
    func associate(_ newID: QUICConnectionID, with existingID: QUICConnectionID) -> Bool {
        true
    }

    func retire(_ connectionID: QUICConnectionID) -> Bool {
        true
    }

    func generateID() -> QUICConnectionID {
        .zero
    }
}

@available(anyAppleOS 26, *)
struct NoOpConnection: QUICConnectionProtocol {
    let localAddress: SocketAddress
    let remoteAddress: SocketAddress

    init(localAddress: SocketAddress, remoteAddress: SocketAddress) {
        self.localAddress = localAddress
        self.remoteAddress = remoteAddress
    }

    func receivePacket(_ packet: ByteBuffer) -> Int {
        0
    }

    func receivePacketsComplete() {
    }

    func nextPacketToSend() -> ByteBuffer? {
        nil
    }

    func close(isApplicationClose: Bool, errorCode: Int64, reason: String) -> Bool {
        true
    }

    func closeAllStreams() -> [EventLoopFuture<Void>] {
        []
    }

    func quiesceStreams() {
    }
}
