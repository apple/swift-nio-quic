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
    private func makeChannel(isSever: Bool = true) throws -> QUICConnectionChannel {
        let parent = EmbeddedChannel()
        try parent.localAddress = SocketAddress(ipAddress: "127.0.0.1", port: 8080)
        try parent.remoteAddress = SocketAddress(ipAddress: "127.0.0.1", port: 8081)

        return QUICConnectionChannel(udpChannel: parent, isServer: isSever)
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
}
