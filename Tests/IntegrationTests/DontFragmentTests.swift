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
import NIOPosix
import Testing

@testable import NIOQUIC

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

@Suite
struct DontFragmentTests {
    /// When the handler is added to the UDP channel.
    enum Installation: CaseIterable {
        /// To the bound and active channel.
        case afterBind
        /// From the async `bind` initializer, before the channel is active.
        case inBindInitializer
    }

    #if canImport(Darwin)
    static let ipv4 = (
        option: ChannelOptions.Types.SocketOption(level: .ip, name: .ip_dontfrag),
        dontFragment: SocketOptionValue(1)
    )
    static let ipv6 = (
        option: ChannelOptions.Types.SocketOption(level: .ipv6, name: .ipv6_dontfrag),
        dontFragment: SocketOptionValue(1)
    )
    #else
    static let ipv4 = (
        option: ChannelOptions.Types.SocketOption(level: .ip, name: .ip_mtu_discover),
        dontFragment: SocketOptionValue(IP_PMTUDISC_PROBE)
    )
    static let ipv6 = (
        option: ChannelOptions.Types.SocketOption(level: .ipv6, name: .ipv6_mtu_discover),
        dontFragment: SocketOptionValue(IPV6_PMTUDISC_PROBE)
    )
    #endif

    static func isIPv6LoopbackAvailable() async -> Bool {
        let channel = try? await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "::1", port: 0)
            .get()
        try? await channel?.close()
        return channel != nil
    }

    @Test
    func setsDontFragmentOnIPv4Socket() async throws {
        let channel = try await Self.bindWithDontFragment(host: "127.0.0.1")
        let value = try await channel.getOption(Self.ipv4.option).get()
        #expect(value == Self.ipv4.dontFragment)
        try await channel.close()
    }

    @Test(.enabled { await Self.isIPv6LoopbackAvailable() })
    func setsDontFragmentOnIPv6Socket() async throws {
        let channel = try await Self.bindWithDontFragment(host: "::1")
        let value = try await channel.getOption(Self.ipv6.option).get()
        #expect(value == Self.ipv6.dontFragment)
        #if os(Linux)
        // Linux sends to IPv4-mapped peers with the IPv4 setting.
        let ipv4Value = try await channel.getOption(Self.ipv4.option).get()
        #expect(ipv4Value == Self.ipv4.dontFragment)
        #endif
        try await channel.close()
    }

    /// Without a local address there is no address family to set the bit for.
    @Test
    func throwsOnUnboundChannel() {
        let channel = EmbeddedChannel()
        #expect(throws: QUICError.noLocalAddress) {
            try QUICDontFragment.set(on: channel)
        }
    }

    /// Unix domain sockets have no IP layer to set the bit on.
    @Test
    func throwsOnUnixDomainSocket() throws {
        let channel = EmbeddedChannel()
        try channel.bind(to: SocketAddress(unixDomainSocketPath: "/tmp/quic.sock"), promise: nil)
        #expect(throws: ChannelError.operationUnsupported) {
            try QUICDontFragment.set(on: channel)
        }
    }

    /// The handler leaves the bit to the user.
    @available(anyAppleOS 26, *)
    @Test(arguments: Installation.allCases)
    func handlerDoesNotSetDontFragment(installation: Installation) async throws {
        let channel = try await Self.makeChannel(host: "127.0.0.1", installation: installation)
        let value = try await channel.getOption(Self.ipv4.option).get()
        #expect(value != Self.ipv4.dontFragment)
        try await channel.close()
    }

    private static func bindWithDontFragment(host: String) async throws -> any Channel {
        let channel = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: host, port: 0)
            .get()
        try await channel.eventLoop.submit {
            try QUICDontFragment.set(on: channel)
        }.get()
        return channel
    }

    @available(anyAppleOS 26, *)
    private static func makeChannel(host: String, installation: Installation) async throws -> any Channel {
        let bootstrap = DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
        switch installation {
        case .afterBind:
            let channel = try await bootstrap.bind(host: host, port: 0).get()
            try await channel.eventLoop.submit {
                try channel.pipeline.syncOperations.addHandler(Self.makeHandler(channel: channel))
            }.get()
            return channel
        case .inBindInitializer:
            return try await bootstrap.bind(host: host, port: 0) { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(Self.makeHandler(channel: channel))
                    return channel
                }
            }
        }
    }

    @available(anyAppleOS 26, *)
    private static func makeHandler(channel: any Channel) -> QUICHandler<QUICStreamChannels> {
        QUICHandler(
            channel: channel,
            quicConfiguration: .forClient(
                verificationConfiguration: .rawPublicKeys(
                    publicKeyFilePath: Bundle.module.url(forResource: "publicKey", withExtension: "der")!.path
                ),
                applicationProtocols: ["http/0.9"]
            ),
            asyncVerifier: nil,
            authenticator: nil,
            logger: Logger(label: "DontFragmentTests"),
            inboundConnectionInitializer: { channel, _ in channel.eventLoop.makeSucceededVoidFuture() },
            inboundStreamInitializer: { channel in channel.eventLoop.makeSucceededVoidFuture() },
            noMoreConnections: {}
        )
    }
}
