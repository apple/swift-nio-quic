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

/// `QUICHandler` sets the don't fragment bit on its UDP socket (RFC 9000, Section 14).
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

    static let isIPv6LoopbackAvailable: Bool = {
        let channel = try? DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .bind(host: "::1", port: 0)
            .wait()
        try? channel?.close().wait()
        return channel != nil
    }()

    @available(anyAppleOS 26, *)
    @Test(arguments: Installation.allCases)
    func setsDontFragmentOnIPv4Socket(installation: Installation) async throws {
        let channel = try await Self.makeChannel(host: "127.0.0.1", installation: installation)
        let value = try await channel.getOption(Self.ipv4.option).get()
        #expect(value == Self.ipv4.dontFragment)
        try await channel.close()
    }

    @available(anyAppleOS 26, *)
    @Test(.enabled(if: Self.isIPv6LoopbackAvailable), arguments: Installation.allCases)
    func setsDontFragmentOnIPv6Socket(installation: Installation) async throws {
        let channel = try await Self.makeChannel(host: "::1", installation: installation)
        let value = try await channel.getOption(Self.ipv6.option).get()
        #expect(value == Self.ipv6.dontFragment)
        #if os(Linux)
        // Linux sends to IPv4-mapped peers with the IPv4 setting.
        let ipv4Value = try await channel.getOption(Self.ipv4.option).get()
        #expect(ipv4Value == Self.ipv4.dontFragment)
        #endif
        try await channel.close()
    }

    /// Without a local address the address family, and so the option to set, is unknown.
    @Test
    func throwsWithoutLocalAddress() {
        let channel = EmbeddedChannel()
        #expect(throws: ChannelError.unknownLocalAddress) {
            try DontFragment.set(on: channel)
        }
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
            quicConfiguration: .client(
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
