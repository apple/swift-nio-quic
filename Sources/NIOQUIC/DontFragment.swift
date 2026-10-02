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

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

/// Sets the don't fragment bit on UDP sockets: QUIC must not let the IP layer fragment its
/// datagrams (RFC 9000, Section 14).
enum DontFragment {
    #if canImport(Darwin)
    private static let ipv4 = (
        option: ChannelOptions.Types.SocketOption(level: .ip, name: .init(rawValue: IP_DONTFRAG)),
        value: SocketOptionValue(1)
    )
    // IPV6_DONTFRAG, which the SDK only defines with __APPLE_USE_RFC_3542.
    private static let ipv6 = (
        option: ChannelOptions.Types.SocketOption(level: .ipv6, name: .init(rawValue: 62)),
        value: SocketOptionValue(1)
    )
    #elseif os(Linux)
    // PROBE sets the bit like DO does, but ignores the path MTU the kernel learned from ICMP: QUIC
    // discovers the path MTU itself.
    private static let ipv4 = (
        option: ChannelOptions.Types.SocketOption(level: .ip, name: .init(rawValue: IP_MTU_DISCOVER)),
        value: SocketOptionValue(IP_PMTUDISC_PROBE)
    )
    private static let ipv6 = (
        option: ChannelOptions.Types.SocketOption(level: .ipv6, name: .init(rawValue: IPV6_MTU_DISCOVER)),
        value: SocketOptionValue(IPV6_PMTUDISC_PROBE)
    )
    #else
    #error("Setting the don't fragment bit is not implemented for this platform.")
    #endif

    /// Sets the bit on the socket of a bound channel, for the family of its local address.
    static func set(on channel: any Channel) throws {
        guard let options = channel.syncOptions else {
            throw ChannelError.operationUnsupported
        }
        guard let localAddress = channel.localAddress else {
            throw ChannelError.unknownLocalAddress
        }
        switch localAddress {
        case .v4:
            try options.setOption(Self.ipv4.option, value: Self.ipv4.value)
        case .v6:
            try options.setOption(Self.ipv6.option, value: Self.ipv6.value)
            #if os(Linux)
            // Linux sends to IPv4-mapped peers with the IPv4 setting. Darwin applies the IPv6
            // option to them and rejects the IPv4 one on IPv6 sockets.
            try options.setOption(Self.ipv4.option, value: Self.ipv4.value)
            #endif
        case .unixDomainSocket:
            // No IP layer, so nothing can be fragmented.
            ()
        }
    }
}
