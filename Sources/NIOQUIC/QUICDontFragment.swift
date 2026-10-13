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

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

/// Sets the don't fragment bit on UDP sockets: QUIC must not let the IP layer fragment its
/// datagrams (RFC 9000, Section 14).
///
/// NIOQUIC doesn't set the channel option. Please make sure to set it on the channel a ``QUICHandler``
/// is added to if that channel is a UDP socket bound to an IPv4 or IPv6 address, e.g. a channel from
/// `DatagramBootstrap`. Do not set it on channels without an IP layer, such as Unix domain sockets.
public enum QUICDontFragment {
    #if canImport(Darwin)
    private static let ipv4 = (
        option: ChannelOptions.Types.SocketOption(level: .ip, name: .ip_dontfrag),
        value: SocketOptionValue(1)
    )
    private static let ipv6 = (
        option: ChannelOptions.Types.SocketOption(level: .ipv6, name: .ipv6_dontfrag),
        value: SocketOptionValue(1)
    )
    #elseif os(Linux)
    // PROBE sets the bit like DO does, but ignores the path MTU the kernel learned from ICMP: QUIC
    // discovers the path MTU itself.
    private static let ipv4 = (
        option: ChannelOptions.Types.SocketOption(level: .ip, name: .ip_mtu_discover),
        value: SocketOptionValue(IP_PMTUDISC_PROBE)
    )
    private static let ipv6 = (
        option: ChannelOptions.Types.SocketOption(level: .ipv6, name: .ipv6_mtu_discover),
        value: SocketOptionValue(IPV6_PMTUDISC_PROBE)
    )
    #endif

    /// Sets socket opitons to avoid fragemention on the socket of a bound channel,
    /// for the family of its local address.
    ///
    /// Call it on the channel's event loop once the channel is bound, before it carries QUIC
    /// traffic: datagrams sent before the call may be fragmented.
    ///
    /// The related socket options are `IP_DONTFRAG` / `IPV6_DONTFRAG` on Darwin and
    /// `IP_MTU_DISCOVER` / `IPV6_MTU_DISCOVER` with the value `IPV6_PMTUDISC_PROBE`
    /// on Linux.
    ///
    /// - Parameter channel: A UDP channel bound to an IPv4 or IPv6 address.
    /// - Throws: ``QUICError/noLocalAddress`` if the channel isn't bound,
    ///   `ChannelError.operationUnsupported` if it's bound to a Unix domain socket or doesn't
    ///   support synchronous options.
    /// - Precondition: The platform is Darwin or Linux.
    public static func set(on channel: any Channel) throws {
        #if canImport(Darwin) || os(Linux)
        guard let options = channel.syncOptions else {
            throw ChannelError.operationUnsupported
        }
        guard let localAddress = channel.localAddress else {
            throw QUICError.noLocalAddress
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
            // No IP layer to set the bit on.
            throw ChannelError.operationUnsupported
        }
        #else
        preconditionFailure("Setting the don't fragment bit is not supported on this platform.")
        #endif
    }
}
