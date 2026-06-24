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
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
import Testing

@testable import NIOQUIC

let testUnixDomainURL = "/definitely/a/path"
let testIPv4Address = "127.0.0.1"
let testIPv6Address = "::1"
let testPort = 443

struct SocketAddress_EndpointTests {
    @available(anyAppleOS 26, *)
    @Test(
        "Convert a IPv4 SocketAddress to an Endpoint"
    )
    func convertIPv4AddressToEndpoint() throws {
        let address = try SocketAddress(ipAddress: testIPv4Address, port: testPort)
        let endpoint = address.toEndpoint()

        switch endpoint.type {
        case .address(let addrEndpoint):
            switch addrEndpoint.type {
            case .v4(let addr, let port):
                #expect(addr.isLoopback)
                #expect(port == testPort)
            default:
                #expect(Bool(false))
            }
        default:
            #expect(Bool(false))
        }
    }

    @available(anyAppleOS 26, *)
    @Test(
        "Convert a IPv6 SocketAddress to an Host Endpoint"
    )
    func convertIPv6AddressToHostEndpoint() throws {
        let address = try SocketAddress(ipAddress: testIPv6Address, port: testPort)
        let endpoint = address.toEndpoint()

        switch endpoint.type {
        case .host(let hostEndpoint):
            #expect(hostEndpoint.name == testIPv6Address)
            #expect(hostEndpoint.port == testPort)
        default:
            #expect(Bool(false))
        }
    }

    @available(anyAppleOS 26, *)
    @Test(
        "Convert a IPv6 SocketAddress to an Endpoint",
        .disabled("Requirement: Implement conversion to address type")
    )
    func convertIPv6AddressToEndpoint() throws {
        let address = try SocketAddress(ipAddress: testIPv6Address, port: testPort)
        let endpoint = address.toEndpoint()

        switch endpoint.type {
        case .address(let addrEndpoint):
            switch addrEndpoint.type {
            case .v6(let addr, let port):
                #expect(addr.isLoopback)
                #expect(port == testPort)
            default:
                #expect(Bool(false))
            }
        default:
            #expect(Bool(false))
        }
    }

    @available(anyAppleOS 26, *)
    @Test(
        "Convert a Unix domain SocketAddress to an Endpoint",
        .disabled("Requirement: Implement conversion to address type")
    )
    func convertUnixDomainAddressToEndpoint() throws {
        let address = try SocketAddress(unixDomainSocketPath: testUnixDomainURL)
        let endpoint = address.toEndpoint()

        switch endpoint.type {
        case .address(let addrEndpoint):
            switch addrEndpoint.type {
            case .unix(let url):
                #expect(url == testUnixDomainURL)
            default:
                #expect(Bool(false))
            }
        default:
            #expect(Bool(false))
        }
    }
}
