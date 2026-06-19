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

@available(anyAppleOS 26, *)
extension SocketAddress {
    /// Returns an `Endpoint` set to a `HostEndpoint` created from this `SocketAddress`.
    func toEndpoint() -> Endpoint {
        switch self {
        case .v4(let addr):
            precondition(self.port != nil)
            return Endpoint(
                address: SwiftNetwork.IPv4Address(UInt32(addr.address.sin_addr.s_addr)),
                port: UInt16(self.port!)
            )
        case .v6(_):
            precondition(self.ipAddress != nil && self.port != nil)
            // TODO: IPv6Address does not (yet) provide the same initializer as v4.
            return Endpoint(HostEndpoint(name: self.ipAddress!, port: UInt16(self.port!)))
        case .unixDomainSocket(_):
            // TODO: We would like to have an initializer for `sockaddr_un`.
            // The URL endpoint initializer requires an URL and recognizes it as a UNIX URL if it starts
            // with {https,http,wss,ws}+unix. IIRC unix domain socket URLs are not well defined. As such,
            // we would like to create an Endpoint containing an AddressEndpoint with a unix AddressEndpointType.
            fatalError("Unix domain sockets are not supported.")
        }
    }
}
