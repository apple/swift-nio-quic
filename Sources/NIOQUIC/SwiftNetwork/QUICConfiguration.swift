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

import Logging
import NIOCore
@_spi(SwiftTLSOptions) import SwiftTLS

public enum Role: Sendable, CustomStringConvertible {
    case server
    case client

    public var description: String {
        if self == .client {
            return "Client"
        } else {
            return "Server"
        }
    }
}

// Configure verification of the peer's certificate.
public enum CertificateVerification: Sendable {
    case noVerification
    case noHostnameVerification
    case fullVerification
}

// The server can either authenticate using x509 certificates or using raw public keys.
public enum AuthenticationConfiguration: Sendable {
    case x509Certificates(certificateChainFilePath: String, privateKeyFilePath: String)
    case rawPublicKeys(publicKeyFilePath: String, privateKeyFilePath: String)
}

public enum VerificationConfiguration: Sendable {
    // Expect X509 cerification from the server, optionally configure the trusted roots.
    case x509Certificates(trustRootsFilePath: String?)
    // Raw public key auth requires a key to trust.
    case rawPublicKeys(publicKeyFilePath: String)
}

public enum KeyExchangeGroup: UInt16, Sendable {
    case secp256 = 0x0017
    case secp384 = 0x0018
    case x25519 = 0x001D
    case x25519MLKEM768 = 0x11EC

    @available(anyAppleOS 26, *)
    internal var swiftTLSKeyExchangeGroup: SwiftTLS.SwiftTLSOptions.KeyExchangeGroup {
        switch self {
        case .secp256:
            return .secp256
        case .secp384:
            return .secp384
        case .x25519:
            return .x25519
        case .x25519MLKEM768:
            return .x25519MLKEM768
        }
    }
}

/// Configure the size of the Initial packet.
///
/// NOTE: With the don't fragment bit set (RFC 9000, § 14, see ``QUICDontFragment``), the network
/// drops datagrams larger than the path MTU instead of fragmenting them, so a size the path can't
/// carry stalls the handshake until the idle timeout.
public struct InitialPacketSize: Sendable, Hashable {
    enum Kind: Sendable, Hashable {
        case fixed(Int)
        case matchingClientInitial(maximum: Int)
    }

    let kind: Kind

    /// Send UDP payloads of up to `size` bytes.
    public static func fixed(_ size: Int) -> Self {
        Self(kind: .fixed(size))
    }

    /// Send UDP payloads as large as the datagram that carried the client's first Initial packet,
    /// but at most `maximum` bytes.
    ///
    /// NOTE: The client's Initial only shows that the path carries packets of its size towards
    /// the server, not back towards the client. Only servers can match a client's Initial, clients
    /// use 1200 bytes (minimum according RFC 9000, § 8.1).
    public static func matchingClientInitial(upTo maximum: Int) -> Self {
        Self(kind: .matchingClientInitial(maximum: maximum))
    }

    /// The packet size to configure a connection with.
    ///
    /// - Parameter clientInitialSize: The size of the datagram that carried the client's first
    ///   Initial packet, or `nil` on the client.
    func size(forClientInitial clientInitialSize: Int?) -> Int {
        switch self.kind {
        case .fixed(let size):
            return size
        case .matchingClientInitial(let maximum):
            if let clientInitialSize {
                return min(clientInitialSize, maximum)
            }
            return 1200
        }
    }
}

@available(anyAppleOS 26, *)
public struct QUICConfiguration: Sendable {
    public struct QLogConfiguration: Sendable {
        /// The directory to where the qlog files are written to.
        public var path: String

        /// The title to use when logging. Empty means no title, the default.
        public var topic: String = ""

        /// The description to use when logging. Empty means no description, the default.
        public var description: String = ""

        public init(path: String) {
            self.path = path
        }
    }
    /// Whether this configures a client or a server. Defaults to ``Role/server``.
    public var role: Role = .server
    /// The host name of the QUIC server created with this configuration. Required for servers.
    public var serverName: String?
    /// The paths for certificates and keys the server authenticates with. Required for servers.
    public var authenticationConfiguration: AuthenticationConfiguration?
    /// Information required to verify the server identity. Required for clients.
    public var verificationConfiguration: VerificationConfiguration?
    /// The key exchange group used for TLS. Defaults to ``KeyExchangeGroup/x25519``.
    public var keyExchangeGroup: KeyExchangeGroup = .x25519
    /// The list of supported application protocols, negotiated with ALPN. Required.
    public var applicationProtocols: [String] = []
    /// The max idle timeout for the connection. Defaults to 30 seconds.
    public var maxIdleTimeout: Duration = .milliseconds(30000)
    /// How many bytes the peer may initially send on the connection. Defaults to 16 MiB.
    public var initialMaxData: Int = 16_777_216
    /// How many bytes the peer may initially send on each bidirectional stream opened locally. Defaults to 2 MiB.
    public var initialMaxStreamDataBidiLocal: Int = 2_097_152
    /// How many bytes the peer may initially send on each bidirectional stream it opens. Defaults to 2 MiB.
    public var initialMaxStreamDataBidiRemote: Int = 2_097_152
    /// How many bytes the peer may initially send on each unidirectional stream. Defaults to 2 MiB.
    public var initialMaxStreamDataUni: Int = 2_097_152
    /// How many bidirectional streams the peer may open concurrently. Defaults to 8.
    public var initialMaxStreamsBidi: Int = 8
    /// How many unidirectional streams the peer may open concurrently. Defaults to 8.
    public var initialMaxStreamsUni: Int = 8
    /// The interval at which to send keep-alives. Defaults to `nil`, no keep-alives.
    public var keepAliveInterval: Duration?
    /// Force version negotiation, mainly to test that it completes successfully. Only supported by the client.
    /// Defaults to `false`.
    public var forceVersionNegotiation: Bool = false
    /// Direct the QUIC server to send retry packets before accepting connections. Only supported by the server.
    /// Defaults to `false`.
    public var sendRetry: Bool = false
    /// The path to the file where the key log should be written to. Defaults to `nil`, no key log.
    public var keyLogPath: String?
    /// Configuration for qlog. Defaults to `nil`, no qlog.
    public var qLogConfiguration: QLogConfiguration?
    /// Configure verification of the peer's certificate. Only supported by the client.
    /// Defaults to ``CertificateVerification/fullVerification``.
    public var peerCertificateVerification: CertificateVerification = .fullVerification
    /// Maximum datagram frame size in bytes. Set to 0 to disable datagrams.
    /// Defaults to 65535 (the max value allowed) as recommended in RFC 9221.
    public var maxDatagramFrameSize: Int = 65535
    /// The maximum UDP payload size the connection sends from its first packet on. Defaults to
    /// 1200 bytes, the minimum every QUIC path must support (RFC 9000, Section 14). Initial packets are
    /// padded to this size, and the connection never lowers its maximum packet size below it.
    ///
    /// NOTE: Only raise it if the whole path and the peer support it. Values of 1200 or less have no effect.
    public var initialPacketSize: InitialPacketSize = .fixed(1200)

    /// Creates a configuration with the default value for every member.
    ///
    /// The defaults are incomplete, a ``QUICHandler`` rejects them with ``QUICError/invalidConfiguration``.
    /// A server needs ``applicationProtocols``, ``serverName`` and ``authenticationConfiguration``. A client sets
    /// ``role`` to ``Role/client`` and needs ``applicationProtocols`` and ``verificationConfiguration``.
    /// ``forServer(serverName:authenticationConfiguration:applicationProtocols:)`` and
    /// ``forClient(verificationConfiguration:applicationProtocols:)`` set all of them at once.
    public init() {}

    /// Factory method to initialise a `QUICConfiguration` for servers. Set further options through the members.
    ///
    /// - Parameters:
    ///     - serverName: The name of the server.
    ///     - authenticationConfiguration: The paths for certificates and keys required for authentication.
    ///     - applicationProtocols: The list of supported application protocols.
    public static func forServer(
        serverName: String,
        authenticationConfiguration: AuthenticationConfiguration,
        applicationProtocols: [String]
    ) -> Self {
        var configuration = Self()
        configuration.serverName = serverName
        configuration.authenticationConfiguration = authenticationConfiguration
        configuration.applicationProtocols = applicationProtocols
        return configuration
    }

    /// Factory method to initialise a `QUICConfiguration` for clients. Set further options through the members.
    ///
    /// - Parameters:
    ///     - verificationConfiguration: Information required to verify the server identity.
    ///     - applicationProtocols: The list of supported application protocols.
    public static func forClient(
        verificationConfiguration: VerificationConfiguration,
        applicationProtocols: [String]
    ) -> Self {
        var configuration = Self()
        configuration.role = .client
        configuration.verificationConfiguration = verificationConfiguration
        configuration.applicationProtocols = applicationProtocols
        return configuration
    }

    /// Throws ``QUICError/invalidConfiguration`` if a required member is missing, after logging each one.
    func validate(logger: Logger) throws {
        var validConfiguration = true
        if self.applicationProtocols.isEmpty {
            logger.error("Missing configuration: 'applicationProtocols'")
            validConfiguration = false
        }
        switch self.role {
        case .client:
            if self.verificationConfiguration == nil {
                logger.error("Missing configuration for client: 'verificationConfiguration'")
                validConfiguration = false
            }
        case .server:
            if self.serverName == nil {
                logger.error("Missing configuration for server: 'serverName'")
                validConfiguration = false
            }
            if self.authenticationConfiguration == nil {
                logger.error("Missing configuration for server: 'authenticationConfiguration'")
                validConfiguration = false
            }
        }
        guard validConfiguration else {
            throw QUICError.invalidConfiguration
        }
    }
}
