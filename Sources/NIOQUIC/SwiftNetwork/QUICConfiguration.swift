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

@available(anyAppleOS 26, *)
public struct QUICConfiguration: Sendable {
    public struct QLogConfiguration: Sendable {
        /// The directory to where the qlog files are written to.
        public var path: String

        /// The title to use when logging.
        public var topic: String

        /// The description to use when logging.
        public var description: String

        public init(path: String, topic: String, description: String) {
            self.path = path
            self.topic = topic
            self.description = description
        }
    }
    public var role: Role
    // The host name of the QUIC server created with this configuration.
    public var serverName: String?
    public var authenticationConfiguration: AuthenticationConfiguration?
    public var verificationConfiguration: VerificationConfiguration?
    public var keyExchangeGroup: KeyExchangeGroup
    public var applicationProtocols: [String]
    public var maxIdleTimeout: Duration
    public var initialMaxData: Int
    public var initialMaxStreamDataBidiLocal: Int
    public var initialMaxStreamDataBidiRemote: Int
    public var initialMaxStreamDataUni: Int
    public var initialMaxStreamsBidi: Int
    public var initialMaxStreamsUni: Int
    // Set a keep-alive interval in seconds
    var keepAliveInterval: Duration?
    // Client flag mainly for testing to ensure that forcing version negotiation completes successfully
    public var forceVersionNegotiation: Bool
    /// Direct the QUIC server to send retry packets before accepting connections.
    public var sendRetry: Bool
    public var keyLogPath: String?
    public var qLogConfiguration: QLogConfiguration?
    /// Configure verification of the peer's certificate. Only supported by the client.
    public var peerCertificateVerification: CertificateVerification
    /// Maximum datagram frame size in bytes. Set to 0 to disable datagrams.
    /// Defaults to 65535 (the max value allowed) as recommended in RFC 9221.
    public var maxDatagramFrameSize: Int

    private init(
        role: Role,
        serverName: String?,
        authenticationConfiguration: AuthenticationConfiguration?,
        verificationConfiguration: VerificationConfiguration?,
        keyExchangeGroup: KeyExchangeGroup,
        applicationProtocols: [String],
        maxIdleTimeout: Duration,
        initialMaxData: Int,
        initialMaxStreamDataBidiLocal: Int,
        initialMaxStreamDataBidiRemote: Int,
        initialMaxStreamDataUni: Int,
        initialMaxStreamsBidi: Int,
        initialMaxStreamsUni: Int,
        keepAliveInterval: Duration?,
        forceVersionNegotiation: Bool,
        sendRetry: Bool,
        keyLogPath: String?,
        qLogConfiguration: QLogConfiguration?,
        peerCertificateVerification: CertificateVerification,
        maxDatagramFrameSize: Int
    ) {
        self.role = role
        self.serverName = serverName
        self.authenticationConfiguration = authenticationConfiguration
        self.verificationConfiguration = verificationConfiguration
        self.keyExchangeGroup = keyExchangeGroup
        self.applicationProtocols = applicationProtocols
        self.maxIdleTimeout = maxIdleTimeout
        self.initialMaxData = initialMaxData
        self.initialMaxStreamDataBidiLocal = initialMaxStreamDataBidiLocal
        self.initialMaxStreamDataBidiRemote = initialMaxStreamDataBidiRemote
        self.initialMaxStreamDataUni = initialMaxStreamDataUni
        self.initialMaxStreamsBidi = initialMaxStreamsBidi
        self.initialMaxStreamsUni = initialMaxStreamsUni
        self.keepAliveInterval = keepAliveInterval
        self.forceVersionNegotiation = forceVersionNegotiation
        self.sendRetry = sendRetry
        self.keyLogPath = keyLogPath
        self.qLogConfiguration = qLogConfiguration
        self.peerCertificateVerification = peerCertificateVerification
        self.maxDatagramFrameSize = maxDatagramFrameSize
    }

    /// Factory method to initialise a `QUICConfiguration` for servers.
    ///
    /// - Parameters:
    ///     - serverName: The name of the server.
    ///     - authenticationConfiguration: The paths for certificates and keys required for authentication.
    ///     - keyExchangeGroup: The key exchange group used for TLS.
    ///     - applicationProtocols: The list of supported application protocols.
    ///     - maxIdleTimeout: The max idle timeout for the connection.
    ///     - keyLogPath: The path to the file where the key log should be written to.
    ///     - qLogConfiguration: Configuration for qlog.
    public static func server(
        serverName: String,
        authenticationConfiguration: AuthenticationConfiguration,
        keyExchangeGroup: KeyExchangeGroup = .x25519,
        applicationProtocols: [String],
        maxIdleTimeout: Duration = .milliseconds(30000),
        initialMaxData: Int = 16_777_216,
        initialMaxStreamDataBidiLocal: Int = 2_097_152,
        initialMaxStreamDataBidiRemote: Int = 2_097_152,
        initialMaxStreamDataUni: Int = 2_097_152,
        initialMaxStreamsBidi: Int = 8,
        initialMaxStreamsUni: Int = 8,
        keepAliveInterval: Duration? = nil,
        sendRetry: Bool = false,
        keyLogPath: String? = nil,
        qLogConfiguration: QLogConfiguration? = nil,
        maxDatagramFrameSize: Int = 65535
    ) -> Self {
        self.init(
            role: .server,
            serverName: serverName,
            authenticationConfiguration: authenticationConfiguration,
            verificationConfiguration: nil,
            keyExchangeGroup: keyExchangeGroup,
            applicationProtocols: applicationProtocols,
            maxIdleTimeout: maxIdleTimeout,
            initialMaxData: initialMaxData,
            initialMaxStreamDataBidiLocal: initialMaxStreamDataBidiLocal,
            initialMaxStreamDataBidiRemote: initialMaxStreamDataBidiRemote,
            initialMaxStreamDataUni: initialMaxStreamDataUni,
            initialMaxStreamsBidi: initialMaxStreamsBidi,
            initialMaxStreamsUni: initialMaxStreamsUni,
            keepAliveInterval: keepAliveInterval,
            forceVersionNegotiation: false,
            sendRetry: sendRetry,
            keyLogPath: keyLogPath,
            qLogConfiguration: qLogConfiguration,
            peerCertificateVerification: .noVerification,
            maxDatagramFrameSize: maxDatagramFrameSize
        )
    }

    /// Factory method to initialise a `QUICConfiguration` for servers.
    ///
    /// - Parameters:
    ///     - verificationConfiguration: Information required to verify the server identity.
    ///     - keyExchangeGroup: The key exchange group used for TLS.
    ///     - applicationProtocols: The list of supported application protocols.
    ///     - maxIdleTimeout: The max idle timeout for the connection.
    ///     - keyLogPath: The path to the file where the key log should be written to.
    ///     - qLogConfiguration: Configuration for qlog.
    ///     - peerCertificateVerification: Customize verification of the peer certificate.
    public static func client(
        verificationConfiguration: VerificationConfiguration,
        keyExchangeGroup: KeyExchangeGroup = .x25519,
        applicationProtocols: [String],
        maxIdleTimeout: Duration = .milliseconds(30000),
        initialMaxData: Int = 16_777_216,
        initialMaxStreamDataBidiLocal: Int = 2_097_152,
        initialMaxStreamDataBidiRemote: Int = 2_097_152,
        initialMaxStreamDataUni: Int = 2_097_152,
        initialMaxStreamsBidi: Int = 8,
        initialMaxStreamsUni: Int = 8,
        keepAliveInterval: Duration? = nil,
        forceVersionNegotiation: Bool = false,
        keyLogPath: String? = nil,
        qLogConfiguration: QLogConfiguration? = nil,
        peerCertificateVerification: CertificateVerification = .fullVerification,
        maxDatagramFrameSize: Int = 65535
    ) -> Self {
        self.init(
            role: .client,
            serverName: nil,
            authenticationConfiguration: nil,
            verificationConfiguration: verificationConfiguration,
            keyExchangeGroup: keyExchangeGroup,
            applicationProtocols: applicationProtocols,
            maxIdleTimeout: maxIdleTimeout,
            initialMaxData: initialMaxData,
            initialMaxStreamDataBidiLocal: initialMaxStreamDataBidiLocal,
            initialMaxStreamDataBidiRemote: initialMaxStreamDataBidiRemote,
            initialMaxStreamDataUni: initialMaxStreamDataUni,
            initialMaxStreamsBidi: initialMaxStreamsBidi,
            initialMaxStreamsUni: initialMaxStreamsUni,
            keepAliveInterval: keepAliveInterval,
            forceVersionNegotiation: forceVersionNegotiation,
            sendRetry: false,
            keyLogPath: keyLogPath,
            qLogConfiguration: qLogConfiguration,
            peerCertificateVerification: peerCertificateVerification,
            maxDatagramFrameSize: maxDatagramFrameSize
        )
    }
}
