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

import Crypto
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
@_spi(SwiftTLSOptions) @_spi(SwiftTLSProtocol) import SwiftTLS

@available(anyAppleOS 26, *)
extension QUICProtocol {
    static func options(
        from config: QUICConfiguration
    ) throws -> ProtocolOptions<QUICStreamProtocol> {
        let options = Self.options()

        options.connectionOptions.initialMaxData = UInt64(config.initialMaxData)
        options.connectionOptions.initialMaxStreamDataBidirectionalLocal = UInt64(config.initialMaxStreamDataBidiLocal)
        options.connectionOptions.initialMaxStreamDataBidirectionalRemote = UInt64(
            config.initialMaxStreamDataBidiRemote
        )
        options.connectionOptions.initialMaxStreamDataUnidirectional = UInt64(config.initialMaxStreamDataUni)
        options.connectionOptions.initialMaxStreamsBidirectional = UInt64(config.initialMaxStreamsBidi)
        options.connectionOptions.maximumConcurrentBidirectionalStreams = config.initialMaxStreamsBidi
        options.connectionOptions.initialMaxStreamsUnidirectional = UInt64(config.initialMaxStreamsUni)
        options.connectionOptions.maximumConcurrentUnidirectionalStreams = config.initialMaxStreamsUni

        guard let perProtocolOptions = options.perProtocolOptions else {
            throw QUICError.invalidConfiguration
        }

        perProtocolOptions.quicConnectionOptions.idleTimeout = NetworkDuration(duration: config.maxIdleTimeout)

        if let qLogConfiguration = config.qLogConfiguration {
            perProtocolOptions.quicConnectionOptions.qlogConfiguration = QLogConfiguration(
                logTitle: qLogConfiguration.topic.isEmpty ? nil : qLogConfiguration.topic,
                logDescription: qLogConfiguration.description.isEmpty ? nil : qLogConfiguration.description,
                logPath: qLogConfiguration.path
            )
        }

        return options
    }
}

@available(anyAppleOS 26, *)
extension TLSProtocol.Options {
    static func serverOptions(
        from config: QUICConfiguration,
        authConfig: AuthenticationConfiguration,
        authenticator: Authenticator?,
        serverName: String
    ) throws -> Self {
        var options = SwiftTLSProtocol.Options()
        options.applicationProtocols = config.applicationProtocols
        options.serverName = serverName
        options.tlsOptions.keyExchangeGroup = config.keyExchangeGroup.swiftTLSKeyExchangeGroup

        switch authConfig {
        case .x509Certificates:
            guard let authenticator else {
                throw QUICError.tlsConfigurationIncomplete
            }
            options.tlsOptions.asyncAuthenticator = .init(
                supportedCertificateTypes: [.x509],
                getCertificateChain: authenticator.produceCertificates(info:),
                signTranscriptHash: authenticator.provideSignature(info:)
            )

        case .rawPublicKeys(let publicKeyFilePath, let privateKeyFilePath):
            let publicKey = try P256.Signing.PublicKey.fromDERFile(publicKeyFilePath)
            options.trustedRawPublicKeyCertificates = [Array(publicKey.derRepresentation)]

            let privateKey = try P256.Signing.PrivateKey.fromDERFile(privateKeyFilePath)
            options.rawPrivateKey = Array(privateKey.rawRepresentation)
        }

        return options
    }

    static func clientOptions(
        from config: QUICConfiguration,
        asyncVerifier: AsyncVerifier?,
        serverName: String
    ) throws -> Self {
        var options = SwiftTLSProtocol.Options()
        options.applicationProtocols = config.applicationProtocols
        options.serverName = serverName
        options.tlsOptions.keyExchangeGroup = config.keyExchangeGroup.swiftTLSKeyExchangeGroup

        if let verificationConfiguration = config.verificationConfiguration {
            switch verificationConfiguration {
            case .rawPublicKeys(let publicKeyFilePath):
                let publicKey = try P256.Signing.PublicKey.fromDERFile(publicKeyFilePath)
                options.trustedRawPublicKeyCertificates = [Array(publicKey.derRepresentation)]

            case .x509Certificates:
                if let asyncVerifier {
                    options.tlsOptions.asyncVerifier = .init(
                        availableCertificateTypes: [.x509],
                        verificationCallback: asyncVerifier.makeCallback(hostname: serverName)
                    )
                } else {
                    throw QUICError.tlsConfigurationIncomplete
                }
            }
        }

        return options
    }

}
