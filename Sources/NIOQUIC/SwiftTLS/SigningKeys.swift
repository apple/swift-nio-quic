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
import Foundation

@available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
extension P256.Signing.PrivateKey {
    static func fromDERFile(_ path: String) throws -> P256.Signing.PrivateKey {
        let data = try Data(contentsOf: URL(fileURLWithPath: path))
        return try .init(derRepresentation: Array(data))
    }
}

@available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
extension P256.Signing.PublicKey {
    static func fromDERFile(_ path: String) throws -> P256.Signing.PublicKey {
        let data = try Data(contentsOf: URL(fileURLWithPath: path))
        return try .init(derRepresentation: Array(data))
    }
}

/// The keys a configuration's `.rawPublicKeys` settings name, read from their DER files.
@available(anyAppleOS 27, *)
struct RawPublicKeys: Sendable {
    /// A server's public key, as DER, and its private key, from `authenticationConfiguration`.
    var server: (publicKey: [UInt8], privateKey: [UInt8])?

    /// The server public key a client trusts, as DER, from `verificationConfiguration`.
    var trustedByClient: [UInt8]?

    /// Reads the key files `configuration` names for its role, the half the role's connections use, as the x509
    /// settings are read. Configurations without raw public keys read nothing.
    init(configuration: QUICConfiguration) throws {
        self.server = nil
        self.trustedByClient = nil

        switch configuration.role {
        case .server:
            switch configuration.authenticationConfiguration {
            case .rawPublicKeys(let publicKeyFilePath, let privateKeyFilePath):
                self.server = (
                    publicKey: Array(try P256.Signing.PublicKey.fromDERFile(publicKeyFilePath).derRepresentation),
                    privateKey: Array(try P256.Signing.PrivateKey.fromDERFile(privateKeyFilePath).rawRepresentation)
                )
            case .x509Certificates, nil:
                ()
            }

        case .client:
            switch configuration.verificationConfiguration {
            case .rawPublicKeys(let publicKeyFilePath):
                let publicKey = try P256.Signing.PublicKey.fromDERFile(publicKeyFilePath)
                self.trustedByClient = Array(publicKey.derRepresentation)
            case .x509Certificates, nil:
                ()
            }
        }
    }
}
