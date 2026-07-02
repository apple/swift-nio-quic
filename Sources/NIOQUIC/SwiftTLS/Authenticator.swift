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
import SwiftASN1
@_spi(SwiftTLSProtocol) import SwiftTLS
import X509

@available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
enum AuthParameters {
    static var legacySignatureAlgorithms: Set<Certificate.SignatureAlgorithm> {
        [
            .sha1WithRSAEncryption
        ]
    }
}

/// Authenticates the server during TLS 1.3 handshakes using X509 certificates.
@available(anyAppleOS 26, *)
public final class Authenticator: Sendable {
    let privateKey: Certificate.PrivateKey
    let certificates: [Certificate]
    let certificateList: CertificateList
    let supportedSignatureAlgorithms: Set<UInt16>

    /// Create a new Authenticator from file paths. Convenience for ``init(privateKey:certificates:)``
    public convenience init(certificateFilePath certificateChainFile: String, privateKeyFilePath: String) throws {
        // Load key and certificates.
        let certificates = try loadCertificates(fromPEMFile: certificateChainFile)
        let privateKey = try loadPrivateKey(fromPEMFile: privateKeyFilePath)

        try self.init(certificates: certificates, privateKey: privateKey)
    }

    /// Create a new authenticator that sends the certificates to a client to authenticate, providing a
    /// signature with the private key.
    ///
    /// - Throws: If there is not at least one certificate, the leaf certificate does not allow server authentication, or the leaf is not suitable to verify digital signatures.
    public init(certificates: [Certificate], privateKey: Certificate.PrivateKey) throws {
        guard let leaf = certificates.first else {
            // Must at least have a leaf certificiate.
            throw QUICError.certificatesMissing
        }

        // If the key usage extension is present, it must contain server auth unless the CA specified "anyExtendedKeyUsage"
        // RFC 5280, Section 4.2.1.12.: "If the extension is present, then the certificate MUST only be used for one of the purposes indicated."
        // "If a CA includes extended key usages to satisfy such applications, but does not wish to restrict usages of the key, the CA can include the special KeyPurposeId anyExtendedKeyUsage in addition to the particular key purposes required by the applications."
        //
        // It would be RFC conformant to require a specific extended key usage to be present ("Certificate using applications MAY require that the extended key usage extension be present and that a particular purpose be indicated in order for the certificate to be acceptable to that application."), but I'm not sure if we should.
        if let keyUsageExtension = try? leaf.extensions.extendedKeyUsage, !keyUsageExtension.contains(.any) {
            guard keyUsageExtension.contains(.serverAuth) else {
                throw QUICError.certificateNotSuitableForAuthentication
            }
        }

        // RFC 8446, Section 4.4.2.2.: "the digitalSignature bit MUST be set if the Key Usage extension is present"
        if let keyUsage = try? leaf.extensions.keyUsage {
            guard keyUsage.digitalSignature else {
                throw QUICError.certificateNotSuitableForAuthentication
            }
        }

        self.privateKey = privateKey
        self.certificates = certificates
        self.certificateList = CertificateList(
            type: .x509,
            entries: try certificates.map { Data(try $0.serializeAsPEM().derBytes) }
        )
        let signatureAlgorithmsSupportedByKey = Set(self.privateKey.supportedSignatureAlgorithms)
        let acceptableOverlap = signatureAlgorithmsSupportedByKey.subtracting(AuthParameters.legacySignatureAlgorithms)
        // These types come from swift certificates and can be safely converted to the RFC8446 value.
        self.supportedSignatureAlgorithms = Set(
            try acceptableOverlap.map { try UInt16($0.rfc8446SignatureSchemeValue) }
        )
    }

    // The `getCertificateChain` callback for the `ServerAuthProvider` of Swift TLS.
    func produceCertificates(info: CertificateInfo) -> CertificateResult {
        guard info.peerOffer.certificateTypes.contains(.x509) else {
            return .unavailable(reason: "unsupported certificate type")
        }
        // TODO: https://github.com/apple/swift-nio-quic/issues/2
        // SwiftTLS currently does not support the signature_algorithm_cert extension. So we can only check the
        // signature_algorithm extension, here `peerOffer.signatureAlgorithms`.
        // TODO: Check that the certificate we provide uses supported signatures.
        // This requires the signature_algorithm_cert extension as well. See RFC 8446, Section 4.4.2.2.
        guard info.peerOffer.signatureAlgorithms.contains(where: { self.supportedSignatureAlgorithms.contains($0) })
        else {
            return .unavailable(reason: "unsupported signature algorithm")
        }

        // TODO: https://github.com/apple/swift-nio-quic/issues/4
        // The RFC says that servers "MAY require the presence of the \"server_name\" extension".

        return .available(certificateList)
    }

    // The `signTranscriptHash` callback for the `ServerAuthProvider` of Swift TLS.
    func provideSignature(info: SignatureInfo) -> SignatureResult {
        // Pick a signature algorithm that both endpoints support. Since the peer usually sends the list in order
        // of preference, pick the first overlap.
        guard
            let selectedRawValue = info.peerOffer.signatureAlgorithms.first(where: {
                self.supportedSignatureAlgorithms.contains($0)
            }), let selectedAlgorithm = Certificate.SignatureAlgorithm(rfc8446SignatureSchemeValue: selectedRawValue)
        else {
            return .unavailable(reason: "unknown signature algorithm")
        }

        guard
            let signature = try? self.privateKey.sign(bytes: info.transcriptHash, signatureAlgorithm: selectedAlgorithm)
        else {
            return .unavailable(reason: "invalid signature")
        }

        return .available(signature: Data(signature.rawRepresentation), algorithm: selectedRawValue)
    }
}
