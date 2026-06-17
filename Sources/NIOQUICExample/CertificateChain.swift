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
import SwiftASN1
import X509

@available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
func makeCertificate(
    commonName cn: String,
    issuer: (Certificate, Certificate.PrivateKey)? = nil,
    canSign: Bool = true,
    validityDays: Int = 365,
    dnsNames: [String] = [],
    ipAddresses: [String] = []
) throws -> (Certificate, Certificate.PrivateKey) {
    let privateKey = P256.Signing.PrivateKey()
    let key = Certificate.PrivateKey(privateKey)
    let now = Date()

    if let (issuerCert, issuerKey) = issuer {
        // Create a signed cert.
        let subjectName = try DistinguishedName {
            CommonName(cn)
        }

        let issuerName = issuerCert.subject
        // Configure extensions for intermediate CA
        let extensions: Certificate.Extensions
        if canSign {
            extensions = try Certificate.Extensions {
                Critical(
                    BasicConstraints.isCertificateAuthority(
                        maxPathLength: nil
                    )
                )
                Critical(
                    KeyUsage(keyCertSign: true, cRLSign: true)
                )
                // Add Authority Key Identifier linking to the root certificate
                try AuthorityKeyIdentifier(
                    keyIdentifier: issuerCert.extensions
                        .subjectKeyIdentifier?
                        .keyIdentifier
                )
            }
        } else {
            var sanElements: [SubjectAlternativeNames.Element] = []

            for dns in dnsNames {
                sanElements.append(.dnsName(dns))
            }

            for ip in ipAddresses {
                let components = ip.split(separator: ".").compactMap { UInt8($0) }
                if components.count == 4 {
                    sanElements.append(.ipAddress(ASN1OctetString(contentBytes: ArraySlice(components))))
                }
            }

            extensions = try Certificate.Extensions {
                BasicConstraints.notCertificateAuthority

                try ExtendedKeyUsage(
                    [.serverAuth]
                )

                SubjectAlternativeNames(sanElements)
            }
        }

        // Create the intermediate certificate
        let certificate = try Certificate(
            version: .v3,
            serialNumber: Certificate.SerialNumber(),
            publicKey: key.publicKey,
            notValidBefore: now.addingTimeInterval(-1),
            notValidAfter: now.addingTimeInterval(60 * 60 * 24 * TimeInterval(validityDays)),
            issuer: issuerName,
            subject: subjectName,
            signatureAlgorithm: .ecdsaWithSHA256,
            extensions: extensions,
            issuerPrivateKey: issuerKey
        )

        return (certificate, key)
    } else {
        // Create a self-signed cert.
        let subjectName = try DistinguishedName {
            CommonName(cn)
        }
        let issuerName = subjectName
        let extensions = try Certificate.Extensions {
            Critical(
                BasicConstraints.isCertificateAuthority(maxPathLength: nil)
            )
            Critical(
                KeyUsage(keyCertSign: true)
            )
        }
        let certificate = try Certificate(
            version: .v3,
            serialNumber: Certificate.SerialNumber(),
            publicKey: key.publicKey,
            notValidBefore: now.addingTimeInterval(-1),
            notValidAfter: now.addingTimeInterval(60 * 60 * 24 * TimeInterval(validityDays)),
            issuer: issuerName,
            subject: subjectName,
            signatureAlgorithm: .ecdsaWithSHA256,
            extensions: extensions,
            issuerPrivateKey: key
        )
        return (certificate, key)
    }
}

@available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
struct CertificateChain {
    let rootCert: Certificate
    let rootKey: Certificate.PrivateKey
    let intermediateCert: Certificate
    let intermediateKey: Certificate.PrivateKey
    let leafCert: Certificate
    let leafKey: Certificate.PrivateKey

    init(
        rootCN: String = "root-cert",
        intermediateCN: String = "intermediate-cert",
        leafCN: String = "leaf-cert",
        dnsNames: [String] = ["leaf-cert"],
        ipAddresses: [String] = ["127.0.0.1"],
        leafValidityDays: Int = 1,
        rootValidityDays: Int = 10
    ) throws {
        let (rootCert, rootKey) = try makeCertificate(
            commonName: rootCN,
            validityDays: rootValidityDays
        )
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: intermediateCN,
            issuer: (rootCert, rootKey),
            validityDays: rootValidityDays
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: leafCN,
            issuer: (intermediateCert, intermediateKey),
            canSign: false,
            validityDays: leafValidityDays,
            dnsNames: dnsNames,
            ipAddresses: ipAddresses
        )

        self.rootCert = rootCert
        self.rootKey = rootKey
        self.intermediateCert = intermediateCert
        self.intermediateKey = intermediateKey
        self.leafCert = leafCert
        self.leafKey = leafKey
    }

    func writeToDirectory(directory: URL) throws -> FilePaths {
        let trustStoreURL = try writeCertificates(
            [rootCert],
            directory: directory,
            name: "trust-store"
        )

        let serverCAChainURL = try writeCertificates(
            [leafCert, intermediateCert],
            directory: directory,
            name: "ca-chain"
        )

        let serverPrivateKeyURL = try writeKey(
            leafKey,
            directory: directory,
            name: "private-key"
        )

        return FilePaths(
            trustStore: trustStoreURL.path,
            serverCert: serverCAChainURL.path,
            serverPrivateKey: serverPrivateKeyURL.path
        )
    }

    struct FilePaths {
        let trustStore: String
        let serverCert: String
        let serverPrivateKey: String
    }

}

@available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
private func writeCertificates(
    _ certificates: [Certificate],
    directory: URL,
    name: String
) throws -> URL {
    let url = directory.appendingPathComponent("\(name).pem")

    let pemData =
        try certificates.map {
            try $0.serializeAsPEM().pemString
        }.joined(separator: "\n") + "\n"
    try pemData.write(to: url, atomically: true, encoding: .utf8)

    return url
}

@available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
private func writeKey(
    _ privateKey: Certificate.PrivateKey,
    directory: URL,
    name: String
) throws -> URL {
    let url = directory.appendingPathComponent("\(name).pem")

    try privateKey.serializeAsPEM().pemString.write(
        to: url,
        atomically: true,
        encoding: .utf8
    )

    return url
}
