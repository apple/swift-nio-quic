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

@available(macOS 10.15, iOS 13, tvOS 13, watchOS 6, *)
func makeCertificate(
    commonName cn: String,
    issuer: (Certificate, Certificate.PrivateKey)? = nil,
    canSign: Bool = true,
    extendedKeyUsages: [ExtendedKeyUsage.Usage]? = nil
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
                    keyIdentifier: issuerCert.extensions.subjectKeyIdentifier?
                        .keyIdentifier
                )
                if let extendedKeyUsages {
                    try ExtendedKeyUsage(
                        extendedKeyUsages
                    )
                }
            }
        } else {
            extensions = try Certificate.Extensions {
                BasicConstraints.notCertificateAuthority
                if let extendedKeyUsages {
                    try ExtendedKeyUsage(
                        extendedKeyUsages
                    )
                }
                SubjectAlternativeNames([
                    .dnsName(cn),
                    .dnsName("test-nio-quic.com"),
                    .ipAddress(ASN1OctetString(contentBytes: [127, 0, 0, 1])),
                    .ipAddress(ASN1OctetString(contentBytes: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])),
                ])
            }
        }
        // Create the intermediate certificate
        let certificate = try Certificate(
            version: .v3,
            serialNumber: Certificate.SerialNumber(),
            publicKey: key.publicKey,
            notValidBefore: now.addingTimeInterval(-1),
            notValidAfter: now.addingTimeInterval(60 * 60 * 24 * 365),  // 1 year
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
            notValidAfter: now.addingTimeInterval(60 * 60 * 24 * 365 * 10),  // 10 years
            issuer: issuerName,
            subject: subjectName,
            signatureAlgorithm: .ecdsaWithSHA256,
            extensions: extensions,
            issuerPrivateKey: key
        )
        return (certificate, key)
    }
}

@available(macOS 10.15, iOS 13, tvOS 13, watchOS 6, *)
func writeCertificates(
    _ certificates: [Certificate],
    fileTag: String = #function,
    description: String
) throws -> URL {
    let fm = FileManager.default
    let directory = fm.temporaryDirectory

    // Store file paths
    let url = directory.appendingPathComponent("\(fileTag).\(description).cert.pem")

    let pemData = try certificates.map({ try $0.serializeAsPEM().pemString }).joined(separator: "\n") + "\n"
    try pemData.write(to: url, atomically: true, encoding: .utf8)

    return url
}

@available(macOS 10.15, iOS 13, tvOS 13, watchOS 6, *)
func writeKey(
    _ privateKey: Certificate.PrivateKey,
    fileTag: String = #function,
    description: String
) throws -> URL {
    let fm = FileManager.default
    let directory = fm.temporaryDirectory

    let url = directory.appendingPathComponent("\(fileTag).\(description).key.pem")

    try privateKey.serializeAsPEM().pemString.write(to: url, atomically: true, encoding: .utf8)

    return url
}

struct TestCAFilePaths {
    let trustStoreFilePath: String
    let serverCertFilePath: String
    let serverPrivateKeyFilePath: String
}

@available(macOS 10.15, iOS 13, tvOS 13, watchOS 6, *)
struct TestCertificates {
    let rootCert: Certificate
    let rootKey: Certificate.PrivateKey

    let intermediateCert: Certificate
    let intermediateKey: Certificate.PrivateKey

    let leafName: String
    let leafCert: Certificate
    let leafKey: Certificate.PrivateKey

    init(leafCN: String = "leaf-cert") throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "root-cert")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "intermediate-cert",
            issuer: (rootCert, rootKey)
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: leafCN,
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        self.rootCert = rootCert
        self.rootKey = rootKey
        self.intermediateCert = intermediateCert
        self.intermediateKey = intermediateKey
        self.leafCert = leafCert
        self.leafKey = leafKey
        self.leafName = leafCN
    }

    func writeToTemp(fileTag: String = #function) throws -> TestCAFilePaths {
        let trustStoreURL = try writeCertificates([rootCert], fileTag: fileTag, description: "trust-store")
        let serverCAChainURL = try writeCertificates(
            [leafCert, intermediateCert],
            fileTag: fileTag,
            description: "ca-chain"
        )
        let serverPrivateKeyURL = try writeKey(leafKey, fileTag: fileTag, description: "private-key")

        return TestCAFilePaths(
            trustStoreFilePath: trustStoreURL.path,
            serverCertFilePath: serverCAChainURL.path,
            serverPrivateKeyFilePath: serverPrivateKeyURL.path
        )
    }
}
