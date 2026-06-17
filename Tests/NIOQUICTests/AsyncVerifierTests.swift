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
@_spi(SwiftTLSProtocol) import SwiftTLS
import X509
import XCTest

@testable import NIOQUIC

enum TaskTypes {
    case authentication
    case testing
}

@available(anyAppleOS 26, *)
final class AsyncVerifierTests: XCTestCase {

    // MARK: Related tests

    func testReadPEMFileFromDisk() throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, _) = try makeCertificate(commonName: "test-intermediate", issuer: (rootCert, rootKey))

        let filePath = try writeCertificates([intermediateCert, rootCert], fileTag: #function, description: "ca-chain")

        let certificatesFromDisk = try loadCertificates(fromPEMFile: filePath.path())

        XCTAssertEqual(certificatesFromDisk.count, 2)
        XCTAssertEqual(certificatesFromDisk[0], intermediateCert)
        XCTAssertEqual(certificatesFromDisk[1], rootCert)
    }

    // MARK: Trust roots

    func testAsyncAuthenticatorVerifiesCertificates() async throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes),
            try Data(intermediateCert.serializeAsPEM().derBytes),
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = try AsyncVerifier(trustRoots: trustStore)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "test-leaf")
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                XCTAssertEqual(result, .valid)
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testAsyncAuthenticatorVerifiesCertificatesIPv4Address() async throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes),
            try Data(intermediateCert.serializeAsPEM().derBytes),
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = try AsyncVerifier(trustRoots: trustStore)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "127.0.0.1")  // IP instead of dns name
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                XCTAssertEqual(result, .valid)
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testAsyncAuthenticatorVerifiesCertificatesIPv6Address() async throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes),
            try Data(intermediateCert.serializeAsPEM().derBytes),
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = try AsyncVerifier(trustRoots: trustStore)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "::1")  // IP instead of dns name
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                XCTAssertEqual(result, .valid)
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testAsyncAuthenticatorDoesNotVerifyBrokenChain() async throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes)
            // <-- intermediate cert is missing here, thus the leaf should not be verifiable.
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = try AsyncVerifier(trustRoots: trustStore)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "test-leaf")
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                switch result {
                case .waiting:
                    XCTFail("deliverResult cannot return .waiting.")
                case .invalid:
                    // Yes. The intermediate cert is missing.
                    break
                case .valid:
                    XCTFail("There is no chain!")
                }
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testAsyncAuthenticatorDoesNotVerifyWrongHostname() async throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes),
            try Data(intermediateCert.serializeAsPEM().derBytes),
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = try AsyncVerifier(trustRoots: trustStore)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "this-is-the-wrong-name")  // <--
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                switch result {
                case .waiting:
                    XCTFail("deliverResult cannot return .waiting.")
                case .invalid:
                    // Yes. The hostname is wrong.
                    break
                case .valid:
                    XCTFail("Hostname verification is not working.")
                }
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testAsyncAuthenticatorDoesNotVerifyWrongHostnameIPv4Address() async throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes),
            try Data(intermediateCert.serializeAsPEM().derBytes),
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = try AsyncVerifier(trustRoots: trustStore)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "127.0.0.2")  // <--
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                switch result {
                case .waiting:
                    XCTFail("deliverResult cannot return .waiting.")
                case .invalid:
                    // Yes. The hostname is wrong.
                    break
                case .valid:
                    XCTFail("Hostname verification is not working.")
                }
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testTrustRootsNoHostnameVerification() async throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes),
            try Data(intermediateCert.serializeAsPEM().derBytes),
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = try AsyncVerifier(trustRoots: trustStore, certificateVerification: .noHostnameVerification)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "this-is-the-wrong-name")  // <--
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                switch result {
                case .waiting:
                    XCTFail("deliverResult cannot return .waiting.")
                case .invalid:
                    XCTFail("Hostname verification should be disabled.")
                case .valid:
                    // Yes. The hostname is not checked.
                    break
                }
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testTrustRootsNoHostnameVerificationCertsFail() async throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes)
            // <-- missing
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = try AsyncVerifier(trustRoots: trustStore, certificateVerification: .noHostnameVerification)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "this-is-the-wrong-name")  // <--
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                switch result {
                case .waiting:
                    XCTFail("deliverResult cannot return .waiting.")
                case .invalid:
                    // Yes. The chain is broken
                    break
                case .valid:
                    XCTFail("The certification verification should fail.")
                }
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testTrustRootsNoVerification() async throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes)
            // <-- missing
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verificationInfo = VerificationInfo(
            certificates: CertificateList(
                type: .x509,
                entries: clientCertificates
            ),
            signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
            signature: Data(signatureData.rawRepresentation),
            transcriptHash: transcriptHashData
        )

        let verifier = try AsyncVerifier(trustRoots: trustStore, certificateVerification: .noVerification)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "this-is-the-wrong-name")  // <--
                let status = callback(verificationInfo)
                XCTAssertEqual(status, .valid)
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    // MARK: Additional trust roots

    func testAdditionalTrustRootsHappyPath() async throws {
        // Since we don't have a certificate signed by a trusted root, we are using the
        // additional trust roots to call into Security framework (on Darwin).

        // Create certificates
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false,
            extendedKeyUsages: [.serverAuth]
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes),
            try Data(intermediateCert.serializeAsPEM().derBytes),
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = AsyncVerifier(additionalTrustRoots: trustStore)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "test-leaf")
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                XCTAssertEqual(result, .valid)
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testAdditionalTrustRootsHappyPathIPAddress() async throws {
        // Since we don't have a certificate signed by a trusted root, we are using the
        // additional trust roots to call into Security framework (on Darwin).

        // Create certificates
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false,
            extendedKeyUsages: [.serverAuth]
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes),
            try Data(intermediateCert.serializeAsPEM().derBytes),
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = AsyncVerifier(additionalTrustRoots: trustStore)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "127.0.0.1")
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                XCTAssertEqual(result, .valid)
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testAdditionalTrustRootsBrokenChain() async throws {
        // Since we don't have a certificate signed by a trusted root, we are using the
        // additional trust roots to call into Security framework (on Darwin).

        // Create certificates
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes)
            // <-- intermediate cert is missing
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = AsyncVerifier(additionalTrustRoots: trustStore)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "test-leaf")
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                switch result {
                case .waiting:
                    XCTFail("deliverResult cannot return .waiting.")
                case .invalid:
                    // Yes. The intermediate cert is missing.
                    break
                case .valid:
                    XCTFail("The chain is not complete!")
                }
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testAdditionalTrustRootsWrongHostname() async throws {
        // Since we don't have a certificate signed by a trusted root, we are using the
        // additional trust roots to call into Security framework (on Darwin).

        // Create certificates
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes),
            try Data(intermediateCert.serializeAsPEM().derBytes),
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = AsyncVerifier(additionalTrustRoots: trustStore)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "this-is-the-wrong-name")  // <--
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                switch result {
                case .waiting:
                    XCTFail("deliverResult cannot return .waiting.")
                case .invalid:
                    // Yes. The hostname is wrong.
                    break
                case .valid:
                    XCTFail("Hostname verification should have failed!")
                }
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testAdditionalTrustRootsHostnameVerificationDisabled() async throws {
        // Since we don't have a certificate signed by a trusted root, we are using the
        // additional trust roots to call into Security framework (on Darwin).

        // Create certificates
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false,
            extendedKeyUsages: [.serverAuth]
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes),
            try Data(intermediateCert.serializeAsPEM().derBytes),
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = AsyncVerifier(additionalTrustRoots: trustStore, certificateVerification: .noHostnameVerification)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "this-is-the-wrong-name")  // <--
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                switch result {
                case .waiting:
                    XCTFail("deliverResult cannot return .waiting.")
                case .invalid:
                    XCTFail("The wrong hostname should not have mattered.")
                case .valid:
                    // Expected. The chain is valid.
                    break
                }
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testAdditionalTrustRootsNoHostnameVerificationCertsFail() async throws {
        // Since we don't have a certificate signed by a trusted root, we are using the
        // additional trust roots to call into Security framework (on Darwin).

        // Create certificates
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes)
            // <-- missing
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verifier = AsyncVerifier(additionalTrustRoots: trustStore, certificateVerification: .noHostnameVerification)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "this-is-the-wrong-name")  // <--
                let result = await withCheckedContinuation { continuation in
                    var verificationInfo = VerificationInfo(
                        certificates: CertificateList(type: .x509, entries: clientCertificates),
                        signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
                        signature: Data(signatureData.rawRepresentation),
                        transcriptHash: transcriptHashData
                    )
                    verificationInfo.deliverResult = { result in
                        continuation.resume(returning: result)
                    }
                    let status = callback(verificationInfo)
                    XCTAssertEqual(status, .waiting)
                }
                switch result {
                case .waiting:
                    XCTFail("deliverResult cannot return .waiting.")
                case .invalid:
                    // Expected. The chain is not valid
                    break
                case .valid:
                    XCTFail("The certification verification should fail.")
                }
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    func testAdditionalTrustRootsNoVerification() async throws {
        // Since we don't have a certificate signed by a trusted root, we are using the
        // additional trust roots to call into Security framework (on Darwin).

        // Create certificates
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes)
            // <-- missing
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verificationInfo = VerificationInfo(
            certificates: CertificateList(
                type: .x509,
                entries: clientCertificates
            ),
            signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
            signature: Data(signatureData.rawRepresentation),
            transcriptHash: transcriptHashData
        )

        let verifier = AsyncVerifier(additionalTrustRoots: trustStore, certificateVerification: .noVerification)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "this-is-the-wrong-name")  // <--
                let status = callback(verificationInfo)
                XCTAssertEqual(status, .valid)
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }

    // MARK: - RawPublicKeys

    func testRejectRawPublicKeyHandshakes() async throws {
        let (rootCert, rootKey) = try makeCertificate(commonName: "test-root")
        let (intermediateCert, intermediateKey) = try makeCertificate(
            commonName: "test-intermediate",
            issuer: (rootCert, rootKey),
            canSign: true
        )
        let (leafCert, leafKey) = try makeCertificate(
            commonName: "test-leaf",
            issuer: (intermediateCert, intermediateKey),
            canSign: false
        )

        let trustStore = [rootCert]
        let clientCertificates = [
            try Data(leafCert.serializeAsPEM().derBytes),
            try Data(intermediateCert.serializeAsPEM().derBytes),
        ]

        // Produce signature data.
        let signatureAlgorithm = leafKey.supportedSignatureAlgorithms.first!
        let transcriptHashData = Data(repeating: 14, count: 20)
        let signatureData = try leafKey.sign(bytes: transcriptHashData, signatureAlgorithm: signatureAlgorithm)

        let verificationInfo = VerificationInfo(
            certificates: CertificateList(
                type: .rawPublicKey,  // <-- Unsupported certificate type
                entries: clientCertificates
            ),
            signatureAlgorithm: try! signatureAlgorithm.rfc8446SignatureSchemeValue,
            signature: Data(signatureData.rawRepresentation),
            transcriptHash: transcriptHashData
        )

        let verifier = try AsyncVerifier(trustRoots: trustStore)

        await withTaskGroup(of: TaskTypes.self) { group in
            group.addTask {
                await verifier.run()
                return .authentication
            }
            group.addTask {
                let callback = verifier.makeCallback(hostname: "test-leaf")
                let status = callback(verificationInfo)
                XCTAssertEqual(status, .invalid(reason: "unsupported certificate type"))
                return .testing
            }
            let taskType = await group.next()  // Result from test
            XCTAssertEqual(taskType, .testing)
            group.cancelAll()
        }
    }
}
