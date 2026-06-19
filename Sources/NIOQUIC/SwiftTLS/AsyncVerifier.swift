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
import SwiftASN1
@_spi(SwiftTLSProtocol) import SwiftTLS
import X509

// On Apple platforms we use Security.framework to access the default trust store.
#if canImport(Darwin)
import Dispatch
import Foundation
@preconcurrency import Security
#endif

// This is returned by backend-specific verificaiton callbacks.
enum CertificateVerificationResult {
    case valid
    case invalid(reason: String)
}

@available(anyAppleOS 26, *)
struct CallbackVerificationInput {
    var verificationInfo: VerificationInfo
    var hostname: String
}

/// Asynchronously perform certificate validation during the TLS handshake
@available(anyAppleOS 26, *)
public final class AsyncVerifier: Sendable {
    private let stream: AsyncStream<CallbackVerificationInput>
    private let continuation: AsyncStream<CallbackVerificationInput>.Continuation
    private let trustStore: CertificateStore?
    private let additionalTrustRoots: [Certificate]
    // On Darwin we use Security framework to access the default trust store.
    private let useSecurityFrameworkVerifier: Bool

    private let certificateVerification: CertificateVerification

    private let eventLoop: (any EventLoop)?

    // MARK: Initializers

    /// Convenience initializer for ``init(trustRoots:certificateVerification:eventLoop:)``.
    ///
    /// - Parameter trustRootsPath:
    /// - Parameter certificateVerification: Configure the certificate authentication steps taken during the handshake.
    /// - Parameter eventLoop: When running outside of the TLS handshake event loop, pass in `eventLoop` to continue the handshake on the correct thread.
    public convenience init(
        trustRootsPath: String,
        certificateVerification: CertificateVerification = .fullVerification,
        eventLoop: (any EventLoop)? = nil
    ) throws {
        let trustRoots = try loadCertificates(fromPEMFile: trustRootsPath)
        try self.init(trustRoots: trustRoots, certificateVerification: certificateVerification, eventLoop: eventLoop)
    }

    /// Create a new asynchronous certificate verifier.
    ///
    /// - Parameter certificateVerification: Configure the certificate authentication steps taken during the handshake.
    /// - Parameter eventLoop: When running outside of the TLS handshake event loop, pass in `eventLoop` to continue the handshake on the correct thread.
    public init(
        certificateVerification: CertificateVerification = .fullVerification,
        eventLoop: (any EventLoop)? = nil
    ) {
        let (stream, continuation) = AsyncStream.makeStream(of: CallbackVerificationInput.self)
        self.stream = stream
        self.continuation = continuation
        #if canImport(Darwin)
        self.trustStore = nil
        self.useSecurityFrameworkVerifier = true
        #else
        self.trustStore = CertificateStore.systemTrustRoots
        self.useSecurityFrameworkVerifier = false
        #endif
        self.additionalTrustRoots = []
        self.certificateVerification = certificateVerification
        self.eventLoop = eventLoop
    }

    /// Create a new asynchronous certificate verifier.
    ///
    /// - Parameter trustRoots: Override the system trust roots. This list must not be empty.
    /// - Parameter certificateVerification: Configure the certificate authentication steps taken during the handshake.
    /// - Parameter eventLoop: When running outside of the TLS handshake event loop, pass in `eventLoop` to continue the handshake on the correct thread.
    public init(
        trustRoots: [Certificate],
        certificateVerification: CertificateVerification = .fullVerification,
        eventLoop: (any EventLoop)? = nil
    ) throws {
        if trustRoots.isEmpty {
            throw QUICError.certificatesMissing
        }

        let (stream, continuation) = AsyncStream.makeStream(of: CallbackVerificationInput.self)
        self.stream = stream
        self.continuation = continuation
        self.useSecurityFrameworkVerifier = false
        self.trustStore = CertificateStore(trustRoots)
        self.additionalTrustRoots = []
        self.certificateVerification = certificateVerification
        self.eventLoop = eventLoop
    }

    /// Create a new asynchronous certificate verifier. Please call `run()` in the task (or similar) to create a context for authentication.
    ///
    /// - Parameter additionalTrustRoots: Add trust roots to the system store.
    /// - Parameter certificateVerification: Configure the certificate authentication steps taken during the handshake.
    /// - Parameter eventLoop: When running outside of the TLS handshake event loop, pass in `eventLoop` to continue the handshake on the correct thread.
    init(
        additionalTrustRoots: [Certificate],
        certificateVerification: CertificateVerification = .fullVerification,
        eventLoop: (any EventLoop)? = nil
    ) {
        let (stream, continuation) = AsyncStream.makeStream(of: CallbackVerificationInput.self)
        self.stream = stream
        self.continuation = continuation

        // On Darwin the default trust comes from Security framework. We add additional
        // certificates to extend the trust.
        #if canImport(Darwin)
        self.trustStore = nil
        self.useSecurityFrameworkVerifier = true
        // Store these to add them as sec trust anchors later.
        self.additionalTrustRoots = additionalTrustRoots
        #else
        // non-Darwin
        var trustStore = CertificateStore.systemTrustRoots
        trustStore.append(contentsOf: additionalTrustRoots)
        self.trustStore = trustStore
        self.useSecurityFrameworkVerifier = false
        self.additionalTrustRoots = []
        #endif
        self.certificateVerification = certificateVerification
        self.eventLoop = eventLoop
    }

    // MARK: Run loop

    // Run this to shutdown the run task.
    func terminate() {
        continuation.finish()
    }

    // This will process the async tasks. Makes sure you call this before
    func run() async {
        await withDiscardingTaskGroup { group in
            for await input in self.stream {
                group.addTask {
                    await self.verifyHandshake(input)
                }
            }
        }
    }

    // MARK: Verification

    // This function will run backend-specific verification.
    private func verifyHandshake(_ verificationInput: CallbackVerificationInput) async {
        let rawDeliverResult = verificationInput.verificationInfo.deliverResult
        assert(rawDeliverResult != nil, "Missing deliver result callback.")
        let deliverResult: (@Sendable (SwiftTLS.VerificationResult) -> Void)? = rawDeliverResult.map { deliver in
            if let eventLoop = self.eventLoop {
                return { result in eventLoop.execute { deliver(result) } }
            } else {
                return deliver
            }
        }

        // Check that we have certificates available, and at least a leaf certificate.
        let certificates = try? verificationInput.verificationInfo.certificates.entries.map {
            try Certificate(derEncoded: Array($0))
        }

        guard let certificates, certificates.count > 0, let leaf = certificates.first else {
            deliverResult?(.invalid(reason: "failed to load certificates"))
            return
        }

        // Verify that we support the signature algorithm. For now this just means that swift certificates supports it.
        guard
            let algorithm = Certificate.SignatureAlgorithm(
                rfc8446SignatureSchemeValue: verificationInput.verificationInfo.signatureAlgorithm
            )
        else {
            deliverResult?(.invalid(reason: "Unsupported signature algorithm"))
            return
        }

        let verificationResult: CertificateVerificationResult
        if self.useSecurityFrameworkVerifier {
            #if canImport(Darwin)
            verificationResult = await performSecurityFrameworkValidation(verificationInput)
            #else
            fatalError("Cannot use SecCertificateCreateWithData on non-Darwin platforms")
            #endif
        } else {
            verificationResult = await performSwiftCertificatesValidation(
                hostname: verificationInput.hostname,
                intermediates: certificates.suffix(from: 1),
                leaf: leaf
            )
        }

        switch verificationResult {
        case .invalid(let reason):
            deliverResult?(.invalid(reason: reason))
            return
        case .valid:
            break
        }

        // The public key from the peer's leaf certificate must be able to validate the signature.
        let publicKey = leaf.publicKey
        guard
            publicKey.isValidSignature(
                verificationInput.verificationInfo.signature,
                for: verificationInput.verificationInfo.transcriptHash,
                signatureAlgorithm: algorithm
            )
        else {
            deliverResult?(.invalid(reason: "Failed to verify signature"))
            return
        }

        deliverResult?(.valid)
    }

    private func performSwiftCertificatesValidation<CertificateSequence: Sequence<Certificate>>(
        hostname: String,
        intermediates: CertificateSequence,
        leaf: Certificate
    ) async -> CertificateVerificationResult {
        guard let trustStore = self.trustStore else {
            return .invalid(reason: "Invalid TLS configuration. Trust store must be be available here!")
        }

        // Build verifier. This requires a configured trust store.
        var verifier = Verifier(rootCertificates: trustStore) {
            RFC5280Policy()
            if self.certificateVerification == .fullVerification {
                if hostname.isIPAddress {
                    ServerIdentityPolicy(serverHostname: nil, serverIP: hostname)
                } else {
                    ServerIdentityPolicy(serverHostname: hostname, serverIP: nil)
                }
            }
        }

        // Run certificate validation
        let result = await verifier.validate(leaf: leaf, intermediates: CertificateStore(intermediates))
        switch result {
        case .validCertificate:
            return .valid
        case .couldNotValidate:
            return .invalid(reason: "verification failed")
        }
    }

    #if canImport(Darwin)
    // This should only be used with the default trust store. Custom trust roots or additional trust roots are not supported.
    private func performSecurityFrameworkValidation(
        _ verificationInput: CallbackVerificationInput
    ) async -> CertificateVerificationResult {
        do {
            var trust: SecTrust? = nil
            let hostname = self.certificateVerification == .fullVerification ? verificationInput.hostname : nil
            let policy = SecPolicyCreateSSL(true, hostname as CFString?)

            let peerCertificates = try verificationInput.verificationInfo.certificates.entries.map {
                guard let secCert = SecCertificateCreateWithData(nil, $0 as CFData) else {
                    throw QUICError.unableToLoadCertificates
                }
                return secCert
            }

            // Setup a "trust" for of verification process.
            let result = SecTrustCreateWithCertificates(peerCertificates as CFArray, policy, &trust)
            guard result == errSecSuccess, let actualTrust = trust else {
                return .invalid(reason: "unabled to load certificates")
            }

            // Add additional trust roots if they are configured.
            // If there are additional trust roots then we need to add them to the SecTrust as anchors.
            if !self.additionalTrustRoots.isEmpty {
                let additionalAnchorCertificates: [SecCertificate] = try additionalTrustRoots.map { cert in
                    var coder = DER.Serializer.init()
                    try coder.serialize(cert)
                    let bytes = coder.serializedBytes
                    guard let secCert = SecCertificateCreateWithData(nil, Data(bytes) as CFData) else {
                        throw QUICError.unableToLoadCertificates
                    }
                    return secCert
                }

                if !additionalAnchorCertificates.isEmpty {
                    guard
                        SecTrustSetAnchorCertificates(actualTrust, additionalAnchorCertificates as CFArray)
                            == errSecSuccess
                    else {
                        throw QUICError.unableToLoadCertificates
                    }
                    // To use additional anchors _and_ the built-in ones we must reenable the built-in ones expicitly.
                    guard SecTrustSetAnchorCertificatesOnly(actualTrust, false) == errSecSuccess else {
                        throw QUICError.unableToLoadCertificates
                    }
                }
            }

            // We create a DispatchQueue here to be called back on, as this validation may perform network activity.
            let callbackQueue = DispatchQueue(label: "io.swiftnioquic.tls.validationCallbackQueue")

            // SecTrustEvaluateAsync and its cousin withError require that they are called from the same queue given to
            // them as a parameter. Thus, we async away now.
            return try await withCheckedThrowingContinuation { continuation in
                callbackQueue.async {
                    let result: OSStatus = SecTrustEvaluateAsyncWithError(actualTrust, callbackQueue) {
                        (_, valid, error) in
                        if valid {
                            continuation.resume(returning: .valid)
                        } else {
                            if let error {
                                let message = CFErrorCopyDescription(error) as String
                                let code = CFErrorGetCode(error) as Int
                                continuation.resume(returning: .invalid(reason: "\(message), code: \(code)"))
                            } else {
                                continuation.resume(returning: .invalid(reason: "unknown error"))
                            }
                        }
                    }

                    // The callback is only called when the method returns errSecSuccess. Resume the continuation here.
                    if result != errSecSuccess {
                        continuation.resume(returning: .invalid(reason: "verification failed"))
                    }
                }
            }

        } catch {
            return .invalid(reason: "verification failed: \(error)")
        }
    }
    #endif

    // MARK: Callbacks

    // Create a callback to pass to Swift TLS. Every connection might have a different hostname.
    func makeCallback(
        hostname: String
    ) -> @Sendable (_ verificationInfo: VerificationInfo) -> SwiftTLS.VerificationResult {
        switch self.certificateVerification {
        case .noVerification:
            return { (_: VerificationInfo) -> SwiftTLS.VerificationResult in
                .valid
            }
        case .noHostnameVerification, .fullVerification:
            return { (verificationInfo: VerificationInfo) -> SwiftTLS.VerificationResult in
                guard verificationInfo.certificates.type == .x509 else {
                    return .invalid(reason: "unsupported certificate type")
                }

                self.continuation.yield(.init(verificationInfo: verificationInfo, hostname: hostname))

                return .waiting
            }
        }
    }
}

// MARK: Extensions
