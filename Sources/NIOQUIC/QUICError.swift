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

/// Errors produced by this module.
public struct QUICError: Error, Hashable, Sendable {
    enum Code: Hashable {
        case noLocalAddress
        case quicHandlerShuttingDown
        case quicHandlerShutdown
        case tlsConfigurationIncomplete
        case invalidConfiguration
        case invalidConnectionIDLength(Int)
        case invalidStreamTypeForRole
        case quicPacketHeaderDecodingFailed
        case unknownVersion(UInt32)
        case invalidStreamState
        case streamLimit
        case streamStopped
        case streamReset
        case unableToLoadCertificates
        case unableToLoadPrivateKey
        case certificatesMissing
        case certificateNotSuitableForAuthentication
        case failedToAssociateConnectionID
        case failedToRetireConnectionID
        case streamWriteFailed
        case streamHandlerNotFound
        case datagramTooLarge
        case datagramWriteFailed
        case peerDoesNotAcceptDatagrams
        case unknownError(Int)
    }

    let code: Code

    /// Indicates that the channel is not bound to a local address.
    public static let noLocalAddress: Self = .init(code: .noLocalAddress)

    /// Indicates that the ``QUICServerHandler`` is currently shutting down.
    public static let quicHandlerShuttingDown: Self = .init(code: .quicHandlerShuttingDown)

    /// Indicates that the ``QUICServerHandler`` is already shutdown.
    public static let quicHandlerShutdown: Self = .init(code: .quicHandlerShutdown)

    /// Indicates that the TLS configuration is incomplete.
    public static let tlsConfigurationIncomplete: Self = .init(code: .tlsConfigurationIncomplete)

    /// Indicates that the QUIC configuration is invalid and cannot continue.
    public static let invalidConfiguration: Self = .init(code: .invalidConfiguration)

    /// Indicates that a QUIC packet header contained an invalid connection ID length
    public static func invalidConnectionIDLength(_ length: Int) -> Self {
        .init(code: .invalidConnectionIDLength(length))
    }

    /// Indicates that a QUIC stream cannot be opened because its stream type does not match the role.
    public static let invalidStreamTypeForRole: Self = .init(code: .invalidStreamTypeForRole)

    /// Indicates that a QUIC packet header failed decoding
    public static let quicPacketHeaderDecodingFailed: Self = .init(code: .quicPacketHeaderDecodingFailed)

    /// Indicates that a QUIC packet header failed decoding
    public static func unknownVersion(_ version: UInt32) -> Self { .init(code: .unknownVersion(version)) }

    /// Indicates that the stream is in a bad state
    public static let invalidStreamState: Self = .init(code: .invalidStreamState)

    /// Indicates that the stream limit was hit
    public static let streamLimit: Self = .init(code: .streamLimit)

    /// Indicates that the stream stopped
    public static let streamStopped: Self = .init(code: .streamStopped)

    /// Indicates that the stream was reset
    public static let streamReset: Self = .init(code: .streamReset)

    /// Indicates that the certificates could not be loaded from storage
    public static let unableToLoadCertificates: Self = .init(code: .unableToLoadCertificates)

    /// Indicates that the private key could not be loaded from storage
    public static let unableToLoadPrivateKey: Self = .init(code: .unableToLoadPrivateKey)

    /// Indicates that required certificates are missing
    public static let certificatesMissing: Self = .init(code: .certificatesMissing)

    /// Indicates that the certificate has extensions that disallow its use for authentication.
    public static let certificateNotSuitableForAuthentication: Self = .init(
        code: .certificateNotSuitableForAuthentication
    )

    /// Indicates that a new connection ID could not be associated with an existing connection.
    public static let failedToAssociateConnectionID: Self = .init(code: .failedToAssociateConnectionID)

    /// Indicates that a retired connection ID could not be removed from an existing connection.
    public static let failedToRetireConnectionID: Self = .init(code: .failedToRetireConnectionID)

    /// Indicates that no stream handler was found for the given stream ID.
    public static let streamHandlerNotFound: Self = .init(code: .streamHandlerNotFound)

    /// Indicates that the transport rejected a stream write.
    public static let streamWriteFailed: Self = .init(code: .streamWriteFailed)

    /// Indicates that a datagram exceeded the peer's advertised `max_datagram_frame_size`.
    ///
    /// - Warning: This is based on an estimate. The check compares only the datagram payload length
    ///   against the advertised limit and does not account for DATAGRAM frame overhead (frame type
    ///   and length fields), so a datagram that passes the check may still be too large once framed.
    ///   The transport enforces the exact on-the-wire size.
    public static let datagramTooLarge: Self = .init(code: .datagramTooLarge)

    /// Indicates that the transport failed to write a datagram, e.g. because it exceeded a
    /// size constraint enforced by the transport.
    public static let datagramWriteFailed: Self = .init(code: .datagramWriteFailed)

    /// Inidicates that the peer advertised a maximum datagram size of 0.
    public static let peerDoesNotAcceptDatagrams: Self = .init(code: .peerDoesNotAcceptDatagrams)

    /// Indicates an unknown error
    public static func unknownError(_ code: Int) -> Self { .init(code: .unknownError(code)) }
}
