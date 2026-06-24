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

/// A type that generates QUIC connection IDs.
///
/// Implement this protocol to control connection ID generation, for example
/// to embed routing information for load balancers.
///
/// All generated CIDs must have a length equal to ``connectionIDLength``
/// and must be unique across the lifetime of the connection (RFC 9000 Section 5.1).
@available(anyAppleOS 26, *)
public protocol QUICConnectionIDGenerator: Sendable {
    /// The length of connection IDs produced by this generator (0-20).
    ///
    /// All generated CIDs must have this length. Used by the handler
    /// to parse short-header packets (RFC 9000 Section 5.2).
    var connectionIDLength: Int { get }

    /// Generate a connection ID for an outbound connection (client side).
    ///
    /// Each generated CID must be unique across the lifetime of the connection
    /// (RFC 9000 Section 5.1).
    mutating func next() -> QUICConnectionID

    /// Generate a connection ID when accepting an inbound connection (server side).
    ///
    /// The source and destination CIDs from the client's Initial packet are provided
    /// for generators that encode routing information. It is valid to just return the
    /// connection ID suggested by the client and setting the length accordingly.
    ///
    /// Each generated CID must be unique across the lifetime of the connection
    /// (RFC 9000 Section 5.1).
    mutating func next(
        sourceConnectionID: QUICConnectionID,
        destinationConnectionID: QUICConnectionID
    ) -> QUICConnectionID
}

/// Generates random connection IDs using a `RandomNumberGenerator`.
@available(anyAppleOS 26, *)
public struct RandomQUICConnectionIDGenerator: QUICConnectionIDGenerator {
    @usableFromInline
    var randomNumberGenerator: any RandomNumberGenerator & Sendable

    public var connectionIDLength: Int

    public init(
        connectionIDLength: Int = Int(QUICConnectionID.randomIDLength),
        randomNumberGenerator: any RandomNumberGenerator & Sendable = SystemRandomNumberGenerator()
    ) {
        precondition(
            (0...Int(QUICConnectionID.maxLength)).contains(connectionIDLength),
            "QUIC connection IDs are between 0 and 20 bytes long"
        )
        self.connectionIDLength = connectionIDLength
        self.randomNumberGenerator = randomNumberGenerator
    }

    @inlinable
    public mutating func next() -> QUICConnectionID {
        QUICConnectionID.random(length: self.connectionIDLength, using: &self.randomNumberGenerator)
    }

    @inlinable
    public mutating func next(
        sourceConnectionID: QUICConnectionID,
        destinationConnectionID: QUICConnectionID
    ) -> QUICConnectionID {
        self.next()
    }
}
