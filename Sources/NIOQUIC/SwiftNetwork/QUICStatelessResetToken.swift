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

@_spi(ProtocolProvider) import SwiftNetwork

/// A QUIC stateless reset token (RFC 9000 § 10.3.2).
///
/// A token has exactly 16 bytes. This wraps the respective SwiftNetwork type.
@available(anyAppleOS 26, *)
public struct QUICStatelessResetToken: Equatable, Sendable, CustomStringConvertible {

    // Wrapped type from SwiftNetwork.
    var token: SwiftNetwork.QUICStatelessResetToken

    /// Create a QUIC stateless reset token.
    public init(_ token: InlineArray<16, UInt8>) {
        // This only returns nil if the length does not match.
        self.token = .init(token.span)!
    }

    /// Create a QUIC stateless reset token.
    ///
    /// - Precondition: The `span` must have exactly 16 bytes.
    public init?(_ span: Span<UInt8>) {
        guard let token = SwiftNetwork.QUICStatelessResetToken(span) else {
            return nil
        }

        self.token = token
    }

    public var description: String {
        self.token.description
    }
}
