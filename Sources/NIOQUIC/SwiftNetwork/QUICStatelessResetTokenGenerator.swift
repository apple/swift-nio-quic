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
import NIOCore
@_spi(ProtocolProvider) import SwiftNetwork

/// Derives the stateless reset tokens for the connection IDs this endpoint issues.
///
/// Tokens are be derived from the key and the connection ID, so they can be
/// regenerated after restarts.
@available(anyAppleOS 26, *)
struct QUICStatelessResetTokenGenerator: Sendable {
    /// The length of a stateless reset token in bytes (RFC 9000 § 10.3).
    static var tokenLength: Int { 16 }

    /// The shortest key accepted. HMAC keys shorter than the hash output weaken the derivation (RFC 2104).
    static var minimumKeyLength: Int { 32 }

    /// Our key input. To send valid stateless resets across restarts the same key must be configured.
    private let key: SymmetricKey

    /// Create a new stateless token generator.
    ///
    /// - Parameter key: The static key to derive tokens from, or `nil` to generate one. A generated
    ///   key is private to this process, so tokens do not survive a restart.
    /// - Precondition: A supplied key is at least ``minimumKeyLength`` bytes long.
    init(key: [UInt8]?) {
        if let key {
            precondition(
                key.count >= Self.minimumKeyLength,
                "The stateless reset key must be at least \(Self.minimumKeyLength) bytes long"
            )
            self.key = SymmetricKey(data: key)
        } else {
            self.key = SymmetricKey(size: .bits256)
        }
    }

    /// The stateless reset token for `connectionID`.
    func token(for connectionID: QUICConnectionID) -> QUICStatelessResetToken {
        // The initializer only rejects tokens which aren't `tokenLength` bytes long.
        // Since the RFC defines the token length, we are safe here.
        QUICStatelessResetToken(self.tokenBytes(for: connectionID))!
    }

    /// Generate the stateless reset token for `connectionID`.
    func tokenBytes(for connectionID: QUICConnectionID) -> [UInt8] {
        let connectionIDBytes = connectionID.withUnsafeBufferPointer { Array($0) }
        let code = HMAC<SHA256>.authenticationCode(for: connectionIDBytes, using: self.key)
        return Array(code.prefix(Self.tokenLength))
    }

    /// Builds a stateless reset datagram for `connectionID`.
    ///
    /// Given the same key and same connection ID this packet will always generate the same token.
    ///
    /// - Parameters:
    ///   - connectionID: The related connection ID.
    ///   - triggeringPacketLength: The size of that packet. The reset is always smaller to
    ///     prohibit amplification.
    ///   - allocator: The allocator for the returned buffer.
    /// - Returns: The datagram, or `nil` if no valid reset fits below `triggeringPacketLength`.
    func statelessResetPacket(
        for connectionID: QUICConnectionID,
        triggeringPacketLength: Int,
        allocator: ByteBufferAllocator
    ) -> ByteBuffer? {
        let bytes = QUICConnectionUtilities.createStatelessResetPacket(
            token: self.token(for: connectionID),
            triggeringPacketLength: triggeringPacketLength
        )
        // No bytes means no reset could be built which is both a plausible QUIC packet (21 bytes
        // at minimum) and smaller than the packet that triggered it.
        guard !bytes.isEmpty else { return nil }

        var buffer = allocator.buffer(capacity: bytes.count)
        buffer.writeBytes(bytes)
        return buffer
    }
}
