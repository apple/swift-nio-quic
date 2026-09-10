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

import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

@available(anyAppleOS 26, *)
extension QUICStreamTable where Consumer: ~Copyable {
    /// Opens a stream, attaching it to the stack and connecting it.
    ///
    /// - Parameters:
    ///   - type: The type of stream to open.
    ///   - state: The consumer's state for the new stream.
    /// - Returns: The handle for the new stream.
    /// - Throws: If the type is not one this endpoint may initiate, or the stack refused the flow,
    ///   in which case `state` is destroyed.
    func open(_ type: QUICStreamType, state: consuming Consumer.StreamState) throws -> QUICStreamHandle {
        if type.isClientInitiated && self.role == .server {
            throw QUICError.invalidStreamTypeForRole
        }

        if type.isServerInitiated && self.role == .client {
            throw QUICError.invalidStreamTypeForRole
        }

        guard let opener = self.opener else { throw QUICError.invalidStreamState }

        let handle = self.insertSlot(id: nil, state: consume state)
        let reference = self.reference(for: handle)
        let linkage: OutboundStreamLinkage

        do throws(NetworkError) {
            linkage = try opener.attach(isUnidirectional: type.isUnidirectional, from: reference)
        } catch {
            self.vacateSlot(handle)
            throw QUICError.invalidStreamState
        }

        if let transport = self.transportState(for: handle) {
            transport.pointee.core.attach(reference: reference, linkage: linkage)
        } else {
            preconditionFailure("slot for \(handle) was recycled while its stream was being opened")
        }

        linkage.invokeConnect(reference)
        return handle
    }
}
