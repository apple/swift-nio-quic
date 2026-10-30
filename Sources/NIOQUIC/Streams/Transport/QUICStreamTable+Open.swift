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
    /// Attaching a new flow acquires the event context itself, so this must be called with none
    /// held, as it is wherever consumer code runs.
    ///
    /// - Parameters:
    ///   - type: The type of stream to open.
    ///   - state: The consumer's state for the new stream.
    /// - Returns: The handle for the new stream.
    /// - Throws: If the type is not one this endpoint may initiate, the table is closed, the table
    ///   has no opener, or the stack refused the flow, in which case `state` is destroyed.
    func open(_ type: QUICStreamType, state: consuming Consumer.StreamState) throws -> QUICStreamHandle {
        if type.isClientInitiated && self.role == .server {
            throw QUICError.invalidStreamTypeForRole
        }
        if type.isServerInitiated && self.role == .client {
            throw QUICError.invalidStreamTypeForRole
        }

        if self.isClosed { throw QUICError.invalidStreamState }
        guard let opener = self.opener else { throw QUICError.invalidStreamState }

        let handle = self.insertSlot(id: nil, state: consume state)
        let identifier = self.registerIdentifier(for: handle)

        do throws(NetworkError) {
            try self.callouts.callFramework { () throws(NetworkError) in
                try opener.attach(
                    isUnidirectional: type.isUnidirectional,
                    upperProtocol: BaseInboundStreamLinkage(external: self)
                )
            }
        } catch {
            self.vacateSlot(handle)
            throw QUICError.invalidStreamState
        }

        guard let linkage = self._pendingAttachLinkage.take() else {
            preconditionFailure("invokeAttachLowerProtocol did not deliver a linkage for \(handle)")
        }

        self.attach(handle, identifier: identifier, linkage: linkage)

        self.withEventContext { eventContext in
            linkage.invokeConnect(for: identifier, in: &eventContext)
        }
        return handle
    }
}
