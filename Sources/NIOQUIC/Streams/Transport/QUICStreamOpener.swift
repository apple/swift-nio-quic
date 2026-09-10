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

@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

/// What a stream table needs to attach a locally-opened stream to the stack.
@available(anyAppleOS 26, *)
struct QUICStreamOpener {
    /// The connection's stream listener.
    let listener: StreamListenerLinkage

    /// The stack's connection instance.
    let connection: ProtocolInstanceReference

    let context: NetworkContext

    /// Attaches a new outbound flow for a stream.
    ///
    /// - Parameters:
    ///   - isUnidirectional: Whether the stream has a send side only.
    ///   - reference: The reference the stack routes the stream's events back through.
    /// - Returns: The flow the stack attached, which is not yet connected.
    func attach(
        isUnidirectional: Bool,
        from reference: ProtocolInstanceReference
    ) throws(NetworkError) -> OutboundStreamLinkage {
        var parameters = SwiftNetwork.Parameters()
        parameters.context = self.context
        let path = SwiftNetwork.PathProperties(parameters: parameters)

        let options = QUICStreamProtocol.options()
        options.isUnidirectional = isUnidirectional
        options.setProtocolInstance(self.connection)
        parameters.defaultStack.prepend(applicationProtocol: options)

        return try self.listener.invokeAttachUpperStreamProtocolToNewFlow(
            reference,
            remote: nil,
            local: nil,
            parameters: parameters,
            path: path
        )
    }
}
