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

@available(anyAppleOS 26, *)
@_spi(ProtocolProvider)
extension QUICStreamTable: ProtocolInstanceContainer where Consumer: ~Copyable {
    @usableFromInline
    func accessInstance<R, E: Error>(
        at index: Int?,
        _ body: (inout any ProtocolInstance) throws(E) -> R
    ) throws(E) -> R {
        var instance = self.proxy(at: index) as any ProtocolInstance
        return try body(&instance)
    }

    @usableFromInline
    func accessUpper<R, E: Error>(
        at index: Int?,
        _ body: (inout any UpperProtocolHandler) throws(E) -> R
    ) throws(E) -> R {
        var instance = self.proxy(at: index) as any UpperProtocolHandler
        return try body(&instance)
    }

    @usableFromInline
    func accessInboundDataHandler<R, E: Error>(
        at index: Int?,
        _ body: (inout any InboundDataHandler) throws(E) -> R
    ) throws(E) -> R {
        var instance = self.proxy(at: index) as any InboundDataHandler
        return try body(&instance)
    }

    @usableFromInline
    func accessInboundStreamHandler<R, E: Error>(
        at index: Int?,
        _ body: (inout any InboundStreamHandler) throws(E) -> R
    ) throws(E) -> R {
        var instance = self.proxy(at: index) as any InboundStreamHandler
        return try body(&instance)
    }

    /// The proxy for the index the stack passed back.
    private func proxy(at index: Int?) -> QUICStreamProxy<Consumer> {
        QUICStreamProxy(
            table: self,
            // Index can't be `nil`: PIRs are only created by the table with an index, so the unwrap
            // is safe.
            handle: QUICStreamHandle(protocolInstanceReferenceIndex: index!)
        )
    }
}
