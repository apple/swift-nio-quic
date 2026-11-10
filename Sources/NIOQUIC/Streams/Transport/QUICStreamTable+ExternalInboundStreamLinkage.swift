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
@_spi(Essentials) @_spi(ProtocolProvider) public import SwiftNetwork

/// `QUICStreamTable` is the single `ExternalInboundStreamLinkage` for every stream it hosts.
///
/// `BaseInboundStreamLinkage.init(external:)` reads `identifier` exactly once, at construction,
/// which is how each stream's upper linkage gets its own identifier. Every event after that
/// arrives `for` the identifier of the flow that sent it, so one class can back arbitrarily many
/// streams, resolving each call via `_byFlowIdentifier`.
///
/// Every witness below is `@usableFromInline`: `QUICStreamTable` itself is `@usableFromInline`
/// (referenced from `@inlinable` code elsewhere in this module), and a member satisfying a
/// requirement of a public/`@_spi` protocol from another module must be at least as visible as
/// the conforming type.
@available(anyAppleOS 27, *)
@_spi(ProtocolProvider)
extension QUICStreamTable: ExternalInboundStreamLinkage where Consumer: ~Copyable {
    @usableFromInline
    var identifier: InstanceIdentifier {
        self._pendingAttachIdentifier
    }

    private func handle(for instance: InstanceIdentifier) -> QUICStreamHandle? {
        self._byFlowIdentifier[instance]
    }

    @usableFromInline
    func invokeAttachLowerProtocol(
        _ lowerProtocol: BaseOutboundStreamLinkage,
        remote: Endpoint?,
        local: Endpoint?,
        parameters: Parameters?,
        path: PathProperties?
    ) throws(NetworkError) {
        // An external upper completes the pairing itself, and this second leg is what sets the flow
        // up in QUIC. `identifier` still reports this stream's, so this linkage equals the one the
        // flow was created with.
        try lowerProtocol.invokeAttachUpperProtocol(
            BaseInboundStreamLinkage(external: self),
            remote: remote,
            local: local,
            parameters: parameters,
            path: path
        )
        // Stashed only once the attach succeeded. `open` takes it right after its call to
        // `invokeAttachUpperProtocolToNewFlow`, which returns `Void`.
        self._pendingAttachLinkage = lowerProtocol
    }

    @usableFromInline
    func handleConnectedEvent(for instance: InstanceIdentifier, in eventContext: inout NetworkContext.EventContext) {
        guard let handle = self.handle(for: instance) else { return }
        self.streamConnected(handle, in: &eventContext)
    }

    @usableFromInline
    func handleDisconnectedEvent(
        error: NetworkError?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        guard let handle = self.handle(for: instance) else { return }
        self.streamDisconnected(handle, error: error)
    }

    @usableFromInline
    func handleNetworkProtocolEvent(
        event: NetworkProtocolEvent,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        // Connection-scoped events are delivered on the connection's own instance, not on a
        // stream's, so there is nothing to do here.
    }

    @usableFromInline
    func handleInboundDataAvailableEvent(
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        guard let handle = self.handle(for: instance) else { return }
        self.markReady(handle: handle, events: .readable)
    }

    @usableFromInline
    func handleOutboundRoomAvailableEvent(
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        guard let handle = self.handle(for: instance) else { return }
        self.markReady(handle: handle, events: .writable)
    }

    @usableFromInline
    func handleInboundAbortedEvent(
        error: NetworkError?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        guard let handle = self.handle(for: instance) else { return }
        switch Self.applicationErrorCode(error) {
        case .some(let code):
            self.peerResetStream(handle, code: code)
        case .none:
            ()
        }
    }

    @usableFromInline
    func handleOutboundAbortedEvent(
        error: NetworkError?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        guard let handle = self.handle(for: instance) else { return }
        switch Self.applicationErrorCode(error) {
        case .some(let code):
            self.peerStoppedSending(handle, code: code, in: &eventContext)
        case .none:
            ()
        }
    }

    private static func applicationErrorCode(_ error: NetworkError?) -> QUICApplicationErrorCode? {
        switch error?.quicApplicationError {
        case .some(let code) where code >= 0:
            return QUICApplicationErrorCode(UInt64(code))
        case .some, .none:
            return nil
        }
    }
}
