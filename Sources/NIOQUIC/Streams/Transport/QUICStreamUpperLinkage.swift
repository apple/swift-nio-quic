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

/// The `ExternalInboundStreamLinkage` of one stream in a ``QUICStreamTable``.
///
/// The stack delivers a stream's events to the linkage the stream attached with, `for` the flow
/// that sent them. Each stream attaching with a linkage of its own means each event arrives with
/// the stream's handle, so the table resolves it without a lookup.
///
/// The flow holds this linkage until it detaches, and the stack's queued events hold it until they
/// run, so an event can arrive after its stream left the table: the table drops it then.
@available(anyAppleOS 27, *)
final class QUICStreamUpperLinkage<Consumer: QUICStreamConsumer & ~Copyable> {
    /// Held strongly. The cycle through the stack ends with the stream: the stack lets go of this
    /// linkage once the stream's flow detaches, which closing the stream does, and its queued
    /// events have run.
    let table: QUICStreamTable<Consumer>

    /// The stream's slot in the table.
    let handle: QUICStreamHandle

    /// The identifier the table registered for the stream.
    ///
    /// `BaseInboundStreamLinkage.init(external:)` reads it once, at construction, and the flow
    /// compares linkages by it.
    let identifier: InstanceIdentifier

    init(table: QUICStreamTable<Consumer>, handle: QUICStreamHandle, identifier: InstanceIdentifier) {
        self.table = table
        self.handle = handle
        self.identifier = identifier
    }
}

@available(anyAppleOS 27, *)
extension QUICStreamUpperLinkage: ExternalInboundStreamLinkage where Consumer: ~Copyable {
    func invokeAttachLowerProtocol(
        _ lowerProtocol: BaseOutboundStreamLinkage,
        remote: Endpoint?,
        local: Endpoint?,
        parameters: Parameters?,
        path: PathProperties?
    ) throws(NetworkError) {
        // An external upper completes the pairing itself, and this second leg is what sets the flow
        // up in QUIC. It hands over this linkage, which equals the one the flow was created with.
        try lowerProtocol.invokeAttachUpperProtocol(
            BaseInboundStreamLinkage(external: self),
            remote: remote,
            local: local,
            parameters: parameters,
            path: path
        )
        // Stashed only once the attach succeeded. `open` takes it right after its call to
        // `invokeAttachUpperProtocolToNewFlow`, which returns `Void`.
        self.table._pendingAttachLinkage = lowerProtocol
    }

    func handleConnectedEvent(for instance: InstanceIdentifier, in eventContext: inout NetworkContext.EventContext) {
        self.table.streamConnected(self.handle, in: &eventContext)
    }

    func handleDisconnectedEvent(
        error: NetworkError?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        self.table.streamDisconnected(self.handle, error: error)
    }

    func handleNetworkProtocolEvent(
        event: NetworkProtocolEvent,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        // Connection-scoped events are delivered on the connection's own instance, not on a
        // stream's, so there is nothing to do here.
    }

    func handleInboundDataAvailableEvent(
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        self.table.streamReady(self.handle, events: .readable)
    }

    func handleOutboundRoomAvailableEvent(
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        self.table.streamReady(self.handle, events: .writable)
    }

    func handleInboundAbortedEvent(
        error: NetworkError?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        switch Self.applicationErrorCode(error) {
        case .some(let code):
            self.table.peerResetStream(self.handle, code: code)
        case .none:
            ()
        }
    }

    func handleOutboundAbortedEvent(
        error: NetworkError?,
        for instance: InstanceIdentifier,
        in eventContext: inout NetworkContext.EventContext
    ) {
        switch Self.applicationErrorCode(error) {
        case .some(let code):
            self.table.peerStoppedSending(self.handle, code: code, in: &eventContext)
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
