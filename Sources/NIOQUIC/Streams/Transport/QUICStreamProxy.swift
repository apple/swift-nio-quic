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

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

@available(anyAppleOS 26, *)
struct QUICStreamProxy<Consumer: QUICStreamConsumer & ~Copyable> {
    typealias LowerProtocol = OutboundStreamLinkage

    let table: QUICStreamTable<Consumer>
    let handle: QUICStreamHandle
}

// MARK: - ProtocolInstance

@available(anyAppleOS 26, *)
extension QUICStreamProxy: ProtocolInstance where Consumer: ~Copyable {
    var context: SwiftNetwork.NetworkContext {
        self.table.context
    }

    var reference: ProtocolInstanceReference {
        self.table.reference(for: self.handle)
    }

    /// The slot's event manager, or the table's spare if the slot has been recycled.
    var eventManager: ProtocolEventManager {
        // The event manager in each `else` branch _should_ be unreachable: a reference is only
        // ever built for a live handle, and all stack callbacks go via the table which drops
        // handles that no longer resolve.
        _read {
            if let transport = self.table.transportState(for: self.handle) {
                yield transport.pointee.eventManager
            } else {
                let eventManager = ProtocolEventManager()
                yield eventManager
            }
        }
        nonmutating _modify {
            if let transport = self.table.transportState(for: self.handle) {
                yield &transport.pointee.eventManager
            } else {
                var eventManager = ProtocolEventManager()
                yield &eventManager
            }
        }
    }
}

// MARK: - InboundStreamHandler

@available(anyAppleOS 26, *)
extension QUICStreamProxy: InboundStreamHandler where Consumer: ~Copyable {
    /// The stack assigned the stream its ID.
    func handleConnectedEvent(_ from: ProtocolInstanceReference) {
        self.table.streamConnected(self.handle)
    }

    /// The stream is finished with, cleanly or not.
    func handleDisconnectedEvent(_ from: ProtocolInstanceReference, error: NetworkError?) {
        self.table.streamDisconnected(self.handle, error: error)
    }

    /// The peer's data can be read.
    func handleInboundDataAvailableEvent(_ from: ProtocolInstanceReference) {
        self.table.markReady(handle: self.handle, events: .readable)
    }

    /// The stack has room for more outbound data.
    func handleOutboundRoomAvailableEvent(_ from: ProtocolInstanceReference) {
        self.table.markReady(handle: self.handle, events: .writable)
    }

    /// The peer sent RESET\_STREAM.
    func handleInboundAbortedEvent(_ from: ProtocolInstanceReference, error: NetworkError?) {
        switch Self.applicationErrorCode(error) {
        case .some(let code):
            self.table.peerResetStream(self.handle, code: code)
        case .none:
            ()  // No application error code: nothing to surface as a reset.
        }
    }

    /// The peer sent STOP\_SENDING.
    func handleOutboundAbortedEvent(_ from: ProtocolInstanceReference, error: NetworkError?) {
        switch Self.applicationErrorCode(error) {
        case .some(let code):
            self.table.peerStoppedSending(self.handle, code: code)
        case .none:
            ()  // No application error code: nothing to surface as a stop-sending.
        }
    }

    func handleNetworkProtocolEvent(_ from: ProtocolInstanceReference, event: NetworkProtocolEvent) {
        // Connection-scoped events are delivered on the connection's own instance, not on a stream's.
        ()
    }

    func attachLowerProtocol(
        _ lowerProtocol: ProtocolInstanceReference,
        remote: Endpoint?,
        local: Endpoint?,
        parameters: Parameters?,
        path: PathProperties?
    ) throws(NetworkError) {
        throw NetworkError.posix(ENOTSUP)
    }

    func attachLowerStreamProtocol(
        _ lowerProtocol: ProtocolInstanceReference,
        remote: Endpoint?,
        local: Endpoint?,
        parameters: Parameters?,
        path: PathProperties?
    ) throws(NetworkError) {
        throw NetworkError.posix(ENOTSUP)
    }

    func attachLowerStreamProtocolToExistingFlow(
        listener: StreamListenerLinkage,
        flowReference: ProtocolInstanceReference
    ) throws(NetworkError) {
        throw NetworkError.posix(ENOTSUP)
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
