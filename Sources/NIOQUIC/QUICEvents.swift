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

import NIOConcurrencyHelpers

/// This event informs of new source connection IDs associated with the connection.
/// It only captures connection IDs created after the initial IDs during connection establishment.
@available(anyAppleOS 26, *)
struct QUICSCIDAssociatedEvent: Sendable {

    /// The newly associated source connection ID.
    var scid: QUICConnectionID

    init(scid: QUICConnectionID) {
        self.scid = scid
    }

}

/// This event informs of retired source connection IDs in this connection.
@available(anyAppleOS 26, *)
struct QUICSCIDRetiredEvent: Sendable {

    /// The retired source connection ID.
    var scid: QUICConnectionID

    init(scid: QUICConnectionID) {
        self.scid = scid
    }

}

/// Request assocating a new connection ID. The `scid` will be propagated to our peer as a connection
/// ID that can be used to contact us.
///
/// This event will be handled by the stream state machine.
@available(anyAppleOS 26, *)
struct QUICRequestAssociateSCIDEvent: Sendable {

    /// The source connection ID to associate.
    var scid: QUICConnectionID

    init(scid: QUICConnectionID) {
        self.scid = scid
    }
}

/// Request retireing a connection ID associated with our peer. Tell the QUIC stack
/// (and our peer) that we will no longer use this `dcid` to contact them.
///
/// This event will be handled by the stream state machine.
@available(anyAppleOS 26, *)
struct QUICRequestRetireDCIDEvent: Sendable {

    /// The destination connection ID to retire.
    var dcid: QUICConnectionID

    init(dcid: QUICConnectionID) {
        self.dcid = dcid
    }
}

/// Internal event fired when Swift QUIC produces outbound data outside of the
/// normal state-machine-initiated write path (e.g. from PTO timers).
/// The connection child channel state machine handles this by draining the
/// finalized output queue.
struct QUICDrainOutputEvent {}

/// Test-only event: Injects a connection ID into the retired SCID set.
/// This allows triggering the protocol violation path when the peer reissues this ID.
/// This event will be handled by the stream state machine.
@available(anyAppleOS 26, *)
internal struct _QUICForTestingPoisonRetiredSCIDEvent: Sendable {
    internal var scid: QUICConnectionID

    internal init(scid: QUICConnectionID) {
        self.scid = scid
    }
}

/// Test-only event: Queries the connection's current active source connection IDs.
/// The result is written into the provided locked box. This allows tests to discover
/// the initial SCID (which doesn't generate an association event).
/// This event will be handled by the stream state machine.
@available(anyAppleOS 26, *)
package struct _QUICForTestingGetActiveSCIDsEvent: Sendable {
    package var result: NIOLockedValueBox<[QUICConnectionID]>

    package init(result: NIOLockedValueBox<[QUICConnectionID]>) {
        self.result = result
    }
}

/// Test-only event: Removes a connection ID from `activeSCIDs` without calling the
/// `retireConnectionID` callback (preserving routing in the parent multiplexer).
/// This allows tests to simulate the internal state after retirements while keeping
/// the packet routing path intact. Mirrors the buffering logic: if this is the last
/// active SCID, it is stored in `scidPendingDeletion` instead of removed.
/// This event will be handled by the stream state machine.
@available(anyAppleOS 26, *)
package struct _QUICForTestingRemoveActiveSCIDEvent: Sendable {
    package var scid: QUICConnectionID

    package init(scid: QUICConnectionID) {
        self.scid = scid
    }
}
