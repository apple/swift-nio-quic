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

/// The state of a stream recorded by the transport.
@available(anyAppleOS 26, *)
@usableFromInline
struct QUICStreamTransportState: ~Copyable {
    /// Events which have occurred on the stream since the last visit.
    @usableFromInline
    var events: QUICStreamEvents

    /// Whether the consumer's state for this stream has been created.
    ///
    /// This is the same as checking whether a value in the ``QUICStreamConsumerStates`` exists
    /// for the stream but stored inline as it's a little cheaper.
    @usableFromInline
    var hasState: Bool

    @usableFromInline
    var core: QUICStreamCore

    /// Why the stream closed, as the consumer sees it.
    @usableFromInline
    var closeError: (any Error)?

    /// The application error code sent by the peer in a RESET\_STREAM frame.
    @usableFromInline
    var resetCode: QUICApplicationErrorCode?

    /// The application error code sent by the peer in a STOP\_SENDING frame.
    @usableFromInline
    var stopSendingCode: QUICApplicationErrorCode?

    /// Why the stream closed, as the stack and the peer see it.
    var disconnectError: NetworkError?

    /// Set while a view over this slot is live.
    ///
    /// See the note in 'QUICStreamTable.withStream' for more info.
    @usableFromInline
    var _isBorrowed: Bool

    @inlinable
    mutating func beginBorrow() {
        precondition(!self._isBorrowed, "This stream is already in use.")
        self._isBorrowed = true
    }

    @inlinable
    mutating func endBorrow() {
        self._isBorrowed = false
    }

    @usableFromInline
    init(core: consuming QUICStreamCore) {
        self.events = []
        self.hasState = false
        self.closeError = nil
        self.disconnectError = nil
        self.resetCode = nil
        self.stopSendingCode = nil
        self.core = consume core
        self._isBorrowed = false
    }
}
