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

import NIOCore

/// The datagram transport interface `QUICDatagramHandler` writes to and reads from.
///
/// This exists so `QUICDatagramHandler` does not depend on the SwiftNetwork-backed
/// `QUICDatagramTransport` directly: production code is backed by `QUICDatagramTransport`, while
/// tests can install a a testing `setTestBackend(transport:)`.
@available(anyAppleOS 26, *)
protocol QUICDatagramProtocol {

    /// Buffers `datagram` to be sent on the next `flush()`.
    ///
    /// The peer's advertised `max_datagram_frame_size` is checked by `QUICDatagramHandler`
    /// *before* this is called (that path fails with `QUICError.datagramTooLarge`), so the return
    /// value is the transport's own accept/reject signal.
    ///
    /// - Returns: `true` if the datagram was accepted and buffered for the next `flush()`; `false`
    ///   if the transport could not accept it (a transport-level constraint), which the handler
    ///   surfaces to the caller as `QUICError.datagramWriteFailed`.
    func write(datagram: ByteBuffer) -> Bool

    /// Sends any datagrams buffered since the last `flush()`.
    func flush()

    /// Detaches the underlying flow and clears the current reader.
    func close()

    /// Sets the delegate that receives datagrams and errors from this transport.
    func setReader(_ reader: any QUICDatagramReaderProtocol)

}

/// Receive datagrams and errors from a `QUICDatagramProtocol` conformance.
@available(anyAppleOS 26, *)
protocol QUICDatagramReaderProtocol: AnyObject {
    /// Called once per datagram received from the peer.
    ///
    /// - Precondition: The propagated datagram must adhere to the frame size
    ///     limits advertised by this peer.
    func read(datagram: ByteBuffer)

    /// Called when the underlying transport reports an error.
    func error(_ error: any Error)
}
