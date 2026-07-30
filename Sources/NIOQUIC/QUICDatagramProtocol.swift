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

/// The datagram transport interface `SwiftNetworkQUICConnection` writes to and reads from.
///
/// This exists so the connection does not depend on the SwiftNetwork-backed
/// `QUICDatagramTransport` directly: production code is backed by `QUICDatagramTransport`, while
/// tests can install a test double.
@available(anyAppleOS 26, *)
protocol QUICDatagramProtocol {

    /// Buffers `datagram` to be sent on the next `flush()`.
    ///
    /// The peer's advertised `max_datagram_frame_size` is checked by the connection channel
    /// *before* this is called (that path fails with `QUICError.datagramTooLarge`), so the return
    /// value is the transport's own accept/reject signal.
    ///
    /// - Returns: `true` if the datagram was accepted and buffered for the next `flush()`; `false`
    ///   if the transport could not accept it (a transport-level constraint), which the channel
    ///   surfaces to the caller as `QUICError.datagramWriteFailed`.
    func write(datagram: ByteBuffer) -> Bool

    /// Sends any datagrams buffered since the last `flush()`.
    func flush()

    /// Detaches the underlying flow and clears the current reader.
    func close()

    /// Sets the connection which receives datagrams and errors from this transport.
    func setReader(connection: SwiftNetworkQUICConnection)

}

@available(anyAppleOS 26, *)
extension SwiftNetworkQUICConnection {
    /// The connection's datagram transport, either the real SwiftNetwork-backed flow
    /// (statically dispatched) or an existential test conformance.
    enum DatagramTransport {
        case live(QUICDatagramTransport)
        case test(any QUICDatagramProtocol)
    }
}

@available(anyAppleOS 26, *)
extension SwiftNetworkQUICConnection.DatagramTransport: QUICDatagramProtocol {
    func write(datagram: ByteBuffer) -> Bool {
        switch self {
        case .live(let transport):
            transport.write(datagram: datagram)
        case .test(let transport):
            transport.write(datagram: datagram)
        }
    }

    func flush() {
        switch self {
        case .live(let transport):
            transport.flush()
        case .test(let transport):
            transport.flush()
        }
    }

    func close() {
        switch self {
        case .live(let transport):
            transport.close()
        case .test(let transport):
            transport.close()
        }
    }

    func setReader(connection: SwiftNetworkQUICConnection) {
        switch self {
        case .live(let transport):
            transport.setReader(connection: connection)
        case .test(let transport):
            transport.setReader(connection: connection)
        }
    }
}
