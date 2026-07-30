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
import NIOQUICHelpers

/// The connection channel's view of its underlying connection.
@available(anyAppleOS 26, *)
protocol QUICConnectionProtocol {
    /// The local address of the connection.
    var localAddress: SocketAddress { get }
    /// The remote address of the peer.
    var remoteAddress: SocketAddress { get }

    /// Hands a datagram to the connection's inbound queue.
    ///
    /// The datagram is enqueued but not processed: call ``receivePacketsComplete()``
    /// once the current read batch is done to have the QUIC stack consume the
    /// queue.
    ///
    /// - Parameter packet: The datagram received from the peer.
    /// - Returns: The number of bytes accepted from `packet`.
    @discardableResult
    func receivePacket(_ packet: ByteBuffer) -> Int

    /// Signals that the current read batch is complete and the queued datagrams
    /// should be processed by the QUIC stack.
    func receivePacketsComplete()

    /// Pops the next finalized datagram the connection wants sent to the peer.
    ///
    /// Call repeatedly until it returns `nil` to drain all pending output.
    ///
    /// - Returns: The next datagram to send, or `nil` if none are queued.
    func nextPacketToSend() -> ByteBuffer?

    /// Initiates a locally-requested close of the connection.
    ///
    /// The `CONNECTION_CLOSE` frame (if any) is finalized synchronously; the
    /// caller should drain output with ``nextPacketToSend()`` afterwards. The
    /// returned action tells the caller whether it initiated the close (and so
    /// must drive `channelInactive`) or the connection was already closing.
    /// Spontaneous (peer- or idle-initiated) closes are *not* reported here —
    /// they arrive via ``QUICConnectionChannel/ConnectionView/connectionClosed(error:)``.
    ///
    /// - Parameters:
    ///   - isApplicationClose: `true` to send an application close, `false` for a
    ///     transport close.
    ///   - errorCode: The error code to send to the peer.
    ///   - reason: The reason phrase to send to the peer.
    /// - Returns: Whether the close was initiated, false otherwise (i.e. already closing).
    func close(
        isApplicationClose: Bool,
        errorCode: Int64,
        reason: String
    ) -> Bool

    /// Buffers an application datagram (RFC 9221) to be sent on the next ``flushDatagrams()``.
    ///
    /// The peer's advertised `max_datagram_frame_size` is checked by the channel *before* this is
    /// called, so the return value is the connection's own accept/reject signal.
    ///
    /// - Parameter datagram: The datagram to send to the peer.
    /// - Returns: `true` if the datagram was buffered, `false` if the connection could not accept
    ///   it (e.g. it has no datagram flow attached).
    func writeDatagram(_ datagram: ByteBuffer) -> Bool

    /// Sends any datagrams buffered since the last ``flushDatagrams()``.
    func flushDatagrams()

    /// Closes all streams on the connection and returns a future per stream that
    /// completes once that stream's teardown finishes.
    ///
    /// - Returns: One future per closed stream.
    func closeAllStreams() -> [EventLoopFuture<Void>]

    /// Fans a quiesce signal (`ChannelShouldQuiesceEvent`) to every stream.
    func quiesceStreams()
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    /// The channel's connection, either the real SwiftNetwork-backed connection
    /// (statically dispatched) or an existential test conformance.
    enum Connection {
        case live(SwiftNetworkQUICConnection)
        case test(any QUICConnectionProtocol)

        func withLiveOnly(
            promise: EventLoopPromise<Void>?,
            execute body: (SwiftNetworkQUICConnection) throws -> Void
        ) {
            switch self {
            case .live(let connection):
                do {
                    try body(connection)
                    promise?.succeed()
                } catch {
                    promise?.fail(error)
                }
            case .test:
                promise?.fail(ChannelError.operationUnsupported)
            }
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel.Connection: QUICConnectionProtocol {
    var localAddress: SocketAddress {
        switch self {
        case .live(let connection):
            connection.localAddress
        case .test(let connection):
            connection.localAddress
        }
    }

    var remoteAddress: SocketAddress {
        switch self {
        case .live(let connection):
            connection.remoteAddress
        case .test(let connection):
            connection.remoteAddress
        }
    }

    @discardableResult
    func receivePacket(_ packet: ByteBuffer) -> Int {
        switch self {
        case .live(let connection):
            connection.receivePacket(packet)
        case .test(let connection):
            connection.receivePacket(packet)
        }
    }

    func receivePacketsComplete() {
        switch self {
        case .live(let connection):
            connection.receivePacketsComplete()
        case .test(let connection):
            connection.receivePacketsComplete()
        }
    }

    func nextPacketToSend() -> ByteBuffer? {
        switch self {
        case .live(let connection):
            connection.nextPacketToSend()
        case .test(let connection):
            connection.nextPacketToSend()
        }
    }

    func close(isApplicationClose: Bool, errorCode: Int64, reason: String) -> Bool {
        switch self {
        case .live(let connection):
            return connection.close(
                isApplicationClose: isApplicationClose,
                errorCode: errorCode,
                reason: reason
            )
        case .test(let connection):
            return connection.close(
                isApplicationClose: isApplicationClose,
                errorCode: errorCode,
                reason: reason
            )
        }
    }

    func closeAllStreams() -> [EventLoopFuture<Void>] {
        switch self {
        case .live(let connection):
            connection.closeAllStreams()
        case .test(let connection):
            connection.closeAllStreams()
        }
    }

    func writeDatagram(_ datagram: ByteBuffer) -> Bool {
        switch self {
        case .live(let connection):
            connection.writeDatagram(datagram)
        case .test(let connection):
            connection.writeDatagram(datagram)
        }
    }

    func flushDatagrams() {
        switch self {
        case .live(let connection):
            connection.flushDatagrams()
        case .test(let connection):
            connection.flushDatagrams()
        }
    }

    func quiesceStreams() {
        switch self {
        case .live(let connection):
            connection.quiesceStreams()
        case .test(let connection):
            connection.quiesceStreams()
        }
    }

    func dropChannelReferences() {
        switch self {
        case .live(let connection):
            connection.dropChannelReferences()
        case .test:
            ()  // Test connections don't retain the channel.
        }
    }
}
