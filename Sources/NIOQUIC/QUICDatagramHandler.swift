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

import Logging
import NIOCore
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

/// The negotiation state of the QUIC datagram extension (RFC 9221) for a connection.
enum QUICDatagramNegotiationState {
    /// The peer's `max_datagram_frame_size` transport parameter is not yet known.
    /// Early writes will be buffered until the peer's advertisement is received.
    case waitingForPeerAdvertisement(earlyWrites: TinyArray<(ByteBuffer, EventLoopPromise<Void>?)>)
    /// The peer advertised a `max_datagram_frame_size` of 0: it does not support datagrams.
    /// Buffered early writes will be discarded (and their promises failed).
    case peerDoesNotAcceptDatagrams
    /// The peer accepts datagrams up to `maximumSize` bytes.
    case peerAcceptsDatagrams(maximumSize: Int)
}

/// `QUICDatagramHandler` is the bridge between SwiftNIOQUIC's pipeline and a QUIC
/// datagram transport (RFC 9221). It is a NIO `ChannelDuplexHandler` that forwards datagrams
/// between the pipeline and whichever `QUICDatagramProtocol`-conforming transport is currently
/// installed as its backend.
@available(anyAppleOS 26, *)
final class QUICDatagramHandler: ChannelDuplexHandler {
    typealias LowerProtocol = OutboundDatagramLinkage

    typealias InboundIn = Any
    typealias InboundOut = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = Any

    private var context: ChannelHandlerContext?

    /// The active datagram backend, or `.none` before one has been installed.
    private var transport: Transport

    private var logPrefix: String
    private let logger: Logger

    private var state: QUICDatagramNegotiationState = .waitingForPeerAdvertisement(earlyWrites: .init())

    init(role: Role, logger: Logger) {
        self.logPrefix = "[\(role.description)][DatagramHandler]"
        self.logger = logger
        self.transport = .none
    }

    func log(_ message: @autoclosure () -> String) {
        #if DEBUG
        self.logger.trace("\(self.logPrefix) \(message())")
        #endif
    }

    /// Installs a `QUICDatagramProtocol` conforming backend for testing, applying the peer's
    /// advertised `max_datagram_frame_size` the same way `setBackend(to:withPeerMaxDatagramFrameSize:)`
    /// does.
    func setTestBackend(to transport: any QUICDatagramProtocol, peerMaxDatagramFrameSize size: Int) {
        transport.setReader(self)
        self.transport = .test(transport)
        self.setPeerMaxDatagramFrameSize(size)
    }

    /// Installs a SwiftNetwork-backed transport.
    ///
    /// - Parameters:
    ///   - transport: The transport to send and receive datagrams through.
    ///   - size: The peer's advertised `max_datagram_frame_size`. `0` means the peer does not
    ///     accept datagrams. Must be >= 0. Buffered and new packets are verified to stay below this limit.
    func setBackend(to transport: QUICDatagramTransport, withPeerMaxDatagramFrameSize size: Int) {
        transport.setReader(self)
        self.transport = .swiftNetwork(transport)
        self.setPeerMaxDatagramFrameSize(size)
    }
}

// MARK: - Transport

@available(anyAppleOS 26, *)
extension QUICDatagramHandler {
    /// The handler's QUIC transport, either the real SwiftNetwork-backed transport
    /// (not yet implemented, statically dispatched) or an existential test conformance.
    enum Transport {
        /// No transport has been installed yet.
        case none
        /// Installed via `setTestBackend(to:withPeerMaxDatagramFrameSize:)`.
        case test(any QUICDatagramProtocol)
        /// Installed via `setBackend(to:withPeerMaxDatagramFrameSize:)`.
        case swiftNetwork(QUICDatagramTransport)
    }
}

@available(anyAppleOS 26, *)
extension QUICDatagramHandler.Transport: QUICDatagramProtocol {
    /// Write a datagram to the transport.
    ///
    /// Note that writes are unreliable.
    ///
    /// - Precondition: A backend must have been installed; traps on `.none`.
    func write(datagram: NIOCore.ByteBuffer) -> Bool {
        switch self {
        case .none:
            fatalError("state violation: cannot perform a write before assigning a transport")
        case .test(let testTransport):
            testTransport.write(datagram: datagram)
        case .swiftNetwork(let swiftNetworkTransport):
            swiftNetworkTransport.write(datagram: datagram)
        }
    }

    /// A no-op on `.none`, so flushing before a backend is installed is safe.
    func flush() {
        switch self {
        case .none:
            break
        case .test(let testTransport):
            testTransport.flush()
        case .swiftNetwork(let swiftNetworkTransport):
            swiftNetworkTransport.flush()
        }
    }

    /// A no-op on `.none`, so closing before a backend is installed is safe.
    func close() {
        switch self {
        case .none:
            break
        case .test(let testTransport):
            testTransport.close()
        case .swiftNetwork(let swiftNetworkTransport):
            swiftNetworkTransport.close()
        }
    }

    /// Set yourself as a reader of incoming datagrams.
    ///
    /// - Precondition: A backend must have been installed; traps on `.none`.
    func setReader(_ reader: any QUICDatagramReaderProtocol) {
        switch self {
        case .none:
            fatalError("state violation: cannot set a reader before assigning a transport")
        case .test(let testTransport):
            testTransport.setReader(reader)
        case .swiftNetwork(let swiftNetworkTransport):
            swiftNetworkTransport.setReader(reader)
        }
    }
}

// MARK: - NIO lifecycle + passthrough

@available(anyAppleOS 26, *)
extension QUICDatagramHandler {
    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        // Fail open writes (peer datagram size was never set).
        if case .waitingForPeerAdvertisement(let earlyWrites) = self.state {
            for (_, promise) in earlyWrites {
                promise?.fail(ChannelError.ioOnClosedChannel)
            }
        }
        self.transport.close()
        self.transport = .none
        self.context = nil
    }

    /// Passes inbound reads from further up the pipeline straight through.
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        context.fireChannelRead(data)
    }
}

// MARK: - Outbound datagrams

@available(anyAppleOS 26, *)
extension QUICDatagramHandler {
    /// Buffers a datagram write, applying the current negotiation state:
    /// held until the peer's advertised size is known, failed if the peer rejects datagrams
    /// or the datagram exceeds the peer's advertised size, otherwise handed to the transport.
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let buffer = self.unwrapOutboundIn(data)
        switch self.state {
        case .waitingForPeerAdvertisement(var earlyWrites):
            // The handshake has not finished and the peer's advertised size is yet unknown.
            earlyWrites.append((buffer, promise))
            self.state = .waitingForPeerAdvertisement(earlyWrites: earlyWrites)
        case .peerDoesNotAcceptDatagrams:
            // Nope. Drop it.
            promise?.fail(QUICError.peerDoesNotAcceptDatagrams)
        case .peerAcceptsDatagrams(let maximumSize):
            // Reject early if the datagram exceeds the peer's advertised limit, then hand it to the
            // transport (which may still fail the write, e.g. a transport-level size constraint).
            //
            // Note: this is an estimate. `max_datagram_frame_size` bounds the whole DATAGRAM frame
            // (type + length + payload), but we compare only the payload length, so a datagram that
            // passes here can still be too large once framed. The transport enforces the real limit.
            guard buffer.readableBytes <= maximumSize else {
                promise?.fail(QUICError.datagramTooLarge)
                return
            }
            if self.transport.write(datagram: buffer) {
                promise?.succeed()
            } else {
                promise?.fail(QUICError.datagramWriteFailed)
            }
        }
    }

    /// Flushes buffered datagrams to the transport, then forwards the flush down the pipeline.
    func flush(context: ChannelHandlerContext) {
        self.transport.flush()
        context.flush()
    }
}

// MARK: - Inbound datagrams

@available(anyAppleOS 26, *)
extension QUICDatagramHandler: QUICDatagramReaderProtocol {
    /// Forwards a datagram received from the transport into the NIO pipeline.
    func read(datagram: NIOCore.ByteBuffer) {
        // The backend is required to check incoming datagram frame
        // sizes and propagate the violation to the connection.
        let buffer = self.wrapInboundOut(datagram)
        self.context?.fireChannelRead(buffer)
    }

    /// Forwards a transport error into the NIO pipeline.
    func error(_ error: any Error) {
        self.context?.fireErrorCaught(error)
    }
}

// MARK: - QUICDatagramProtocol

@available(anyAppleOS 26, *)
extension QUICDatagramHandler {
    /// Applies the peer's advertised `max_datagram_frame_size`, transitioning out of
    /// `.waitingForPeerAdvertisement` and resolving any writes buffered while waiting.
    ///
    /// - Precondition: Must only be called once per connection; the peer only advertises this once.
    private func setPeerMaxDatagramFrameSize(_ size: Int) {
        switch self.state {
        case .waitingForPeerAdvertisement(let earlyWrites):
            // Filter previously buffered messages to account for advertised size
            if size == 0 {
                self.state = .peerDoesNotAcceptDatagrams
                for (_, promise) in earlyWrites {
                    promise?.fail(QUICError.peerDoesNotAcceptDatagrams)
                }
            } else {
                self.state = .peerAcceptsDatagrams(maximumSize: size)
                for (buffer, promise) in earlyWrites {
                    // Same estimated size check as `write` (payload-only, ignores frame overhead;
                    // the transport enforces the real limit).
                    guard buffer.readableBytes <= size else {
                        promise?.fail(QUICError.datagramTooLarge)
                        continue
                    }
                    if self.transport.write(datagram: buffer) {
                        promise?.succeed()
                    } else {
                        promise?.fail(QUICError.datagramWriteFailed)
                    }
                }
                if earlyWrites.count > 0 {
                    self.transport.flush()
                }
            }
        case .peerDoesNotAcceptDatagrams:
            assertionFailure("peer max datagram size must not be updated more than once")
            self.logger.error(
                "\(self.logPrefix) received peer max datagram size more than once",
                metadata: [
                    "knownSize": "0",
                    "newSize": "\(size)",
                ]
            )
        case .peerAcceptsDatagrams(let knownSize):
            assertionFailure("peer max datagram size must not be updated more than once")
            self.logger.error(
                "\(self.logPrefix) received peer max datagram size more than once",
                metadata: [
                    "knownSize": "\(knownSize)",
                    "newSize": "\(size)",
                ]
            )
        }
    }
}
