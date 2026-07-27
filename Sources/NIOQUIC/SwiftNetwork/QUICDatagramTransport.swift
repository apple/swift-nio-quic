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

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

/// Implementation of `QUICDatagramProtocol` backed by a SwiftNetwork QUIC datagram flow.
///
/// Conforms to SwiftNetwork's `InboundDatagramHandler` to receive inbound datagram events and
/// `ProtocolInstanceContainer` to be addressable as a protocol instance; `QUICDatagramHandler`
/// talks to it only through `QUICDatagramProtocol`.
@available(anyAppleOS 26, *)
final class QUICDatagramTransport: ProtocolInstanceContainer, InboundDatagramHandler {

    // Dispatch to support testing as well as concrete implementation.
    enum Reader {
        case none
        case testing(any QUICDatagramReaderProtocol)
        case datagramhandler(QUICDatagramHandler)

        func error(_ error: any Error) {
            switch self {
            case .none:
                break
            case .testing(let reader):
                reader.error(error)
            case .datagramhandler(let handler):
                handler.error(error)
            }
        }

        func read(datagram: ByteBuffer) {
            switch self {
            case .none:
                break
            case .testing(let reader):
                reader.read(datagram: datagram)
            case .datagramhandler(let handler):
                handler.read(datagram: datagram)
            }
        }
    }

    typealias LowerProtocol = OutboundDatagramLinkage

    internal var reference = ProtocolInstanceReference()
    internal var eventManager = ProtocolEventManager()
    /// The datagram flow this transport sends and receives on. Replaced with a live linkage by
    /// `setFlowLinkage(_:)` once the flow has been attached.
    internal var lower: OutboundDatagramLinkage = .init(reference: .init())
    internal var context: SwiftNetwork.NetworkContext

    internal var logPrefix: String
    internal let logger: Logger

    /// Datagrams written since the last successful `flush()`.
    private var bufferedDatagrams: TinyArray<ByteBuffer> = []

    /// The delegate notified of inbound datagrams and errors, set via `setReader(_:)`.
    private var quicDatagramReader: Reader = .none

    init(role: Role, logger: Logger, context: NetworkContext) {
        self.logPrefix = "[\(role.description)][DatagramHandler]"
        self.logger = logger
        self.context = context
        self.reference = ProtocolInstanceReference(custom: self)
    }

    /// Installs the flow linkage returned by attaching this transport to a new datagram flow.
    func setFlowLinkage(_ linkage: OutboundDatagramLinkage) {
        self.lower = linkage
    }

    func log(_ message: @autoclosure () -> String) {
        #if DEBUG
        self.logger.trace("\(self.logPrefix) \(message())")
        #endif
    }
}

@available(anyAppleOS 26, *)
extension QUICDatagramTransport: QUICDatagramProtocol {

    /// Buffers `datagram` for the next `flush()`, always returning `true`.
    ///
    /// Datagram delivery is unreliable: buffering here always succeeds, but the datagram may still
    /// be dropped later. SwiftNetwork enforces the real on-the-wire size at packetization (the
    /// peer's `max_datagram_frame_size` and the path MTU), so this method does not itself
    /// reject oversized datagrams.
    func write(datagram: NIOCore.ByteBuffer) -> Bool {
        self.bufferedDatagrams.append(datagram)
        return true
    }

    /// Sends all buffered datagrams if the flow is connected.
    func flush() {
        guard self.bufferedDatagrams.count > 0, lower.isConnected else {
            return
        }

        do {
            self.log("writing \(self.bufferedDatagrams.count) QUIC datagrams")
            var frameArray = FrameArray(capacity: self.bufferedDatagrams.count)
            for var datagram in self.bufferedDatagrams {
                datagram.withUnsafeMutableReadableBytesWithStorageManagement2 { buffer, owner in
                    frameArray.add(frame: Frame(customBuffer: buffer, owner: owner))
                }
            }
            try lower.invokeSendDatagrams(self.reference, datagrams: frameArray)
            self.bufferedDatagrams.removeAll(where: { _ in true })
        } catch {
            self.logger.error("QUIC send datagrams: \(error)")
            self.quicDatagramReader.error(error)
        }
    }

    /// Clears the reader and detaches the underlying flow.
    func close() {
        // Log dropped datagrams.
        if !self.bufferedDatagrams.isEmpty {
            self.logger.debug(
                "\(self.logPrefix) dropping \(self.bufferedDatagrams.count) unflushed datagrams on close"
            )
        }

        // Cleanup linkage with SwiftNetwork.
        do throws(NetworkError) {
            try lower.invokeDetach(self.reference)
        } catch {
            self.logger.error("datagram flow detach failed: \(error)")
        }
        self.reference = ProtocolInstanceReference()

        // Remove reference to reader.
        self.quicDatagramReader = .none
    }

    /// Register a reader for datagrams.
    ///
    /// This can only hold one endpoint. Each call overwrite the previously set reader.
    func setReader(_ reader: any QUICDatagramReaderProtocol) {
        self.quicDatagramReader = .testing(reader)
    }
}

@available(anyAppleOS 26, *)
extension QUICDatagramTransport {
    /// Drains all datagrams currently available on the flow and forwards each to the reader.
    /// Does nothing if no reader has been set.
    func handleInboundDataAvailableEvent(_ from: ProtocolInstanceReference) {
        switch self.quicDatagramReader {
        case .none:
            self.log("handle inbound data, but no reader set")
            return
        case .datagramhandler, .testing:
            break
        }

        do throws(NetworkError) {
            while var frames = try lower.invokeReceiveDatagrams(self.reference, maximumDatagramCount: .max),
                frames.count > 0
            {
                frames.iterateMutableFrames { frame in
                    frame.span?.withUnsafeBufferPointer { spanBuffer in
                        var datagram = ByteBuffer()
                        datagram.writeWithUnsafeMutableBytes(
                            minimumWritableBytes: spanBuffer.count
                        ) { buffer in
                            buffer.copyMemory(from: UnsafeRawBufferPointer(spanBuffer))
                            return spanBuffer.count
                        }
                        self.quicDatagramReader.read(datagram: datagram)
                    }
                    frame.finalize(success: true)
                    return true
                }
            }
        } catch {
            self.logger.error("QUIC datagram receive error: \(error)")
        }
    }
}

// Connection events are already handled by the `QUICChannelNewFlowHandler`.
@available(anyAppleOS 26, *)
extension QUICDatagramTransport {
    func attachLowerDatagramProtocol(
        _ lowerProtocol: SwiftNetwork.ProtocolInstanceReference,
        remote: SwiftNetwork.Endpoint?,
        local: SwiftNetwork.Endpoint?,
        parameters: SwiftNetwork.Parameters?,
        path: SwiftNetwork.PathProperties?
    ) throws(SwiftNetwork.NetworkError) {
        throw NetworkError.posix(ENOTSUP)
    }

    func handleOutboundRoomAvailableEvent(_ from: SwiftNetwork.ProtocolInstanceReference) {
        // nop
    }

    func attachLowerProtocol(
        _ lowerProtocol: SwiftNetwork.ProtocolInstanceReference,
        remote: SwiftNetwork.Endpoint?,
        local: SwiftNetwork.Endpoint?,
        parameters: SwiftNetwork.Parameters?,
        path: SwiftNetwork.PathProperties?
    ) throws(SwiftNetwork.NetworkError) {
        throw NetworkError.posix(ENOTSUP)
    }

    func handleConnectedEvent(_ from: SwiftNetwork.ProtocolInstanceReference) {
        log("received connected event")
        // Defer connection-level handling to `SwiftNetworkQUICConnection`.
    }

    func handleDisconnectedEvent(_ from: SwiftNetwork.ProtocolInstanceReference, error: SwiftNetwork.NetworkError?) {
        log("received disconnected event, error \(error.debugDescription)")
        // Defer connection-level handling to `SwiftNetworkQUICConnection`.
    }

    func handleNetworkProtocolEvent(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        event: SwiftNetwork.NetworkProtocolEvent
    ) {
        // nop
    }
}
