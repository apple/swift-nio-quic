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

/// One network path for a connection: its own output handler, GSO coalescer, and inbound queue.
@available(anyAppleOS 26, *)
final class QUICConnectionPath {
    /// The bridge to SwiftNetwork for this path.
    let outputHandler: QUICChannelOutputHandler
    /// The endpoint information (IP, port) for this path.
    let remoteAddress: SocketAddress
    /// Representation used by SwiftNetwork events.
    let addressEndpoint: SwiftNetwork.AddressEndpoint
    /// QUIC path validation status.
    var isValidated: Bool

    private var coalescer: GSOCoalescer
    private var inputPacketQueue: FrameArray
    private let logger: Logger

    init(
        remoteAddress: SocketAddress,
        outputHandler: QUICChannelOutputHandler,
        framePool: FramePool,
        isValidated: Bool,
        maxSegments: Int,
        bufferPoolCapacity: Int,
        logger: Logger
    ) {
        self.remoteAddress = remoteAddress
        self.addressEndpoint = remoteAddress.toAddressEndpoint()
        self.outputHandler = outputHandler
        self.isValidated = isValidated
        self.coalescer = GSOCoalescer(
            remoteAddress: remoteAddress,
            framePool: framePool,
            maxSegments: maxSegments,
            bufferPoolCapacity: bufferPoolCapacity
        )
        self.inputPacketQueue = FrameArray(capacity: 10)
        self.logger = logger
    }

    deinit {
        self.inputPacketQueue.finalizeAllFramesAsFailed()
        self.coalescer.finalizeAllFramesAsFailed()
    }

    // MARK: - Inbound

    var hasQueuedInboundPackets: Bool {
        !self.inputPacketQueue.isEmpty
    }

    func enqueueInboundPacket(_ packet: NIOCore.ByteBuffer) {
        var packet = packet
        packet.withUnsafeMutableReadableBytesWithStorageManagement2 { buffer, owner in
            self.inputPacketQueue.add(frame: Frame(customBuffer: buffer, owner: owner))
        }
    }

    func drainInboundFrames(maximumDatagramCount: Int) -> FrameArray? {
        if self.inputPacketQueue.count == 0 {
            return nil
        }
        return self.inputPacketQueue.drainArray(maximumFrameCount: maximumDatagramCount)
    }

    func finalizeQueuedInboundFramesAsFailed() {
        self.inputPacketQueue.finalizeAllFramesAsFailed()
    }

    // MARK: - Outbound

    var hasQueuedOutboundData: Bool {
        !self.coalescer.isEmpty
    }

    func appendOutboundFrames(_ frames: consuming FrameArray) {
        self.coalescer.append(frames: frames)
    }

    func finalizeQueuedOutboundFramesAsFailed() {
        self.coalescer.finalizeAllFramesAsFailed()
    }

    func nextPacketToSend() -> AddressedEnvelope<ByteBuffer>? {
        self.coalescer.next()
    }

    // MARK: - Teardown

    func clearHandlers() {
        self.outputHandler.clearHandlers()
    }
}
