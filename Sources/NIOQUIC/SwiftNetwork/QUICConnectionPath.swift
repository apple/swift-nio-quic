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

/// The connection-side interface of a ``QUICConnectionPath``.
@available(anyAppleOS 26, *)
protocol QUICConnectionPathDelegate: AnyObject {
    /// SwiftNetwork handed `path` a batch of `count` outbound datagrams, which the path queued for sending.
    func outboundDatagramsQueued(on path: QUICConnectionPath, count: Int)
}

/// One network path for a connection: its GSO coalescer and inbound queue.
///
/// The path is the bridge between SwiftNetwork and our code on the network-side. It is attached to
/// the SwiftNetwork `QUICConnectionImplementation` as the lower datagram protocol of this path and
/// deals with both getting bytes in and out of it.
@available(anyAppleOS 26, *)
final class QUICConnectionPath: ProtocolInstanceContainer, OutboundDatagramHandler {

    typealias UpperProtocol = InboundDatagramLinkage

    /// The endpoint information (IP, port) for this path.
    let remoteAddress: SocketAddress
    /// Representation used by SwiftNetwork events.
    let addressEndpoint: SwiftNetwork.AddressEndpoint
    /// QUIC path validation status.
    var isValidated: Bool

    // Private Constant state
    private let logger: Logger
    private let defaultFrameSize: Int = 1400

    // Internal Mutable state
    internal var logPrefix: String
    internal var reference: ProtocolInstanceReference { ProtocolInstanceReference(custom: self) }
    internal var eventManager = ProtocolEventManager()
    internal var context: SwiftNetwork.NetworkContext

    // Private mutable state
    private var upperProtocol = UpperProtocol(reference: .init())
    private var asLower: OutboundDatagramLinkage { .init(reference: reference) }

    /// The connection this path reports to. Held strongly: the delegate owns this path, so it must break
    /// the cycle with `clearDelegate()`. While `nil`, the path is detached: it drops outbound datagrams and
    /// doesn't deliver inbound ones.
    private var delegate: (any QUICConnectionPathDelegate)?

    private let framePool: FramePool
    private var coalescer: GSOCoalescer
    private var inputPacketQueue: FrameArray

    init(
        role: Role,
        remoteAddress: SocketAddress,
        context: NetworkContext,
        framePool: FramePool,
        isValidated: Bool,
        maxSegments: Int,
        bufferPoolCapacity: Int,
        logger: Logger
    ) {
        self.remoteAddress = remoteAddress
        self.addressEndpoint = remoteAddress.toAddressEndpoint()
        self.isValidated = isValidated
        self.logPrefix = "[\(role.description)][Path]"
        self.logger = logger
        self.context = context
        self.framePool = framePool
        self.coalescer = GSOCoalescer(
            remoteAddress: remoteAddress,
            framePool: framePool,
            maxSegments: maxSegments,
            bufferPoolCapacity: bufferPoolCapacity
        )
        self.inputPacketQueue = FrameArray(capacity: 10)
    }

    deinit {
        self.inputPacketQueue.finalizeAllFramesAsFailed()
        self.coalescer.finalizeAllFramesAsFailed()
    }

    /// Sets the connection this path reports to. Each call overwrites the previous delegate.
    func setDelegate(_ delegate: any QUICConnectionPathDelegate) {
        self.delegate = delegate
    }

    /// Local logging function to debug the datapath
    ///
    /// This layer adds the context and fetches the message only if the debug flags are enabled.
    ///
    /// - Parameters:
    ///     - logMessage: The logMessage that is fetched by an autoclosure.  For performance reasons we could gate this behind a flag.
    func log(_ logMessage: @autoclosure () -> String) {
        #if DEBUG
        let message = logMessage()
        self.logger.trace("\(self.logPrefix) \(message)")
        #endif
    }

    // Called from SwiftNetworkQUICConnection to notify the stack that there are inbound packets available.
    // This function is important for getting data into the stack
    func invokeInputAvailable() {
        let reference = self.reference
        reference.fromExternal {
            self.upperProtocol.deliverInboundDataAvailableEvent(reference)
        }
    }

    final internal func getMetadata<P>(_ from: ProtocolInstanceReference) -> ProtocolMetadata<P>?
    where P: NetworkProtocol {
        nil
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

    /// Drops the reference to the delegate, breaking the cycle with it.
    func clearDelegate() {
        self.delegate = nil
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionPath: LowerProtocolHandler {
    func getMetrics(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        requestedNetworkMetric: SwiftNetwork.RequestedNetworkMetrics
    ) -> SwiftNetwork.NetworkMetrics? {
        nil
    }

    internal func disconnect(_ from: SwiftNetwork.ProtocolInstanceReference, error: SwiftNetwork.NetworkError?) {
        log("received disconnect")
        upperProtocol.deliverDisconnectedEvent(reference, error: error)
    }

    func handleApplicationEvent(_ from: SwiftNetwork.ProtocolInstanceReference, event: SwiftNetwork.ApplicationEvent) {
        log("application event: \(event)")
    }

    // Output handler connected
    internal func connect(_ from: ProtocolInstanceReference) {
        log("received connect")
        upperProtocol.deliverConnectedEvent(reference)
    }

    func attachUpperProtocol<Linkage>(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        remote: SwiftNetwork.Endpoint?,
        local: SwiftNetwork.Endpoint?,
        parameters: SwiftNetwork.Parameters?,
        path: SwiftNetwork.PathProperties?
    ) throws(SwiftNetwork.NetworkError) -> Linkage where Linkage: SwiftNetwork.LowerProtocolLinkage {
        guard Linkage.self == OutboundDatagramLinkage.self,
            let lower = asLower as? Linkage
        else {
            throw NetworkError.posix(ENOTSUP)
        }
        log("received attach upper protocol")
        upperProtocol = InboundDatagramLinkage(reference: from)
        return lower
    }

    func detach(_ from: SwiftNetwork.ProtocolInstanceReference) throws(SwiftNetwork.NetworkError) {
        log("received detach")
        // Do not reset the upper linkage here so the last packets can get out the door.
        // For example, when the outputhandler is being removed all of the packets need to be flushed first so that
        // frames such as APPLICATION_CLOSE or CONNECTION_CLOSE make it to the peer.  Resetting the linkage here stop
        // prevents that from happening.
    }

    func attachUpperDatagramProtocol(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        remote: SwiftNetwork.Endpoint?,
        local: SwiftNetwork.Endpoint?,
        parameters: SwiftNetwork.Parameters?,
        path: SwiftNetwork.PathProperties?
    ) throws(SwiftNetwork.NetworkError) -> SwiftNetwork.OutboundDatagramLinkage {
        upperProtocol = InboundDatagramLinkage(reference: from)
        return asLower
    }

    // Gets the inbound packets queued by `enqueueInboundPacket(_:)`. A detached path delivers none.
    func receiveDatagrams(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        maximumDatagramCount: Int
    ) throws(SwiftNetwork.NetworkError) -> SwiftNetwork.FrameArray? {
        if self.delegate == nil {
            return nil
        }
        return self.drainInboundFrames(maximumDatagramCount: maximumDatagramCount)
    }

    // Allocates storage for a default frame array to be filled with data
    func getDatagramsToSend(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        maximumDatagramCount: Int,
        minimumDatagramSize: Int
    ) throws(SwiftNetwork.NetworkError) -> SwiftNetwork.FrameArray? {
        var array = FrameArray(capacity: maximumDatagramCount)

        for _ in 0..<maximumDatagramCount {
            let frame = self.framePool.takeOrCreateFrame(minimumSize: minimumDatagramSize)
            array.add(frame: frame)
        }

        return array
    }

    // Queues the datagram frames for sending and tells the delegate about them.
    func sendDatagrams(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        datagrams: consuming SwiftNetwork.FrameArray
    ) throws(SwiftNetwork.NetworkError) {
        log("received finalize output frames")
        guard let delegate = self.delegate else {
            self.logger.error("path has no delegate: dropping frame array with \(datagrams.count) frames")
            datagrams.finalizeAllFramesAsFailed()
            return
        }
        let count = datagrams.count
        self.appendOutboundFrames(datagrams)
        delegate.outboundDatagramsQueued(on: self, count: count)
    }
}
