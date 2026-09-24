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

/// One network path for a connection.
///
/// The path is the bridge between SwiftNetwork and our code on the network-side. It is attached to
/// the SwiftNetwork `QUICConnection` as the lower datagram protocol of this path and deals with
/// both getting bytes in and out of it.
@available(anyAppleOS 26, *)
final class QUICConnectionPath<Consumer: QUICStreamConsumer & ~Copyable>:
    ProtocolInstanceContainer, OutboundDatagramHandler
{

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

    private enum State {
        /// Not attached to a connection yet.
        case idle
        /// Attached to the related connection.
        case attached(SwiftNetworkQUICConnection<Consumer>.PathView)
        /// Detached from its connection on teardown.
        case detached
    }

    private var state: State = .idle

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

    /// Attaches the path to the connection behind `view`, which it reports to.
    func attach(_ view: SwiftNetworkQUICConnection<Consumer>.PathView) {
        self.state = .attached(view)
    }

    /// Log a message. Disabled in DEBUG builds.
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
        switch self.state {
        case .idle, .attached:
            var packet = packet
            packet.withUnsafeMutableReadableBytesWithStorageManagement2 { buffer, owner in
                self.inputPacketQueue.add(frame: Frame(customBuffer: buffer, owner: owner))
            }
        case .detached:
            // A late packet for a torn-down connection: drop it instead of holding it until deinit.
            return
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

    /// Detaches the path from its connection, breaking the cycle with it. A detached path drops inbound
    /// packets and outbound datagrams.
    func detach() {
        self.state = .detached
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionPath: LowerProtocolHandler where Consumer: ~Copyable {
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

    // Gets the inbound packets queued by `enqueueInboundPacket(_:)`.
    func receiveDatagrams(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        maximumDatagramCount: Int
    ) throws(SwiftNetwork.NetworkError) -> SwiftNetwork.FrameArray? {
        self.drainInboundFrames(maximumDatagramCount: maximumDatagramCount)
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

    // Queues the datagram frames for sending and tells the connection about them.
    func sendDatagrams(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        datagrams: consuming SwiftNetwork.FrameArray
    ) throws(SwiftNetwork.NetworkError) {
        log("received finalize output frames")
        switch self.state {
        case .attached(let connectionView):
            let count = datagrams.count
            self.appendOutboundFrames(datagrams)
            connectionView.outboundDatagramsQueued(on: self, count: count)
        case .idle, .detached:
            self.logger.error("path is not attached: dropping frame array with \(datagrams.count) frames")
            datagrams.finalizeAllFramesAsFailed()
        }
    }
}
