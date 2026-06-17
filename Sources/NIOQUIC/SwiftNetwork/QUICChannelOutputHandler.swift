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

/// `QUICChannelOutputHandler` is the bridge between SwiftNetwork and our code on the network-side.
/// It is registered with the SwiftNetwork `QUICConnectionImplementation` as an output handler.
/// This object deals with both getting bytes in and out of the `QUICConnectionImplementation` on the network-side.
final class QUICChannelOutputHandler: ProtocolInstanceContainer, OutboundDatagramHandler {

    typealias UpperProtocol = InboundDatagramLinkage

    // Private Constant state
    private let role: Role
    private let logger: Logger
    private let remoteAddress: SocketAddress
    private let defaultFrameSize: Int = 1400

    // Internal Mutable state
    internal var logPrefix: String
    internal var reference: ProtocolInstanceReference { ProtocolInstanceReference(custom: self) }
    internal var eventManager = ProtocolEventManager()
    internal var context: SwiftNetwork.NetworkContext

    // Private mutable state
    private var upperProtocol = UpperProtocol(reference: .init())
    private var asLower: OutboundDatagramLinkage { .init(reference: reference) }
    private var inputFramesHandler: ((Int) -> FrameArray?)?
    private var flushOutputFramesHandler: (() -> Void)?
    private var finalizeOutputFramesHandler: ((consuming FrameArray, Role, SocketAddress) -> Void)?

    init(
        role: Role,
        logger: Logger,
        remoteAddress: SocketAddress,
        context: NetworkContext
    ) {
        self.logPrefix = "[\(role.description)][OutputHandler]"
        self.role = role
        self.logger = logger
        self.remoteAddress = remoteAddress
        self.context = context
    }

    func setInputFramesHandler(inputFramesHandler: @escaping (Int) -> FrameArray?) {
        self.inputFramesHandler = inputFramesHandler
    }

    func setFinalizeOutputFramesHandler(
        finalizeOutputFramesHandler: @escaping (consuming FrameArray, Role, SocketAddress) -> Void
    ) {
        self.finalizeOutputFramesHandler = finalizeOutputFramesHandler
    }

    func setFlushOutputFramesHandler(flushOutputFramesHandler: @escaping () -> Void) {
        self.flushOutputFramesHandler = flushOutputFramesHandler
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
        self.fromExternal {
            self.upperProtocol.deliverInboundDataAvailableEvent(self.reference)
        }
    }

    final internal func getMetadata<P>(_ from: ProtocolInstanceReference) -> ProtocolMetadata<P>?
    where P: NetworkProtocol {
        nil
    }
}

extension QUICChannelOutputHandler: LowerProtocolHandler {
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

    // Gets the inbound packets from the inputPacketQueue in SwiftNetworkConnection.
    func receiveDatagrams(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        maximumDatagramCount: Int
    ) throws(SwiftNetwork.NetworkError) -> SwiftNetwork.FrameArray? {
        guard let inputFramesHandler = self.inputFramesHandler else {
            return nil
        }
        return inputFramesHandler(maximumDatagramCount)
    }

    // Allocates storage for a default frame array to be filled with data
    func getDatagramsToSend(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        maximumDatagramCount: Int,
        minimumDatagramSize: Int
    ) throws(SwiftNetwork.NetworkError) -> SwiftNetwork.FrameArray? {
        let frameSize = min(minimumDatagramSize, self.defaultFrameSize)
        var frameArray = FrameArray(capacity: maximumDatagramCount)
        for _ in 0..<maximumDatagramCount {
            frameArray.add(frame: Frame(allocatingCustomFinalizerBufferOfSize: frameSize))
        }
        return frameArray
    }

    // Sends the datagram frames out to the SwiftNetworkConnection object to be queued for writing in writeOutboundData
    func sendDatagrams(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        datagrams: consuming SwiftNetwork.FrameArray
    ) throws(SwiftNetwork.NetworkError) {
        log("received finalize output frames")
        guard let finalizeOutputFramesHandler = self.finalizeOutputFramesHandler else {
            self.logger.error("output frame handler is not set: dropping frame array with \(datagrams.count) frames")
            datagrams.finalizeAllFramesAsFailed()
            return
        }
        finalizeOutputFramesHandler(
            datagrams,
            self.role,
            self.remoteAddress
        )
    }
}
