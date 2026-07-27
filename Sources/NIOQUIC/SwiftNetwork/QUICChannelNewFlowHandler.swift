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
import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

/// `QUICChannelNewFlowHandler` is responsible for dealing with new SwiftNetwork 'flows' initiated by the other end of the connection.
/// A flow in this case is a new stream of data that is registered with the QUIC stack and is represented here by `QUICChannelStreamHandler`.
/// The `QUICChannelNewFlowHandler` registered with the SwiftNetwork `QUICConnectionImplementation` as a new flow handler.
/// This object deals with creating and linking the objects describing a new flow, it creates a new `QUICChannelStreamHandler`
/// for each new flow, registers it and keeps track of it.
@available(anyAppleOS 26, *)
final class QUICChannelNewFlowHandler: ProtocolInstanceContainer, InboundFlowHandler {

    internal typealias LowerProtocol = StreamListenerLinkage
    typealias UpperStreamHandlerType = QUICChannelStreamHandler

    // Internal mutable state
    internal var eventManager = ProtocolEventManager()
    internal var logPrefix: String
    internal var reference: ProtocolInstanceReference { ProtocolInstanceReference(custom: self) }
    internal var context: SwiftNetwork.NetworkContext

    // Private constant state
    let local: Endpoint
    let remote: Endpoint
    let parameters: Parameters
    let path: PathProperties
    let role: Role
    let logger: Logger
    let remoteAddress: SocketAddress
    let localAddress: SocketAddress
    /// The connection channel which is the parent of inbound stream channels created by this handler.
    /// Set via ``setConnectionChannel(_:)`` once the connection channel has been created.
    private var connectionChannel: (any Channel)?

    // Private mutable state
    // This view is set in the call the `start` and is required for operation. It uses
    // an implicitly unwrapped optionals because Swift's initialization rules prevent
    // passing 'self' method references during init.
    private var connectionView: SwiftNetworkQUICConnection.NewFlowView!
    private var lowerProtocol = LowerProtocol(reference: .init())

    private var datagramListener: DatagramListenerLinkage

    // Internal mutable state
    var keepAliveInterval: Duration?

    internal init?(
        local: Endpoint,
        remote: Endpoint,
        parameters: Parameters,
        path: PathProperties,
        logger: Logger,
        remoteAddress: SocketAddress,
        localAddress: SocketAddress,
        role: Role,
        streamListenerProtocol: StreamListenerLinkage,
        datagramListenerProtocol: DatagramListenerLinkage,
        keepAliveInterval: Duration? = nil
    ) {
        self.local = local
        self.remote = remote
        self.parameters = parameters
        self.path = path
        self.role = role
        self.logger = logger
        self.remoteAddress = remoteAddress
        self.context = parameters.context
        self.keepAliveInterval = keepAliveInterval
        self.logPrefix = "[\(self.role.description)][NewFlowHandler]"
        self.localAddress = localAddress
        self.datagramListener = datagramListenerProtocol
        do throws(NetworkError) {
            self.lowerProtocol = try streamListenerProtocol.invokeAttachNewStreamFlowProtocol(
                self.reference,
                remote: remote,
                local: local,
                parameters: parameters,
                path: path
            )
        } catch {
            return nil
        }
    }

    // Set the parent (connection) channel that inbound streams will use as their parent.
    // Called once the connection child channel has been created.
    func setConnectionChannel(_ channel: any Channel) {
        self.connectionChannel = channel
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

    // Start the new flow handler
    func start(_ view: SwiftNetworkQUICConnection.NewFlowView) {
        log("start")
        self.connectionView = view
        self.fromExternal {
            self.lowerProtocol.invokeConnect(self.reference)
        }
    }

    // Fetch QUIC metadata via the main connection (listener) linkage.
    // `connectionMetadata.activeConnectionIDLimit` is the peer's advertised cap
    // on how many connection IDs we may issue (RFC 9000 §18.2).
    func getConnectionMetadata() -> ProtocolMetadata<QUICProtocol>? {
        self.fromExternal {
            self.lowerProtocol.invokeGetMetadata(self.reference) as? ProtocolMetadata<QUICProtocol>
        }
    }

    // Stop the new flow handler
    func stop(error: NetworkError? = nil) {
        log("stop")
        self.fromExternal {
            self.lowerProtocol.invokeDisconnect(self.reference, error: error)
        }
    }

    // Teardown the new flow handler
    internal func teardown() {
        self.fromExternal {
            do throws(NetworkError) {
                try self.lowerProtocol.invokeDetach(self.reference)
                self.lowerProtocol = .init(reference: .init())
            } catch {
                self.log("Failed to detach lower protocol: \(error)")
            }
        }
    }

    // Received connected event
    func handleConnectedEvent(_ from: SwiftNetwork.ProtocolInstanceReference) {
        log("connected received")
        if self.connectionView.connected() {
            // Only attach if the connection setup succeeded. The state machine
            // in the connection will reject repeated events and connected
            // events while the connection is already closing.
            self.attachDatagramFlow()
        }
    }

    // Received disconnected event
    func handleDisconnectedEvent(_ from: SwiftNetwork.ProtocolInstanceReference, error: SwiftNetwork.NetworkError?) {
        log("received disconnected with error: \(String(describing: error))")
        self.connectionView.disconnected(error: error)
        self.connectionView = nil
    }

    // Receive a new inbound flow to create a QUICChannelStreamHandler from.
    internal func handleNewInboundFlowEvent(
        _ from: ProtocolInstanceReference,
        flowReference: ProtocolInstanceReference,
        flowMetadata: AbstractProtocolMetadata?
    ) {
        log("received new inbound flow")

        guard let connectionChannel = self.connectionChannel else {
            fatalError("connection channel is not available")
        }

        do throws(NetworkError) {
            guard let metadata = flowMetadata as? ProtocolMetadata<QUICProtocol>,
                let inputHandlerStreamID = metadata.streamID
            else {
                logger.error("Could not create new stream handler: invalid metadata")
                return
            }
            let streamHandler = QUICChannelStreamHandler(
                role: self.role,
                parameters: self.parameters,
                streamID: QUICStreamID(rawValue: inputHandlerStreamID),
                logger: self.logger,
                remoteAddress: self.remoteAddress,
                localAddress: self.localAddress,
                connectionChannel: connectionChannel,
                keepAliveInterval: keepAliveInterval
            )

            let linkage = try self.lowerProtocol.invokeAttachUpperStreamProtocolToExistingFlow(
                streamHandler.reference,
                flowReference: flowReference
            )
            streamHandler.swiftNetworkStreamHandle = SwiftNetworkStreamHandle(linkage: linkage)
            streamHandler.setNewFlowMetadata(metadata)
            streamHandler.start(fromNewFlowHandler: true)
            self.connectionView.newInboundStream(streamHandler)

            // For new inbound flows, only set the keep-alive interval once for the connection.
            // That means only the first flow should send the interval into QUICChannelStreamHandler
            self.keepAliveInterval = nil
        } catch {
            self.log("Failed to attach new inbound flow: \(error)")
            return
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICChannelNewFlowHandler: UpperProtocolHandler {
    func handleNetworkProtocolEvent(
        _ from: SwiftNetwork.ProtocolInstanceReference,
        event: SwiftNetwork.NetworkProtocolEvent
    ) {
        self.log("Network protocol event from \(from): \(event)")
        if let quicEvent = event.quicEvent {
            switch quicEvent {
            case .newInboundConnectionID(let connectionID):
                self.log("New inbound connection ID \(connectionID)")
                let nioConnectionID = NIOQUIC.QUICConnectionID(connectionID)
                self.connectionView.associateConnectionID(nioConnectionID)
            case .retiredInboundConnectionID(let connectionID):
                self.log("Retired inbound connection ID \(connectionID)")
                let nioConnectionID = NIOQUIC.QUICConnectionID(connectionID)
                self.connectionView.retireConnectionID(nioConnectionID)
            case .newOutboundConnectionID:
                break
            case .retiredOutboundConnectionID:
                break
            case .earlyDataRejected:
                break
            case .maxStreamsLimitBidirectionalUpdated:
                break
            case .maxStreamsLimitUnidirectionalUpdated:
                break
            case .receivedRemoteTransportParameters:
                // Note: This one is for early data.
                break
            case .remoteBidirectionalStreamsBlocked:
                break
            case .remoteUnidirectionalStreamsBlocked:
                break
            }
        } else {
            // There might be more that we are interested in.
        }
    }

    // Conform to UpperProtocolHandler but the function is unused
    func attachLowerProtocol(
        _ lowerProtocol: SwiftNetwork.ProtocolInstanceReference,
        remote: SwiftNetwork.Endpoint?,
        local: SwiftNetwork.Endpoint?,
        parameters: SwiftNetwork.Parameters?,
        path: SwiftNetwork.PathProperties?
    ) throws(SwiftNetwork.NetworkError) {
        throw NetworkError.posix(EINVAL)
    }

    // Request association of a new connection ID that our peer can use to contact us.
    func requestAssociationOfConnectionID(_ connectionID: QUICConnectionID) {
        self.log("New connection ID: '\(connectionID)'")

        let cid = SwiftNetwork.QUICConnectionID(connectionID)
        let resetToken = QUICStatelessResetToken.init()

        let quicEvent = QUICApplicationEvent.announceNewInboundConnectionID(
            cid,
            statelessResetToken: resetToken
        )
        let event = ApplicationEvent(quicEvent: quicEvent)

        self.fromExternal {
            self.lowerProtocol.invokeApplicationEvent(self.reference, event: event)
        }
    }

    // Request retirement of a connection ID that we are using to address our peer.
    func requestsRetirementOfConnectionID(_ connectionID: QUICConnectionID) {
        self.log("Retiring connection ID: '\(connectionID)'")

        let cid = SwiftNetwork.QUICConnectionID(connectionID)
        let quicEvent = QUICApplicationEvent.retireOutboundConnectionID(cid)
        let event = ApplicationEvent(quicEvent: quicEvent)

        self.fromExternal {
            self.lowerProtocol.invokeApplicationEvent(self.reference, event: event)
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICChannelNewFlowHandler {
    /// Attach the datagram flow once the connection is established.
    ///
    /// Note: This must run after the peer's transport parameters are applied: SwiftNetwork computes the
    /// flow's usable datagram size only at attach time (`updateUsableDatagramFrameSize`), from
    /// `remoteMaxDatagramFrameSize`, which is `0` until the handshake completes. Attaching earlier
    /// freezes the usable size at `0` and datagrams can never be sent.
    private func attachDatagramFlow() {
        guard
            let datagramHandler = try? self.connectionChannel?.pipeline.syncOperations.handler(
                type: QUICDatagramHandler.self
            )
        else {
            self.logger.warning("\(self.logPrefix) No datagram channel handler found in pipeline")
            return
        }

        do {
            let transport = QUICDatagramTransport(
                role: self.role,
                logger: self.logger,
                context: self.context
            )

            var swiftNetworkParameters = SwiftNetwork.Parameters()
            swiftNetworkParameters.context = self.context
            let swiftNetworkPath = SwiftNetwork.PathProperties(parameters: swiftNetworkParameters)
            let quicOptions = QUICStreamProtocol.options()
            guard let perProtocolOptions = quicOptions.perProtocolOptions else {
                throw QUICError.invalidConfiguration
            }
            perProtocolOptions.isDatagram = true
            quicOptions.setProtocolInstance(self.datagramListener.reference)
            swiftNetworkParameters.defaultStack.prepend(applicationProtocol: quicOptions)

            let linkage = try self.datagramListener.invokeAttachUpperDatagramProtocolToNewFlow(
                transport.reference,
                remote: nil,
                local: nil,
                parameters: swiftNetworkParameters,
                path: swiftNetworkPath
            )
            transport.setFlowLinkage(linkage)
            linkage.invokeConnect(transport.reference)

            // TODO: Once SwiftNetwork (a40d5771fc586e75f3534e55178398baa1db2966) is released,
            // replace the hardcoded `UInt16.max` with the peer's advertised `max_datagram_frame_size`, read
            // from the connection metadata via `getConnectionMetadata()` (as with `activeConnectionIDLimit`).
            // A zero says that the peer does not accept datagrams.
            //
            // Better still, use SwiftNetwork's *usable* datagram size, which already subtracts framing
            // overhead and path MTU (`QUICDatagramFlow.updateUsableDatagramFrameSize`) — passing that
            // would make the handler's size check exact instead of the current payload-only estimate.
            datagramHandler.setBackend(to: transport, withPeerMaxDatagramFrameSize: Int(UInt16.max))
        } catch {
            self.logger.error("\(self.logPrefix) Failed to attach QUIC datagram flow: \(error)")
        }
    }
}
