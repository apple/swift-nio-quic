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

import ChildChannelMultiplexer
import Logging
import NIOCore
import NIOQUICHelpers

@available(macOS 11, iOS 14.0, tvOS 14.0, watchOS 7.0, macCatalyst 14.0, *)
struct QUICConnectionChildChannelStateMachine: ChildChannelStateMachine {
    enum Task: Hashable, CustomStringConvertible {
        case writeToParent(ByteBuffer)

        var description: String {
            switch self {
            case .writeToParent(let buffer):
                return "writeToParent(\(buffer.readableBytes)"
            }
        }
    }

    /// The lifecycle state of the child channel.
    private enum ChannelLifecycleState {
        /// Channel is initializing, waiting for connection to become ready.
        case initializing
        /// Channel is activated and processing data.
        case activated
        /// Channel close has been requested but not yet completed.
        case closing(promise: EventLoopPromise<Void>?)
        /// Channel is closed (terminal state).
        case closed
    }

    typealias ParentChannelInboundMessage = AddressedEnvelope<ByteBuffer>
    typealias ParentChannelOutboundMessage = AddressedEnvelope<ByteBuffer>
    typealias ChildChannelInboundMessage = QUICConnectionChannelInboundMessage
    typealias ChildChannelOutboundMessage = QUICConnectionChannelOutboundMessage
    typealias ChildChannelID = QUICConnectionID
    typealias ChildChannelIDProperties = Never

    /// The underlying QUIC connection.
    private let quicConnection: SwiftNetworkQUICConnection
    /// The local address of the connection.
    private let localAddress: SocketAddress
    /// The remote address of the connection.
    private let remoteAddress: SocketAddress
    /// The byte buffer allocator of the parent channel.
    private let allocator: ByteBufferAllocator
    /// The current lifecycle state of this child channel.
    private var lifecycleState: ChannelLifecycleState = .initializing

    private let logger: Logger

    // MARK: - Lifecycle State Operations

    /// Transitions to `.closed` and returns the pending close promise, if any.
    private mutating func close() -> EventLoopPromise<Void>? {
        switch self.lifecycleState {
        case .closing(let promise):
            self.lifecycleState = .closed
            self.quicConnection.channel = nil
            return promise
        case .initializing, .activated, .closed:
            self.lifecycleState = .closed
            self.quicConnection.channel = nil
            return nil
        }
    }

    /// Determines the action to take after outbound data has been written.
    private func outboundDataWritten() -> QUICConnectionStateMachine.OutboundDataProcessedAction {
        let isInitializing: Bool
        switch self.lifecycleState {
        case .initializing:
            isInitializing = true
        case .activated, .closing, .closed:
            isInitializing = false
        }
        return self.quicConnection.outboundDataProcessed(isChannelInitializing: isInitializing)
    }

    /// Initializes a new state machine.
    /// - Parameters:
    ///   - quicConnection: The underlying QUIC connection.
    ///   - localAddress: The remote address of the connection.
    ///   - remoteAddress: The remote address of the connection.
    ///   - allocator: The byte buffer allocator of the parent channel.
    ///   - logger: The logger
    init(
        quicConnection: SwiftNetworkQUICConnection,
        localAddress: SocketAddress,
        remoteAddress: SocketAddress,
        allocator: ByteBufferAllocator,
        logger: Logger
    ) {
        self.quicConnection = quicConnection
        self.localAddress = localAddress
        self.remoteAddress = remoteAddress
        self.allocator = allocator
        self.logger = logger
    }

    mutating func childChannelInitializationSucceeded() -> Actions {
        self.logger.trace("QUICConnectionChildChannelStateMachine childChannelInitializationSucceeded")
        // It might be that the connection is already closed, if that happened we have to close our channel right away.
        if self.quicConnection.isTerminating {
            // This is failing the activation completion as well.
            var actions = Actions()
            self.closeCleanly(into: &actions)
            return actions
        }

        var actions = Actions()
        self.writeOutboundData(into: &actions)

        return actions
    }

    mutating func childChannelInitializationFailed(error: any Error) -> Actions {
        self.logger.trace(
            "QUICConnectionChildChannelStateMachine childChannelInitializationFailed",
            metadata: ["error": "\(error)"]
        )
        var actions = Actions()

        self.quicConnection.outboundDrainScheduled()
        defer { self.quicConnection.outboundDrainFinished() }

        _ = self.quicConnection.close(
            sendApplicationClose: false,
            errorCode: QUICTransportErrorCode.internalError.rawValue,
            reason: ""
        )
        self.writeOutboundData(into: &actions)
        self.lifecycleState = .closed
        actions.append(.childChannelEncounterError(error: error, promise: nil))
        return actions
    }

    func childChannelGetOption<Option>(_: Option) throws -> Option.Value where Option: ChannelOption {
        fatalError()
    }

    func childChannelSetOption<Option>(_ option: Option, value: Option.Value) throws where Option: ChannelOption {
        fatalError()
    }

    func childChannelIDGenerated(childChannelID: QUICConnectionID) -> Actions {
        self.logger.trace(
            "QUICConnectionChildChannelStateMachine childChannelIDGenerated",
            metadata: ["childChannelID": "\(childChannelID)"]
        )
        return Actions()
    }

    func childChannelCanWrite() throws {
        self.logger.trace("QUICConnectionChildChannelStateMachine childChannelCanWrite")
        if self.quicConnection.isTerminating {
            throw ChannelError.alreadyClosed
        }
    }

    mutating func childChannelWriteMessage(
        _ message: ChildChannelOutboundMessage,
        promise: EventLoopPromise<Void>?
    ) -> Actions {
        self.logger.trace(
            "QUICConnectionChildChannelStateMachine childChannelWriteMessage",
            metadata: ["message": "\(message)"]
        )
        var actions = Actions()
        self.writeOutboundData(into: &actions)
        return actions
    }

    func childChannelCanRead() -> Bool {
        self.logger.trace("QUICConnectionChildChannelStateMachine childChannelCanRead")
        return true
    }

    func childChannelReadFromParent() -> Actions {
        self.logger.trace("QUICConnectionChildChannelStateMachine childChannelReadFromParent")
        return Actions(.parentChannelRead)
    }

    func childChannelReadMessage(_ message: ChildChannelInboundMessage) -> Actions {
        self.logger.trace(
            "QUICConnectionChildChannelStateMachine childChannelReadMessage",
            metadata: ["message": "\(message)"]
        )
        return Actions(.childChannelFireChannelRead(message: message))
    }

    func childChannelShouldChangeWritability() -> Bool {
        self.logger.trace("QUICConnectionChildChannelStateMachine childChannelShouldChangeWritability")
        return true
    }

    mutating func childChannelTriggerUserOutboundEvent(_ event: Any, promise: EventLoopPromise<Void>?) -> Actions {
        self.logger.trace(
            "QUICConnectionChildChannelStateMachine childChannelTriggerUserOutboundEvent",
            metadata: ["event": "\(event)"]
        )

        if let event = event as? NIOQUICHelpers.QUICCloseConnectionEvent {
            return self.closeConnection(
                promise: promise,
                isApplicationClose: true,
                errorCode: Int64(event.code.rawValue),
                reasonPhrase: event.reasonPhrase ?? ""
            )
        }

        // This event should only be triggered on write paths that are not initiated by the state machine.
        // Swift QUIC produces packets outside of the state machine path, e.g., due to recovery, that should
        // not linger, but be sent out timely.
        if event is QUICDrainOutputEvent {
            var actions = Actions()
            self.drainFinalizedOutputAndHandleLifecycle(into: &actions)
            promise?.succeed(())
            return actions
        }

        guard let promise else {
            return Actions()
        }

        // We are not expecting any outbound events here so we are simply failing the promise
        return Actions(.failPromise(promise, withError: ChannelError.operationUnsupported))
    }

    mutating func childChannelClose(error: any Error, mode: CloseMode, promise: EventLoopPromise<Void>?) -> Actions {
        self.logger.trace(
            "QUICConnectionChildChannelStateMachine childChannelClose",
            metadata: ["error": "\(error)", "mode": "\(mode)"]
        )

        if let channelError = error as? ChannelError {
            switch channelError {
            case .outputClosed, .inputClosed, .eof:
                return self.closeConnection(
                    promise: promise,
                    isApplicationClose: false,
                    errorCode: QUICTransportErrorCode.noError.rawValue,
                    reasonPhrase: ""
                )

            case .connectPending, .connectTimeout, .operationUnsupported, .ioOnClosedChannel, .alreadyClosed,
                .writeMessageTooLarge, .writeHostUnreachable, .unknownLocalAddress, .badMulticastGroupAddressFamily,
                .badInterfaceAddressFamily, .illegalMulticastAddress, .multicastNotSupported,
                .inappropriateOperationForState, .unremovableHandler:
                return self.closeConnection(
                    promise: promise,
                    isApplicationClose: false,
                    errorCode: QUICTransportErrorCode.internalError.rawValue,
                    reasonPhrase: ""
                )
            }
        }

        return self.closeConnection(
            promise: promise,
            isApplicationClose: false,
            errorCode: QUICTransportErrorCode.internalError.rawValue,
            reasonPhrase: ""
        )
    }

    private mutating func closeConnection(
        promise: EventLoopPromise<Void>?,
        isApplicationClose: Bool,
        errorCode: Int64,
        reasonPhrase: String
    ) -> Actions {
        switch self.lifecycleState {
        case .closing(let existingPromise):
            // Already closing - cascade the new promise to the existing one
            existingPromise?.futureResult.cascade(to: promise)
            return .init()
        case .closed:
            // Already closed - succeed immediately
            promise?.succeed(())
            return .init()
        case .initializing, .activated:
            break
        }

        self.quicConnection.outboundDrainScheduled()
        defer { self.quicConnection.outboundDrainFinished() }

        // Request close and handle the result
        let closeAction = self.quicConnection.close(
            sendApplicationClose: isApplicationClose,
            errorCode: errorCode,
            reason: reasonPhrase
        )

        switch closeAction {
        case .closeInitiated:
            self.lifecycleState = .closing(promise: promise)
            var actions = Actions()
            // If the QUIC connection got closed this will append the closed cleanly action
            self.writeOutboundData(into: &actions)
            return actions
        case .alreadyClosed:
            promise?.succeed(())
            return .init()
        }
    }

    mutating func parentChannelInactive() -> Actions {
        self.logger.trace("QUICConnectionChildChannelStateMachine parentChannelInactive")

        self.quicConnection.outboundDrainScheduled()
        defer { self.quicConnection.outboundDrainFinished() }

        _ = self.quicConnection.close(
            sendApplicationClose: false,
            errorCode: QUICTransportErrorCode.noError.rawValue,
            reason: ""
        )

        var actions = Actions()
        self.closeCleanly(into: &actions)
        return actions
    }

    mutating func parentChannelReadMessage(_ message: AddressedEnvelope<ByteBuffer>) -> Actions {
        self.logger.trace(
            "QUICConnectionChildChannelStateMachine parentChannelReadMessage",
            metadata: ["message": "\(message)"]
        )
        var message = message
        do {
            try self.quicConnection.receivePacket(
                &message.data,
                localAddress: self.localAddress,
                remoteAddress: message.remoteAddress
            )
        } catch {
            let promise = self.close()
            return Actions(.childChannelEncounterError(error: error, promise: promise))
        }
        return Actions()
    }

    mutating func parentChannelReadComplete() -> Actions {
        // Flush input queue
        var actions = Actions()
        self.quicConnection.outboundDrainScheduled()
        defer { self.quicConnection.outboundDrainFinished() }
        self.quicConnection.flushInputQueue()
        let newStreamIDs = self.quicConnection.newlyConnectedStreamIDs()
        for streamID in newStreamIDs {
            // We have to inform our connection channel about streams that are newly availabe
            // so that the proper infrastructure can be created for it when read / writing becomes available
            let inboundMessage = QUICConnectionChannelInboundMessage(streamID: streamID)
            actions.append(.childChannelBufferRead(message: inboundMessage))
        }
        self.writeOutboundData(into: &actions)
        return actions
    }

    mutating func parentChannelUserInboundEventTriggered(_ event: Any) -> Actions {
        self.logger.trace(
            "QUICConnectionChildChannelStateMachine parentChannelUserInboundEventTriggered",
            metadata: ["event": "\(event)"]
        )
        switch event {
        case is ChannelShouldQuiesceEvent where self.quicConnection.isTerminating:
            var actions = Actions()
            self.closeCleanly(into: &actions)
            return actions
        default:
            return Actions(.childChannelFireUserInboundEventTriggered(event: event))
        }
    }

    mutating func childChannelExecuteTask(_ task: Task) -> Actions {
        self.logger.trace(
            "QUICConnectionChildChannelStateMachine childChannelExecuteTask",
            metadata: ["task": "\(task)"]
        )
        switch task {
        case .writeToParent(let buffer):
            return Actions(
                .parentChannelWrite(
                    message: AddressedEnvelope(remoteAddress: self.remoteAddress, data: buffer),
                    promise: nil
                ),
                .childChannelFlush
            )
        }
    }

    public func extraChannelIDAssigned(_ extraChannelID: ChildChannelID) -> Actions {
        Actions(.childChannelFireUserInboundEventTriggered(event: QUICSCIDAssociatedEvent(scid: extraChannelID)))
    }

    public func channelIDRetired(_ retiredChannelID: ChildChannelID) -> Actions {
        Actions(.childChannelFireUserInboundEventTriggered(event: QUICSCIDRetiredEvent(scid: retiredChannelID)))
    }
}

@available(macOS 11, iOS 14.0, tvOS 14.0, watchOS 7.0, macCatalyst 14.0, *)
extension QUICConnectionChildChannelStateMachine {
    /// Writes all outbound data from the QUIC connection to the parent channel.
    ///
    /// - Parameters:
    ///   - actions: An inout ``Actions`` collection where the generated actions are appended to.
    private mutating func writeOutboundData(into actions: inout Actions) {
        self.logger.trace("QUICConnectionChildChannelStateMachine writeOutboundData")

        self.quicConnection.outboundDrainScheduled()
        defer { self.quicConnection.outboundDrainFinished() }

        // Then, we drain and write out packets produced by SwiftQUIC.
        self.drainFinalizedOutputAndHandleLifecycle(into: &actions)
    }

    /// Drains the finalized output queue, writing all available QUIC packets to the parent channel
    /// as scheduled tasks. Handles lifecycle transitions (close, activation, etc.).
    private mutating func drainFinalizedOutputAndHandleLifecycle(into actions: inout Actions) {

        // Drain.
        self.logger.trace("QUICConnectionChildChannelStateMachine drainFinalizedOutputAndHandleLifecycle loop")
        var shouldFlush = false
        while let buffer = self.quicConnection.nextOutboundPacket() {
            actions.append(
                .parentChannelWrite(
                    message: AddressedEnvelope(remoteAddress: self.remoteAddress, data: buffer),
                    promise: nil
                )
            )
            shouldFlush = true
        }
        if shouldFlush {
            actions.append(.childChannelFlush)
        }

        // Lifecycle.
        switch self.outboundDataWritten() {
        case .closeCleanly:
            self.logger.trace(
                "QUICConnectionChildChannelStateMachine drainFinalizedOutputAndHandleLifecycle closed connection cleanly"
            )
            actions.append(.childChannelFlush)
            self.closeCleanly(into: &actions)
        case .closeWithError(let error):
            self.logger.trace(
                "QUICConnectionChildChannelStateMachine drainFinalizedOutputAndHandleLifecycle closed connection with error",
                metadata: ["error": "\(error)"]
            )
            self.closeWithError(into: &actions, error: error)
        case .completeActivation:
            self.logger.trace(
                "QUICConnectionChildChannelStateMachine drainFinalizedOutputAndHandleLifecycle activating channel complete"
            )
            self.lifecycleState = .activated
            actions.append(.childChannelCompleteActivation)
        case .noAction:
            self.logger.trace(
                "QUICConnectionChildChannelStateMachine drainFinalizedOutputAndHandleLifecycle no action"
            )
        }
    }

    private mutating func closeCleanly(into actions: inout Actions) {
        let promise = self.close()
        actions.append(.childChannelCloseCleanly(promise: promise))
    }

    private mutating func closeWithError(into actions: inout Actions, error: any Error) {
        let promise = self.close()
        actions.append(.childChannelEncounterError(error: error, promise: promise))
    }
}
