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

private enum StreamInitializer {
    /// Every new stream should be given to SwiftNetwork to initialize.
    case multiplexer(any StreamMultiplexerContinuation)
    /// Every new stream should be initialized using this closure.
    case closure(@Sendable (any Channel) -> EventLoopFuture<Void>)
}

/// A channel handler for QUIC connections.
/// Lives in a connection child channel and multiplexes the streams of a connections into new child channels.
@available(anyAppleOS 26, *)
final class QUICConnectionChannelHandler {
    /// This machine makes sure that we don't fire inactive whilst in the middle of initializing a stream.
    struct StateMachine {
        private enum State {
            /// Normal case.
            case running(streamsBeingInitialized: Int)
            /// We received a channelInactive but did not fire it forward yet.
            case stopping(streamsBeingInitialized: Int)
            /// We have fired a channelInactive.
            case stopped
        }

        private var state: State = .running(streamsBeingInitialized: 0)

        enum ChannelInactiveAction: Hashable {
            case fireInactive
            case none
        }

        mutating func channelInactive() -> ChannelInactiveAction {
            switch self.state {
            case .running(let streamsBeingInitialized):
                guard streamsBeingInitialized == 0 else {
                    self.state = .stopping(streamsBeingInitialized: streamsBeingInitialized)
                    return .none
                }
                self.state = .stopped
                return .fireInactive
            case .stopped, .stopping:
                assertionFailure("Stopped after already stopped")
                return .none
            }
        }

        mutating func startingInitializingAStream() {
            switch self.state {
            case .running(let streamsBeingInitialized):
                self.state = .running(streamsBeingInitialized: streamsBeingInitialized + 1)
            case .stopping(let streamsBeingInitialized):
                self.state = .stopping(streamsBeingInitialized: streamsBeingInitialized + 1)
            case .stopped:
                assertionFailure("Can't init a stream after already stopped")
            }
        }

        enum FinishedAction: Hashable {
            case fireInactive
            case none
        }

        mutating func finishedInitializingAStream() -> FinishedAction {
            switch self.state {
            case .running(let streamsBeingInitialized):
                self.state = .running(streamsBeingInitialized: streamsBeingInitialized - 1)
                return .none
            case .stopping(let streamsBeingInitialized):
                let newValue = streamsBeingInitialized - 1
                guard newValue == 0 else {
                    self.state = .stopping(streamsBeingInitialized: newValue)
                    return .none
                }
                self.state = .stopped
                return .fireInactive
            case .stopped:
                assertionFailure("Can't init a stream after already stopped")
                return .none
            }
        }
    }

    /// The QUIC connection instance.
    private let quicConnection: SwiftNetworkQUICConnection
    /// The event loop of the channel we are added to.
    private let eventLoop: any EventLoop
    /// The logger used by this handler.
    private let logger: Logger
    /// The context of the channel handler.
    private var context: ChannelHandlerContext?
    /// The QUIC metrics.
    private var metrics: QUICMetrics?
    /// What to run for every incoming stream.
    private var inboundStreamInitializer: StreamInitializer?
    private var state: StateMachine = .init()
    /// The role of the connection (client or server)
    private let role: Role

    /// Initializes a new ``QUICConnectionChannelHandler`` and ``QUICConnection``.
    ///
    /// - Parameters:
    ///   - quicConnection: The quic configuration.
    ///   - role: The role of the connection.
    ///   - channel: The channel where the handler resides in.
    ///   - logger: The logger.
    ///   - metrics: The metrics.
    ///   - inboundStreamInitializer: Called for each incoming stream. Returns the object which will be given back from inboundStreams.
    /// - Returns: The ``QUICConnectionChannelHandler`` and the ``QUICConnection``.
    static func makeHandlerAndQUICConnection<Output: Sendable>(
        quicConnection: SwiftNetworkQUICConnection,
        role: Role,
        channel: any Channel,
        logger: Logger,
        metrics: QUICMetrics?,
        inboundStreamInitializer: @escaping @Sendable (any Channel) -> EventLoopFuture<Output>
    ) -> (QUICConnectionChannelHandler, QUICConnection<Output>) {
        let handler = QUICConnectionChannelHandler(
            quicConnection: quicConnection,
            eventLoop: channel.eventLoop,
            role: role,
            logger: logger,
            metrics: metrics,
            inboundStreamInitializer: nil
        )

        let connection = QUICConnection(
            inboundStreamInitializer: inboundStreamInitializer,
            streamCreator: handler.makeStreamCreator(role: role)
        )
        handler.inboundStreamInitializer = .multiplexer(connection)
        return (handler, connection)
    }

    /// Initialises a new server handler.
    ///
    /// - Parameters:
    ///   - quicConnection:
    ///   - eventLoop: The event loop of the channel this handler resides in.
    ///   - logger: A logger used for logging debug and trace information.
    ///   - metrics: The optional ``QUICMetrics`` containing all metrics to be recorded.
    ///   - inboundStreamInitializer: Called for each incoming stream. Returns the object which will be given back from inboundStreams.
    init(
        quicConnection: SwiftNetworkQUICConnection,
        eventLoop: any EventLoop,
        role: Role,
        logger: Logger,
        metrics: QUICMetrics?,
        inboundStreamInitializer: (@Sendable (any Channel) -> EventLoopFuture<Void>)?
    ) {
        self.quicConnection = quicConnection
        self.eventLoop = eventLoop
        self.role = role
        self.logger = logger
        self.metrics = metrics
        self.inboundStreamInitializer = inboundStreamInitializer.map { .closure($0) }
        quicConnection.setStreamChannelCreationHandler(
            self.createStreamChannel(for:channelActivationPromise:streamChannelInitializer:)
        )
    }

    func makeStreamCreator(role: Role) -> QUICStreamCreator {
        QUICStreamCreator(
            eventLoop: self.eventLoop,
            role: role,
            createOutboundStream: NIOLoopBound(self.createOutboundStream, eventLoop: self.eventLoop)
        )
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannelHandler: ChannelInboundHandler, ChannelOutboundHandler {
    typealias InboundIn = QUICConnectionChannelInboundMessage
    typealias InboundOut = Never
    typealias OutboundIn = Never
    typealias OutboundOut = QUICConnectionChannelOutboundMessage

    func handlerAdded(context: ChannelHandlerContext) {
        self.logger.trace("QUICConnectionChannelHandler added to channel pipeline")
        self.context = context
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        self.logger.trace("QUICConnectionChannelHandler removed from channel pipeline")
        self.context = nil
    }

    func channelInactive(context: ChannelHandlerContext) {
        self.logger.trace("QUICConnectionChannelHandlers' parent channel became inactive")
        switch self.state.channelInactive() {
        case .fireInactive:
            self.fireChannelInactive()
        case .none:
            break
        }
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        self.logger.trace(
            "QUICConnectionChannelHandlers' parent channel writability changed",
            metadata: [
                LoggingKeys.channelWritability: "\(context.channel.isWritable)"
            ]
        )
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let readableStreamMessage = self.unwrapInboundIn(data)

        #if DEBUG
        self.logger.trace(
            "QUICConnectionChannelHandler read packet",
            metadata: [
                LoggingKeys.streamID: "\(readableStreamMessage.streamID)"
            ]
        )
        #endif
        guard let stream = self.quicConnection.streamInputHandler(streamID: readableStreamMessage.streamID) else {
            self.logger.info("Could not find stream: \(readableStreamMessage.streamID)")
            return
        }

        switch stream.pipelineStateMachine.startInitializer(channelActive: stream.isStreamChannelActive) {
        case .runInitializer:
            self.logger.trace(
                "QUICConnectionChannelHandler read with newly created id: \(readableStreamMessage.streamID)"
            )

            self.setAutoReadOnStreamChannel(context: context, streamChannel: stream) {
                switch self.inboundStreamInitializer {
                case .multiplexer(let continuation):
                    stream.initialize(multiplexerContinuation: continuation, streamID: readableStreamMessage.streamID)
                case .closure(let initializer):
                    stream.initialize(initializer)
                case .none:
                    stream.initialize()
                }
            }

        case .ignore:
            // Trigger the first read.
            stream.tryToAutoRead()
        }
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        self.logger.trace("QUICConnectionChannelHandler read complete")
        context.fireChannelReadComplete()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        switch event {
        case is ChannelShouldQuiesceEvent:
            let promise = context.eventLoop.makePromise(of: Void.self)
            let boundedContext = NIOLoopBound(context, eventLoop: context.eventLoop)
            promise.futureResult.whenComplete { _ in
                boundedContext.value.close(promise: nil)
            }
            self.eventLoop.assumeIsolated().scheduleTask(
                in: .seconds(30),
                {
                    let _ = self.quicConnection.close(sendApplicationClose: false, errorCode: 0, reason: "")
                    promise.succeed()
                }
            )
            if let event = event as? ChannelShouldQuiesceEvent {
                self.quicConnection.fireUserInboundEventOnAllStreams(event)
            }
        case let event as QUICSCIDAssociatedEvent:
            self.quicConnection.fireUserInboundEventOnAllStreams(event)
        case let event as QUICSCIDRetiredEvent:
            self.quicConnection.fireUserInboundEventOnAllStreams(event)
        case let event as QUICRequestAssociateSCIDEvent:
            self.quicConnection.fireUserInboundEventOnAllStreams(event)
        case let event as QUICRequestRetireDCIDEvent:
            self.quicConnection.fireUserInboundEventOnAllStreams(event)
        default:
            context.fireUserInboundEventTriggered(event)
        }
    }

    func triggerUserOutboundEvent(context: ChannelHandlerContext, event: Any, promise: EventLoopPromise<Void>?) {
        switch event {
        case let event as QUICRequestRetireDCIDEvent:
            let result = Result {
                try self.quicConnection.requestRetirementOfConnectionID(event.dcid)
            }
            promise?.completeWith(result)

        case let event as QUICRequestAssociateSCIDEvent:
            let result = Result {
                try self.quicConnection.requestAssociationOfConnectionID(event.scid)
            }
            promise?.completeWith(result)

        #if DEBUG
        case let event as _QUICForTestingPoisonRetiredSCIDEvent:
            self.quicConnection._forTesting_addRetiredSCID(event.scid)
            promise?.succeed()

        case let event as _QUICForTestingGetActiveSCIDsEvent:
            let scids = self.quicConnection._forTesting_getActiveSCIDs()
            event.result.withLockedValue { $0 = scids }
            promise?.succeed()

        case let event as _QUICForTestingRemoveActiveSCIDEvent:
            self.quicConnection._forTesting_removeFromActiveSCIDs(event.scid)
            promise?.succeed()
        #endif

        default:
            // Forward unhandled events.
            context.triggerUserOutboundEvent(event, promise: promise)
        }
    }

    private func finishedInitializingAStream() {
        switch self.state.finishedInitializingAStream() {
        case .fireInactive:
            self.fireChannelInactive()
        case .none:
            break
        }
    }

    private func fireChannelInactive() {
        switch self.inboundStreamInitializer {
        case .multiplexer(let continuation):
            continuation.finish()
            self.inboundStreamInitializer = nil
        case .closure, .none:
            break
        }
        // Make sure all of the stream channels complete their futures
        // before firing channel inactive here.
        let closeFutures = self.quicConnection.closeAllStreamHandlers()
        if closeFutures.isEmpty {
            // No streams: safe to fire immediately
            self.context?.fireChannelInactive()
            return
        }

        // Only fire channelInactive on the connection after ALL streams have
        // completed _close() and fired fireChannelInactive
        EventLoopFuture
            .andAllComplete(closeFutures, on: self.eventLoop)
            .assumeIsolated()
            .whenComplete { _ in
                self.context?.fireChannelInactive()
            }
    }
}

extension NIOLoopBound {
    func execute<Result: Sendable>(
        _ execute: @Sendable @escaping (Value, EventLoopPromise<Result>) -> Void
    ) -> EventLoopFuture<Result> {
        let promise = self.eventLoop.makePromise(of: Result.self)

        self.eventLoop.execute {
            execute(self.value, promise)
        }

        return promise.futureResult
    }

    func execute(_ execute: @Sendable @escaping (Value) -> Void) {
        self.eventLoop.execute {
            execute(self.value)
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannelHandler {

    /// Initiates creation of a new locally-initiated QUIC stream.
    ///
    /// Asks the underlying QUIC connection to open the stream. The child channel is created later, once Swift QUIC confirms
    /// the real stream ID via ``outboundStreamConnected(streamID:channelActivationPromise:streamChannelInitializer:)``.
    ///
    /// - Parameters:
    ///   - channelActivationPromise: Fulfilled with the new ``Channel`` once the stream is fully set up, or failed on error.
    ///   - streamType: The directionality and initiator of the stream.
    ///   - streamChannelInitializer: Called with the new channel and its confirmed stream ID to configure the channel pipeline.
    func createOutboundStream(
        channelActivationPromise: EventLoopPromise<any Channel>,
        streamType: QUICStreamType,
        streamChannelInitializer: @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
    ) {
        guard let context = self.context else {
            channelActivationPromise.fail(QUICError.noLocalAddress)
            return
        }

        self.eventLoop.assertInEventLoop()

        do {
            try self.quicConnection.addNewOutboundStreamInputHandler(
                streamType: streamType,
                channelActivationPromise: channelActivationPromise,
                connectionChannel: context.channel,
                streamChannelInitializer: streamChannelInitializer,
            )
        } catch {
            channelActivationPromise.fail(error)
            return
        }
        self.metrics?.connectionMetrics?.openStreams?.increment()
    }

    /// Create a new child channel for a QUIC stream initialized by Swift QUIC.
    ///
    /// - Parameters:
    ///   - streamID: The stream ID assigned by Swift QUIC upon connection.
    ///   - channelActivationPromise: Fulfilled with the new ``Channel`` once the stream is fully set up, or failed on error.
    ///   - streamChannelInitializer: Called with the new channel and its confirmed stream ID to configure the channel pipeline.
    func createStreamChannel(
        for streamID: QUICStreamID,
        channelActivationPromise: EventLoopPromise<any Channel>,
        streamChannelInitializer: @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
    ) {
        guard let context = self.context else {
            channelActivationPromise.fail(QUICError.noLocalAddress)
            self.logger.error(
                "outboundStreamConnected called but context is nil, stream ID: \(streamID))"
            )
            return
        }
        self.logger.trace("QUICConnectionChannelHandlercreateStreamChannel streamID: \(streamID)")

        self.eventLoop.assertInEventLoop()
        guard let streamHandler = self.quicConnection.streamInputHandler(streamID: streamID) else {
            channelActivationPromise.fail(QUICError.streamHandlerNotFound)
            return
        }

        self.setAutoReadOnStreamChannel(context: context, streamChannel: streamHandler) {
            streamChannelInitializer(streamHandler, streamID).whenComplete { result in
                switch result {
                case .success:
                    channelActivationPromise.succeed(streamHandler)
                    // Trigger the first read.
                    streamHandler.tryToAutoRead()
                case .failure(let error):
                    channelActivationPromise.fail(error)
                }
            }
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannelHandler {
    /// Obtains the parent channel's `autoRead` option, and sets that `autoRead` value on `streamChannel`. Uses the
    /// `syncOptions` path if the parent channel supports it, and otherwise falls back to the asynchronous API.
    ///
    /// - Important: Closes `streamChannel` if there was an error in (1) obtaining the `autoRead` option from the
    ///   parent, or (2) applying the option to `streamChannel`.
    ///
    /// - Parameters:
    ///   - context: The context of the parent connection channel.
    ///   - streamChannel: The newly created stream channel that should inherit the parent channel's `autoRead` value.
    ///   - onSuccess: Invoked after the `autoRead` option has been successfully applied to `streamChannel`.
    private func setAutoReadOnStreamChannel(
        context: ChannelHandlerContext,
        streamChannel: QUICChannelStreamHandler,
        onSuccess: @escaping () -> Void
    ) {
        let handleResult = { (result: Result<Void, any Error>) in
            switch result {
            case .success:
                onSuccess()

            case .failure(let error):
                self.logger.error(
                    "Failed to inherit autoRead option onto stream channel",
                    metadata: ["error": "\(error)"]
                )
                streamChannel.close(promise: nil)
            }
        }

        if let syncOptions = context.channel.syncOptions {
            let result = self.inheritAutoRead(syncOptions: syncOptions, streamChannel: streamChannel)
            handleResult(result)
        } else {
            // The parent channel does not support sync options. Try to obtain `autoRead` via the async API.
            self.inheritAutoRead(autoReadFuture: context.channel.getOption(.autoRead), streamChannel: streamChannel)
                .assumeIsolated()
                .whenComplete { handleResult($0) }
        }
    }

    /// Synchronously reads the parent channel's `autoRead` option via `syncOptions` and applies it to `streamChannel`.
    private func inheritAutoRead(
        syncOptions: any NIOSynchronousChannelOptions,
        streamChannel: QUICChannelStreamHandler
    ) -> Result<Void, any Error> {
        Result {
            let autoReadValue = try syncOptions.getOption(.autoRead)

            // Force unwrap is safe here. `QUICChannelStreamHandler` always provides `syncOptions`.
            try streamChannel.syncOptions!.setOption(.autoRead, value: autoReadValue)
        }
    }

    /// Asynchronously reads the parent channel's `autoRead` option and applies it to `streamChannel`. Used when the
    /// parent channel does not expose `syncOptions`.
    private func inheritAutoRead(
        autoReadFuture: EventLoopFuture<Bool>,
        streamChannel: QUICChannelStreamHandler
    ) -> EventLoopFuture<Void> {
        autoReadFuture.flatMapThrowing { autoReadValue in
            // Force unwrap is safe here. `QUICChannelStreamHandler` always provides `syncOptions`.
            try streamChannel.syncOptions!.setOption(.autoRead, value: autoReadValue)
        }
    }
}
