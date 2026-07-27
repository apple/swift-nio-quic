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

/// Internal type to abstract away the `Output` type of the multiplexer. This means we are going through an existential
/// in the `QUICHandler` when yielding a new `Channel`. However, this is okay for now otherwise
/// we would need to make the handler generic as well.
@available(anyAppleOS 26, *)
protocol ConnectionMultiplexerContinuation: Sendable {
    /// We have to do a bit of an awkward dance here to carry the `Output` between the initializer and the continuation where
    /// we yield to. That's why we are using `Any` here to avoid making the handler generic.
    func initialize(
        channel: any Channel,
        connectionID: QUICConnectionID,
        quicConnection: SwiftNetworkQUICConnection,
        metrics: QUICMetrics?,
        logger: Logger
    ) -> EventLoopFuture<any Sendable>
    func yield(connection: any Sendable, channel: any Channel)
    func finish()
}

@available(anyAppleOS 26, *)
extension QUICHandler {
    /// A multiplexer for the QUIC connections of a ``QUICHandler``.
    ///
    /// This type allows to iterate the incoming connections and create new connections in the case of being a client.
    public final class ConnectionMultiplexer<Output: Sendable>: ConnectionMultiplexerContinuation {
        /// Channel initializer called for each new inbound stream.
        private let inboundStreamInitializer: @Sendable (any Channel) -> EventLoopFuture<Output>
        /// The inboundConnections' continuation.
        private let inboundConnectionsContinuation: AsyncStream<QUICConnection<Output>>.Continuation
        /// The event loop of the `QUICHandler`.
        let eventLoop: any EventLoop
        /// The role of the underlying `QUICHandler` (client or server).
        private let role: Role
        /// A method to create a new connection.
        internal let _createNewConnection:
            NIOLoopBound<
                (
                    EventLoopPromise<any Channel>, String, SocketAddress,
                    @Sendable @escaping (
                        any Channel, QUICConnectionID, SwiftNetworkQUICConnection, QUICMetrics?, Logger
                    ) ->
                        EventLoopFuture<Void>
                ) throws -> Void
            >

        /// An asynchronous sequence of inbound connections.
        public let inboundConnections: InboundConnections

        init(
            eventLoop: any EventLoop,
            role: Role,
            inboundStreamInitializer: @escaping @Sendable (any Channel) -> EventLoopFuture<Output>,
            createNewConnection: NIOLoopBound<
                (
                    EventLoopPromise<any Channel>, String, SocketAddress,
                    @Sendable @escaping (
                        any Channel, QUICConnectionID, SwiftNetworkQUICConnection, QUICMetrics?, Logger
                    ) ->
                        EventLoopFuture<Void>
                ) throws -> Void
            >
        ) {
            self.eventLoop = eventLoop
            self.role = role
            self.inboundStreamInitializer = inboundStreamInitializer
            self._createNewConnection = createNewConnection
            let (stream, continuation) = AsyncStream<QUICConnection<Output>>.makeStream()
            self.inboundConnections = .init(stream: stream)
            self.inboundConnectionsContinuation = continuation
        }

        func initialize(
            channel: any Channel,
            connectionID: QUICConnectionID,
            quicConnection: SwiftNetworkQUICConnection,
            metrics: QUICMetrics?,
            logger: Logger
        ) -> EventLoopFuture<any Sendable> {
            channel.eventLoop.makeCompletedFuture {
                let (connectionChannelHandler, connection) = QUICConnectionChannelHandler.makeHandlerAndQUICConnection(
                    quicConnection: quicConnection,
                    role: self.role,
                    channel: channel,
                    logger: logger,
                    metrics: metrics,
                    inboundStreamInitializer: self.inboundStreamInitializer
                )
                try channel.pipeline.syncOperations.addHandler(connectionChannelHandler)
                let datagramChannelHandler = QUICDatagramHandler(role: role, logger: logger)
                try channel.pipeline.syncOperations.addHandler(datagramChannelHandler)

                if let connectionDurationTimer = metrics?.connectionCloseMetrics?.connectionDuration {
                    let connectionDurationHandler = ChannelDurationHandler(durationTimer: connectionDurationTimer)
                    try channel.pipeline.syncOperations.addHandler(connectionDurationHandler)
                }

                let errorCatchingHandler = ErrorCatchingHandler(logger: logger)
                try channel.pipeline.syncOperations.addHandler(errorCatchingHandler)
                metrics?.quicConnectionHandlerMetrics?.openConnections?.increment()

                return connection
            }
        }

        func yield(connection: any Sendable, channel: any Channel) {
            self.inboundConnectionsContinuation.yield(connection as! QUICConnection<Output>)
        }

        func finish() {
            self.inboundConnectionsContinuation.finish()
        }

        /// Establishes a new connection to the remote address.
        ///
        /// - Parameters:
        ///   - serverName: The server name of the peer used to verify the peer's certificate.
        ///   - remoteAddress: The socket address of the peer.
        ///   - inboundStreamInitializer: The initializer for new inbound streams on the connection.
        /// - Returns: The newly established  ``QUICConnection``.
        public func createNewConnection<InitializerOutput: Sendable>(
            serverName: String,
            remoteAddress: SocketAddress,
            inboundStreamInitializer:
                @escaping @Sendable (any Channel) -> EventLoopFuture<
                    InitializerOutput
                >
        ) async throws -> QUICConnection<InitializerOutput> {
            let channelPromise = self.eventLoop.makePromise(of: (any Channel).self)
            let outputPromise = self.eventLoop.makePromise(of: QUICConnection<InitializerOutput>.self)
            channelPromise.futureResult.cascadeFailure(to: outputPromise)
            // We have to await both futures here because of two reasons:
            // 1. The channelPromise future is indicating if creating the channel actually succeeded.
            //    We have to await this to know if we actually created a new child channel.
            // 2. While the outputPromise might succeed the channelPromise can still fail for unrelated
            //    reasons.
            let finalResult = channelPromise.futureResult
                .flatMap { _ in
                    outputPromise.futureResult
                }
            self._createNewConnection.execute { createNewConnection in
                do {
                    try createNewConnection(channelPromise, serverName, remoteAddress) {
                        channel,
                        connectionID,
                        quicConnection,
                        metrics,
                        logger in
                        channel.eventLoop.makeCompletedFuture {
                            let (connectionChannelHandler, connection) =
                                QUICConnectionChannelHandler.makeHandlerAndQUICConnection(
                                    quicConnection: quicConnection,
                                    role: self.role,
                                    channel: channel,
                                    logger: logger,
                                    metrics: metrics,
                                    inboundStreamInitializer: inboundStreamInitializer
                                )

                            try channel.pipeline.syncOperations.addHandler(connectionChannelHandler)
                            let datagramChannelHandler = QUICDatagramHandler(role: self.role, logger: logger)
                            try channel.pipeline.syncOperations.addHandler(datagramChannelHandler)

                            if let connectionDurationTimer = metrics?.connectionCloseMetrics?.connectionDuration {
                                let connectionDurationHandler = ChannelDurationHandler(
                                    durationTimer: connectionDurationTimer
                                )
                                try channel.pipeline.syncOperations.addHandler(connectionDurationHandler)
                            }

                            let errorCatchingHandler = ErrorCatchingHandler(logger: logger)
                            try channel.pipeline.syncOperations.addHandler(errorCatchingHandler)
                            metrics?.quicConnectionHandlerMetrics?.openConnections?.increment()

                            return connection
                        }
                        .flatMapErrorThrowing { error in
                            outputPromise.fail(error)
                            throw error
                        }
                        .map { (connection: QUICConnection<InitializerOutput>) in
                            outputPromise.succeed(connection)
                            return ()
                        }
                    }
                } catch {
                    channelPromise.fail(error)
                }
            }

            return try await finalResult.get()
        }

        /// An asynchronous sequence of inbound streams.
        public struct InboundConnections: AsyncSequence, Sendable {
            public typealias Element = QUICConnection<Output>

            private let stream: AsyncStream<QUICConnection<Output>>

            init(stream: AsyncStream<QUICConnection<Output>>) {
                self.stream = stream
            }

            public func makeAsyncIterator() -> AsyncIterator {
                AsyncIterator(iterator: self.stream.makeAsyncIterator())
            }

            public struct AsyncIterator: AsyncIteratorProtocol {
                private var iterator: AsyncStream<QUICConnection<Output>>.Iterator

                init(iterator: AsyncStream<QUICConnection<Output>>.Iterator) {
                    self.iterator = iterator
                }

                public mutating func next() async -> QUICConnection<Output>? {
                    await self.iterator.next()
                }
            }
        }
    }
}

@available(*, unavailable)
extension QUICHandler.ConnectionMultiplexer.InboundConnections.AsyncIterator: Sendable {}
