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

import NIOCore
import NIOQUICHelpers

/// A struct representing the ability to make outbound streams.
/// When an inbound connection is received, or an outbound connection is created on ``QUICHandler``, the connection initializer closure will give you an instance of this object to allow you to create outbound streams on that connection.
public struct QUICStreamCreator: Sendable, NIOQUICHelpers.QUICStreamCreator {
    private let eventLoop: any EventLoop
    private let role: Role

    private let _createOutboundStream:
        NIOLoopBound<
            (
                EventLoopPromise<any Channel>, QUICStreamType,
                @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
            ) -> Void
        >

    init(
        eventLoop: any EventLoop,
        role: Role,
        createOutboundStream: NIOLoopBound<
            (
                EventLoopPromise<any Channel>, QUICStreamType,
                @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
            ) -> Void
        >
    ) {
        self.eventLoop = eventLoop
        self._createOutboundStream = createOutboundStream
        self.role = role
    }

    public func assumeIsolated() -> Isolated {
        .init(eventLoop: self.eventLoop, role: self.role, _createOutboundStream: self._createOutboundStream.value)
    }

    /// Creates a new bidirectional stream on the connection.
    ///
    /// - Parameters:
    ///   - streamInitializer: A callback that will be invoked to allow you to configure the channel pipeline for the newly created stream channel.
    /// - Returns: A future which is fulfilled once the stream is active.
    public func createBidirectionalStream<InitializerOutput>(
        streamInitializer:
            @escaping @Sendable (NIOQUICHelpers.QUICStreamInitializerParameters) ->
            NIOCore.EventLoopFuture<InitializerOutput>
    ) -> NIOCore.EventLoopFuture<InitializerOutput> where InitializerOutput: Sendable {
        self.eventLoop.flatSubmit {
            self.assumeIsolated().createBidirectionalStream(streamInitializer: streamInitializer)
        }
    }

    /// Creates a new unidirectional stream on the connection.
    ///
    /// - Parameters:
    ///   - streamInitializer: A callback that will be invoked to allow you to configure the channel pipeline for the newly created stream channel.
    /// - Returns: A future which is fulfilled once the stream is active.
    public func createUnidirectionalStream<InitializerOutput>(
        streamInitializer:
            @escaping @Sendable (NIOQUICHelpers.QUICStreamInitializerParameters) ->
            NIOCore.EventLoopFuture<InitializerOutput>
    ) -> NIOCore.EventLoopFuture<InitializerOutput> where InitializerOutput: Sendable {
        self.eventLoop.flatSubmit {
            self.assumeIsolated().createUnidirectionalStream(streamInitializer: streamInitializer)
        }
    }

    public struct Isolated: IsolatedQUICStreamCreator {
        private let eventLoop: any EventLoop
        private let role: Role
        private let _createOutboundStream:
            (
                EventLoopPromise<any Channel>, QUICStreamType,
                @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
            ) -> Void

        init(
            eventLoop: any EventLoop,
            role: Role,
            _createOutboundStream:
                @escaping (
                    EventLoopPromise<any Channel>, QUICStreamType,
                    @escaping (any Channel, QUICStreamID) -> EventLoopFuture<Void>
                ) -> Void
        ) {
            self.eventLoop = eventLoop
            self.role = role
            self._createOutboundStream = _createOutboundStream
        }

        /// Creates a new bidirectional stream on the connection.
        ///
        /// - Parameters:
        ///   - streamInitializer: A callback that will be invoked to allow you to configure the channel pipeline for the newly created stream channel.
        /// - Returns: A future which is fulfilled once the stream is active.
        public func createBidirectionalStream<InitializerOutput: Sendable>(
            streamInitializer:
                @escaping (QUICStreamInitializerParameters) -> EventLoopFuture<
                    InitializerOutput
                >
        ) -> EventLoopFuture<InitializerOutput> {
            self.createStream(isBidirectional: true, streamInitializer: streamInitializer)
        }

        /// Creates a new unidirectional stream on the connection.
        ///
        /// - Parameters:
        ///   - streamInitializer: A callback that will be invoked to allow you to configure the channel pipeline for the newly created stream channel.
        /// - Returns: A future which is fulfilled once the stream is active.
        public func createUnidirectionalStream<InitializerOutput: Sendable>(
            streamInitializer:
                @escaping (QUICStreamInitializerParameters) -> EventLoopFuture<
                    InitializerOutput
                >
        ) -> EventLoopFuture<InitializerOutput> {
            self.createStream(isBidirectional: false, streamInitializer: streamInitializer)
        }

        private func createStream<InitializerOutput: Sendable>(
            isBidirectional: Bool,
            streamInitializer: @escaping (QUICStreamInitializerParameters) -> EventLoopFuture<InitializerOutput>
        ) -> EventLoopFuture<InitializerOutput> {
            let channelPromise = self.eventLoop.makePromise(of: (any Channel).self)
            let outputPromise = self.eventLoop.makePromise(of: InitializerOutput.self)
            channelPromise.futureResult.cascadeFailure(to: outputPromise)

            let streamType: QUICStreamType
            switch (self.role, isBidirectional) {
            case (.client, true):
                streamType = .clientInitiatedBidirectional
            case (.client, false):
                streamType = .clientInitiatedUnidirectional
            case (.server, true):
                streamType = .serverInitiatedBidirectional
            case (.server, false):
                streamType = .serverInitiatedUnidirectional
            }

            self._createOutboundStream(channelPromise, streamType) { channel, id in
                streamInitializer(.init(channel: channel, streamID: id))
                    .flatMapErrorThrowing { error in
                        outputPromise.fail(error)
                        throw error
                    }
                    .map {
                        outputPromise.succeed($0)
                        return ()
                    }
            }

            // We have to await both futures here because of two reasons:
            // 1. The channelPromise future is indicating if creating the channel actually succeeded.
            //    We have to await this to know if we actually created a new child channel.
            // 2. While the outputPromise might succeed the channelPromise can still fail for unrelated
            //    reasons.
            return channelPromise.futureResult
                .flatMap { _ in
                    outputPromise.futureResult
                }
        }
    }
}
