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

/// Internal type to abstract away the `Output` type of the multiplexer. This means we are going through an existential
/// in the `QUICConnectionChannelHandler` when yielding a new `Channel`. However, this is okay for now otherwise
/// we would need to make the handler generic as well.
protocol StreamMultiplexerContinuation: Sendable {
    /// We have to do a bit of an awkward dance here to carry the `Output` between the initializer and the continuation where
    /// we yield to. That's why we are using `Any` here to avoid making the handler generic.
    func initialize(channel: any Channel, streamID: QUICStreamID) -> EventLoopFuture<any Sendable>
    /// Put the output (from calling the intializer) into the inbound streams AsyncSequence. If the yield fails, the corresponding channel will be closed.
    func yield(output: any Sendable, channel: any Channel)
    func finish()
}

/// A struct representing a single QUIC connection.
/// Can iterate the inbound streams and create new outbound streams.
@available(anyAppleOS 26, *)
public struct QUICConnection<Output: Sendable>: Sendable, StreamMultiplexerContinuation {
    /// Channel initializer called for each new inbound stream.
    private let inboundStreamInitializer: @Sendable (any Channel) -> EventLoopFuture<Output>
    /// The inboundStreams' continuation.
    private let inboundStreamsContinuation: AsyncStream<Output>.Continuation

    /// A method to create a new outbound stream.
    private let streamCreator: QUICStreamCreator

    /// An asynchronous sequence of inbound streams.
    public let inboundStreams: InboundStreams

    init(
        inboundStreamInitializer: @escaping @Sendable (any Channel) -> EventLoopFuture<Output>,
        streamCreator: QUICStreamCreator
    ) {
        self.inboundStreamInitializer = inboundStreamInitializer
        self.streamCreator = streamCreator
        let (stream, continuation) = AsyncStream<Output>.makeStream()
        self.inboundStreams = .init(stream: stream)
        self.inboundStreamsContinuation = continuation
    }

    func initialize(channel: any Channel, streamID: QUICStreamID) -> EventLoopFuture<any Sendable> {
        self.inboundStreamInitializer(channel).map { $0 }
    }

    func yield(output: any Sendable, channel: any Channel) {
        let result = self.inboundStreamsContinuation.yield(output as! Output)
        switch result {
        case .dropped, .terminated:
            // We must shut down the stream channel because the user never got it.
            // Especially if `output` here is a NIOAsyncChannel, because those must not be dropped when active.
            channel.close(promise: nil)
            // We need to keep `output` alive until the channel is closed.
            channel.closeFuture.whenComplete { _ in
                withExtendedLifetime(output) {}
            }
        case .enqueued:
            break  // this output is no longer our problem
        @unknown default:
            break
        }
    }

    func finish() {
        self.inboundStreamsContinuation.finish()
    }

    /// Creates a new bidirectional stream on the connection.
    ///
    /// - Parameters:
    ///   - streamInitializer: A callback that will be invoked to allow you to configure the channel pipeline for the newly created stream channel.
    /// - Returns: A future which is fulfilled once the stream is active.
    public func createBidirectionalStream<InitializerOutput: Sendable>(
        streamInitializer: @escaping @Sendable (QUICStreamInitializerParameters) -> EventLoopFuture<InitializerOutput>
    ) async throws -> InitializerOutput {
        try await self.streamCreator.createBidirectionalStream(streamInitializer: streamInitializer).get()
    }

    /// Creates a new unidirectional stream on the connection.
    ///
    /// - Parameters:
    ///   - streamInitializer: A callback that will be invoked to allow you to configure the channel pipeline for the newly created stream channel.
    /// - Returns: A future which is fulfilled once the stream is active.
    public func createUnidirectionalStream<InitializerOutput: Sendable>(
        streamInitializer: @escaping @Sendable (QUICStreamInitializerParameters) -> EventLoopFuture<InitializerOutput>
    ) async throws -> InitializerOutput {
        try await self.streamCreator.createUnidirectionalStream(streamInitializer: streamInitializer).get()
    }

    /// An asynchronous sequence of inbound streams.
    public struct InboundStreams: AsyncSequence, Sendable {
        public typealias Element = Output

        private let stream: AsyncStream<Output>

        init(stream: AsyncStream<Output>) {
            self.stream = stream
        }

        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(iterator: self.stream.makeAsyncIterator())
        }

        public struct AsyncIterator: AsyncIteratorProtocol {
            private var iterator: AsyncStream<Output>.Iterator

            init(iterator: AsyncStream<Output>.Iterator) {
                self.iterator = iterator
            }

            public mutating func next() async -> Output? {
                await self.iterator.next()
            }
        }
    }
}

@available(*, unavailable)
extension QUICConnection.InboundStreams.AsyncIterator: Sendable {}
