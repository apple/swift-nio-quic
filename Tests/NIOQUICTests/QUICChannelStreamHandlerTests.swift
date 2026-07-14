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

import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOQUICHelpers
@_spi(ProtocolProvider) import SwiftNetwork
import Testing

@testable import NIOQUIC

@Suite
struct QUICChannelStreamHandlerTests {
    @available(anyAppleOS 26, *)
    @Test("autoRead can be configured on stream channel")
    func testConfigureAutoReadOnStreamChannel() throws {
        try Self.withServerStream { streamChannel in
            let recorder = RecordingHandler()
            try streamChannel.pipeline.syncOperations.addHandler(recorder)

            let streamChannelOptions = try #require(streamChannel.syncOptions)

            // The default value should be inherited from the connection channel. `EmbeddedChannel` defaults `autoRead`
            // to `true`.
            #expect(try streamChannelOptions.getOption(.autoRead) == true)

            try streamChannelOptions.setOption(.autoRead, value: false)
            #expect(try streamChannelOptions.getOption(.autoRead) == false)
            #expect(try streamChannel.syncOptions?.getOption(.autoRead) == false)

            try streamChannelOptions.setOption(.autoRead, value: true)
            #expect(try streamChannelOptions.getOption(.autoRead) == true)
            #expect(try streamChannel.syncOptions?.getOption(.autoRead) == true)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Calling read twice is idempotent and does not double-deliver")
    func callingReadTwiceIsIdempotent() throws {
        try Self.withServerStream { streamChannel in
            try streamChannel.syncOptions?.setOption(.autoRead, value: false)
            let readHolder = ManualReadHandler()
            let recorder = RecordingHandler()
            try streamChannel.pipeline.syncOperations.addHandlers([readHolder, recorder])

            // Fire two reads.
            streamChannel.pipeline.read()
            readHolder.releasePendingReadRequest(to: streamChannel.pipeline)
            streamChannel.pipeline.read()
            readHolder.releasePendingReadRequest(to: streamChannel.pipeline)
            #expect(recorder.events == [.read, .read])

            // Data arrives once.
            let testData = ByteBuffer(string: "test")
            streamChannel._testOnly_appendToBufferedReadData(testData)
            streamChannel.handleInboundDataAvailableEvent(.init())

            // The buffer should be drained exactly once.
            #expect(recorder.events == [.read, .read, .channelRead(testData), .channelReadComplete])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("read called before inbound data arrives", arguments: [true, false])
    func readCalledBeforeInboundDataArrives(autoRead: Bool) throws {
        try Self.withServerStream { streamChannel in
            // Set the `autoRead` channel option.
            try streamChannel.syncOptions?.setOption(.autoRead, value: autoRead)

            let readHolder = ManualReadHandler()
            let recorder = RecordingHandler()
            try streamChannel.pipeline.syncOperations.addHandlers([readHolder, recorder])

            // The downstream consumer now requests a read.
            streamChannel.pipeline.read()
            readHolder.releasePendingReadRequest(to: streamChannel.pipeline)
            // Since no data has arrived from the network yet, the read request cannot be satisfied.
            #expect(recorder.events == [.read])

            // Now simulate data arriving from the network.
            let testData = ByteBuffer(string: "test")
            streamChannel._testOnly_appendToBufferedReadData(testData)
            streamChannel.handleInboundDataAvailableEvent(.init())

            // The data should be delivered downstream.
            #expect(recorder.channelReadCount == 1)
            #expect(recorder.totalReadBytes == testData.readableBytes)

            switch autoRead {
            case false:
                // The `read` event shouldn't have been fired.
                #expect(recorder.events == [.read, .channelRead(testData), .channelReadComplete])
                #expect(readHolder.pendingReadRequests.count == 0)

                // Manually fire a read request down the pipeline.
                streamChannel.pipeline.read()

                // After manually firing the read, the behaviour should be equivalent to that of `autoRead == true`.
                fallthrough

            case true:
                #expect(recorder.events == [.read, .channelRead(testData), .channelReadComplete, .read])
                #expect(readHolder.pendingReadRequests.count == 1)

                // Now tell `ReadHolderHandler` to release the pending read request and deliver it to the channel.
                readHolder.releasePendingReadRequest(to: streamChannel.pipeline)

                // Since there is no data to consume from the network, `channelRead`/`channelReadComplete` should not be
                // fired, and therefore, auto-read should not be triggered (the `pendingRead` flag stays `true`).
                #expect(recorder.events == [.read, .channelRead(testData), .channelReadComplete, .read])
            }
        }
    }

    @available(anyAppleOS 26, *)
    @Test("read called after inbound data arrives", arguments: [true, false])
    func readCalledAfterInboundDataArrives(autoRead: Bool) throws {
        try Self.withServerStream { streamChannel in
            // Set the `autoRead` channel option.
            try streamChannel.syncOptions?.setOption(.autoRead, value: autoRead)

            let readHolder = ManualReadHandler()
            let recorder = RecordingHandler()
            try streamChannel.pipeline.syncOperations.addHandlers([readHolder, recorder])

            // Simulate data arriving from the network.
            let testData = ByteBuffer(string: "test")
            streamChannel._testOnly_appendToBufferedReadData(testData)
            streamChannel.handleInboundDataAvailableEvent(.init())

            // Since the downstream has not requested a read, the data should not be delivered downstream just yet.
            #expect(recorder.channelReadCount == 0)
            #expect(recorder.totalReadBytes == 0)
            // and receives the buffered data.
            streamChannel.pipeline.read()
            #expect(readHolder.pendingReadRequests.count == 1)
            readHolder.releasePendingReadRequest(to: streamChannel.pipeline)

            // The downstream should have received this buffered data.
            #expect(recorder.channelReadCount == 1)
            #expect(recorder.totalReadBytes == testData.readableBytes)

            switch autoRead {
            case false:
                #expect(recorder.events == [.read, .channelRead(testData), .channelReadComplete])
                #expect(readHolder.pendingReadRequests.count == 0)

                // Manually fire a read request down the pipeline.
                streamChannel.pipeline.read()

                // After manually firing the read, the behaviour should be equivalent to that of `autoRead == true`.
                fallthrough

            case true:
                // The downstream should have automatically requested another read after reading the first data.
                #expect(recorder.events == [.read, .channelRead(testData), .channelReadComplete, .read])
                #expect(readHolder.pendingReadRequests.count == 1)
                // Now tell `ReadHolderHandler` to release the pending read request.
                readHolder.releasePendingReadRequest(to: streamChannel.pipeline)
                #expect(readHolder.pendingReadRequests.count == 0)

                // Since there is no data to consume from the network, `channelRead`/`channelReadComplete` should not be
                // fired, and therefore, auto-read should not be triggered (the `pendingRead` flag stays `true`).
                #expect(recorder.events == [.read, .channelRead(testData), .channelReadComplete, .read])
            }
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Interleaved read requests and inbound data arrival", arguments: [true, false])
    func interleavedReadRequestsAndInboundDataArrival(autoRead: Bool) throws {
        try Self.withServerStream { streamChannel in
            // Set the `autoRead` channel option.
            try streamChannel.syncOptions?.setOption(.autoRead, value: autoRead)

            let readHolder = ManualReadHandler()
            let recorder = RecordingHandler()
            try streamChannel.pipeline.syncOperations.addHandlers([readHolder, recorder])

            // Data arrives from the network before `read` is called.
            let testData = ByteBuffer(string: "test")
            streamChannel._testOnly_appendToBufferedReadData(testData)
            streamChannel.handleInboundDataAvailableEvent(.init())

            // Since the downstream has not requested a read, the data should not be delivered downstream just yet.
            #expect(recorder.channelReadCount == 0)
            #expect(recorder.totalReadBytes == 0)

            // Now the downstream requests a read;
            streamChannel.pipeline.read()
            #expect(readHolder.pendingReadRequests.count == 1)
            readHolder.releasePendingReadRequest(to: streamChannel.pipeline)
            // and receives the buffered data.
            #expect(recorder.channelReadCount == 1)
            #expect(recorder.totalReadBytes == testData.readableBytes)

            // Some more data arrives from the network.
            for i in 1...3 {
                let testData = ByteBuffer(string: "test\(i)")
                streamChannel._testOnly_appendToBufferedReadData(testData)
                streamChannel.handleInboundDataAvailableEvent(.init())
            }

            switch autoRead {
            case false:
                #expect(recorder.events == [.read, .channelRead(testData), .channelReadComplete])
                #expect(readHolder.pendingReadRequests.count == 0)

                // Now manually fire a read request down the pipeline.
                streamChannel.pipeline.read()
                fallthrough

            case true:
                // The downstream should have automatically requested another read after reading the first data.
                #expect(recorder.events == [.read, .channelRead(testData), .channelReadComplete, .read])
                #expect(readHolder.pendingReadRequests.count == 1)

                // Release the read request.
                readHolder.releasePendingReadRequest(to: streamChannel.pipeline)
            }

            // Now the downstream should receive all the buffered data.
            let expectedEvents: [RecordingHandler.Event] = [
                .read,
                .channelRead(testData), .channelReadComplete,
                .read,
                .channelRead(ByteBuffer(string: "test1test2test3")), .channelReadComplete,
            ]

            switch autoRead {
            case false:
                #expect(recorder.events == expectedEvents)
                #expect(readHolder.pendingReadRequests.count == 0)

                // Manually fire a read request down the pipeline.
                streamChannel.pipeline.read()

                // After manually firing the read, the behaviour should be equivalent to that of `autoRead == true`.
                fallthrough

            case true:
                // The downstream should have automatically requested another read after reading the first data.
                #expect(recorder.events == expectedEvents + [.read])
                #expect(readHolder.pendingReadRequests.count == 1)

                // Release the read request.
                readHolder.releasePendingReadRequest(to: streamChannel.pipeline)
                #expect(readHolder.pendingReadRequests.count == 0)

                // Since there is no data to consume from the network, `channelRead`/`channelReadComplete` should not be
                // fired, and therefore, auto-read should not be triggered (the `pendingRead` flag stays `true`).
                #expect(recorder.events == expectedEvents + [.read])
            }
        }
    }

    @available(anyAppleOS 26, *)
    @Test("FIN-only packet fires channelReadComplete with no data")
    func finOnlyFiresChannelReadComplete() throws {
        try Self.withServerStream { streamChannel in
            try streamChannel.syncOptions?.setOption(.autoRead, value: false)

            let recorder = RecordingHandler()
            try streamChannel.pipeline.syncOperations.addHandler(recorder)

            // FIN arrives with no data.
            _ = try streamChannel.streamStateMachine.receiveFin(finalSize: 0)
            streamChannel.handleInboundDataAvailableEvent(.init())

            streamChannel.pipeline.read()

            // No data, but inputClosed and channelReadComplete must both fire.
            #expect(recorder.events == [.read, .inputClosedEvent, .channelReadComplete])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Packet with FIN delivered to a pending read request")
    func finPacketDeliveredToPendingReadRequest() throws {
        try Self.withServerStream { streamChannel in
            // Set `autoRead` to `false`.
            try streamChannel.syncOptions?.setOption(.autoRead, value: false)

            let readHolder = ManualReadHandler()
            let recorder = RecordingHandler()
            try streamChannel.pipeline.syncOperations.addHandlers([readHolder, recorder])

            // Simulate data with a FIN arriving from the network.
            let testData = ByteBuffer(string: "test")
            streamChannel._testOnly_appendToBufferedReadData(testData)
            // Tell the state machine we have received a FIN.
            _ = try streamChannel.streamStateMachine.receiveFin(finalSize: 0)
            streamChannel.handleInboundDataAvailableEvent(.init())

            // Since the downstream has not requested a read, the data should not be delivered downstream just yet.
            #expect(recorder.channelReadCount == 0)
            #expect(recorder.totalReadBytes == 0)

            // Now the downstream requests a read.
            streamChannel.pipeline.read()
            #expect(readHolder.pendingReadRequests.count == 1)
            readHolder.releasePendingReadRequest(to: streamChannel.pipeline)

            // The downstream should have received this buffered data;
            #expect(recorder.channelReadCount == 1)
            #expect(recorder.totalReadBytes == testData.readableBytes)
            // and an `inputClosed` event, since we received a FIN.
            #expect(recorder.events == [.read, .channelRead(testData), .inputClosedEvent, .channelReadComplete])
            #expect(readHolder.pendingReadRequests.count == 0)
        }
    }

    /// `handleDisconnectedEvent` can fire on an already-detached stream (e.g. connection-level close);
    /// we shouldn't leak the close promise.
    @available(anyAppleOS 26, *)
    @Test("handleDisconnectedEvent still resolves closeFuture when linkage is already detached")
    func handleDisconnectedEventResolvesCloseFutureWhenLinkageAlreadyDetached() throws {
        try Self.withServerStream { streamChannel in
            streamChannel._isActive.store(true, ordering: .sequentiallyConsistent)

            let closeFutureFulfilled = NIOLockedValueBox(false)
            streamChannel.closeFuture.whenComplete { _ in
                closeFutureFulfilled.withLockedValue { $0 = true }
            }

            // Detach the linkage simulating prior teardown
            switch streamChannel.swiftNetworkStreamHandle.invokeDetach() {
            case .proceed:
                break
            case .skipAlreadyDetached:
                Issue.record("Pre-condition: handle should have been attached")
            }

            // Detached linkage + active channel, close future must not have resolved.
            let isAttachedBefore = streamChannel.swiftNetworkStreamHandle.isAttached
            #expect(!isAttachedBefore)
            let isActiveBefore = streamChannel.isActive
            #expect(isActiveBefore)

            // Fire `handleDisconnectedEvent`, actually perform the full close
            streamChannel.handleDisconnectedEvent(.init(), error: nil)

            // `_close` defers its body to the next event-loop tick; flush.
            (streamChannel.eventLoop as! EmbeddedEventLoop).run()

            let isActiveAfter = streamChannel.isActive
            #expect(!isActiveAfter)
            let fulfilled = closeFutureFulfilled.withLockedValue { $0 }
            #expect(
                fulfilled,
                "closeFuture must resolve when handleDisconnectedEvent runs on an active channel"
            )
        }
    }

    /// `handleDisconnectedEvent` must flip `_isActive` synchronously; deferring
    /// races the multiplexer's `removeHandlers` and drops `fireChannelInactive`.
    @available(anyAppleOS 26, *)
    @Test("handleDisconnectedEvent flips _isActive synchronously")
    func handleDisconnectedEventFlipsIsActiveSynchronously() throws {
        try Self.withServerStream { streamChannel in
            // Set up the normal case: linkage attached, channel active.
            streamChannel._isActive.store(true, ordering: .sequentiallyConsistent)
            let isAttachedBefore = streamChannel.swiftNetworkStreamHandle.isAttached
            #expect(isAttachedBefore)
            let isActiveBefore = streamChannel.isActive
            #expect(isActiveBefore)

            // Fire `handleDisconnectedEvent`. After it returns —
            // *without* having drained the event loop — `_isActive` must
            // already be `false`. If the cascade were deferred via
            // `eventLoop.execute` (the pre-fix shape), `_isActive` would
            // still be `true` here.
            streamChannel.handleDisconnectedEvent(.init(), error: nil)

            let isActiveAfterImmediately = streamChannel.isActive
            #expect(
                !isActiveAfterImmediately,
                "handleDisconnectedEvent must flip _isActive synchronously to resolve _closePromise on the same tick as ChildChannelMultiplexer.removeHandlers"
            )

            // Drain the deferred pipeline body.
            (streamChannel.eventLoop as! EmbeddedEventLoop).run()
        }
    }

    /// `invoke*` wrappers must release their borrow on `swiftNetworkStreamHandle` before the linkage call;
    /// SwiftNetwork's synchronous drain otherwise re-enters `invokeDetach` on a live read borrow and traps.
    @available(anyAppleOS 26, *)
    @Test("invoke* wrappers release the borrow on swiftNetworkStreamHandle before returning")
    func invokeWrappersReleaseBorrowBeforeReturning() throws {
        try Self.withServerStream { streamChannel in
            // Extract the linkage from the non-mutating send-data wrapper.
            let linkage: OutboundStreamLinkage
            switch streamChannel.swiftNetworkStreamHandle.invokeSendStreamData() {
            case .proceed(let extracted):
                linkage = extracted
            case .handleViolation:
                Issue.record("Pre-condition: handle should have been attached")
                return
            }

            // The borrow taken by `invokeSendStreamData()` is released by
            // the time it returns. Take a *modify* access on the same
            // property right here — under the pre-fix shape this would
            // overlap the still-live outer read borrow and trap.
            switch streamChannel.swiftNetworkStreamHandle.invokeDetach() {
            case .proceed:
                break
            case .skipAlreadyDetached:
                Issue.record("Pre-condition: handle should have been attached")
            }

            // `linkage` is still usable independently of the handle — it's
            // a value-type copy, not a borrow.
            _ = linkage
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICChannelStreamHandlerTests {
    static func withServerStream(
        streamID: UInt64 = 0,
        direction: QUICStreamDirection = .bidirectional,
        autoRead: Bool = true,
        body: (QUICChannelStreamHandler) throws -> Void
    ) throws {
        let testPrivateKeyPath = Bundle.module.url(forResource: "privateKey", withExtension: "der")!.path
        let testPublicKeyPath = Bundle.module.url(forResource: "publicKey", withExtension: "der")!.path

        var rng: any RandomNumberGenerator = SystemRandomNumberGenerator()

        let eventLoop = EmbeddedEventLoop()
        let udpChannel = EmbeddedChannel(loop: eventLoop)
        let connectionChannel = EmbeddedChannel(loop: eventLoop)

        let connection = try SwiftNetworkQUICConnection.server(
            configuration: .server(
                serverName: "quic-test.local",
                authenticationConfiguration: .rawPublicKeys(
                    publicKeyFilePath: testPublicKeyPath,
                    privateKeyFilePath: testPrivateKeyPath
                ),
                applicationProtocols: []
            ),
            sourceConnectionID: .random(using: &rng),
            authenticator: nil,
            localAddress: try SocketAddress(ipAddress: "127.0.0.1", port: 1234),
            remoteAddress: try SocketAddress(ipAddress: "127.0.0.1", port: 1234),
            logger: Logger(label: "test"),
            eventLoop: eventLoop
        )

        connection.setConnectionChannel(connectionChannel)

        connection.registerConnectedStubStreamHandler(
            for: QUICStreamID(rawValue: streamID),
            direction: direction
        )
        let streamChannel = connection.streamInputHandler(streamID: QUICStreamID(rawValue: streamID))!

        try streamChannel.syncOptions!.setOption(.autoRead, value: autoRead)

        try body(streamChannel)

        try udpChannel.close().wait()
        try connectionChannel.close().wait()
    }
}

extension QUICChannelStreamHandlerTests {
    // Records events from the parent channel.
    private final class RecordingHandler: ChannelDuplexHandler {
        typealias OutboundIn = ByteBuffer
        typealias InboundIn = ByteBuffer

        enum Event: Equatable {
            case read
            case channelRead(ByteBuffer)
            case channelReadComplete
            case inputClosedEvent
        }

        var events: [Event] = []

        var totalReadBytes: Int {
            self.events.reduce(0) { accumulated, event in
                switch event {
                case .channelRead(let buffer):
                    return accumulated + buffer.readableBytes

                default:
                    return accumulated
                }
            }
        }

        var channelReadCount: Int {
            self.events.count {
                switch $0 {
                case .channelRead:
                    true

                default:
                    false
                }
            }
        }

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            let buf = self.unwrapInboundIn(data)
            self.events.append(.channelRead(buf))
            context.fireChannelRead(data)
        }

        func channelReadComplete(context: ChannelHandlerContext) {
            self.events.append(.channelReadComplete)
            context.fireChannelReadComplete()
        }

        func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
            if event as? ChannelEvent == .inputClosed {
                self.events.append(.inputClosedEvent)
            } else {
                fatalError("Received an unexpected event: \(event)")
            }

            context.fireUserInboundEventTriggered(event)
        }

        func read(context: ChannelHandlerContext) {
            self.events.append(.read)
            context.read()
        }
    }

    /// Queues outbound `read()` requests and forwards them only when `releasePendingReadRequest(to:)` is called.
    private final class ManualReadHandler: ChannelOutboundHandler {
        typealias OutboundIn = ByteBuffer

        var pendingReadRequests: [EventLoopPromise<Void>] = []

        func read(context: ChannelHandlerContext) {
            // Don't call context.read() now; create a promise that will call `context.read()` and store that promise.
            // That promise will be fulfilled when `releasePendingReadRequest` is called. At that point, the read will
            // be propagated down the pipeline.
            let readPromise = context.eventLoop.makePromise(of: Void.self)
            readPromise.futureResult.assumeIsolated().whenComplete { _ in
                context.read()
            }
            self.pendingReadRequests.append(readPromise)
        }

        /// Release a read request we received previously and propagate it down the pipeline.
        func releasePendingReadRequest(to pipeline: ChannelPipeline) {
            guard let promise = self.pendingReadRequests.popLast() else { return }
            promise.succeed()
        }
    }
}
