import Foundation
import Logging
import NIOCore
import NIOEmbedded
@_spi(ProtocolProvider) import SwiftNetwork
import Testing

@testable import NIOQUIC

@Suite
struct QUICChannelStreamHandlerTests {
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
            #expect(streamChannel._testOnly_autoRead == false)

            try streamChannelOptions.setOption(.autoRead, value: true)
            #expect(try streamChannelOptions.getOption(.autoRead) == true)
            #expect(streamChannel._testOnly_autoRead == true)
        }
    }

    @Test("Calling read sets the pendingRead flag")
    func callingReadSetsPendingReadFlag() throws {
        try Self.withServerStream { streamChannel in
            let recorder = RecordingHandler()
            try streamChannel.pipeline.syncOperations.addHandler(recorder)

            #expect(streamChannel._testOnly_pendingRead == false)

            streamChannel.pipeline.read()
            #expect(recorder.pendingReadRequests.count == 1)
            recorder.releasePendingReadRequest(to: streamChannel.pipeline)
            #expect(streamChannel._testOnly_pendingRead == true)

            #expect(recorder.events == [.read, .channelReadComplete])
        }
    }

    @Test("Calling read when pendingRead flag is true")
    func callingReadWhenAlreadyPendingRead() throws {
        try Self.withServerStream { streamChannel in
            let recorder = RecordingHandler()
            try streamChannel.pipeline.syncOperations.addHandler(recorder)

            #expect(streamChannel._testOnly_pendingRead == false)

            streamChannel.pipeline.read()
            recorder.releasePendingReadRequest(to: streamChannel.pipeline)
            #expect(streamChannel._testOnly_pendingRead == true)
            #expect(recorder.events == [.read, .channelReadComplete])

            // Call `read` again
            streamChannel.pipeline.read()
            recorder.releasePendingReadRequest(to: streamChannel.pipeline)
            #expect(streamChannel._testOnly_pendingRead == true)
            #expect(recorder.events == [.read, .channelReadComplete, .read, .channelReadComplete])
        }
    }

    @Test("read called before inbound data arrives", arguments: [true, false])
    func readCalledBeforeInboundDataArrives(autoRead: Bool) throws {
        try Self.withServerStream { streamChannel in
            let recorder = RecordingHandler()
            // Set the `autoRead` channel option.
            try streamChannel.syncOptions?.setOption(.autoRead, value: autoRead)
            try streamChannel.pipeline.syncOperations.addHandler(recorder)

            // The downstream consumer now requests a read.
            streamChannel.pipeline.read()
            recorder.releasePendingReadRequest(to: streamChannel.pipeline)
            // Since no data has arrived from the network yet, the read request cannot be satisfied. As such, the
            // `pendingRead` flag should be set to `true`.
            #expect(streamChannel._testOnly_pendingRead == true)
            #expect(recorder.events == [.read, .channelReadComplete])

            // Now simulate data arriving from the network.
            let testData = ByteBuffer(string: "test")
            streamChannel._testOnly_appendToBufferedReadData(testData)
            streamChannel.handleInboundDataAvailableEvent(.init())

            // The data should be delivered downstream.
            #expect(recorder.channelReadCount == 1)
            #expect(recorder.totalReadBytes == testData.readableBytes)

            switch autoRead {
            case false:
                // The `read` event shouldn't have been fired. As such, the `pendingRead` flag should still be `false`.
                #expect(recorder.events == [.read, .channelReadComplete, .channelRead(testData), .channelReadComplete])
                #expect(recorder.pendingReadRequests.count == 0)
                #expect(streamChannel._testOnly_pendingRead == false)

                // Manually fire a read request down the pipeline.
                streamChannel.pipeline.read()

                // After manually firing the read, the behaviour should be equivalent to that of `autoRead == true`.
                fallthrough

            case true:
                #expect(
                    recorder.events == [
                        .read, .channelReadComplete,
                        .channelRead(testData), .channelReadComplete,
                        .read,
                    ]
                )
                #expect(recorder.pendingReadRequests.count == 1)
                #expect(streamChannel._testOnly_pendingRead == false)

                // Now tell `RecorderHandler` to release the pending read request and deliver it to the channel.
                recorder.releasePendingReadRequest(to: streamChannel.pipeline)
                // The `pendingRead` flag should be set to `true` now.
                #expect(streamChannel._testOnly_pendingRead == true)

                #expect(
                    recorder.events == [
                        .read, .channelReadComplete,
                        .channelRead(testData), .channelReadComplete,
                        .read, .channelReadComplete,
                    ]
                )
            }
        }
    }

    @Test("read called after inbound data arrives", arguments: [true, false])
    func readCalledAfterInboundDataArrives(autoRead: Bool) throws {
        try Self.withServerStream { streamChannel in
            let recorder = RecordingHandler()
            // Set the `autoRead` channel option.
            try streamChannel.syncOptions?.setOption(.autoRead, value: autoRead)
            try streamChannel.pipeline.syncOperations.addHandler(recorder)

            // Simulate data arriving from the network.
            let testData = ByteBuffer(string: "test")
            streamChannel._testOnly_appendToBufferedReadData(testData)
            streamChannel.handleInboundDataAvailableEvent(.init())

            // Since the downstream has not requested a read, the data should not be delivered downstream just yet.
            #expect(streamChannel._testOnly_pendingRead == false)
            #expect(recorder.channelReadCount == 0)
            #expect(recorder.totalReadBytes == 0)

            // Now the downstream requests a read.
            streamChannel.pipeline.read()
            #expect(recorder.pendingReadRequests.count == 1)
            recorder.releasePendingReadRequest(to: streamChannel.pipeline)

            // The downstream should have received this buffered data.
            #expect(recorder.channelReadCount == 1)
            #expect(recorder.totalReadBytes == testData.readableBytes)
            #expect(streamChannel._testOnly_pendingRead == false)

            switch autoRead {
            case false:
                #expect(recorder.events == [.read, .channelRead(testData), .channelReadComplete])
                #expect(recorder.pendingReadRequests.count == 0)
                #expect(streamChannel._testOnly_pendingRead == false)

                // Manually fire a read request down the pipeline.
                streamChannel.pipeline.read()

                // After manually firing the read, the behaviour should be equivalent to that of `autoRead == true`.
                fallthrough

            case true:
                // The downstream should have automatically requested another read after reading the first data.
                #expect(recorder.events == [.read, .channelRead(testData), .channelReadComplete, .read])
                #expect(recorder.pendingReadRequests.count == 1)
                // Now tell `RecorderHandler` to release the pending read request.
                recorder.releasePendingReadRequest(to: streamChannel.pipeline)
                #expect(recorder.pendingReadRequests.count == 0)

                // Since there is no further data to deliver, the downstream consumer's second read request remains
                // pending.
                #expect(streamChannel._testOnly_pendingRead == true)
            }
        }
    }

    @Test("Interleaved read requests and inbound data arrival", arguments: [true, false])
    func interleavedReadRequestsAndInboundDataArrival(autoRead: Bool) throws {
        try Self.withServerStream { streamChannel in
            let recorder = RecordingHandler()
            // Set the `autoRead` channel option.
            try streamChannel.syncOptions?.setOption(.autoRead, value: autoRead)
            try streamChannel.pipeline.syncOperations.addHandler(recorder)

            // Data arrives from the network before `read` is called.
            let testData = ByteBuffer(string: "test")
            streamChannel._testOnly_appendToBufferedReadData(testData)
            streamChannel.handleInboundDataAvailableEvent(.init())

            // Since the downstream has not requested a read, the data should not be delivered downstream just yet.
            #expect(streamChannel._testOnly_pendingRead == false)
            #expect(recorder.channelReadCount == 0)
            #expect(recorder.totalReadBytes == 0)

            // Now the downstream requests a read;
            streamChannel.pipeline.read()
            #expect(recorder.pendingReadRequests.count == 1)
            recorder.releasePendingReadRequest(to: streamChannel.pipeline)
            // and receives the buffered data.
            #expect(recorder.channelReadCount == 1)
            #expect(recorder.totalReadBytes == testData.readableBytes)
            #expect(streamChannel._testOnly_pendingRead == false)

            // Some more data arrives from the network before the downstream issues another read.
            for i in 1...3 {
                let testData = ByteBuffer(string: "test\(i)")
                streamChannel._testOnly_appendToBufferedReadData(testData)
                streamChannel.handleInboundDataAvailableEvent(.init())
            }

            switch autoRead {
            case false:
                #expect(recorder.events == [.read, .channelRead(testData), .channelReadComplete])
                #expect(recorder.pendingReadRequests.count == 0)
                #expect(streamChannel._testOnly_pendingRead == false)

                // Now manually fire a read request down the pipeline.
                streamChannel.pipeline.read()
                fallthrough

            case true:
                // The downstream should have automatically requested another read after reading the first data.
                #expect(recorder.events == [.read, .channelRead(testData), .channelReadComplete, .read])
                #expect(recorder.pendingReadRequests.count == 1)

                // Release the read request.
                recorder.releasePendingReadRequest(to: streamChannel.pipeline)
            }

            // Now the downstream should receive all the buffered data.
            let expectedEvents: [RecordingHandler.Event] = [
                .read,
                .channelRead(testData),
                .channelReadComplete,
                .read,
                .channelRead(ByteBuffer(string: "test1test2test3")),
                .channelReadComplete,
            ]

            switch autoRead {
            case false:
                #expect(recorder.events == expectedEvents)
                #expect(recorder.pendingReadRequests.count == 0)
                #expect(streamChannel._testOnly_pendingRead == false)

                // Manually fire a read request down the pipeline.
                streamChannel.pipeline.read()

                // After manually firing the read, the behaviour should be equivalent to that of `autoRead == true`.
                fallthrough

            case true:
                // The downstream should have automatically requested another read after reading the first data.
                #expect(recorder.events == expectedEvents + [.read])
                #expect(recorder.pendingReadRequests.count == 1)

                // Release the read request.
                recorder.releasePendingReadRequest(to: streamChannel.pipeline)
                #expect(recorder.pendingReadRequests.count == 0)

                // Since there is no further data to deliver, the downstream consumer's read request remains pending.
                #expect(streamChannel._testOnly_pendingRead == true)
            }
        }
    }

    @Test("Packet with FIN delivered to a pending read request")
    func finPacketDeliveredToPendingReadRequest() throws {
        try Self.withServerStream { streamChannel in
            let recorder = RecordingHandler()
            // Set `autoRead` to `false`.
            try streamChannel.syncOptions?.setOption(.autoRead, value: false)
            try streamChannel.pipeline.syncOperations.addHandler(recorder)

            // Simulate data with a FIN arriving from the network.
            let testData = ByteBuffer(string: "test")
            streamChannel._testOnly_appendToBufferedReadData(testData)
            // Tell the state machine we have recieved a FIN.
            _ = try streamChannel.streamStateMachine.receiveFin(finalSize: 0)
            streamChannel.handleInboundDataAvailableEvent(.init())

            // Since the downstream has not requested a read, the data should not be delivered downstream just yet.
            #expect(streamChannel._testOnly_pendingRead == false)
            #expect(recorder.channelReadCount == 0)
            #expect(recorder.totalReadBytes == 0)

            // Now the downstream requests a read.
            streamChannel.pipeline.read()
            #expect(recorder.pendingReadRequests.count == 1)
            recorder.releasePendingReadRequest(to: streamChannel.pipeline)

            // The downstream should have received this buffered data;
            #expect(recorder.channelReadCount == 1)
            #expect(recorder.totalReadBytes == testData.readableBytes)
            #expect(streamChannel._testOnly_pendingRead == false)
            // and an `inputClosed` event, since we received a FIN.
            #expect(recorder.events == [.read, .channelRead(testData), .inputClosedEvent, .channelReadComplete])
            #expect(recorder.pendingReadRequests.count == 0)
            #expect(streamChannel._testOnly_pendingRead == false)
        }
    }
}

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

        let connection = try SwiftNetworkQUICConnection(
            configuration: .server(
                serverName: "quic-test.local",
                authenticationConfiguration: .rawPublicKeys(
                    publicKeyFilePath: testPublicKeyPath,
                    privateKeyFilePath: testPrivateKeyPath
                ),
                applicationProtocols: []
            ),
            sourceConnectionID: .random(using: &rng),
            originalDestinationConnectionID: .random(using: &rng),
            authenticator: nil,
            localAddress: try SocketAddress(ipAddress: "127.0.0.1", port: 1234),
            remoteAddress: try SocketAddress(ipAddress: "127.0.0.1", port: 1234),
            logger: Logger(label: "test"),
            eventLoop: eventLoop,
            udpChannel: udpChannel
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
    // Records events from the parent channel and allows control over propagating read requests.
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

        var pendingReadRequests: [EventLoopPromise<Void>] = []

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
            self.events.filter {
                switch $0 {
                case .channelRead:
                    true

                default:
                    false
                }
            }.count
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

            // Don't call context.read() now; create a promise that will call `context.read()` and store that promise.
            // That promise will be fulfilled when `releasePendingReadRequest` is called. At that point, the read will
            // be propagated further down the pipeline.
            let readPromise = context.eventLoop.makePromise(of: Void.self)
            let loopBoundContext = context.loopBound
            readPromise.futureResult.whenComplete { _ in
                loopBoundContext.value.read()
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
