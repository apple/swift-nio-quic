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
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import Testing

@testable import NIOQUIC

@Suite
struct DatagramTests {

    func getChannelLoggers() -> (serverLogger: Logger, clientLogger: Logger) {
        var clientLogger = Logger(label: "Client")
        clientLogger.logLevel = .info
        var serverLogger = Logger(label: "Server")
        serverLogger.logLevel = .info
        return (serverLogger, clientLogger)
    }

    @available(anyAppleOS 26, *)
    @Test
    func datagramRoundTrip() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup.singleton
        let loggers = getChannelLoggers()
        let host = "127.0.0.1"
        let payload = ByteBuffer(string: "hello datagram")
        let syncSignal = ByteBuffer(string: "ready")

        let noMoreConnectionsPromise = eventLoopGroup.any().makePromise(of: Void.self)
        let serverReceivedSyncPromise = eventLoopGroup.any().makePromise(of: Void.self)
        let serverReceivedPromise = eventLoopGroup.any().makePromise(of: ByteBuffer.self)
        let clientReceivedEchoPromise = eventLoopGroup.any().makePromise(of: ByteBuffer.self)

        let serverChannel = try await createServerChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: loggers.serverLogger,
            inboundConnectionInitializer: { connectionChannel, streamCreator in
                connectionChannel.eventLoop.makeCompletedFuture {
                    try connectionChannel.pipeline.syncOperations.addHandler(
                        DatagramCapture(onDatagram: { buffer in
                            serverReceivedPromise.succeed(buffer)
                            connectionChannel.writeAndFlush(buffer, promise: nil)
                        })
                    )
                }
            },
            inboundStreamInitializer: { streamChannel in
                streamChannel.pipeline.eventLoop.makeCompletedFuture {
                    try streamChannel.pipeline.syncOperations.addHandler(
                        SyncSignalHandler(receivedPromise: serverReceivedSyncPromise)
                    )
                }
            },
            noMoreConnections: {
                noMoreConnectionsPromise.succeed()
            }
        ).get()
        let serverPort = serverChannel.localAddress!.port!

        let clientChannel = try await createClientChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: loggers.clientLogger
        ).get()

        let (clientConnectionChannel, streamCreator) = try await connectOutbound(
            clientChannel,
            host: host,
            port: serverPort
        ) { connectionChannel, _ in
            connectionChannel.eventLoop.makeCompletedFuture {
                try connectionChannel.pipeline.syncOperations.addHandler(
                    DatagramCapture(onDatagram: { clientReceivedEchoPromise.succeed($0) })
                )
            }
        }

        // Open a tiny synchronization stream first. Once the server reads it, the connection was established.
        try await performSyncHandshake(streamCreator, signal: syncSignal, serverReceived: serverReceivedSyncPromise)

        clientConnectionChannel.writeAndFlush(payload, promise: nil)

        let serverReceived = try await serverReceivedPromise.futureResult.get()
        #expect(serverReceived == payload)

        let clientReceived = try await clientReceivedEchoPromise.futureResult.get()
        #expect(clientReceived == payload)

        try await serverChannel.close()
        try await noMoreConnectionsPromise.futureResult.get()
    }

    @available(anyAppleOS 26, *)
    @Test
    func datagramsSentBeforeConnectionEstablishmentAreBuffered() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup.singleton
        let loggers = getChannelLoggers()
        let host = "127.0.0.1"
        let payload = ByteBuffer(string: "server speaks first")

        let noMoreConnectionsPromise = eventLoopGroup.any().makePromise(of: Void.self)
        let clientReceivedPromise = eventLoopGroup.any().makePromise(of: ByteBuffer.self)

        let datagramSentPromise = eventLoopGroup.any().makePromise(of: Void.self)

        let serverChannel = try await createServerChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: loggers.serverLogger,
            inboundConnectionInitializer: { connectionChannel, streamCreator in
                connectionChannel.eventLoop.makeCompletedFuture {
                    try connectionChannel.pipeline.syncOperations.addHandler(
                        DirectlySendADatagramHandler(datagramPayload: payload, eventLoopPromise: datagramSentPromise)
                    )
                }
            },
            inboundStreamInitializer: { streamChannel in
                streamChannel.eventLoop.makeSucceededVoidFuture()
            },
            noMoreConnections: {
                noMoreConnectionsPromise.succeed()
            }
        ).get()
        let serverPort = serverChannel.localAddress!.port!

        let clientChannel = try await createClientChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: loggers.clientLogger
        ).get()

        _ = try await connectOutbound(clientChannel, host: host, port: serverPort) { connectionChannel, _ in
            connectionChannel.eventLoop.makeCompletedFuture {
                try connectionChannel.pipeline.syncOperations.addHandler(
                    DatagramCapture(onDatagram: { clientReceivedPromise.succeed($0) })
                )
            }
        }

        try await datagramSentPromise.futureResult.get()

        let clientReceived = try await clientReceivedPromise.futureResult.get()
        #expect(clientReceived == payload)

        try await serverChannel.close()
        try await noMoreConnectionsPromise.futureResult.get()
    }

    @available(anyAppleOS 26, *)
    @Test
    func streamRoundTripWithDatagramsEnabled() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup.singleton
        let loggers = getChannelLoggers()
        let host = "127.0.0.1"
        let request = ByteBuffer(string: "GET /foo")
        let response = ByteBuffer(string: "<b>Success</b>")

        let noMoreConnectionsPromise = eventLoopGroup.any().makePromise(of: Void.self)
        let serverReceivedPromise = eventLoopGroup.any().makePromise(of: ByteBuffer.self)
        let clientReceivedPromise = eventLoopGroup.any().makePromise(of: ByteBuffer.self)

        let serverChannel = try await createServerChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: loggers.serverLogger,
            inboundConnectionInitializer: { connectionChannel, streamCreator in
                connectionChannel.eventLoop.makeSucceededVoidFuture()
            },
            inboundStreamInitializer: { streamChannel in
                streamChannel.pipeline.eventLoop.makeCompletedFuture {
                    try streamChannel.pipeline.syncOperations.addHandler(
                        StreamEchoHandler(
                            expectedRequest: request,
                            response: response,
                            receivedPromise: serverReceivedPromise
                        )
                    )
                }
            },
            noMoreConnections: {
                noMoreConnectionsPromise.succeed()
            }
        ).get()
        let serverPort = serverChannel.localAddress!.port!

        let clientChannel = try await createClientChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: loggers.clientLogger
        ).get()

        let (_, streamCreator) = try await connectOutbound(clientChannel, host: host, port: serverPort)

        let streamChannel = try await streamCreator.createBidirectionalStream { streamInitializer in
            streamInitializer.channel.eventLoop.makeCompletedFuture {
                try streamInitializer.channel.pipeline.syncOperations.addHandler(
                    ResponseCapture(response: response, receivedPromise: clientReceivedPromise)
                )
                streamInitializer.channel.writeAndFlush(request, promise: nil)
                return streamInitializer.channel
            }
        }.get()

        try await streamChannel.closeFuture.get()

        let serverReceived = try await serverReceivedPromise.futureResult.get()
        #expect(serverReceived == request)

        let clientReceived = try await clientReceivedPromise.futureResult.get()
        #expect(clientReceived == response)

        try await serverChannel.close()
        try await noMoreConnectionsPromise.futureResult.get()
    }

    @available(anyAppleOS 26, *)
    @Test
    func unidirectionalDatagramAdvertisement() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup.singleton
        let loggers = getChannelLoggers()
        let host = "127.0.0.1"
        let syncSignal = ByteBuffer(string: "ready")
        let clientToServer = ByteBuffer(string: "client to server datagram")
        let serverToClient = ByteBuffer(string: "server to client datagram")

        let serverGotSync = eventLoopGroup.any().makePromise(of: Void.self)

        let serverConnectionChannelPromise = eventLoopGroup.any().makePromise(of: (any Channel).self)
        let serverReceivedClientToServer = eventLoopGroup.any().makePromise(of: Void.self)
        // Expected to stay empty.
        let clientReceivedDatagrams = NIOLockedValueBox<[ByteBuffer]>([])
        // No errors are expected on either connection channel.
        let caughtErrors = NIOLockedValueBox<[String]>([])

        // Server advertises datagram support (.max); client advertises none (0).
        let serverChannel = try await createServerChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: loggers.serverLogger,
            maxDatagramFrameSize: .max,
            inboundConnectionInitializer: { connectionChannel, streamCreator in
                connectionChannel.eventLoop.makeCompletedFuture {
                    try connectionChannel.pipeline.syncOperations.addHandler(
                        ErrorRecordingHandler(errors: caughtErrors)
                    )
                    try connectionChannel.pipeline.syncOperations.addHandler(
                        DatagramCapture(onDatagram: { buffer in
                            if buffer == clientToServer {
                                serverReceivedClientToServer.succeed()
                            }
                        })
                    )
                    serverConnectionChannelPromise.succeed(connectionChannel)
                }
            },
            inboundStreamInitializer: { streamChannel in
                streamChannel.eventLoop.makeCompletedFuture {
                    try streamChannel.pipeline.syncOperations.addHandler(
                        SyncSignalHandler(receivedPromise: serverGotSync)
                    )
                }
            },
            noMoreConnections: {}
        ).get()
        let serverPort = serverChannel.localAddress!.port!

        let clientChannel = try await createClientChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: loggers.clientLogger,
            maxDatagramFrameSize: 0
        ).get()

        let clientConnectionClosed = NIOLockedValueBox<Bool>(false)
        let (clientConnectionChannel, streamCreator) = try await connectOutbound(
            clientChannel,
            host: host,
            port: serverPort
        ) { connectionChannel, _ in
            connectionChannel.eventLoop.makeCompletedFuture {
                try connectionChannel.pipeline.syncOperations.addHandler(ErrorRecordingHandler(errors: caughtErrors))
                try connectionChannel.pipeline.syncOperations.addHandler(
                    DatagramCapture(onDatagram: { buffer in
                        clientReceivedDatagrams.withLockedValue { $0.append(buffer) }
                    })
                )
            }
        }

        // Establish the connection with a tiny sync stream before sending datagrams.
        try await performSyncHandshake(streamCreator, signal: syncSignal, serverReceived: serverGotSync)

        // The connection must stay open for this test; record it if it ever closes.
        clientConnectionChannel.closeFuture.whenComplete { _ in
            clientConnectionClosed.withLockedValue { $0 = true }
        }

        // client -> server: allowed, because the server advertised support.
        clientConnectionChannel.writeAndFlush(clientToServer, promise: nil)
        try await serverReceivedClientToServer.futureResult.get()

        // server -> client: dropped on send, because the client advertised 0.
        let serverConnectionChannel = try await serverConnectionChannelPromise.futureResult.get()
        serverConnectionChannel.writeAndFlush(serverToClient, promise: nil)

        // Give the datagram a chance to (not) arrive; the connection should stay open.
        try await Task.sleep(for: .seconds(2))

        let clientReceived = clientReceivedDatagrams.withLockedValue { $0 }
        #expect(
            clientReceived.isEmpty,
            "server must not deliver datagrams to the client (client advertised 0)"
        )
        #expect(
            !clientConnectionClosed.withLockedValue { $0 },
            "the connection should stay open when the server sends toward a peer that advertised 0"
        )
        let errors = caughtErrors.withLockedValue { $0 }
        #expect(errors.isEmpty, "unexpected error(s) on a connection channel: \(errors)")

        try await serverChannel.close()
    }

    /// SwiftNetwork silently drops outbound datagrams larger than the size advertised by the peer..
    @available(anyAppleOS 26, *)
    @Test
    func oversizedDatagramIsDroppedAndConnectionStaysOpen() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup.singleton
        let loggers = getChannelLoggers()
        let host = "127.0.0.1"
        let syncSignal = ByteBuffer(string: "ready")

        // The server accepts DATAGRAM frames up to 256 bytes. The oversized payload clearly exceeds
        // that while still fitting a single QUIC packet; the small payload is comfortably under it.
        let serverMaxDatagramFrameSize = 256
        let oversizedPayload = ByteBuffer(repeating: UInt8(ascii: "x"), count: 800)
        let smallPayload = ByteBuffer(string: "small datagram")

        let serverGotSync = eventLoopGroup.any().makePromise(of: Void.self)
        // The oversized datagram is not expected to arrive.
        let serverReceivedDatagrams = NIOLockedValueBox<[ByteBuffer]>([])
        let clientReceivedSmall = eventLoopGroup.any().makePromise(of: Void.self)
        // No errors are expected on either connection channel.
        let caughtErrors = NIOLockedValueBox<[String]>([])

        let serverChannel = try await createServerChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: loggers.serverLogger,
            maxDatagramFrameSize: serverMaxDatagramFrameSize,
            inboundConnectionInitializer: { connectionChannel, streamCreator in
                connectionChannel.eventLoop.makeCompletedFuture {
                    try connectionChannel.pipeline.syncOperations.addHandler(
                        ErrorRecordingHandler(errors: caughtErrors)
                    )
                    try connectionChannel.pipeline.syncOperations.addHandler(
                        DatagramCapture(onDatagram: { buffer in
                            serverReceivedDatagrams.withLockedValue { $0.append(buffer) }
                            // Echo it back so the client can prove the connection still works.
                            connectionChannel.writeAndFlush(buffer, promise: nil)
                        })
                    )
                }
            },
            inboundStreamInitializer: { streamChannel in
                streamChannel.eventLoop.makeCompletedFuture {
                    try streamChannel.pipeline.syncOperations.addHandler(
                        SyncSignalHandler(receivedPromise: serverGotSync)
                    )
                }
            },
            noMoreConnections: {}
        ).get()
        let serverPort = serverChannel.localAddress!.port!

        let clientChannel = try await createClientChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: loggers.clientLogger
        ).get()

        let (clientConnectionChannel, streamCreator) = try await connectOutbound(
            clientChannel,
            host: host,
            port: serverPort
        ) { connectionChannel, _ in
            connectionChannel.eventLoop.makeCompletedFuture {
                try connectionChannel.pipeline.syncOperations.addHandler(ErrorRecordingHandler(errors: caughtErrors))
                try connectionChannel.pipeline.syncOperations.addHandler(
                    DatagramCapture(onDatagram: { buffer in
                        if buffer == smallPayload {
                            clientReceivedSmall.succeed()
                        }
                    })
                )
            }
        }

        // Establish the connection with a tiny sync stream before sending datagrams.
        try await performSyncHandshake(streamCreator, signal: syncSignal, serverReceived: serverGotSync)

        // Send the oversized datagram first, then a small one. Our `write` buffers and succeeds for
        // both (they are below the hardcoded `.max` guard); SwiftNetwork drops the oversized one at
        // packetization while the small one goes through.
        clientConnectionChannel.writeAndFlush(oversizedPayload, promise: nil)
        clientConnectionChannel.writeAndFlush(smallPayload, promise: nil)

        // The connection is proven alive if the small datagram round-trips (client -> server ->
        // client) after the oversized one was dropped.
        try await clientReceivedSmall.futureResult.get()

        let serverReceived = serverReceivedDatagrams.withLockedValue { $0 }
        #expect(
            !serverReceived.contains(oversizedPayload),
            "oversized datagram must be dropped on send, not delivered to the server"
        )
        #expect(
            serverReceived.contains(smallPayload),
            "server should receive the small datagram"
        )
        let errors = caughtErrors.withLockedValue { $0 }
        #expect(errors.isEmpty, "unexpected error(s) on a connection channel: \(errors)")

        try await serverChannel.close()
    }
}

/// Opens an outbound QUIC connection on `clientChannel` to `host:port`, running
/// `connectionInitializer` on the connection channel. The client never accepts inbound streams in
/// these tests.
@available(anyAppleOS 26, *)
func connectOutbound(
    _ clientChannel: any Channel,
    host: String,
    port: Int,
    connectionInitializer: @escaping @Sendable (any Channel, QUICStreamCreator) -> EventLoopFuture<Void> = {
        connectionChannel,
        _ in connectionChannel.eventLoop.makeSucceededVoidFuture()
    }
) async throws -> (any Channel, QUICStreamCreator) {
    try await clientChannel.pipeline.handler(type: QUICHandler.self).flatMap { quicHandler in
        quicHandler.createOutboundConnection(
            serverName: "\(host):\(port)",
            remoteAddress: try! .init(ipAddress: host, port: port),
            connectionInitializer: connectionInitializer,
            inboundStreamInitializer: { _ in fatalError() }
        )
    }.get()
}

/// Opens a bidirectional stream, writes `signal`, and waits for the server to report receipt via
/// `serverReceived` — a lightweight handshake proving the connection reached the connected state.
@available(anyAppleOS 26, *)
func performSyncHandshake(
    _ streamCreator: QUICStreamCreator,
    signal: ByteBuffer,
    serverReceived: EventLoopPromise<Void>
) async throws {
    let stream = try await streamCreator.createBidirectionalStream { streamInitializer in
        streamInitializer.channel.eventLoop.makeSucceededFuture(streamInitializer.channel)
    }.get()
    stream.writeAndFlush(signal, promise: nil)
    try await serverReceived.futureResult.get()
}

/// Succeeds `receivedPromise` on the first byte of stream data. Used purely as a
/// synchronization signal to prove a connection has reached the connected state.
@available(anyAppleOS 26, *)
private final class SyncSignalHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let receivedPromise: EventLoopPromise<Void>

    init(receivedPromise: EventLoopPromise<Void>) {
        self.receivedPromise = receivedPromise
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.receivedPromise.succeed()
    }
}

/// Captures inbound QUIC datagrams delivered on the connection channel.
@available(anyAppleOS 26, *)
final class DatagramCapture: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    let onDatagram: @Sendable (ByteBuffer) -> Void

    init(onDatagram: @escaping @Sendable (ByteBuffer) -> Void) {
        self.onDatagram = onDatagram
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = self.unwrapInboundIn(data)
        self.onDatagram(buffer)
    }
}

/// Sets `flag` to `true` on the first stream read.
@available(anyAppleOS 26, *)
private final class StreamReadFlagHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let flag: NIOLockedValueBox<Bool>

    init(flag: NIOLockedValueBox<Bool>) {
        self.flag = flag
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.flag.withLockedValue { $0 = true }
    }
}

/// Echoes a fixed response once it has received the expected request bytes,
/// then closes the stream.
@available(anyAppleOS 26, *)
private final class StreamEchoHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let expectedRequest: ByteBuffer
    private let response: ByteBuffer
    private let receivedPromise: EventLoopPromise<ByteBuffer>
    private var requestBuffer = ByteBuffer()

    init(expectedRequest: ByteBuffer, response: ByteBuffer, receivedPromise: EventLoopPromise<ByteBuffer>) {
        self.expectedRequest = expectedRequest
        self.response = response
        self.receivedPromise = receivedPromise
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.requestBuffer.writeImmutableBuffer(self.unwrapInboundIn(data))
        guard self.requestBuffer.readableBytes >= self.expectedRequest.readableBytes else {
            return
        }
        self.receivedPromise.succeed(self.requestBuffer)
        context.writeAndFlush(self.wrapOutboundOut(self.response), promise: nil)
        context.close(mode: .output, promise: nil)
    }
}

/// Captures the first response buffer received on a request stream.
@available(anyAppleOS 26, *)
private final class ResponseCapture: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private let response: ByteBuffer
    private let receivedPromise: EventLoopPromise<ByteBuffer>
    private var responseBuffer = ByteBuffer()

    init(response: ByteBuffer, receivedPromise: EventLoopPromise<ByteBuffer>) {
        self.response = response
        self.receivedPromise = receivedPromise
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.responseBuffer.writeImmutableBuffer(self.unwrapInboundIn(data))
        guard self.responseBuffer.readableBytes >= self.response.readableBytes else {
            return
        }
        self.receivedPromise.succeed(self.responseBuffer)
        context.close(promise: nil)
    }
}

@available(anyAppleOS 26, *)
private final class DirectlySendADatagramHandler: ChannelOutboundHandler {
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let datagramPayload: ByteBuffer
    private let eventLoopPromise: EventLoopPromise<Void>

    init(datagramPayload: ByteBuffer, eventLoopPromise: EventLoopPromise<Void>) {
        self.datagramPayload = datagramPayload
        self.eventLoopPromise = eventLoopPromise
    }

    func handlerAdded(context: ChannelHandlerContext) {
        let buffer = self.wrapOutboundOut(self.datagramPayload)
        context.writeAndFlush(buffer, promise: self.eventLoopPromise)
    }
}

/// Records any error that reaches it (thread-safe) and forwards it. `Issue.record` can't be used
/// from here — `errorCaught` runs on an event-loop thread outside the test's task, so under the
/// concurrent test execution our CI uses the issue would misattribute. Tests assert on the recorded
/// errors from their own task instead.
@available(anyAppleOS 26, *)
private final class ErrorRecordingHandler: ChannelInboundHandler {
    typealias InboundIn = Any

    let errors: NIOLockedValueBox<[String]>

    init(errors: NIOLockedValueBox<[String]>) {
        self.errors = errors
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        self.errors.withLockedValue { $0.append("\(error)") }
        context.fireErrorCaught(error)
    }
}
