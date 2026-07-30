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
import Synchronization
import Testing

@testable import NIOQUIC

/// Unit tests for the connection channel's datagram (RFC 9221) I/O: the negotiation state machine
/// over a `RecordingConnection`, and the connection-to-transport seam over a real
/// `SwiftNetworkQUICConnection` with a `DatagramTestTransport`.
@Suite
struct QUICConnectionChannelDatagramTests {

    // MARK: - Buffering before the peer advertisement

    @available(anyAppleOS 26, *)
    @Test("Writes buffered before the peer advertisement flush once the peer accepts")
    func bufferedWritesFlushWhenPeerAccepts() throws {
        try Self.withChannel { channel, connection, _ in
            let first = ByteBuffer(string: "first")
            let second = ByteBuffer(string: "second")

            // The handshake hasn't completed yet: both writes are held, not handed to the connection.
            let firstResult = Self.write(channel, first)
            let secondResult = Self.write(channel, second)
            #expect(Self.outcome(firstResult) == nil)
            #expect(Self.outcome(secondResult) == nil)
            #expect(connection.writtenDatagrams.isEmpty)

            // Peer advertises a limit that fits both: the buffered writes flush in order and succeed.
            channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 100)
            #expect(connection.writtenDatagrams == [first, second])
            #expect(Self.succeeded(firstResult))
            #expect(Self.succeeded(secondResult))
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Writes buffered before the peer advertisement fail when the peer advertises 0")
    func bufferedWritesFailWhenPeerRejects() throws {
        try Self.withChannel { channel, connection, _ in
            let result = Self.write(channel, ByteBuffer(string: "buffered"))
            #expect(Self.outcome(result) == nil)

            channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 0)
            #expect(Self.failedQUICError(result) == .peerDoesNotAcceptDatagrams)
            #expect(connection.writtenDatagrams.isEmpty)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A buffered write larger than the advertised limit fails as datagramTooLarge")
    func bufferedOversizedWriteFailsAsTooLarge() throws {
        try Self.withChannel { channel, connection, _ in
            // Buffered while waiting, then the peer accepts a smaller limit than this datagram.
            let result = Self.write(channel, ByteBuffer(repeating: UInt8(ascii: "x"), count: 200))
            #expect(Self.outcome(result) == nil)

            channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 100)
            // Peer *does* accept datagrams, this one just does not fit: distinct from "not accepted".
            #expect(Self.failedQUICError(result) == .datagramTooLarge)
            #expect(connection.writtenDatagrams.isEmpty)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A buffered write exactly at the advertised limit is accepted")
    func bufferedWriteAtExactLimitSucceeds() throws {
        try Self.withChannel { channel, connection, _ in
            // The size check is `<=` and payload-only, so a payload equal to the advertised limit
            // is accepted here.
            let payload = ByteBuffer(repeating: UInt8(ascii: "x"), count: 100)
            let result = Self.write(channel, payload)
            channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 100)
            #expect(Self.succeeded(result))
            #expect(connection.writtenDatagrams == [payload])
        }
    }

    // MARK: - Steady-state writes (peer advertisement already known)

    @available(anyAppleOS 26, *)
    @Test("A write within the advertised limit is handed to the connection and succeeds")
    func writeWithinLimitSucceeds() throws {
        try Self.withChannel { channel, connection, _ in
            channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 100)

            let payload = ByteBuffer(string: "datagram")
            let result = Self.write(channel, payload)
            #expect(Self.succeeded(result))
            #expect(connection.writtenDatagrams == [payload])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A write larger than the advertised limit fails as datagramTooLarge and is not sent")
    func oversizedWriteFailsAsTooLarge() throws {
        try Self.withChannel { channel, connection, _ in
            channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 100)

            let result = Self.write(channel, ByteBuffer(repeating: UInt8(ascii: "x"), count: 200))
            #expect(Self.failedQUICError(result) == .datagramTooLarge)
            #expect(connection.writtenDatagrams.isEmpty)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A write exactly at the advertised limit is accepted")
    func writeAtExactLimitSucceeds() throws {
        try Self.withChannel { channel, connection, _ in
            channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 100)

            // `<=` and payload-only: a payload equal to the advertised limit is accepted.
            let payload = ByteBuffer(repeating: UInt8(ascii: "x"), count: 100)
            let result = Self.write(channel, payload)
            #expect(Self.succeeded(result))
            #expect(connection.writtenDatagrams == [payload])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A within-limit write the connection rejects fails as datagramWriteFailed")
    func connectionRejectionFails() throws {
        try Self.withChannel { channel, connection, _ in
            connection.writeDatagramResult = false
            channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 100)

            // The datagram passes the size check, so it is handed to the connection, which rejects it.
            let result = Self.write(channel, ByteBuffer(string: "datagram"))
            #expect(Self.failedQUICError(result) == .datagramWriteFailed)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A write fails as peerDoesNotAcceptDatagrams when the peer advertised 0")
    func writeFailsWhenPeerRejects() throws {
        try Self.withChannel { channel, connection, _ in
            channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 0)

            let result = Self.write(channel, ByteBuffer(string: "datagram"))
            #expect(Self.failedQUICError(result) == .peerDoesNotAcceptDatagrams)
            #expect(connection.writtenDatagrams.isEmpty)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A channel flush propagates to the connection")
    func flushPropagatesToConnection() throws {
        try Self.withChannel { channel, connection, _ in
            channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 100)

            let flushesBefore = connection.datagramFlushCount
            _ = Self.write(channel, ByteBuffer(string: "datagram"))
            #expect(connection.datagramFlushCount > flushesBefore)
        }
    }

    // MARK: - Inbound

    @available(anyAppleOS 26, *)
    @Test("An inbound datagram from the connection is fired as a channelRead")
    func inboundDatagramIsDeliveredAsChannelRead() throws {
        try Self.withChannel { channel, _, recorder in
            let payload = ByteBuffer(string: "inbound")
            channel.connectionView.datagramRead(payload)
            #expect(recorder.reads == [payload])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A datagram flow error is fired as an errorCaught")
    func datagramErrorIsFiredAsErrorCaught() throws {
        try Self.withChannel { channel, _, recorder in
            channel.connectionView.datagramError(TestError())
            #expect(recorder.errors.count == 1)
            #expect(recorder.errors.first is TestError)
        }
    }

    // MARK: - Teardown

    @available(anyAppleOS 26, *)
    @Test("Closing the channel while still waiting fails buffered writes")
    func closeFailsBufferedWrites() throws {
        try Self.withChannel { channel, _, _ in
            // Buffered while waiting; the handshake never completes.
            let result = Self.write(channel, ByteBuffer(string: "buffered"))
            #expect(Self.outcome(result) == nil)

            channel.close(promise: nil)
            #expect(Self.failedChannelError(result) == .ioOnClosedChannel)
        }
    }

    // MARK: - Connection to transport

    @available(anyAppleOS 26, *)
    @Test("The connection refuses datagram writes until a transport is attached")
    func connectionWithoutTransportRefusesWrites() throws {
        try Self.withLiveConnection { connection, _, _ in
            #expect(!connection.writeDatagram(ByteBuffer(string: "datagram")))
            // Flushing without a transport is a no-op rather than a trap.
            connection.flushDatagrams()
        }
    }

    @available(anyAppleOS 26, *)
    @Test("The connection forwards writes and flushes to its datagram transport")
    func connectionForwardsWritesToTransport() throws {
        try Self.withLiveConnection { connection, transport, _ in
            connection.setDatagramTransport(.test(transport))

            let payload = ByteBuffer(string: "datagram")
            #expect(connection.writeDatagram(payload))
            #expect(transport.writtenDatagrams == [payload])
            #expect(transport.flushCount == 0)

            connection.flushDatagrams()
            #expect(transport.flushCount == 1)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A datagram the transport receives is fired as a channelRead on the connection channel")
    func transportInboundDatagramReachesThePipeline() throws {
        try Self.withLiveConnection { connection, transport, recorder in
            connection.setDatagramTransport(.test(transport))

            let payload = ByteBuffer(string: "inbound")
            transport.deliverInbound(payload)
            #expect(recorder.reads == [payload])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("An error the transport reports is fired as an errorCaught on the connection channel")
    func transportErrorReachesThePipeline() throws {
        try Self.withLiveConnection { connection, transport, recorder in
            connection.setDatagramTransport(.test(transport))

            transport.deliverError(TestError())
            #expect(recorder.errors.count == 1)
            #expect(recorder.errors.first is TestError)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Closing the connection closes its datagram transport")
    func connectionCloseClosesTransport() throws {
        try Self.withLiveConnection { connection, transport, _ in
            connection.setDatagramTransport(.test(transport))
            #expect(transport.closeCount == 0)

            _ = connection.close(
                isApplicationClose: false,
                errorCode: QUICTransportErrorCode.noError.rawValue,
                reason: ""
            )
            #expect(transport.closeCount == 1)

            // The transport is dropped with the rest of the connection's state, so later writes are
            // refused rather than handed to a detached flow.
            #expect(!connection.writeDatagram(ByteBuffer(string: "after close")))
        }
    }
}

// MARK: - Test harness

@available(anyAppleOS 26, *)
extension QUICConnectionChannelDatagramTests {
    /// Builds an initialized (but not yet active) server connection channel over a
    /// `RecordingConnection`, with a `DatagramRecorder` in its pipeline.
    static func withChannel(
        body: (QUICConnectionChannel, RecordingConnection, DatagramRecorder) throws -> Void
    ) throws {
        let connection = RecordingConnection()
        let recorder = DatagramRecorder()

        let channel = QUICConnectionChannel(
            udpChannel: EmbeddedChannel(),
            connection: .test(connection),
            registrar: .test(RecordingRegistrar()),
            transport: .test(RecordingTransport()),
            isServer: true
        )

        // A server channel's initializer promise completes without waiting for the handshake, so
        // the pipeline is set up while the channel is still negotiating.
        let promise = channel.eventLoop.makePromise(of: Void.self)
        channel.transportView.initialize(promise: promise) { $0.pipeline.addHandler(recorder) }
        try promise.futureResult.wait()

        try body(channel, connection, recorder)
    }

    /// Builds a real `SwiftNetworkQUICConnection` and the connection channel driving it, with a
    /// `DatagramRecorder` in the channel's pipeline and a `DatagramTestTransport` the body installs
    /// via `setDatagramTransport(_:)`. Exercises the connection <-> transport seam without a real
    /// SwiftNetwork datagram flow.
    static func withLiveConnection(
        body: (SwiftNetworkQUICConnection, DatagramTestTransport, DatagramRecorder) throws -> Void
    ) throws {
        let privateKeyPath = Bundle.module.url(forResource: "privateKey", withExtension: "der")!.path
        let publicKeyPath = Bundle.module.url(forResource: "publicKey", withExtension: "der")!.path

        var rng: any RandomNumberGenerator = SystemRandomNumberGenerator()
        let eventLoop = EmbeddedEventLoop()
        let udpChannel = EmbeddedChannel(loop: eventLoop)

        let connection = try SwiftNetworkQUICConnection.server(
            configuration: .server(
                serverName: "quic-test.local",
                authenticationConfiguration: .rawPublicKeys(
                    publicKeyFilePath: publicKeyPath,
                    privateKeyFilePath: privateKeyPath
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

        // Constructing the channel wires it into the connection via setDriver; the connection needs
        // it to hand inbound datagrams and flow errors to the pipeline.
        let channel = QUICConnectionChannel(
            udpChannel: udpChannel,
            connection: .live(connection),
            registrar: .test(RecordingRegistrar()),
            transport: .test(RecordingTransport()),
            isServer: true
        )

        let recorder = DatagramRecorder()
        let promise = eventLoop.makePromise(of: Void.self)
        channel.transportView.initialize(promise: promise) { $0.pipeline.addHandler(recorder) }
        try promise.futureResult.wait()

        try body(connection, DatagramTestTransport(), recorder)

        try udpChannel.close().wait()
    }

    /// Writes and flushes `buffer` on the channel, returning the write's future so the test can
    /// inspect whether it succeeded, failed, or is still buffered.
    static func write(_ channel: QUICConnectionChannel, _ buffer: ByteBuffer) -> EventLoopFuture<Void> {
        let promise = channel.eventLoop.makePromise(of: Void.self)
        channel.writeAndFlush(buffer, promise: promise)
        return promise.futureResult
    }

    /// The resolved result of `future`, or `nil` if it is still pending. Safe to read synchronously
    /// because the channel runs on an `EmbeddedEventLoop` single-threaded and in-line, so promises
    /// are resolved before this returns, and `whenComplete` on an already-resolved future fires
    /// immediately. This would be racy against a real threaded `EventLoopGroup`.
    static func outcome(_ future: EventLoopFuture<Void>) -> Result<Void, any Error>? {
        let box = NIOLockedValueBox<Result<Void, any Error>?>(nil)
        future.whenComplete { result in box.withLockedValue { $0 = result } }
        return box.withLockedValue { $0 }
    }

    static func succeeded(_ future: EventLoopFuture<Void>) -> Bool {
        if case .success = Self.outcome(future) { return true }
        return false
    }

    static func failedQUICError(_ future: EventLoopFuture<Void>) -> QUICError? {
        guard case .failure(let error) = Self.outcome(future) else { return nil }
        return error as? QUICError
    }

    static func failedChannelError(_ future: EventLoopFuture<Void>) -> ChannelError? {
        guard case .failure(let error) = Self.outcome(future) else { return nil }
        return error as? ChannelError
    }
}

/// A `QUICDatagramProtocol` test double: records outbound datagrams and lets tests inject inbound
/// datagrams and errors back through the reader the connection installs.
@available(anyAppleOS 26, *)
final class DatagramTestTransport: QUICDatagramProtocol {
    private(set) var writtenDatagrams: [ByteBuffer] = []
    private(set) var flushCount = 0
    private(set) var closeCount = 0
    private var reader: SwiftNetworkQUICConnection?

    func write(datagram: ByteBuffer) -> Bool {
        self.writtenDatagrams.append(datagram)
        return true
    }

    func flush() {
        self.flushCount += 1
    }

    func close() {
        self.closeCount += 1
        // Mirror the production transport: dropping the reader breaks the connection <-> transport
        // cycle.
        self.reader = nil
    }

    func setReader(connection: SwiftNetworkQUICConnection) {
        self.reader = connection
    }

    /// Test-only: simulate an inbound datagram arriving from the peer.
    func deliverInbound(_ datagram: ByteBuffer) {
        self.reader?.read(datagram: datagram)
    }

    /// Test-only: simulate the transport reporting an error.
    func deliverError(_ error: any Error) {
        self.reader?.error(error)
    }
}

/// Records the inbound datagrams and errors fired down the connection channel's pipeline.
///
/// The recorded state is held under a lock so the recorder can be captured by the `@Sendable`
/// pipeline-initializer closure and read back from the test body.
@available(anyAppleOS 26, *)
final class DatagramRecorder: ChannelInboundHandler, Sendable {
    typealias InboundIn = ByteBuffer

    private let _reads: Mutex<[ByteBuffer]> = Mutex([])
    private let _errors: Mutex<[any Error]> = Mutex([])

    var reads: [ByteBuffer] { self._reads.withLock { $0 } }
    var errors: [any Error] { self._errors.withLock { $0 } }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = self.unwrapInboundIn(data)
        self._reads.withLock { $0.append(buffer) }
        context.fireChannelRead(data)
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        self._errors.withLock { $0.append(error) }
        context.fireErrorCaught(error)
    }
}

private struct TestError: Error {}
