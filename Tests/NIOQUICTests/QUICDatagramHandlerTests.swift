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
import NIOEmbedded
import Testing

@testable import NIOQUIC

/// Unit tests for `QUICDatagramHandler` driven through the `setTestBackend` seam and a
/// `DatagramTestTransport`, so the negotiation state machine and datagram passthrough can be
/// exercised in an `EmbeddedChannel` without a real SwiftNetwork flow.
@Suite
struct QUICDatagramHandlerTests {

    // MARK: - Buffering before the peer advertisement

    @available(anyAppleOS 26, *)
    @Test("Writes buffered before the peer advertisement flush once the peer accepts")
    func bufferedWritesFlushWhenPeerAccepts() throws {
        try Self.withHandler { channel, handler, transport, _ in
            let first = ByteBuffer(string: "first")
            let second = ByteBuffer(string: "second")

            // No backend installed yet: both writes are held, not handed to any transport.
            let firstResult = Self.write(channel, first)
            let secondResult = Self.write(channel, second)
            #expect(Self.outcome(firstResult) == nil)
            #expect(Self.outcome(secondResult) == nil)
            #expect(transport.writtenDatagrams.isEmpty)

            // Peer advertises a limit that fits both: the buffered writes flush in order and succeed.
            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 100)
            #expect(transport.writtenDatagrams == [first, second])
            #expect(Self.succeeded(firstResult))
            #expect(Self.succeeded(secondResult))
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Writes buffered before the peer advertisement fail when the peer advertises 0")
    func bufferedWritesFailWhenPeerRejects() throws {
        try Self.withHandler { channel, handler, transport, _ in
            let result = Self.write(channel, ByteBuffer(string: "buffered"))
            #expect(Self.outcome(result) == nil)

            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 0)
            #expect(Self.failedQUICError(result) == .peerDoesNotAcceptDatagrams)
            #expect(transport.writtenDatagrams.isEmpty)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A buffered write larger than the advertised limit fails as datagramTooLarge")
    func bufferedOversizedWriteFailsAsTooLarge() throws {
        try Self.withHandler { channel, handler, transport, _ in
            // Buffered while waiting, then the peer accepts a smaller limit than this datagram.
            let result = Self.write(channel, ByteBuffer(repeating: UInt8(ascii: "x"), count: 200))
            #expect(Self.outcome(result) == nil)

            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 100)
            // Peer *does* accept datagrams, this one just does not fit: distinct from "not accepted".
            #expect(Self.failedQUICError(result) == .datagramTooLarge)
            #expect(transport.writtenDatagrams.isEmpty)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A buffered write exactly at the advertised limit is accepted")
    func bufferedWriteAtExactLimitSucceeds() throws {
        try Self.withHandler { channel, handler, transport, _ in
            // The size check is `<=` and payload-only (ignores frame overhead), so a payload equal to
            // the advertised limit is accepted here; the transport enforces the exact framed size.
            let payload = ByteBuffer(repeating: UInt8(ascii: "x"), count: 100)
            let result = Self.write(channel, payload)
            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 100)
            #expect(Self.succeeded(result))
            #expect(transport.writtenDatagrams == [payload])
        }
    }

    // MARK: - Steady-state writes (peer advertisement already known)

    @available(anyAppleOS 26, *)
    @Test("A write within the advertised limit is handed to the transport and succeeds")
    func writeWithinLimitSucceeds() throws {
        try Self.withHandler { channel, handler, transport, _ in
            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 100)

            let payload = ByteBuffer(string: "datagram")
            let result = Self.write(channel, payload)
            #expect(Self.succeeded(result))
            #expect(transport.writtenDatagrams == [payload])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A write larger than the advertised limit fails as datagramTooLarge and is not sent")
    func oversizedWriteFailsAsTooLarge() throws {
        try Self.withHandler { channel, handler, transport, _ in
            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 100)

            let result = Self.write(channel, ByteBuffer(repeating: UInt8(ascii: "x"), count: 200))
            #expect(Self.failedQUICError(result) == .datagramTooLarge)
            #expect(transport.writtenDatagrams.isEmpty)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A write exactly at the advertised limit is accepted")
    func writeAtExactLimitSucceeds() throws {
        try Self.withHandler { channel, handler, transport, _ in
            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 100)

            // `<=`, payload-only (ignores frame overhead): a payload equal to the advertised limit is
            // accepted here; the transport enforces the exact framed size.
            let payload = ByteBuffer(repeating: UInt8(ascii: "x"), count: 100)
            let result = Self.write(channel, payload)
            #expect(Self.succeeded(result))
            #expect(transport.writtenDatagrams == [payload])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A within-limit write the transport rejects fails as datagramWriteFailed")
    func transportRejectionFails() throws {
        try Self.withHandler { channel, handler, transport, _ in
            transport.writeResult = false
            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 100)

            // The datagram passes the size check, so it is handed to the transport, which rejects it.
            let payload = ByteBuffer(string: "datagram")
            let result = Self.write(channel, payload)
            #expect(Self.failedQUICError(result) == .datagramWriteFailed)
            #expect(transport.writtenDatagrams == [payload])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A write fails as peerDoesNotAcceptDatagrams when the peer advertised 0")
    func writeFailsWhenPeerRejects() throws {
        try Self.withHandler { channel, handler, transport, _ in
            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 0)

            let result = Self.write(channel, ByteBuffer(string: "datagram"))
            #expect(Self.failedQUICError(result) == .peerDoesNotAcceptDatagrams)
            #expect(transport.writtenDatagrams.isEmpty)
        }
    }

    // MARK: - Inbound

    @available(anyAppleOS 26, *)
    @Test("An inbound datagram from the transport is fired as a channelRead")
    func inboundDatagramIsDeliveredAsChannelRead() throws {
        try Self.withHandler { _, handler, transport, recorder in
            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 100)

            let payload = ByteBuffer(string: "inbound")
            transport.deliverInbound(payload)
            #expect(recorder.reads == [payload])
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A transport error is fired as an errorCaught")
    func transportErrorIsFiredAsErrorCaught() throws {
        try Self.withHandler { _, handler, transport, recorder in
            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 100)

            transport.deliverError(TestError())
            #expect(recorder.errors.count == 1)
            #expect(recorder.errors.first is TestError)
        }
    }

    // MARK: - Teardown

    @available(anyAppleOS 26, *)
    @Test("Removing the handler while still waiting fails buffered writes")
    func handlerRemovalFailsBufferedWrites() throws {
        try Self.withHandler { channel, _, _, _ in
            // Buffered while waiting, no backend ever installed.
            let result = Self.write(channel, ByteBuffer(string: "buffered"))
            #expect(Self.outcome(result) == nil)

            // `close0` defers `removeHandlers` (and thus `handlerRemoved`) via `eventLoop.execute`,
            // so drain the loop to make it run before asserting. The harness's later `finish()` then
            // throws `.alreadyClosed` (channel is closed), which its `try?` swallows.
            channel.close(promise: nil)
            (channel.eventLoop as! EmbeddedEventLoop).run()
            #expect(Self.failedChannelError(result) == .ioOnClosedChannel)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("Removing the handler after a backend is installed closes the transport")
    func handlerRemovalClosesTransport() throws {
        try Self.withHandler { channel, handler, transport, _ in
            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 100)

            channel.close(promise: nil)
            (channel.eventLoop as! EmbeddedEventLoop).run()
            #expect(transport.closeCount == 1)
        }
    }

    @available(anyAppleOS 26, *)
    @Test("A channel flush propagates to the transport")
    func flushPropagatesToTransport() throws {
        try Self.withHandler { channel, handler, transport, _ in
            handler.setTestBackend(to: transport, peerMaxDatagramFrameSize: 100)

            let flushesBefore = transport.flushCount
            _ = Self.write(channel, ByteBuffer(string: "datagram"))
            #expect(transport.flushCount > flushesBefore)
        }
    }
}

// MARK: - Test harness

@available(anyAppleOS 26, *)
extension QUICDatagramHandlerTests {
    /// Builds an `EmbeddedChannel` with a `QUICDatagramHandler` under test and a downstream
    /// `DatagramRecorder`, plus a `DatagramTestTransport` the body installs via `setTestBackend`.
    static func withHandler(
        role: Role = .client,
        body: (EmbeddedChannel, QUICDatagramHandler, DatagramTestTransport, DatagramRecorder) throws -> Void
    ) throws {
        let channel = EmbeddedChannel()
        let handler = QUICDatagramHandler(role: role, logger: Logger(label: "test"))
        let recorder = DatagramRecorder()
        try channel.pipeline.syncOperations.addHandlers([handler, recorder])
        let transport = DatagramTestTransport()
        defer { _ = try? channel.finish() }
        try body(channel, handler, transport, recorder)
    }

    /// Writes and flushes `buffer` on the channel, returning the write's future so the test can
    /// inspect whether it succeeded, failed, or is still buffered.
    static func write(_ channel: EmbeddedChannel, _ buffer: ByteBuffer) -> EventLoopFuture<Void> {
        let promise = channel.eventLoop.makePromise(of: Void.self)
        channel.writeAndFlush(buffer, promise: promise)
        return promise.futureResult
    }

    /// The resolved result of `future`, or `nil` if it is still pending. Safe to read synchronously
    /// because `EmbeddedChannel` runs the pipeline single-threaded and in-line, so the datagram
    /// handler's promises are resolved before this returns, and `whenComplete` on an already-resolved
    /// future fires immediately. This would be racy against a real threaded `EventLoopGroup`.
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
/// datagrams and errors back through the reader the handler installs.
@available(anyAppleOS 26, *)
final class DatagramTestTransport: QUICDatagramProtocol {
    private(set) var writtenDatagrams: [ByteBuffer] = []
    private(set) var flushCount = 0
    private(set) var closeCount = 0
    private var reader: (any QUICDatagramReaderProtocol)?

    /// What `write(datagram:)` returns. Flip to `false` to simulate the transport rejecting a
    /// datagram that passed the size check.
    var writeResult = true

    func write(datagram: ByteBuffer) -> Bool {
        self.writtenDatagrams.append(datagram)
        return self.writeResult
    }

    func flush() {
        self.flushCount += 1
    }

    func close() {
        self.closeCount += 1
        // Mirror the production transport: dropping the reader breaks the handler <-> transport cycle.
        self.reader = nil
    }

    func setReader(_ reader: any QUICDatagramReaderProtocol) {
        self.reader = reader
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

/// Records the inbound datagrams and errors the handler fires down the pipeline.
final class DatagramRecorder: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    var reads: [ByteBuffer] = []
    var errors: [any Error] = []

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.reads.append(self.unwrapInboundIn(data))
        context.fireChannelRead(data)
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        self.errors.append(error)
        context.fireErrorCaught(error)
    }
}

private struct TestError: Error {}
