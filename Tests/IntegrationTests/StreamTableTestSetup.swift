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
import NIOPosix
import NIOQUICHelpers
import Testing

@testable import NIOQUIC

// MARK: - Pair

@available(anyAppleOS 26, *)
struct ConsumerPair<Client: QUICStreamConsumer, Server: QUICStreamConsumer>: Sendable {
    var clientConnection: QUICStreamConnection<Client>
    var clientChannel: any Channel
    var serverChannel: any Channel

    func close() async throws {
        do {
            try await self.serverChannel.close()
        } catch {
            try? await self.clientChannel.close()
            throw error
        }

        try await self.clientChannel.close()
    }

    func openClientStream(
        _ type: QUICStreamType = .clientInitiatedBidirectional,
        state: @autoclosure @escaping @Sendable () -> Client.StreamState
    ) async throws -> QUICStreamHandle {
        try await self._openClientStream(type, state: state(), writing: nil, fin: false)
    }

    func openClientStream(
        _ type: QUICStreamType = .clientInitiatedBidirectional,
        state: @autoclosure @escaping @Sendable () -> Client.StreamState,
        writing payload: ByteBuffer,
        fin: Bool
    ) async throws -> QUICStreamHandle {
        try await self._openClientStream(type, state: state(), writing: payload, fin: fin)
    }

    private func _openClientStream(
        _ type: QUICStreamType,
        state: @autoclosure @escaping @Sendable () -> Client.StreamState,
        writing payload: ByteBuffer?,
        fin: Bool
    ) async throws -> QUICStreamHandle {
        let handle = try await self.clientConnection.withStreams { streams in
            let handle = try streams.open(type, state: state())
            if let payload {
                try streams.withStream(handle: handle) { stream, _ in
                    stream.write(payload)
                    try stream.flush(fin: fin)
                }
            }
            return handle
        }.get()

        return try #require(handle)
    }

    func write(_ payload: ByteBuffer, to handle: QUICStreamHandle, fin: Bool = true) async throws {
        _ = try await self.clientConnection.withStreams { streams in
            try streams.withStream(handle: handle) { stream, _ in
                stream.write(payload)
                try stream.flush(fin: fin)
            }
        }.get()
    }
}

@available(anyAppleOS 26, *)
extension ConsumerPair where Client.StreamState == ByteBuffer {
    func openClientStream(
        _ type: QUICStreamType = .clientInitiatedBidirectional,
        writing payload: ByteBuffer,
        fin: Bool
    ) async throws -> QUICStreamHandle {
        try await self._openClientStream(type, state: ByteBuffer(), writing: payload, fin: fin)
    }
}

@available(anyAppleOS 26, *)
func withConsumerPair<
    Client: QUICStreamConsumer & Sendable,
    Server: QUICStreamConsumer & Sendable,
    Result
>(
    eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
    host: String = "127.0.0.1",
    maxIdleTimeout: Duration = .seconds(30),
    initialMaxStreamsBidi: Int = 8,
    initialMaxStreamsUni: Int = 8,
    client: Client,
    server: Server,
    onServerConnection: @escaping @Sendable (_ connection: QUICStreamConnection<Server>) -> Void = { _ in },
    _ body: (_ pair: ConsumerPair<Client, Server>) async throws -> Result
) async throws -> Result {
    let pair = try await makeConsumerPair(
        eventLoopGroup: eventLoopGroup,
        host: host,
        maxIdleTimeout: maxIdleTimeout,
        initialMaxStreamsBidi: initialMaxStreamsBidi,
        initialMaxStreamsUni: initialMaxStreamsUni,
        makeClientConsumer: { _ in client },
        makeServerConsumer: { connection in
            onServerConnection(connection)
            return server
        }
    )

    do {
        let value = try await body(pair)
        try await pair.close()
        return value
    } catch {
        try? await pair.close()
        throw error
    }
}

@available(anyAppleOS 26, *)
func makeConsumerPair<Client: QUICStreamConsumer, Server: QUICStreamConsumer>(
    eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
    host: String = "127.0.0.1",
    maxIdleTimeout: Duration = .seconds(30),
    initialMaxStreamsBidi: Int = 8,
    initialMaxStreamsUni: Int = 8,
    makeClientConsumer: @escaping @Sendable (_ connection: QUICStreamConnection<Client>) -> Client,
    makeServerConsumer: @escaping @Sendable (_ connection: QUICStreamConnection<Server>) -> Server
) async throws -> ConsumerPair<Client, Server> {
    let publicKeyPath = Bundle.module.url(forResource: "publicKey", withExtension: "der")!.path
    let privateKeyPath = Bundle.module.url(forResource: "privateKey", withExtension: "der")!.path

    let serverConfiguration = QUICConfiguration.server(
        serverName: "quic-test.local",
        authenticationConfiguration: .rawPublicKeys(
            publicKeyFilePath: publicKeyPath,
            privateKeyFilePath: privateKeyPath
        ),
        applicationProtocols: ["http/0.9"],
        maxIdleTimeout: maxIdleTimeout,
        initialMaxStreamsBidi: initialMaxStreamsBidi,
        initialMaxStreamsUni: initialMaxStreamsUni
    )

    let clientConfiguration = QUICConfiguration.client(
        verificationConfiguration: .rawPublicKeys(publicKeyFilePath: publicKeyPath),
        applicationProtocols: ["http/0.9"],
        maxIdleTimeout: maxIdleTimeout,
        initialMaxStreamsBidi: initialMaxStreamsBidi,
        initialMaxStreamsUni: initialMaxStreamsUni
    )

    let serverChannel = try await DatagramBootstrap(group: eventLoopGroup)
        .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
        .channelOption(ChannelOptions.maxMessagesPerRead, value: 32)
        .bind(host: host, port: 0)
        .flatMapThrowing { channel in
            let handler = QUICHandler<Server>(
                channel: channel,
                quicConfiguration: serverConfiguration,
                asyncVerifier: nil,
                authenticator: nil,
                logger: Logger(label: "Server"),
                makeConsumer: makeServerConsumer
            )
            try channel.pipeline.syncOperations.addHandler(handler)
            return channel
        }
        .get()

    let serverPort = serverChannel.localAddress!.port!

    do {
        let remoteAddress = try SocketAddress(ipAddress: host, port: serverPort)

        let clientChannel = try await DatagramBootstrap(group: eventLoopGroup)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelOption(ChannelOptions.maxMessagesPerRead, value: 32)
            .bind(host: host, port: 0)
            .flatMapThrowing { channel -> (any Channel) in
                let handler = QUICHandler<Client>(
                    channel: channel,
                    quicConfiguration: clientConfiguration,
                    asyncVerifier: nil,
                    authenticator: nil,
                    logger: Logger(label: "Client"),
                    makeConsumer: makeClientConsumer
                )
                try channel.pipeline.syncOperations.addHandler(handler)
                return channel
            }
            .get()

        let clientConnection = try await clientChannel.pipeline
            .handler(type: QUICHandler<Client>.self)
            .flatMap { handler in
                handler.createOutboundConnection(
                    serverName: "\(host):\(serverPort)",
                    remoteAddress: remoteAddress
                )
            }
            .flatMapErrorThrowing { error in
                clientChannel.close(promise: nil)
                throw error
            }
            .get()

        return ConsumerPair(
            clientConnection: clientConnection,
            clientChannel: clientChannel,
            serverChannel: serverChannel
        )
    } catch {
        try? await serverChannel.close()
        throw error
    }
}

/// Thrown by a signal which was never completed.
struct TimedOut: Error, CustomStringConvertible {
    var description: String
}

/// A promise for a consumer to succeed once something the test is waiting for has happened.
func makePromise<Value>(
    of: Value.Type = Value.self,
    timeout: TimeAmount,
    on eventLoop: any EventLoop = MultiThreadedEventLoopGroup.singleton.next(),
    function: String = #function,
    file: String = #file,
    line: UInt = #line
) -> EventLoopPromise<Value> {
    let promise = eventLoop.makePromise(of: Value.self)
    eventLoop.scheduleTask(in: timeout) {
        promise.fail(TimedOut(description: "Timed out waiting in \(function) (\(file):\(line))"))
    }
    return promise
}

// MARK: - Recording

/// Every visit a ``RecordConsumer`` made, in order.
@available(anyAppleOS 26, *)
final class StreamRecording: Sendable {
    /// One visit to one stream.
    struct Visit: Sendable {
        // Handle of the visited stream
        var handle: QUICStreamHandle
        // ID of the visited stream
        var id: QUICStreamID?
        /// Events shown to the stream
        var events: QUICStreamEvents
        /// Everything read from this stream up to and including this visit.
        var bytesReadSoFar: ByteBuffer
        /// What the last read of this visit returned, or `nil` if nothing was read.
        var readOutcome: QUICStreamReadOutcome?
        var closeError: (any Error)?
        var resetCode: QUICApplicationErrorCode?
        var stopSendingCode: QUICApplicationErrorCode?
        var isSendOpen: Bool
        var isReceiveOpen: Bool

        var closeErrorDescription: String? {
            self.closeError.map(String.init(describing:))
        }
    }

    private let _continuation: AsyncStream<Visit>.Continuation
    private let _visits: AsyncStream<Visit>

    init() {
        (self._visits, self._continuation) = AsyncStream<Visit>.makeStream()
    }

    /// Records a visit. Called on the connection's event loop.
    func record(_ visit: Visit) {
        self._continuation.yield(visit)
    }

    /// Every visit recorded up to and including the first one matching `predicate`.
    ///
    /// Waits indefinitely: a test which never sees its visit is ended by the suite's time limit.
    func visits(
        until predicate: @Sendable (_ visit: Visit) -> Bool,
    ) async -> [Visit] {
        var recorded: [Visit] = []

        for await visit in self._visits {
            recorded.append(visit)
            if predicate(visit) {
                break
            }
        }

        return recorded
    }

    /// Every visit recorded up to and including the one which closes the `count`th distinct stream.
    func visits(untilClosedStreams count: Int) async -> [Visit] {
        let closed = NIOLockedValueBox(Set<QUICStreamHandle>())

        return await self.visits { visit in
            guard visit.events.contains(.closed) else { return false }

            return closed.withLockedValue { seen in
                seen.insert(visit.handle)
                return seen.count == count
            }
        }
    }
}

@available(anyAppleOS 26, *)
extension StreamRecording.Visit {
    /// Snapshots `visit` without reading from it.
    init<Consumer: QUICStreamConsumer>(_ visit: inout QUICStreamVisit<Consumer>) {
        var isSendOpen = false
        var isReceiveOpen = false

        visit.withStream { stream, _ in
            isSendOpen = stream.isSendOpen
            isReceiveOpen = stream.isReceiveOpen
        }

        self.init(
            handle: visit.handle,
            id: visit.id,
            events: visit.events,
            bytesReadSoFar: ByteBuffer(),
            readOutcome: nil,
            closeError: visit.closeError,
            resetCode: visit.resetCode,
            stopSendingCode: visit.stopSendingCode,
            isSendOpen: isSendOpen,
            isReceiveOpen: isReceiveOpen
        )
    }

    /// Reads everything available into the stream's state, then snapshots `visit`.
    init<Consumer: QUICStreamConsumer>(
        reading visit: inout QUICStreamVisit<Consumer>
    ) where Consumer.StreamState == ByteBuffer {
        let outcome = visit.events.contains(.readable) ? visit.readAll() : nil

        self.init(&visit)
        self.bytesReadSoFar = visit.state
        self.readOutcome = outcome
    }
}

@available(anyAppleOS 26, *)
extension QUICStreamVisit where Consumer.StreamState == ByteBuffer {
    /// Reads everything available on the stream into its state.
    ///
    /// - Returns: What the last read returned.
    @discardableResult
    mutating func readAll() -> QUICStreamReadOutcome {
        var outcome = QUICStreamReadOutcome.nothingAvailable

        self.withStream { stream, bytes in
            reading: while true {
                outcome = stream.read(into: &bytes)
                switch outcome {
                case .read:
                    ()
                case .nothingAvailable, .endOfStream:
                    break reading
                }
            }
        }

        return outcome
    }
}

@available(anyAppleOS 26, *)
extension [StreamRecording.Visit] {
    /// Every event across all of these visits.
    var allEvents: QUICStreamEvents {
        self.reduce(into: QUICStreamEvents()) { $0.formUnion($1.events) }
    }
}

// MARK: - Consumers

/// Reads every stream to its end, writes what it read back, and finishes.
@available(anyAppleOS 26, *)
struct EchoConsumer: QUICStreamConsumer {
    typealias StreamState = ByteBuffer

    /// Anything which went wrong while echoing.
    ///
    /// `Issue.record` can't be used from a consumer: `processStreams` runs on an event-loop thread
    /// outside the test's task, so under concurrent test execution the issue would misattribute.
    /// Assert on this from the test's own task instead.
    let failures: NIOLockedValueBox<[String]>

    init(failures: NIOLockedValueBox<[String]> = NIOLockedValueBox([])) {
        self.failures = failures
    }

    func makeStreamState(_ stream: inout QUICStream<Self>) -> ByteBuffer {
        ByteBuffer()
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        let failures = self.failures

        while var visit = streams.next() {
            guard visit.events.contains(.readable) else { continue }

            visit.withStream { stream, buffer in
                reading: while true {
                    switch stream.read(into: &buffer) {
                    case .read:
                        ()
                    case .nothingAvailable:
                        break reading
                    case .endOfStream:
                        if stream.isSendOpen {
                            stream.write(buffer)
                            do {
                                try stream.flush(fin: true)
                            } catch {
                                failures.withLockedValue { $0.append("echo flush failed: \(error)") }
                            }
                        }
                        break reading
                    }
                }
            }
        }
    }
}

/// Reads a whole request, then responds with `chunkCount` separate writes of `chunkSize` bytes
/// followed by a "Success" marker, all in the one flush which carries the FIN.
@available(anyAppleOS 26, *)
struct StreamingConsumer: QUICStreamConsumer {
    typealias StreamState = ByteBuffer

    private let chunk: ByteBuffer
    private let chunkCount: Int
    let failures: NIOLockedValueBox<[String]>

    init(chunkSize: Int, chunkCount: Int, failures: NIOLockedValueBox<[String]> = NIOLockedValueBox([])) {
        self.chunk = ByteBuffer(repeating: UInt8(ascii: "a"), count: chunkSize)
        self.chunkCount = chunkCount
        self.failures = failures
    }

    static func expectedResponse(chunkSize: Int, chunkCount: Int) -> ByteBuffer {
        var response = ByteBuffer()
        response.writeRepeatingByte(UInt8(ascii: "a"), count: chunkSize * chunkCount)
        response.writeString("Success")
        return response
    }

    func makeStreamState(_ stream: inout QUICStream<Self>) -> ByteBuffer {
        ByteBuffer()
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        let chunk = self.chunk
        let chunkCount = self.chunkCount
        let failures = self.failures

        while var visit = streams.next() {
            guard visit.events.contains(.readable) else { continue }

            visit.withStream { stream, buffer in
                reading: while true {
                    switch stream.read(into: &buffer) {
                    case .read:
                        ()
                    case .nothingAvailable:
                        break reading
                    case .endOfStream:
                        if stream.isSendOpen {
                            for _ in 0..<chunkCount {
                                stream.write(chunk)
                            }
                            stream.write(ByteBuffer(string: "Success"))
                            do {
                                try stream.flush(fin: true)
                            } catch {
                                failures.withLockedValue { $0.append("response flush failed: \(error)") }
                            }
                        }
                        break reading
                    }
                }
            }
        }
    }
}

/// Reads every readable stream and records each visit into a ``StreamRecording``.
@available(anyAppleOS 26, *)
struct RecordConsumer: QUICStreamConsumer {
    typealias StreamState = ByteBuffer

    let recording: StreamRecording

    func makeStreamState(_ stream: inout QUICStream<Self>) -> StreamState {
        ByteBuffer()
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        while var visit = streams.next() {
            self.recording.record(StreamRecording.Visit(reading: &visit))
        }
    }
}

/// Writes `request` to a stream once the stack has assigned it an ID, and otherwise behaves like a
/// ``RecordConsumer``.
///
/// This is the shape a client has to take when it opens more streams than the peer's limit allows:
/// the flush of a stream without an ID is refused, so the write waits for the
/// ``QUICStreamEvents/opened`` visit.
@available(anyAppleOS 26, *)
struct RequestOnOpenConsumer: QUICStreamConsumer {
    typealias StreamState = ByteBuffer

    let request: ByteBuffer
    let recording: StreamRecording
    let failures: NIOLockedValueBox<[String]>

    init(
        request: ByteBuffer,
        recording: StreamRecording,
        failures: NIOLockedValueBox<[String]> = NIOLockedValueBox([])
    ) {
        self.request = request
        self.recording = recording
        self.failures = failures
    }

    func makeStreamState(_ stream: inout QUICStream<Self>) -> ByteBuffer {
        ByteBuffer()
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        let request = self.request
        let failures = self.failures

        while var visit = streams.next() {
            if visit.events.contains(.opened) {
                visit.withStream { stream, _ in
                    stream.write(request)
                    do {
                        try stream.flush(fin: true)
                    } catch {
                        failures.withLockedValue { $0.append("flush on opened failed: \(error)") }
                    }
                }
            }

            self.recording.record(StreamRecording.Visit(reading: &visit))
        }
    }
}

/// Opens a stream back to the peer from inside the first visit it is handed, and otherwise
/// behaves like a ``RecordConsumer``.
///
/// The new stream goes through `visit.streams` rather than the connection, because the consumer is
/// already inside a drain.
@available(anyAppleOS 26, *)
struct OpeningConsumer: QUICStreamConsumer {
    typealias StreamState = ByteBuffer

    let type: QUICStreamType
    /// Written to the opened stream, so the peer's visit for it is identifiable.
    let payload: ByteBuffer
    /// Whether ``payload`` is flushed with a FIN, closing the send side.
    var fin: Bool = true
    let recording: StreamRecording
    let failures: NIOLockedValueBox<[String]>
    /// A box rather than a stored `var`: the consumer is a value and is copied per connection.
    let hasOpened = NIOLockedValueBox(false)

    func makeStreamState(_ stream: inout QUICStream<Self>) -> ByteBuffer {
        ByteBuffer()
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        let type = self.type
        let payload = self.payload
        let fin = self.fin
        let failures = self.failures

        while var visit = streams.next() {
            let isFirstVisit = self.hasOpened.withLockedValue { hasOpened -> Bool in
                if hasOpened { return false }
                hasOpened = true
                return true
            }

            if isFirstVisit {
                var all = visit.streams
                do {
                    let handle = try all.open(type, state: ByteBuffer())
                    try all.withStream(handle: handle) { stream, _ in
                        stream.write(payload)
                        try stream.flush(fin: fin)
                    }
                } catch {
                    failures.withLockedValue { $0.append("open from visit failed: \(error)") }
                }
            }

            self.recording.record(StreamRecording.Visit(reading: &visit))
        }
    }
}

@available(anyAppleOS 26, *)
struct CallbackConsumer<State>: QUICStreamConsumer, Sendable {
    typealias StreamState = State

    let makeState: @Sendable () -> State
    let onVisit: @Sendable (_ visit: consuming QUICStreamVisit<Self>) -> Void

    func makeStreamState(_ stream: inout QUICStream<Self>) -> State {
        self.makeState()
    }

    mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        while let visit = streams.next() {
            self.onVisit(visit)
        }
    }
}
