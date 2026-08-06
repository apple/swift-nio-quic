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

import DequeModule
import NIOCore
import NIOEmbedded
import NIOQUICHelpers
import Synchronization
import Testing

@testable import NIOQUIC

@available(anyAppleOS 26, *)
private func makeChannel(
    isServer: Bool = true,
    parent: EmbeddedChannel = EmbeddedChannel(),
    connection: (any QUICConnectionProtocol)? = nil,
    registrar: any QUICConnectionIDRegistrar = RecordingRegistrar(),
    transport: any QUICTransport = RecordingTransport(),
    initializer: ((any Channel) -> EventLoopFuture<Void>)? = nil
) throws -> QUICConnectionChannel {
    let connection = connection ?? NoOpConnection()

    let channel = QUICConnectionChannel(
        udpChannel: parent,
        connection: .test(connection),
        registrar: .test(registrar),
        transport: .test(transport),
        isServer: isServer
    )

    if let initializer {
        let promise = channel.eventLoop.makePromise(of: Void.self)
        channel.transportView.initialize(promise: promise, initializer: initializer)

        // Activate the connection.
        channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 0)
        channel.connectionView.drainOutbound()

        try promise.futureResult.wait()

        // Events will be recorded as part of the init, so clear them.
        if let transport = transport as? RecordingTransport {
            transport.events.removeAll()
        }
    }

    return channel
}

struct QUICConnectionChannelTests {
    @available(anyAppleOS 26, *)
    @Test
    func getAutoRead() throws {
        let channel = try makeChannel()

        let autoRead = try channel.getOption(.autoRead).wait()
        #expect(autoRead)

        let autoReadSync = try channel.syncOptions!.getOption(.autoRead)
        #expect(autoReadSync)
    }

    @available(anyAppleOS 26, *)
    @Test
    func setAutoRead() throws {
        let channel = try makeChannel()

        try channel.setOption(.autoRead, value: false).wait()
        let autoRead = try channel.getOption(.autoRead).wait()
        #expect(!autoRead)

        try channel.syncOptions!.setOption(.autoRead, value: true)
        let autoReadSync = try channel.syncOptions!.getOption(.autoRead)
        #expect(autoReadSync)
    }

    @available(anyAppleOS 26, *)
    @Test
    func getUnsupportedOption() throws {
        let channel = try makeChannel()

        #expect(throws: ChannelError.self) {
            try channel.getOption(.backlog).wait()
        }

        #expect(throws: ChannelError.self) {
            try channel.syncOptions!.getOption(.backlog)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func setUnsupportedOption() throws {
        let channel = try makeChannel()

        #expect(throws: ChannelError.self) {
            try channel.setOption(.backlog, value: 42).wait()
        }

        #expect(throws: ChannelError.self) {
            try channel.syncOptions!.setOption(.backlog, value: 43)
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func address() throws {
        let local = try SocketAddress(ipAddress: "127.0.0.1", port: 1234)
        let remote = try SocketAddress(ipAddress: "127.0.0.1", port: 5678)
        let connection = NoOpConnection(localAddress: local, remoteAddress: remote)

        let channel = try makeChannel(connection: connection)
        #expect(channel.localAddress == local)
        #expect(channel.remoteAddress == remote)
    }

    @available(anyAppleOS 26, *)
    @Test
    func newChannel() throws {
        let channel = try makeChannel()
        #expect(!channel.isActive)
        #expect(channel.isWritable)
    }

    @available(anyAppleOS 26, *)
    @Test
    func closeViaChannelAPI() throws {
        let connection = RecordingConnection()
        let recorder = LifecycleRecorder()
        let channel = try makeChannel(connection: connection) {
            $0.pipeline.addHandler(recorder)
        }

        let closeFuture = channel.close()
        let noError = QUICTransportErrorCode.noError.rawValue
        #expect(connection.events.contains(.close(false, noError, "")))
        #expect(recorder.inactiveCount == 1)
        #expect(!channel.isActive)

        // Teardown and the close future resolve on the next loop tick.
        channel.embeddedEventLoop.run()
        try closeFuture.wait()
        try channel.closeFuture.wait()
    }

    @available(anyAppleOS 26, *)
    @Test
    func closeConnectionEventClosesWithApplicationError() throws {
        let connection = RecordingConnection()
        let recorder = LifecycleRecorder()
        let channel = try makeChannel(connection: connection) {
            $0.pipeline.addHandler(recorder)
        }

        let event = QUICCloseConnectionEvent(
            code: QUICApplicationErrorCode(42),
            reasonPhrase: "bye"
        )
        let promise = channel.eventLoop.makePromise(of: Void.self)
        channel.pipeline.triggerUserOutboundEvent(event, promise: promise)

        #expect(connection.events.contains(.close(true, 42, "bye")))
        #expect(recorder.inactiveCount == 1)
        #expect(!channel.isActive)

        channel.embeddedEventLoop.run()
        try promise.futureResult.wait()
        try channel.closeFuture.wait()
    }

    @available(anyAppleOS 26, *)
    @Test
    func unsupportedOutboundEventFails() throws {
        let channel = try makeChannel()
        #expect(throws: ChannelError.operationUnsupported) {
            try channel.triggerUserOutboundEvent("unsupported event").wait()
        }
    }

    // MARK: Connection view

    struct ConnectionView {
        /// Records the SCID events `ConnectionView` fires into the channel's pipeline.
        @available(anyAppleOS 26, *)
        final class SCIDEventRecorder: ChannelInboundHandler, Sendable {
            typealias InboundIn = Any

            enum Event: Hashable, Sendable {
                case associated(QUICConnectionID)
                case retired(QUICConnectionID)
            }

            // Only store Event, we can't smuggle a non-Sendable Any out via Mutex.
            private let _events: Mutex<[Event]> = Mutex([])
            var events: [Event] { self._events.withLock { $0 } }

            func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
                if let event = event as? QUICSCIDAssociatedEvent {
                    self._events.withLock { $0.append(.associated(event.scid)) }
                } else if let event = event as? QUICSCIDRetiredEvent {
                    self._events.withLock { $0.append(.retired(event.scid)) }
                }
            }
        }

        @available(anyAppleOS 26, *)
        @Test
        func associateAndRetire() throws {
            let registrar = RecordingRegistrar()
            let recorder = SCIDEventRecorder()
            let channel = try makeChannel(registrar: registrar) { $0.pipeline.addHandler(recorder) }

            var generator = RandomQUICConnectionIDGenerator()
            let id1 = generator.next()

            let view = channel.connectionView
            #expect(view.associate(id1))
            #expect(view.retire(id1))

            #expect(registrar.events == [.associated(id1), .retired(id1)])
            // Both succeeded, so both are reported to the pipeline.
            #expect(recorder.events == [.associated(id1), .retired(id1)])
        }

        @available(anyAppleOS 26, *)
        @Test
        func associateFailureIsNotReportedToThePipeline() throws {
            let registrar = RecordingRegistrar()
            registrar.associateResult = false
            let recorder = SCIDEventRecorder()
            let channel = try makeChannel(registrar: registrar) { $0.pipeline.addHandler(recorder) }

            var generator = RandomQUICConnectionIDGenerator()
            let id = generator.next()

            // The registrar rejected the association, so no event is fired.
            #expect(channel.connectionView.associate(id) == false)
            #expect(registrar.events == [.associated(id)])
            #expect(recorder.events.isEmpty)
        }

        @available(anyAppleOS 26, *)
        @Test
        func retireFailureIsNotReportedToThePipeline() throws {
            let registrar = RecordingRegistrar()
            registrar.retireResult = false
            let recorder = SCIDEventRecorder()
            let channel = try makeChannel(registrar: registrar) { $0.pipeline.addHandler(recorder) }

            var generator = RandomQUICConnectionIDGenerator()
            let id = generator.next()

            // The registrar rejected the retirement, so no event is fired.
            #expect(channel.connectionView.retire(id) == false)
            #expect(registrar.events == [.retired(id)])
            #expect(recorder.events.isEmpty)
        }

        @available(anyAppleOS 26, *)
        @Test
        func generate() throws {
            var generator = RandomQUICConnectionIDGenerator()
            let id = generator.next()

            // Have generate always return the same ID.
            let registrar = RecordingRegistrar { id }
            let channel = try makeChannel(registrar: registrar)

            let view = channel.connectionView
            #expect(view.generateID() == id)
            #expect(view.generateID() == id)
            #expect(view.generateID() == id)
        }

        @available(anyAppleOS 26, *)
        @Test
        func drainOutbound() throws {
            let transport = RecordingTransport()
            let connection = RecordingConnection()
            let channel = try makeChannel(connection: connection, transport: transport)

            // No buffers, so just a flush.
            channel.connectionView.drainOutbound()
            #expect(transport.events.popFirst() == .flushed)
            #expect(transport.events.isEmpty)

            // Try again with a packet.
            let buffer = ByteBuffer(string: "Hello!")
            connection.outboundPackets.append(buffer)
            channel.connectionView.drainOutbound()

            let expected = AddressedEnvelope(remoteAddress: .ipv4Loopback8081, data: buffer)
            #expect(transport.events.popFirst() == .wrote(expected))
            #expect(transport.events.popFirst() == .flushed)
            #expect(transport.events.isEmpty)
        }

        @available(anyAppleOS 26, *)
        @Test
        func spontaneousConnectionCloseWithError() throws {
            let connection = RecordingConnection()
            let recorder = LifecycleRecorder()
            let channel = try makeChannel(connection: connection) {
                $0.pipeline.addHandler(recorder)
            }

            // The peer/transport closed the connection out-of-band. The pending close is
            // applied on the next drain.
            struct Boom: Error {}
            channel.connectionView.connectionClosed(error: Boom())
            channel.connectionView.drainOutbound()

            #expect(recorder.errors.last is Boom)
            #expect(recorder.inactiveCount == 1)
            #expect(!channel.isActive)

            // A spontaneous close is not locally-initiated, so the connection isn't asked to
            // close.
            #expect(connection.events.isEmpty)

            channel.embeddedEventLoop.run()
            try channel.closeFuture.wait()
        }

        @available(anyAppleOS 26, *)
        @Test
        func closeErrorIsFiredBeforeStreamsAreClosed() throws {
            let connection = RecordingConnection()
            let recorder = LifecycleRecorder()
            let channel = try makeChannel(connection: connection) {
                $0.pipeline.addHandler(recorder)
            }

            // Hold up channelInactive on an outstanding stream-close future to make the window
            // between firing the error and firing inactive observable.
            let streamClose = channel.eventLoop.makePromise(of: Void.self)
            connection.streamCloseFutures = [streamClose.futureResult]

            struct Boom: Error {}
            channel.connectionView.connectionClosed(error: Boom())
            channel.connectionView.drainOutbound()

            // The error is fired while the streams are still closing: a handler may rely on the
            // connection level error to fail its streams.
            #expect(recorder.errors.count == 1)
            #expect(recorder.errors.last is Boom)
            #expect(recorder.inactiveCount == 0)

            // Once the streams have closed inactive fires; the error isn't fired again.
            streamClose.succeed()
            #expect(recorder.errors.count == 1)
            #expect(recorder.inactiveCount == 1)

            channel.embeddedEventLoop.run()
            try channel.closeFuture.wait()
        }
    }

    // MARK: Transport View

    struct TransportView {
        @available(anyAppleOS 26, *)
        @Test
        func initializeServerChannel() throws {
            let channel = try makeChannel()
            let view = channel.transportView
            let promise = channel.eventLoop.makePromise(of: Void.self)
            view.initialize(promise: promise) { $0.eventLoop.makeSucceededVoidFuture() }
            try promise.futureResult.wait()

            // Channel isn't active yet.
            #expect(!channel.isActive)

            // Complete handshake to activate.
            channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 0)
            channel.connectionView.drainOutbound()
            #expect(channel.isActive)
        }

        @available(anyAppleOS 26, *)
        @Test
        func initializeClientChannel() throws {
            let channel = try makeChannel(isServer: false)
            let view = channel.transportView
            let promise = channel.eventLoop.makePromise(of: Void.self)
            view.initialize(promise: promise) { $0.eventLoop.makeSucceededVoidFuture() }

            // Only completes when the handshake completes.
            channel.connectionView.handshakeCompleted(peerMaxDatagramFrameSize: 0)
            channel.connectionView.drainOutbound()

            try promise.futureResult.wait()

            // Channel became active.
            #expect(channel.isActive)
        }

        @available(anyAppleOS 26, *)
        @Test
        func initializeClientChannelCloseDuringInit() throws {
            let channel = try makeChannel(isServer: false)
            let view = channel.transportView
            let promise = channel.eventLoop.makePromise(of: Void.self)
            view.initialize(promise: promise) { $0.eventLoop.makeSucceededVoidFuture() }

            struct Boom: Error {}
            channel.connectionView.connectionClosed(error: Boom())
            channel.connectionView.drainOutbound()

            #expect(throws: Boom.self) {
                try promise.futureResult.wait()
            }

            // Channel didn't become active.
            #expect(!channel.isActive)
        }

        @available(anyAppleOS 26, *)
        @Test
        func initializeTwiceFails() throws {
            let channel = try makeChannel()
            let view = channel.transportView
            let p1 = channel.eventLoop.makePromise(of: Void.self)
            view.initialize(promise: p1) { $0.eventLoop.makeSucceededVoidFuture() }
            try p1.futureResult.wait()

            // Another attempt should fail.
            let p2 = channel.eventLoop.makePromise(of: Void.self)
            view.initialize(promise: p2) { $0.eventLoop.makeSucceededVoidFuture() }
            #expect(throws: ChannelError.operationUnsupported) {
                try p2.futureResult.wait()
            }
        }

        @available(anyAppleOS 26, *)
        @Test
        func parentChannelRead() throws {
            let connection = RecordingConnection()
            let transport = RecordingTransport()
            let channel = try makeChannel(connection: connection, transport: transport) {
                $0.eventLoop.makeSucceededVoidFuture()
            }

            let view = channel.transportView
            view.parentChannelRead(ByteBuffer(string: "Hello,"))
            #expect(connection.events.popFirst() == .receivedPacket(ByteBuffer(string: "Hello,")))
            #expect(transport.events.isEmpty)

            view.parentChannelRead(ByteBuffer(string: "QUIC!"))
            #expect(connection.events.popFirst() == .receivedPacket(ByteBuffer(string: "QUIC!")))
            #expect(transport.events.isEmpty)

            // Read-complete will trigger a drain which will in turn trigger a flush.
            view.parentChannelReadComplete()
            #expect(connection.events.popFirst() == .receivedPacketsComplete)
            #expect(transport.events.popFirst() == .flushed)
        }

        @available(anyAppleOS 26, *)
        @Test
        func parentChannelInactive() throws {
            let connection = RecordingConnection()
            let transport = RecordingTransport()
            let channel = try makeChannel(connection: connection, transport: transport) {
                $0.eventLoop.makeSucceededVoidFuture()
            }

            #expect(channel.isActive)

            // Prepare an outbound packet. The inactive should cause a drain.
            connection.outboundPackets.append(ByteBuffer(string: "Bye!"))
            channel.transportView.parentChannelInactive()

            #expect(!channel.isActive)

            let expected = AddressedEnvelope(
                remoteAddress: .ipv4Loopback8081,
                data: ByteBuffer(string: "Bye!")
            )
            #expect(transport.events.popFirst() == .wrote(expected))
            #expect(transport.events.popFirst() == .flushed)
            #expect(transport.events.isEmpty)
        }

        @available(anyAppleOS 26, *)
        @Test
        func parentChannelWritability() throws {
            final class WritabilityRecorder: ChannelInboundHandler, Sendable {
                typealias InboundIn = Any

                private let _changes: Mutex<[Bool]> = Mutex([])
                var changes: [Bool] { self._changes.withLock { $0 } }

                func channelWritabilityChanged(context: ChannelHandlerContext) {
                    self._changes.withLock { $0.append(context.channel.isWritable) }
                }
            }

            let recorder = WritabilityRecorder()
            let channel = try makeChannel { $0.pipeline.addHandler(recorder) }

            #expect(recorder.changes.isEmpty)

            let view = channel.transportView
            view.parentChannelWritabilityChanged(to: false)
            #expect(recorder.changes == [false])

            view.parentChannelWritabilityChanged(to: true)
            #expect(recorder.changes == [false, true])

            // Check duplicate notifications are suppressed by the channel.
            view.parentChannelWritabilityChanged(to: true)
            #expect(recorder.changes == [false, true])

            view.parentChannelWritabilityChanged(to: false)
            #expect(recorder.changes == [false, true, false])
        }

        @available(anyAppleOS 26, *)
        @Test
        func parentChannelUserInboundEvents() throws {
            enum TestEvent: Hashable, Sendable { case one, two, three }

            final class UserInboundEventRecorder: ChannelInboundHandler, Sendable {
                typealias InboundIn = Any

                // Only store TestEvent, we can't smuggle a non-Sendable Any out via Mutex.
                private let _events: Mutex<[TestEvent]> = Mutex([])
                var events: [TestEvent] { self._events.withLock { $0 } }

                func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
                    if let event = event as? TestEvent {
                        self._events.withLock { $0.append(event) }
                    } else {
                        Issue.record("Unexpected event")
                    }
                }
            }

            let recorder = UserInboundEventRecorder()
            let channel = try makeChannel { $0.pipeline.addHandler(recorder) }

            #expect(recorder.events.isEmpty)

            let view = channel.transportView
            view.parentChannelUserInboundEventTriggered(TestEvent.one)
            view.parentChannelUserInboundEventTriggered(TestEvent.two)
            view.parentChannelUserInboundEventTriggered(TestEvent.three)

            #expect(recorder.events == [.one, .two, .three])
        }

        @available(anyAppleOS 26, *)
        @Test
        func shutdownGracefullyClosesConnection() throws {
            let connection = RecordingConnection()
            let recorder = LifecycleRecorder()
            let channel = try makeChannel(connection: connection) {
                $0.pipeline.addHandler(recorder)
            }

            #expect(channel.isActive)

            let promise = channel.eventLoop.makePromise(of: Void.self)
            channel.transportView.shutdown(promise: promise)

            // Shutdown is graceful: the connection is closed cleanly (no error) and
            // channelInactive fires exactly once.
            let noError = QUICTransportErrorCode.noError.rawValue
            #expect(connection.events.contains(.close(false, noError, "")))
            #expect(recorder.inactiveCount == 1)
            #expect(!channel.isActive)

            channel.embeddedEventLoop.run()
            try promise.futureResult.wait()
            try channel.closeFuture.wait()
        }

        @available(anyAppleOS 26, *)
        @Test
        func forceCloseFiresInactiveImmediately() throws {
            let connection = RecordingConnection()
            let recorder = LifecycleRecorder()
            let channel = try makeChannel(connection: connection) {
                $0.pipeline.addHandler(recorder)
            }

            #expect(channel.isActive)

            // Force-close doesn't wait for the QUIC close handshake; channelInactive fires
            // synchronously and the connection is not asked to close gracefully.
            channel.transportView.forceClose()
            #expect(recorder.inactiveCount == 1)
            #expect(!channel.isActive)
            #expect(connection.events == [])

            channel.embeddedEventLoop.run()
            try channel.closeFuture.wait()
        }

        @available(anyAppleOS 26, *)
        @Test
        func forceCloseWhenAlreadyClosedIsNoOp() throws {
            let recorder = LifecycleRecorder()
            let channel = try makeChannel {
                $0.pipeline.addHandler(recorder)
            }

            // Close the channel
            channel.transportView.forceClose()
            channel.embeddedEventLoop.run()
            try channel.closeFuture.wait()
            #expect(recorder.inactiveCount == 1)

            // A second force-close on the closed channel is a no-op.
            channel.transportView.forceClose()
            channel.embeddedEventLoop.run()
            #expect(recorder.inactiveCount == 1)
        }

        @available(anyAppleOS 26, *)
        @Test
        func shutdownWhenAlreadyClosedCompletesPromise() throws {
            let channel = try makeChannel()

            channel.transportView.forceClose()
            channel.embeddedEventLoop.run()
            try channel.closeFuture.wait()

            // Shutting down an already closed channel must still complete the promise,
            // otherwise callers waiting on it (e.g. shutting down every connection in the
            // registry) wait until their deadline expires.
            let promise = channel.eventLoop.makePromise(of: Void.self)
            channel.transportView.shutdown(promise: promise)
            channel.embeddedEventLoop.run()
            try promise.futureResult.wait()
        }

        @available(anyAppleOS 26, *)
        @Test
        func forceCloseWhileInactiveDeferredDoesNotDoubleFire() throws {
            let connection = RecordingConnection()
            let recorder = LifecycleRecorder()

            let channel = try makeChannel(connection: connection) {
                $0.pipeline.addHandler(recorder)
            }

            // Hold up channelInactive on an outstanding stream-close future.
            let streamClose = channel.eventLoop.makePromise(of: Void.self)
            connection.streamCloseFutures = [streamClose.futureResult]

            // Begin a graceful close: inactive is committed but deferred until the stream
            // close future completes.
            channel.transportView.shutdown(promise: nil)
            #expect(recorder.inactiveCount == 0)

            // Force-close during that window must not fire a second (or early) inactive.
            channel.transportView.forceClose()
            #expect(recorder.inactiveCount == 0)

            // Once the stream finishes closing, inactive fires exactly once.
            streamClose.succeed()
            #expect(recorder.inactiveCount == 1)
            channel.embeddedEventLoop.run()
            try channel.closeFuture.wait()
        }
    }
}

@available(anyAppleOS 26, *)
final class LifecycleRecorder: ChannelInboundHandler, Sendable {
    typealias InboundIn = Any

    private let _inactive: Mutex<Int> = Mutex(0)
    private let _errors: Mutex<[any Error]> = Mutex([])

    var inactiveCount: Int { self._inactive.withLock { $0 } }
    var errors: [any Error] { self._errors.withLock { $0 } }

    func channelInactive(context: ChannelHandlerContext) {
        self._inactive.withLock { $0 += 1 }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        self._errors.withLock { $0.append(error) }
        context.fireErrorCaught(error)
    }
}

final class RecordingTransport: QUICTransport {
    enum Event: Hashable {
        case wrote(AddressedEnvelope<ByteBuffer>)
        case flushed
        case read
    }

    var events: Deque<Event>

    init() {
        self.events = []
    }

    func writeDatagram(_ envelope: AddressedEnvelope<ByteBuffer>, promise: EventLoopPromise<Void>?) {
        self.events.append(.wrote(envelope))
        promise?.succeed()
    }

    func flush() {
        self.events.append(.flushed)
    }

    func read() {
        self.events.append(.read)
    }
}

@available(anyAppleOS 26, *)
final class RecordingRegistrar: QUICConnectionIDRegistrar {
    enum Event: Hashable {
        case associated(QUICConnectionID)
        case retired(QUICConnectionID)
    }

    private(set) var events: [Event]
    private let generate: () -> QUICConnectionID

    /// What `associate(_:)` returns.
    var associateResult: Bool

    /// What `retire(_:)` returns.
    var retireResult: Bool

    init(_ generate: @escaping () -> QUICConnectionID = { .zero }) {
        self.events = []
        self.generate = generate
        self.associateResult = true
        self.retireResult = true
    }

    func associate(_ newID: QUICConnectionID) -> Bool {
        self.events.append(.associated(newID))
        return self.associateResult
    }

    func retire(_ connectionID: QUICConnectionID) -> Bool {
        self.events.append(.retired(connectionID))
        return self.retireResult
    }

    func generateID() -> QUICConnectionID {
        self.generate()
    }
}

@available(anyAppleOS 26, *)
final class RecordingConnection: QUICConnectionProtocol {
    let localAddress: SocketAddress
    let remoteAddress: SocketAddress

    var outboundPackets: Deque<ByteBuffer>
    var events: Deque<Event>

    /// Datagrams accepted by `writeDatagram(_:)`, and the number of times they were flushed.
    var writtenDatagrams: [ByteBuffer]
    var datagramFlushCount: Int
    /// What `writeDatagram(_:)` returns. Flip to `false` to simulate a connection which can't
    /// accept datagrams (e.g. it has no datagram flow attached).
    var writeDatagramResult: Bool

    /// Futures returned by `closeAllStreams()`. Empty means `channelInactive` fires
    /// synchronously; a pending future defers it until the future completes.
    var streamCloseFutures: [EventLoopFuture<Void>]

    enum Event: Hashable {
        case receivedPacket(ByteBuffer)
        case receivedPacketsComplete
        case close(Bool, Int64, String)
    }

    init(
        localAddress: SocketAddress = .ipv4Loopback8080,
        remoteAddress: SocketAddress = .ipv4Loopback8081
    ) {
        self.localAddress = localAddress
        self.remoteAddress = remoteAddress
        self.outboundPackets = []
        self.events = []
        self.streamCloseFutures = []
        self.writtenDatagrams = []
        self.datagramFlushCount = 0
        self.writeDatagramResult = true
    }

    func receivePacket(_ packet: ByteBuffer) -> Int {
        self.events.append(.receivedPacket(packet))
        return packet.readableBytes
    }

    func receivePacketsComplete() {
        self.events.append(.receivedPacketsComplete)
    }

    func nextPacketToSend() -> AddressedEnvelope<ByteBuffer>? {
        if let buffer = self.outboundPackets.popFirst() {
            return AddressedEnvelope(remoteAddress: self.remoteAddress, data: buffer)
        } else {
            return nil
        }
    }

    func close(isApplicationClose: Bool, errorCode: Int64, reason: String) -> Bool {
        self.events.append(.close(isApplicationClose, errorCode, reason))
        return true
    }

    func closeAllStreams() -> [EventLoopFuture<Void>] {
        self.streamCloseFutures
    }

    func quiesceStreams() {
    }

    func writeDatagram(_ datagram: ByteBuffer) -> Bool {
        if self.writeDatagramResult {
            self.writtenDatagrams.append(datagram)
        }
        return self.writeDatagramResult
    }

    func flushDatagrams() {
        self.datagramFlushCount += 1
    }
}

@available(anyAppleOS 26, *)
struct NoOpConnection: QUICConnectionProtocol {
    let localAddress: SocketAddress
    let remoteAddress: SocketAddress

    init(
        localAddress: SocketAddress = .ipv4Loopback8080,
        remoteAddress: SocketAddress = .ipv4Loopback8081,
    ) {
        self.localAddress = localAddress
        self.remoteAddress = remoteAddress
    }

    func receivePacket(_ packet: ByteBuffer) -> Int {
        0
    }

    func receivePacketsComplete() {
    }

    func nextPacketToSend() -> AddressedEnvelope<ByteBuffer>? {
        nil
    }

    func close(isApplicationClose: Bool, errorCode: Int64, reason: String) -> Bool {
        true
    }

    func closeAllStreams() -> [EventLoopFuture<Void>] {
        []
    }

    func quiesceStreams() {
    }

    func writeDatagram(_ datagram: ByteBuffer) -> Bool {
        false
    }

    func flushDatagrams() {
    }
}

extension SocketAddress {
    static var ipv4Loopback8080: Self {
        try! SocketAddress(ipAddress: "127.0.0.1", port: 8080)
    }

    static var ipv4Loopback8081: Self {
        try! SocketAddress(ipAddress: "127.0.0.1", port: 8081)
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    fileprivate var embeddedEventLoop: EmbeddedEventLoop {
        self.eventLoop as! EmbeddedEventLoop
    }
}
