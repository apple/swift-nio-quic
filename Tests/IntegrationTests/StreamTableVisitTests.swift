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

import NIOConcurrencyHelpers
import NIOCore
import NIOQUICHelpers
import Testing

@testable import NIOQUIC

@Suite(.timeLimit(.minutes(5)))
struct StreamTableVisitTests {
    @available(anyAppleOS 26, *)
    struct SkipFirstDrainConsumer: QUICStreamConsumer {
        typealias StreamState = ByteBuffer

        let recording: StreamRecording
        let skipped: EventLoopPromise<Void>
        let hasSkipped = NIOLockedValueBox(false)

        func makeStreamState(_ stream: inout QUICStream<Self>) -> ByteBuffer {
            ByteBuffer()
        }

        mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
            // Skip the first drain where there's pending work.
            let shouldSkip = self.hasSkipped.withLockedValue { hasSkipped -> Bool in
                if hasSkipped { return false }
                if !streams._table.hasPendingWork { return false }
                hasSkipped = true
                return true
            }

            if shouldSkip {
                self.skipped.succeed()
            } else {
                while var visit = streams.next() {
                    self.recording.record(StreamRecording.Visit(reading: &visit))
                }
            }
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func unpulledStreamIsPresentedAgainWithItsEvents() async throws {
        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])
        let skipped = makePromise(of: Void.self, timeout: .seconds(5))

        try await withConsumerPair(
            client: SkipFirstDrainConsumer(recording: recording, skipped: skipped),
            server: EchoConsumer(failures: failures)
        ) { pair in
            let firstHandle = try await pair.openClientStream(
                writing: ByteBuffer(string: "GET /quic"),
                fin: true
            )

            try await skipped.futureResult.get()

            // Open a new stream to drive the server's consumer again.
            _ = try await pair.openClientStream(writing: ByteBuffer(string: "poke"), fin: true)

            let visits = await recording.visits {
                $0.handle == firstHandle && $0.events.contains(.closed)
            }

            // Get the visit for the "real" handle. (i.e. ignore the poke.)
            let firstStream = visits.filter { $0.handle == firstHandle }

            // The skipped drain had 'opened', so it should show up again here.
            let first = try #require(firstStream.first)
            #expect(first.events.contains(.opened))

            #expect(firstStream.allEvents.contains(.readable))
            #expect(firstStream.allEvents.contains(.closed))
            #expect(firstStream.last?.bytesReadSoFar == ByteBuffer(string: "GET /quic"))
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func finishHandsBackTheStateAndInvalidatesTheHandle() async throws {
        let statePromise = makePromise(of: (ByteBuffer, QUICStreamHandle).self, timeout: .seconds(5))
        let failures = NIOLockedValueBox<[String]>([])

        try await withConsumerPair(
            client: CallbackConsumer(makeState: { ByteBuffer() }) { visit in
                var visit = visit

                let events = visit.events
                let handle = visit.handle

                if events.contains(.readable) {
                    visit.readAll()
                }

                if events.contains(.closed), let bytes = visit.finish() {
                    statePromise.succeed((bytes, handle))
                }
            },
            server: EchoConsumer(failures: failures)
        ) { pair in
            let firstHandle = try await pair.openClientStream(
                writing: ByteBuffer(string: "GET /quic"),
                fin: true
            )

            // Wait for the consumer state.
            let (bytes, handle) = try await statePromise.futureResult.get()
            #expect(bytes == ByteBuffer(string: "GET /quic"))
            #expect(handle == firstHandle)

            // The first slot was recycled: this should reuse it.
            let secondHandle = try await pair.openClientStream(
                writing: ByteBuffer(string: "GET /quic"),
                fin: true
            )

            #expect(secondHandle.index == firstHandle.index)
            #expect(secondHandle.generation != firstHandle.generation)

            let oldHandleResolves = try await pair.clientConnection.withStreams { streams in
                streams.withStream(handle: firstHandle) { _, _ in true } ?? false
            }.get()

            #expect(oldHandleResolves == false)
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func streamCanBeOpenedFromInsideAVisit() async throws {
        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])

        try await withConsumerPair(
            client: OpeningConsumer(
                type: .clientInitiatedBidirectional,
                payload: ByteBuffer(string: "opened from inside a visit"),
                recording: recording,
                failures: failures
            ),
            server: EchoConsumer(failures: failures)
        ) { pair in
            let firstHandle = try await pair.openClientStream(
                writing: ByteBuffer(string: "opened from the connection"),
                fin: true
            )

            let visits = await recording.visits {
                $0.bytesReadSoFar == ByteBuffer(string: "opened from inside a visit")
            }
            let reentrant = try #require(visits.last)

            #expect(reentrant.handle != firstHandle)
            #expect(reentrant.bytesReadSoFar == ByteBuffer(string: "opened from inside a visit"))
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }

    @available(anyAppleOS 26, *)
    @Test
    func writingFromOffTheEventLoopReachesThePeer() async throws {
        let recording = StreamRecording()
        let failures = NIOLockedValueBox<[String]>([])

        try await withConsumerPair(
            client: RecordConsumer(recording: recording),
            server: EchoConsumer(failures: failures)
        ) { pair in
            let handle = try await pair.openClientStream(state: ByteBuffer())

            let inEventLoop = try await Task {
                try await pair.write(ByteBuffer(string: "written out of band"), to: handle)
                return pair.clientConnection.eventLoop.inEventLoop
            }.value

            #expect(!inEventLoop)

            let visits = await recording.visits {
                $0.bytesReadSoFar == ByteBuffer(string: "written out of band")
            }
            #expect(visits.last?.bytesReadSoFar == ByteBuffer(string: "written out of band"))
        }

        #expect(failures.withLockedValue { $0 }.isEmpty)
    }
}
