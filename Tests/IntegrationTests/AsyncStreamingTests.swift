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
import XCTest

@testable import NIOQUIC

final class AsyncStreamingTests: XCTestCase {

    // MARK: - async-wrapper body transfer

    private func runAsyncBodyTransfer(
        totalBytes: Int,
        chunkSize: Int,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        let (_, serverChannel, serverMux, clientMux) = try await makeClientAndServerPair(
            initialMaxData: max(100_000_000, totalBytes * 2),
            initialMaxStreamDataBidi: max(10_000_000, totalBytes * 2)
        )

        let connection = try await clientMux.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )

        let chunk = ByteBuffer(repeating: 0x61, count: chunkSize)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for await connection in serverMux.inboundConnections {
                    for await stream in connection.inboundStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await _ in inbound {}  // drain request
                            var remaining = totalBytes
                            while remaining > 0 {
                                let n = min(chunkSize, remaining)
                                try await outbound.write(chunk.getSlice(at: 0, length: n)!)
                                remaining -= n
                            }
                            outbound.finish()
                        }
                    }
                }
            }

            let stream = try await connection.createBidirectionalStream { streamInitializer in
                streamInitializer.channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel(
                        wrappingChannelSynchronously: streamInitializer.channel,
                        configuration: .init(
                            isOutboundHalfClosureEnabled: true,
                            inboundType: ByteBuffer.self,
                            outboundType: ByteBuffer.self
                        )
                    )
                }
            }
            try await stream.executeThenClose { inbound, outbound in
                try await outbound.write(.init(string: "GET /foo"))
                outbound.finish()
                var received = 0
                for try await buf in inbound {
                    received += buf.readableBytes
                }
                XCTAssertEqual(received, totalBytes, file: file, line: line)
            }
            group.cancelAll()
        }
    }

    func testAsyncBody1Byte() async throws {
        try await self.runAsyncBodyTransfer(totalBytes: 1, chunkSize: 1)
    }

    func testAsyncBody1KB() async throws {
        try await self.runAsyncBodyTransfer(totalBytes: 1_024, chunkSize: 1_024)
    }

    func testAsyncBody16KB() async throws {
        try await self.runAsyncBodyTransfer(totalBytes: 16_384, chunkSize: 1_024)
    }

    func testAsyncBody32KB() async throws {
        try await self.runAsyncBodyTransfer(totalBytes: 32_768, chunkSize: 1_024)
    }

    func testAsyncBody48KB() async throws {
        try await self.runAsyncBodyTransfer(totalBytes: 49_152, chunkSize: 1_024)
    }

    func testAsyncBody64KB() async throws {
        try await self.runAsyncBodyTransfer(totalBytes: 65_536, chunkSize: 1_024)
    }

    func testAsyncBody1MB() async throws {
        try await self.runAsyncBodyTransfer(totalBytes: 1_048_576, chunkSize: 1_024)
    }

    func testAsyncBodySustained1MB() async throws {
        try await self.runAsyncBodyTransfer(totalBytes: 1_048_576, chunkSize: 1_024)
    }

    func testAsyncBodySustained5MB() async throws {
        try await self.runAsyncBodyTransfer(totalBytes: 5_242_880, chunkSize: 1_024)
    }

    // MARK: - async-wrapper concurrent streams

    private static func sendOneRequest(
        connection: QUICConnection<Never>,
        responses: Counter
    ) async throws {
        let stream = try await connection.createBidirectionalStream { streamInitializer in
            streamInitializer.channel.eventLoop.makeCompletedFuture {
                try NIOAsyncChannel(
                    wrappingChannelSynchronously: streamInitializer.channel,
                    configuration: .init(
                        isOutboundHalfClosureEnabled: true,
                        inboundType: ByteBuffer.self,
                        outboundType: ByteBuffer.self
                    )
                )
            }
        }
        try await stream.executeThenClose { inbound, outbound in
            try await outbound.write(.init(string: "GET /foo"))
            outbound.finish()
            // Collect reponse parts.
            var accumulated = ByteBuffer()
            for try await buf in inbound {
                accumulated.writeImmutableBuffer(buf)
            }
            if accumulated == .init(string: "<b>Success</b>") {
                responses.increment()
            }
        }
    }

    private func runAsyncConcurrentRequests(
        requestCount: Int,
        inFlight: Int,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        let (_, serverChannel, serverMux, clientMux) = try await makeClientAndServerPair(
            initialMaxStreamsBidi: max(inFlight * 2, 200)
        )

        let connection = try await clientMux.createNewConnection(
            serverName: serverChannel.localAddress!.ipAddress!,
            remoteAddress: serverChannel.localAddress!,
            inboundStreamInitializer: { channel in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )
        let responses = Counter()

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                await withThrowingTaskGroup(of: Void.self) { connGroup in
                    for await connection in serverMux.inboundConnections {
                        connGroup.addTask {
                            await withThrowingTaskGroup(of: Void.self) { streamGroup in
                                for await stream in connection.inboundStreams {
                                    streamGroup.addTask {
                                        try await stream.executeThenClose { inbound, outbound in
                                            for try await _ in inbound {}
                                            try await outbound.write(.init(string: "<b>Success</b>"))
                                            outbound.finish()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            try await withThrowingTaskGroup(of: Void.self) { reqGroup in
                var issued = 0
                for _ in 0..<min(inFlight, requestCount) {
                    reqGroup.addTask {
                        try await AsyncStreamingTests.sendOneRequest(
                            connection: connection,
                            responses: responses
                        )
                    }
                    issued += 1
                }
                while issued < requestCount {
                    try await reqGroup.next()
                    reqGroup.addTask {
                        try await AsyncStreamingTests.sendOneRequest(
                            connection: connection,
                            responses: responses
                        )
                    }
                    issued += 1
                }
                try await reqGroup.waitForAll()
            }
            group.cancelAll()
        }

        XCTAssertEqual(
            responses.load(),
            requestCount,
            file: file,
            line: line
        )
    }

    func testAsyncConcurrent100Sequential() async throws {
        try await self.runAsyncConcurrentRequests(requestCount: 100, inFlight: 1)
    }

    func testAsyncConcurrent1000Sequential() async throws {
        try await self.runAsyncConcurrentRequests(requestCount: 1_000, inFlight: 1)
    }

    func testAsyncConcurrent100In10() async throws {
        try await self.runAsyncConcurrentRequests(requestCount: 100, inFlight: 10)
    }

    func testAsyncConcurrent1000In100() async throws {
        try await self.runAsyncConcurrentRequests(requestCount: 1_000, inFlight: 100)
    }

    func testAsyncConcurrent1000In200() async throws {
        try await self.runAsyncConcurrentRequests(requestCount: 1_000, inFlight: 200)
    }

    func testAsyncConcurrent10000In100() async throws {
        try await self.runAsyncConcurrentRequests(requestCount: 10_000, inFlight: 100)
    }
}
