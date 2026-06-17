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
import NIOEmbedded
import Synchronization
import XCTest

// taken from swift-nio-http2
extension XCTestCase {
    /// Have two `NIOAsyncTestingChannel` objects send and receive data from each other until
    /// they make no forward progress.
    ///
    /// ** This function is racy and can lead to deadlocks, prefer the one-way variant which is less error-prone**
    func interactInMemory(
        _ first: NIOAsyncTestingChannel,
        _ second: NIOAsyncTestingChannel,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        var operated: Bool

        func readBytesFromChannel(
            _ channel: NIOAsyncTestingChannel
        ) async -> NIOCore.AddressedEnvelope<NIOCore.ByteBuffer>? {
            try? await assertNoThrowWithValue(
                await channel.readOutbound(as: NIOCore.AddressedEnvelope<NIOCore.ByteBuffer>.self)
            )
        }

        repeat {
            operated = false

            if let data = await readBytesFromChannel(first) {
                operated = true
                try await assertNoThrow(try await second.writeInbound(data), file: file, line: line)
            }
            if let data = await readBytesFromChannel(second) {
                operated = true
                try await assertNoThrow(try await first.writeInbound(data), file: file, line: line)
            }
        } while operated
    }

    /// Have a `NIOAsyncTestingChannel` send data to another until it makes no forward progress.
    static func deliverAllBytes(
        from source: NIOAsyncTestingChannel,
        to destination: NIOAsyncTestingChannel,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        var operated: Bool

        func readBytesFromChannel(
            _ channel: NIOAsyncTestingChannel
        ) async -> NIOCore.AddressedEnvelope<NIOCore.ByteBuffer>? {
            try? await assertNoThrowWithValue(
                await channel.readOutbound(as: NIOCore.AddressedEnvelope<NIOCore.ByteBuffer>.self)
            )
        }

        repeat {
            operated = false
            if let data = await readBytesFromChannel(source) {
                operated = true
                try await assertNoThrow(try await destination.writeInbound(data), file: file, line: line)
            }
        } while operated
    }
}

// taken from swift-nio-http2
func assertNoThrow<T>(
    _ body: @autoclosure () async throws -> T,
    defaultValue: T? = nil,
    message: String? = nil,
    file: StaticString = #filePath,
    line: UInt = #line
) async throws {
    do {
        try await _ = body()
    } catch {
        XCTFail("\(message.map { $0 + ": " } ?? "")unexpected error \(error) thrown", file: (file), line: line)
        throw error
    }
}

// taken from swift-nio-http2
func assertNoThrowWithValue<T>(
    _ body: @autoclosure () async throws -> T,
    defaultValue: T? = nil,
    message: String? = nil,
    file: StaticString = #filePath,
    line: UInt = #line
) async throws -> T {
    do {
        return try await body()
    } catch {
        XCTFail("\(message.map { $0 + ": " } ?? "")unexpected error \(error) thrown", file: (file), line: line)
        if let defaultValue = defaultValue {
            return defaultValue
        } else {
            throw error
        }
    }
}

final class Counter: Sendable {
    private let value = Atomic<Int>(0)

    @discardableResult
    func increment() -> Int {
        self.value.wrappingAdd(1, ordering: .sequentiallyConsistent).newValue
    }

    func load() -> Int {
        self.value.load(ordering: .sequentiallyConsistent)
    }
}
