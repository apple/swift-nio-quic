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

import Benchmark
import NIOQUICHelpers

private struct StreamState {}
private final class StreamStateObject: Sendable {}

private let lookupsPerStream = 8

private let concurrentStreams = 64
private let filledStreams = 4096

let benchmarks: @Sendable () -> Void = {
    Benchmark.defaultConfiguration = Benchmark.Configuration(
        metrics: [.cpuTotal, .instructions, .mallocCountTotal],
        timeUnits: .nanoseconds,
        units: [.instructions: .count, .mallocCountTotal: .count],
        scalingFactor: .kilo
    )

    Benchmark("Stream churn (value)") { benchmark in
        streamChurn(benchmark, StreamState())
    }

    Benchmark("Stream churn (reference)") { benchmark in
        streamChurn(benchmark, StreamStateObject())
    }

    Benchmark("Stream lookup (value)") { benchmark in
        streamLookup(benchmark, StreamState())
    }

    Benchmark("Stream lookup (reference)") { benchmark in
        streamLookup(benchmark, StreamStateObject())
    }

    Benchmark("Steady state stream churn (value)") { benchmark in
        steadyStateStreamChurn(benchmark, StreamState())
    }

    Benchmark("Steady state stream churn (reference)") { benchmark in
        steadyStateStreamChurn(benchmark, StreamStateObject())
    }

    Benchmark("Fill (value)") { benchmark in
        fill(benchmark, StreamState())
    }

    Benchmark("Fill (reference)") { benchmark in
        fill(benchmark, StreamStateObject())
    }
}

private func streamChurn<Value>(_ benchmark: Benchmark, _ value: Value) {
    var dictionary = QUICStreamIDDictionary<Value>()
    var id = QUICStreamID(rawValue: 0)

    for _ in benchmark.scaledIterations {
        dictionary[id] = value
        for _ in 0..<lookupsPerStream {
            blackHole(dictionary[id])
        }
        blackHole(dictionary.removeValue(forID: id))
        id = QUICStreamID(rawValue: id.rawValue &+ 4)
    }
}

private func streamLookup<Value>(_ benchmark: Benchmark, _ value: Value) {
    var dictionary = QUICStreamIDDictionary<Value>()
    let ids = openStreamIDs(count: concurrentStreams)
    for id in ids {
        dictionary[id] = value
    }

    benchmark.startMeasurement()

    for _ in benchmark.scaledIterations {
        for id in ids {
            blackHole(dictionary[id])
        }
    }
}

private func steadyStateStreamChurn<Value>(_ benchmark: Benchmark, _ value: Value) {
    var dictionary = QUICStreamIDDictionary<Value>()
    var ids = openStreamIDs(count: concurrentStreams)
    for id in ids {
        dictionary[id] = value
    }

    benchmark.startMeasurement()

    var next = QUICStreamID(rawValue: UInt64(concurrentStreams) * 4)
    var index = 0
    for _ in benchmark.scaledIterations {
        blackHole(dictionary.removeValue(forID: ids[index]))
        dictionary[next] = value
        for _ in 0..<lookupsPerStream {
            blackHole(dictionary[next])
        }

        ids[index] = next
        index = (index &+ 1) & (concurrentStreams - 1)
        next = QUICStreamID(rawValue: next.rawValue &+ 4)
    }
}

private func fill<Value>(_ benchmark: Benchmark, _ value: Value) {
    let ids = openStreamIDs(count: filledStreams)

    benchmark.startMeasurement()

    for _ in benchmark.scaledIterations {
        var dictionary = QUICStreamIDDictionary<Value>()
        for id in ids {
            dictionary[id] = value
        }
        blackHole(dictionary.count)
    }
}

private func openStreamIDs(count: Int) -> [QUICStreamID] {
    (0..<count).map { QUICStreamID(rawValue: UInt64($0) << 2) }
}
