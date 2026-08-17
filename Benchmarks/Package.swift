// swift-tools-version:6.3
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

import PackageDescription

let package = Package(
    name: "swift-nio-quic-benchmarks",
    platforms: [.macOS(.v26)],
    dependencies: [
        .package(url: "https://github.com/ordo-one/benchmark", from: "1.36.2"),
        .package(url: "https://github.com/apple/swift-nio-quic-helpers.git", .upToNextMinor(from: "0.1.0")),
    ],
    targets: [
        .executableTarget(
            name: "QUICStreamIDDictionaryBenchmarks",
            dependencies: [
                .product(name: "Benchmark", package: "benchmark"),
                .product(name: "NIOQUICHelpers", package: "swift-nio-quic-helpers"),
            ],
            path: "Benchmarks/QUICStreamIDDictionaryBenchmarks",
            plugins: [.plugin(name: "BenchmarkPlugin", package: "benchmark")]
        )
    ]
)
