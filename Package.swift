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

let isRunningInCI = Context.environment["CI"] != nil

// Enable the QlogOutput in CI as some tests depend on it; also enable a setting our
// tests can depend on to gate whether the Qlog tests are expected to run or not.
let swiftNetworkTraits: Set<Package.Dependency.Trait>
let qlogSetting: [SwiftSetting]
if isRunningInCI {
    swiftNetworkTraits = [.defaults, "QlogOutput"]
    qlogSetting = [.define("QLOG_ENABLED")]
} else {
    swiftNetworkTraits = [.defaults]
    qlogSetting = []
}

// controls logs emitted, lower levels are compiled out
// `[.init(stringLiteral: "MaxLogLevelNone")]` gives best performance
// `[]` gives all log output
let maxLogLevel: Set<Package.Dependency.Trait> = [.init(stringLiteral: "MaxLogLevelDebug")]

let swiftSettings: [SwiftSetting] =
    [
        .enableUpcomingFeature("ExistentialAny"),
        .enableUpcomingFeature("StrictConcurrency"),
        .enableExperimentalFeature("AnyAppleOSAvailability"),
    ]

let package = Package(
    name: "swift-nio-quic",
    products: [
        .library(name: "NIOQUIC", targets: ["NIOQUIC"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio", from: "2.92.0"),
        .package(url: "https://github.com/apple/swift-log", from: "1.12.1", traits: maxLogLevel),
        .package(url: "https://github.com/apple/swift-metrics", from: "2.4.1"),
        .package(url: "https://github.com/apple/swift-certificates.git", branch: "swift-crypto-5.x"),
        .package(url: "https://github.com/apple/swift-collections.git", from: "1.4.1"),
        .package(url: "https://github.com/apple/swift-crypto.git", exact: "5.0.0-beta.2"),
        .package(url: "https://github.com/apple/swift-nio-quic-helpers.git", branch: "main"),
        .package(
            url: "https://github.com/apple/swift-network-evolution",
            revision: "ca1af826afc6408a2a68eb795db978c16e5ced89",
            traits: swiftNetworkTraits
        ),
        .package(url: "https://github.com/apple/swift-tls", branch: "main"),
    ],
    targets: [
        .executableTarget(
            name: "NIOQUICExample",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .target(name: "NIOQUIC"),
            ],
            swiftSettings: swiftSettings
        ),
        .target(
            name: "NIOQUIC",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOQUICHelpers", package: "swift-nio-quic-helpers"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "X509", package: "swift-certificates"),
                .product(name: "SwiftTLS", package: "swift-tls"),
                .product(name: "SwiftNetwork", package: "swift-network-evolution"),
                .product(name: "DequeModule", package: "swift-collections"),
                .target(name: "ChildChannelMultiplexer"),
            ],
            swiftSettings: swiftSettings
        ),
        .target(
            name: "ChildChannelMultiplexer",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "HeapModule", package: "swift-collections"),
            ]
        ),
        .testTarget(
            name: "NIOQUICTests",
            dependencies: [
                .target(name: "NIOQUIC"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
                .product(name: "NIOTestUtils", package: "swift-nio"),
                .product(name: "X509", package: "swift-certificates"),
            ],
            resources: [
                .copy("testcert.pem"),
                .copy("testkey.pem"),
                .copy("privateKey.der"),
                .copy("publicKey.der"),
            ],
            swiftSettings: swiftSettings + qlogSetting
        ),
        .testTarget(
            name: "IntegrationTests",
            dependencies: [
                "NIOQUIC",
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .product(name: "NIOQUICHelpers", package: "swift-nio-quic-helpers"),
            ],
            resources: [
                .copy("testcert.pem"),
                .copy("testkey.pem"),
                .copy("privateKey.der"),
                .copy("publicKey.der"),
            ],
            swiftSettings: swiftSettings
        ),
    ]
)
