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

/// A consumer which provides a `Channel` per QUIC stream.
@available(anyAppleOS 26, *)
public enum QUICStreamChannels: QUICStreamConsumer, ~Copyable {
    public typealias StreamState = Never

    public mutating func makeStreamState(_ stream: inout QUICStream<Self>) -> Never {
        fatalError("\(Self.self) is uninhabited")
    }

    public mutating func processStreams(_ streams: inout QUICStreamIterator<Self>) {
        fatalError("\(Self.self) is uninhabited")
    }
}
