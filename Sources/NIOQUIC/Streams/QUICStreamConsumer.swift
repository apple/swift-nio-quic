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

/// Consumes streams for a single QUIC connection.
@available(anyAppleOS 26, *)
public protocol QUICStreamConsumer: SendableMetatype, ~Copyable {
    /// Per-stream state stored by the connection.
    #if swift(>=6.4)
    associatedtype StreamState: ~Copyable
    #else  // 6.3 doesn't support ~Copyable associatedtypes
    associatedtype StreamState
    #endif
}
