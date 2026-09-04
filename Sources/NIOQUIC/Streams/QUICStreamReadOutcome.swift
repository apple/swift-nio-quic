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

/// What a read took from a stream.
public enum QUICStreamReadOutcome: Hashable, Sendable {
    /// The number of bytes read.
    case read(Int)
    /// The number of bytes read, after which the stream was closed.
    case endOfStream(Int)
    /// No bytes were available to read.
    case nothingAvailable
}
