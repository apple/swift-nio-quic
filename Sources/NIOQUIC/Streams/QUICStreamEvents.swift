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

/// A set of events which have happened to a stream since it was last visited.
public struct QUICStreamEvents: OptionSet, Hashable, Sendable {
    public var rawValue: UInt8

    @inlinable
    public init(rawValue: UInt8) {
        self.rawValue = rawValue
    }

    /// The stream was opened and assigned an ID.
    @inlinable
    public static var opened: Self {
        Self(rawValue: 1 << 0)
    }

    /// The stream is readable.
    ///
    /// This indicates whether you should attempt to read data from the stream. Note that this isn't
    /// a guarantee that bytes are available, only a hint that they might be. While this is a hint,
    /// the outcome of the read isn't: it should be taken as the source of truth.
    ///
    /// Use this to gate a read during a visit in order to avoid an unnecessary trip through the
    /// stack. You only need to check it once per visit and may then read from the stream as many
    /// times as is necessary.
    @inlinable
    public static var readable: Self {
        Self(rawValue: 1 << 1)
    }

    /// Indicates that writes can make progress.
    ///
    /// This indicates that you may start writing _again_: it isn't a precondition to start writing.
    /// In other words: a newly opened stream is implicitly writable.
    ///
    /// Using this can be helpful for consumers which stop writing when ``QUICStream/writableBytes``
    /// is zero.
    @inlinable
    public static var writable: Self {
        Self(rawValue: 1 << 2)
    }

    /// The peer sent RESET\_STREAM.
    @inlinable
    public static var reset: Self {
        Self(rawValue: 1 << 3)
    }

    /// The peer sent STOP\_SENDING.
    @inlinable
    public static var stopSending: Self {
        Self(rawValue: 1 << 4)
    }

    /// The stream is closed, terminal.
    @inlinable
    public static var closed: Self {
        Self(rawValue: 1 << 5)
    }
}
