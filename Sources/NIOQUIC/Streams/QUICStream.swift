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
import NIOQUICHelpers

/// A view over a QUIC stream as presented to a ``QUICStreamConsumer``.
@available(anyAppleOS 26, *)
public struct QUICStream<Consumer: QUICStreamConsumer & ~Copyable>: ~Copyable, ~Escapable {
    /// The table this stream's slot belongs to.
    @usableFromInline
    let table: QUICStreamTable<Consumer>

    /// The transport state for the stream.
    @usableFromInline
    let transport: UnsafeMutablePointer<QUICStreamTransportState>

    /// An opaque handle identifying this stream.
    ///
    /// The handle is invalidated when the stream is closed.
    public let handle: QUICStreamHandle

    // 'immortal' because the lifetime really depends on the lifetime of the 'transport' pointer
    // which isn't `~Escapable`. The lifetime of the 'table' is a superset of the lifetime of the
    // stream so using it as the bound also isn't valid. Instead, the stream is immortal for the
    // lifetime of its lexical scope. See also the note in 'QUICStreamTable.withStream' about
    // "borrowing" the stream more than once.
    @inlinable
    @_lifetime(immortal)
    init(
        table: QUICStreamTable<Consumer>,
        transport: UnsafeMutablePointer<QUICStreamTransportState>,
        handle: QUICStreamHandle
    ) {
        self.table = table
        self.transport = transport
        self.handle = handle
    }

    /// The ID the stack assigned, or `nil` if it hasn't been assigned one yet.
    @inlinable
    public var id: QUICStreamID? {
        self.transport.pointee.core.id
    }

    /// Whether more data can be written to the stream.
    @inlinable
    public var isSendOpen: Bool {
        self.transport.pointee.core.isSendOpen
    }

    /// Whether more data can be read from the stream.
    @inlinable
    public var isReceiveOpen: Bool {
        self.transport.pointee.core.isReceiveOpen
    }
}

// MARK: - Reading

@available(anyAppleOS 26, *)
extension QUICStream where Consumer: ~Copyable {
    /// Hands the stream's inbound bytes to `body`, one chunk of contiguous bytes at a time.
    ///
    /// `body` must return the number of bytes it consumed from the bytes it was shown. Bytes
    /// it leaves behind are held for the stream and shown in the next read call.
    ///
    /// Once the peer has finished sending data, and all bytes have been consumed, then read
    /// will return ``QUICStreamReadOutcome/endOfStream(_:)`` each time.
    ///
    /// - Parameters:
    ///   - maxBytes: The maximum number of bytes to read from the network stack in this call. Note
    ///     that this isn't a limit on the total number of bytes passed to the span passed to
    ///     `body`; the actual byte count may differ so you should consider this a hint.
    ///   - minContiguousBytes: The shortest run of contiguous bytes to hand to `body`. Data is
    ///     coalesced to reach it (which may incur additional copies and allocations); `1` never
    ///     coalesces (and therefore does not incur additional copies and allocations).
    ///   - body: Called with a chunk of contiguous bytes, returning how many of them it consumed.
    ///     May be called multiple times.
    /// - Returns: What was handed over, and whether the peer's data is now exhausted.
    @inlinable
    public mutating func read(
        maxBytes: Int,
        minContiguousBytes: Int = 1,
        _ body: (_ span: borrowing RawSpan) -> Int
    ) -> QUICStreamReadOutcome {
        self.transport.pointee.core.read(maxBytes: maxBytes, minContiguous: minContiguousBytes, body)
    }

    /// Appends the stream's inbound bytes to `buffer`.
    ///
    /// - Parameters:
    ///   - maxBytes: The most bytes to take out of the stack.
    ///   - buffer: The buffer to append to.
    /// - Returns: What was appended, and whether the peer's data is now exhausted.
    @inlinable
    public mutating func read(
        maxBytes: Int,
        into buffer: inout ByteBuffer
    ) -> QUICStreamReadOutcome {
        self.transport.pointee.core.read(maxBytes: maxBytes, minContiguous: 1) { bytes in
            buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: bytes.byteCount) { target in
                bytes.withUnsafeBytes { target.copyMemory(from: $0) }
                return bytes.byteCount
            }
        }
    }
}

// MARK: - Writing

@available(anyAppleOS 26, *)
extension QUICStream where Consumer: ~Copyable {
    /// How many bytes the stream will accept right now.
    ///
    /// This is advisory: a write past this is queued in the stack. Writing more than this delays
    /// the next ``QUICStreamEvents/writable`` event.
    @inlinable
    public var writableBytes: Int {
        self.transport.pointee.core.writableBytes
    }

    /// Enqueue data to send at the next ``flush(fin:)``.
    ///
    /// - Parameters:
    ///   - count: The number of writable bytes to hand to `body`.
    ///   - body: Handed `count` writable bytes, returning how many it wrote.
    @inlinable
    public mutating func withWritableSpan(
        count: Int,
        _ body: (_ span: inout MutableRawSpan) -> Int
    ) {
        self.transport.pointee.core.withWritableSpan(count: count, body)
    }

    /// Enqueue data to send at the next ``flush(fin:)``.
    ///
    /// - Parameter buffer: The bytes to write.
    @inlinable
    public mutating func write(_ buffer: ByteBuffer) {
        self.transport.pointee.core.write(buffer)
    }

    /// Write any queued data to the network.
    ///
    /// - Parameter fin: Whether to finish the send side.
    /// - Throws: If the send side can't write the queued data. For example, the stream is already
    ///   closed, or FIN has already been sent.
    @inlinable
    public mutating func flush(fin: Bool = false) throws {
        self.table.markOutputPending()
        try self.transport.pointee.core.flush(fin: fin)
    }
}

// MARK: - Closing

@available(anyAppleOS 26, *)
extension QUICStream where Consumer: ~Copyable {
    /// Resets the send side, telling the peer to discard what it has of this stream.
    ///
    /// - Parameter code: The application error code to send in RESET\_STREAM.
    @inlinable
    public mutating func sendResetStream(code: QUICApplicationErrorCode) {
        self.table.markOutputPending()
        self.transport.pointee.core.sendResetStream(code: code)
    }

    /// Closes the receive side, telling the peer to stop sending on this stream.
    ///
    /// - Parameter code: The application error code to send in STOP\_SENDING.
    @inlinable
    public mutating func sendStopSending(code: QUICApplicationErrorCode) {
        self.table.markOutputPending()
        self.transport.pointee.core.sendStopSending(code: code)
    }
}

@available(anyAppleOS 26, *)
@available(*, unavailable)
extension QUICStream: Sendable where Consumer: ~Copyable {}
