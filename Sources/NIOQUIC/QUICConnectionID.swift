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

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

/// A QUIC connection ID.
@available(anyAppleOS 26, *)
public struct QUICConnectionID: CustomStringConvertible, Sendable {
    /// The maximum length of a connection ID.
    public static let maxLength = UInt8(20)

    /// The length of the random generated connection IDs.
    public static let randomIDLength = UInt8(8)

    /// Convenience static property for a zero initialised connection ID.
    static let zero = Self(repeating: 0)

    /// Convenience static method to generate a random connection ID.
    ///
    /// - Parameters:
    ///     - using: The `RandomNumberGenerator` to use for generating the connection ID.
    /// - Note: The generated ID has a length of 8.
    static func random(using generator: inout any RandomNumberGenerator) -> Self {
        self.random(length: Int(Self.randomIDLength), using: &generator)
    }

    /// Generate a random connection ID with the specified length.
    ///
    /// - Parameters:
    ///     - length: The length of the connection ID to generate (0...20).
    ///     - using: The `RandomNumberGenerator` to use for generating the connection ID.
    public static func random<T: RandomNumberGenerator>(length: Int, using generator: inout T) -> Self {
        precondition((0...Int(Self.maxLength)).contains(length), "QUIC connection IDs are between 0 and 20 bytes long")

        var bytes = InlineArray<20, UInt8>(repeating: 0)
        if length > 0 {
            // Fill with random bytes. Each call to generator.next() gives 8 bytes.
            var mutableSpan = bytes.mutableSpan
            mutableSpan.withUnsafeMutableBufferPointer { buffer in
                var offset = 0
                while offset < Int(length) {
                    let random = generator.next()
                    for shift in stride(from: 0, to: 64, by: 8) {
                        guard offset < Int(length) else { break }
                        buffer[offset] = UInt8(truncatingIfNeeded: (random >> shift) & 0xFF)
                        offset += 1
                    }
                }
            }
        }

        return Self(bytes: bytes, length: UInt8(length))
    }

    public var description: String {
        self.withUnsafeBufferPointer {
            $0.hexEncoded
        }
    }

    /// The bytes of the connection ID.
    var bytes: InlineArray<20, UInt8>

    /// The storage for the length of the bytes of the connection ID.
    var _length: UInt8

    /// The length of the bytes of the connection ID.
    var length: Int {
        get {
            Int(self._length)
        }
        set {
            precondition(
                (0...Int(Self.maxLength)).contains(newValue),
                "QUIC connection IDs are between 0 and 20 bytes long"
            )
            self._length = UInt8(newValue)
        }
    }

    /// An initialiser to construct a connection ID from the passed bytes.
    ///
    /// - Parameters:
    ///     - bytes: The bytes of the connection ID.
    ///     - length: The length of the bytes parameter.
    ///
    /// - Precondition: The length must be in the range (0...20).
    public init(bytes: InlineArray<20, UInt8>, length: UInt8) {
        precondition((0...Self.maxLength).contains(length), "QUIC connection IDs are between 0 and 20 bytes long")
        self.bytes = bytes
        self._length = length
    }

    /// An initialiser to construct a connection ID from a `Span` of `UInt8`s.
    ///
    /// - Parameters:
    ///     - bytes: The bytes of the connection ID. Must be 0-20 bytes.
    public init(bytes: Span<UInt8>) {
        precondition(
            (0...Int(Self.maxLength)).contains(bytes.count),
            "QUIC connection IDs are between 0 and 20 bytes long"
        )
        let length = UInt8(bytes.count)
        var storage = InlineArray<20, UInt8>(repeating: 0)
        if length > 0 {
            var mutableSpan = storage.mutableSpan
            mutableSpan.withUnsafeMutableBufferPointer { destBuffer in
                bytes.withUnsafeBufferPointer { srcBuffer in
                    _ = destBuffer.update(fromContentsOf: srcBuffer)
                }
            }
        }
        self.init(bytes: storage, length: length)
    }

    /// An initialiser to construct a connection ID where all bytes are the same.
    /// This is really only useful to construct the zero connection ID.
    ///
    /// - Parameters:
    ///     - repeating: The byte to repeat for every byte in the connection ID.
    private init(repeating byte: UInt8) {
        self.bytes = InlineArray(repeating: byte)
        self._length = Self.maxLength
    }

    /// Invokes the given closure with a pointer to the bytes of the connection ID.
    @usableFromInline
    func withUnsafeBufferPointer<ReturnType>(
        _ body: (UnsafeBufferPointer<UInt8>) throws -> ReturnType
    ) rethrows -> ReturnType {
        try self.bytes.span.extracting(first: self.length).withUnsafeBufferPointer { bytesPointer in
            try body(bytesPointer)
        }
    }

    /// Calls the given closure with a mutable pointer to the bytes of the connection ID.
    ///
    /// - Important: You have to update the description yourself.
    mutating func withUnsafeMutableBufferPointer<ReturnType>(
        _ body: (UnsafeMutableBufferPointer<UInt8>) throws -> ReturnType
    ) rethrows -> ReturnType {
        let length = self.length
        var mutableSpan = self.bytes.mutableSpan
        return try mutableSpan.withUnsafeMutableBufferPointer { fullBufferPointer in
            let fittedBufferPointer = UnsafeMutableBufferPointer(rebasing: fullBufferPointer[0..<length])
            return try body(fittedBufferPointer)
        }
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionID: Equatable {
    public static func == (lhs: QUICConnectionID, rhs: QUICConnectionID) -> Bool {
        guard lhs.length == rhs.length else {
            return false
        }

        guard lhs.length > 0 else {
            return true
        }

        let result = lhs.withUnsafeBufferPointer { lhsPtr in
            rhs.withUnsafeBufferPointer { rhsPtr in
                memcmp(lhsPtr.baseAddress!, rhsPtr.baseAddress!, lhsPtr.count)
            }
        }

        return result == 0
    }
}

@available(anyAppleOS 26, *)
extension QUICConnectionID: Hashable {
    public func hash(into hasher: inout Hasher) {
        self.withUnsafeBufferPointer {
            hasher.combine(bytes: UnsafeRawBufferPointer($0))
        }
    }
}
