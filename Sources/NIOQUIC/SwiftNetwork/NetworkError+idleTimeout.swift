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

@_spi(ProtocolProvider) import SwiftNetwork

#if canImport(Glibc)
import Glibc
#elseif canImport(Android)
import Android
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

@available(anyAppleOS 26, *)
extension NetworkError {
    /// Whether this is SwiftNetwork's idle timeout. That closes a connection silently (RFC 9000,
    /// Section 10.1), so the connection and its streams treat it as a clean close.
    var isIdleTimeout: Bool {
        self == .posix(ETIMEDOUT)
    }
}
