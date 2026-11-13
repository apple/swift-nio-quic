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

@available(anyAppleOS 27, *)
extension NetworkError {
    /// Whether SwiftNetwork closed the connection because of a timeout: the idle timeout, which closes
    /// it silently (RFC 9000, Section 10.1), or the limit on how long it may stay in PTO recovery
    /// (`maxPTODuration`, two minutes by default), which sends CONNECTION_CLOSE with NO_ERROR. Both
    /// report the same error, and the connection and its streams treat either as a clean close.
    var isTimeout: Bool {
        self == .posix(ETIMEDOUT)
    }
}
