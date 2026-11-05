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

import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
import Testing

@testable import NIOQUIC

@Suite("QUIC application abort errors")
struct QUICApplicationAbortErrorTests {
    @available(anyAppleOS 26, *)
    @Test("Missing application error code does not create an abort")
    func missingApplicationErrorCodeDoesNotCreateAbort() {
        #expect(makeQUICApplicationAbortError(nil) == nil)
    }

    @available(anyAppleOS 26, *)
    @Test("Application error code is preserved")
    func applicationErrorCodeIsPreserved() throws {
        let abort = try #require(makeQUICApplicationAbortError(QUICApplicationErrorCode(42)))
        #expect(abort.networkError.quicApplicationError == 42)
    }
}
