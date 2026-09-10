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

@available(anyAppleOS 26, *)
struct QUICApplicationAbort {
    let networkError: NetworkError
}

@available(anyAppleOS 26, *)
func makeQUICApplicationAbortError(_ applicationErrorCode: QUICApplicationErrorCode?) -> QUICApplicationAbort? {
    guard let applicationErrorCode else {
        return nil
    }

    return QUICApplicationAbort(
        networkError: NetworkError(quicApplicationError: applicationErrorCode.rawValue)
    )
}

@available(anyAppleOS 26, *)
extension QUICChannelStreamHandler {
    func abortInbound(error applicationAbort: QUICApplicationAbort?) {
        guard let applicationAbort else {
            return
        }

        self.abortInbound(error: applicationAbort.networkError)
    }
}
