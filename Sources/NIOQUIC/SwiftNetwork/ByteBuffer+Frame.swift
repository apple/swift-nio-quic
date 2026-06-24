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
@_spi(ProtocolProvider) import SwiftNetwork

@available(anyAppleOS 26, *)
extension ByteBuffer {
    @discardableResult
    mutating func writeFrame(_ frame: consuming SwiftNetwork.Frame) -> Int {
        let bytesWrittern =
            frame.span?.withUnsafeBufferPointer {
                self.writeBytes($0)
            } ?? 0
        frame.finalize(success: true)
        return bytesWrittern
    }

    init(frame: consuming SwiftNetwork.Frame) {
        self.init()
        self.writeFrame(frame)
    }
}
