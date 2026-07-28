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

extension NIOLoopBound {
    func flatSubmit<Result: Sendable>(
        _ execute: @Sendable @escaping (Value) -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {
        self.eventLoop.flatSubmit {
            execute(self.value)
        }
    }

    func execute(_ execute: @Sendable @escaping (Value) -> Void) {
        self.eventLoop.execute {
            execute(self.value)
        }
    }
}
