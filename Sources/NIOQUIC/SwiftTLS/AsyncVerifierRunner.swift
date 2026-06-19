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

@available(anyAppleOS 26, *)
final class AsyncVerifierRunner: Sendable {
    let asyncVerifier: AsyncVerifier
    let asyncVerifierTask: Task<(), Never>

    init(asyncVerifier: AsyncVerifier) {
        self.asyncVerifierTask = Task {
            await asyncVerifier.run()
        }
        self.asyncVerifier = asyncVerifier
    }

    func terminate() {
        asyncVerifierTask.cancel()
    }

    func join() async {
        await asyncVerifierTask.value
    }

    func terminateAndJoin() async {
        asyncVerifierTask.cancel()
        await self.asyncVerifierTask.value
    }
}
