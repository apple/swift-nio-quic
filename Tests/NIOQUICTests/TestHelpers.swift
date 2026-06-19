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

import Foundation
import Synchronization
import XCTest

extension XCTestCase {
    static let goodTestCertificatePath: String = Bundle.module.url(forResource: "testcert", withExtension: "pem")!.path

    static let goodTestCertificateKeyPath: String = Bundle.module.url(forResource: "testkey", withExtension: "pem")!
        .path

    static let testPrivateKeyPath: String = Bundle.module.url(forResource: "privateKey", withExtension: "der")!
        .path

    static let testPublicKeyPath: String = Bundle.module.url(forResource: "publicKey", withExtension: "der")!
        .path
}

final class Counter: Sendable {
    private let value = Atomic<Int>(0)

    @discardableResult
    func increment() -> Int {
        self.value.wrappingAdd(1, ordering: .sequentiallyConsistent).newValue
    }

    func load() -> Int {
        self.value.load(ordering: .sequentiallyConsistent)
    }
}
