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
extension QUICConnectionChannel {
    /// The lifecycle state of a ``QUICConnectionChannel``.
    enum Lifecycle: Hashable {
        case idle
        case initializing
        case initialized
        case activated
        case closing
        case closed

        mutating func initialize() -> Bool {
            let canInitialize: Bool
            switch self {
            case .idle:
                self = .initializing
                canInitialize = true
            case .initializing, .initialized, .activated, .closing, .closed:
                canInitialize = false
            }
            return canInitialize
        }

        mutating func initialized() {
            switch self {
            case .initializing:
                self = .initialized
            case .idle, .initialized, .activated:
                fatalError("Internal inconsistency")
            case .closing, .closed:
                // The channel was closed while the user's initializer was still in flight.
                // Don't transition; we're already on the close path.
                break
            }
        }

        mutating func activated() {
            switch self {
            case .initialized:
                self = .activated
            case .idle, .initializing, .activated, .closed:
                fatalError("Internal inconsistency")
            case .closing:
                // We're being torn down; don't activate.
                break
            }
        }

        mutating func closing() {
            switch self {
            case .idle, .initializing, .initialized, .activated:
                self = .closing
            case .closing, .closed:
                // Already on or past the close path; nothing to do.
                break
            }
        }

        mutating func closed() {
            switch self {
            case .idle, .initializing, .initialized, .activated, .closing:
                self = .closed
            case .closed:
                break
            }
        }

        func outboundDataProcessed() -> Bool {
            let isInitializing: Bool
            switch self {
            case .idle, .initializing, .initialized:
                isInitializing = true
            case .activated, .closing, .closed:
                isInitializing = false
            }
            return isInitializing
        }
    }
}
