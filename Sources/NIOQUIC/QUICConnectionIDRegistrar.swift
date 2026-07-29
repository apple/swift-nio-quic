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
protocol QUICConnectionIDRegistrar {
    /// Associate `newID` to the same connection as `existingID`.
    ///
    /// - Parameters:
    ///   - newID: A connection ID to associate with an existing ID.
    ///   - existingID: A connection ID which already exists.
    /// - Returns: whether the association was made. Returns `false` if `existingID`
    ///   doesn't exist (i.e. connection closed) or `newID` collides with an existing ID.
    func associate(_ newID: QUICConnectionID, with existingID: QUICConnectionID) -> Bool

    /// Retires `connectionID`.
    ///
    /// - Parameter connectionID: The ID to retire.
    /// - Returns: The outcome of the retirement, including the replacement key if the connection
    ///   was registered under `connectionID` and had to be re-keyed.
    func retire(_ connectionID: QUICConnectionID) -> OnRetireConnectionID

    /// Generates a new connection ID.
    ///
    /// - Returns: A new connection ID.
    func generateID() -> QUICConnectionID
}

/// The outcome of retiring a connection ID from the routing table.
@available(anyAppleOS 26, *)
enum OnRetireConnectionID: Hashable {
    /// The ID wasn't registered, nothing was retired.
    case notRegistered
    /// The ID was retired and the connection is still registered under the same key.
    case retired
    /// The ID was retired. It was the key the connection was registered under, so `key` was
    /// promoted in its place: the connection must use `key` to unregister itself.
    case retiredAndRekeyed(key: QUICConnectionID)
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    enum ConnectionIDRegistrar: QUICConnectionIDRegistrar {
        case live(QUICHandler.RegistrarView)
        case test(any QUICConnectionIDRegistrar)

        func associate(_ newID: QUICConnectionID, with existingID: QUICConnectionID) -> Bool {
            switch self {
            case .live(let registrar):
                return registrar.associate(newID, with: existingID)
            case .test(let registrar):
                return registrar.associate(newID, with: existingID)
            }
        }

        func retire(_ connectionID: QUICConnectionID) -> OnRetireConnectionID {
            switch self {
            case .live(let registrar):
                return registrar.retire(connectionID)
            case .test(let registrar):
                return registrar.retire(connectionID)
            }
        }

        func generateID() -> QUICConnectionID {
            switch self {
            case .live(let registrar):
                return registrar.generateID()
            case .test(let registrar):
                return registrar.generateID()
            }
        }
    }
}
