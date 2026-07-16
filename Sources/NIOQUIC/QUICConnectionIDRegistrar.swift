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
public protocol QUICConnectionIDRegistrar {
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
    /// - Returns: `false` if it wasn't registered.
    func retire(_ connectionID: QUICConnectionID) -> Bool

    /// Generates a new connection ID.
    ///
    /// - Returns: A new connection ID.
    func generateID() -> QUICConnectionID
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    enum ConnectionIDRegistrar: QUICConnectionIDRegistrar {
        case test(any QUICConnectionIDRegistrar)

        func associate(_ newID: QUICConnectionID, with existingID: QUICConnectionID) -> Bool {
            switch self {
            case .test(let registrar):
                return registrar.associate(newID, with: existingID)
            }
        }

        func retire(_ connectionID: QUICConnectionID) -> Bool {
            switch self {
            case .test(let registrar):
                return registrar.retire(connectionID)
            }
        }

        func generateID() -> QUICConnectionID {
            switch self {
            case .test(let registrar):
                return registrar.generateID()
            }
        }
    }
}
