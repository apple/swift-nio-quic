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
    /// Associate `newID` with the connection this registrar belongs to.
    ///
    /// - Parameter newID: A connection ID to route to this connection.
    /// - Returns: Whether the association was made. Returns `false` if the connection is no
    ///   longer registered (i.e. it closed) or `newID` already routes to a connection.
    func associate(_ newID: QUICConnectionID) -> Bool

    /// Retires `connectionID`, so packets carrying it are no longer routed to this connection.
    ///
    /// The connection itself is unaffected: its identity does not depend on any connection ID,
    /// so retiring its last one leaves it live but unreachable by the peer.
    ///
    /// - Parameter connectionID: The ID to retire.
    /// - Returns: Whether the ID was retired. Returns `false` if it wasn't routing to this
    ///   connection, so one connection cannot retire another's route.
    func retire(_ connectionID: QUICConnectionID) -> Bool

    /// Generates a new connection ID.
    ///
    /// - Returns: A new connection ID.
    func generateID() -> QUICConnectionID
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    enum ConnectionIDRegistrar: QUICConnectionIDRegistrar {
        case live(QUICHandler.RegistrarView)
        case test(any QUICConnectionIDRegistrar)

        func associate(_ newID: QUICConnectionID) -> Bool {
            switch self {
            case .live(let registrar):
                return registrar.associate(newID)
            case .test(let registrar):
                return registrar.associate(newID)
            }
        }

        func retire(_ connectionID: QUICConnectionID) -> Bool {
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
