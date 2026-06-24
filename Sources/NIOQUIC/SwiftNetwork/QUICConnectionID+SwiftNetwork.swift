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

@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

@available(anyAppleOS 26, *)
extension NIOQUIC.QUICConnectionID {
    /// Creates a NIOQUIC connection ID from a SwiftNetwork connection ID.
    init(_ connectionID: SwiftNetwork.QUICConnectionID) {
        self.init(
            bytes: connectionID.connectionIDStorage,
            length: UInt8(connectionID.length)
        )
    }
}

@available(anyAppleOS 26, *)
extension SwiftNetwork.QUICConnectionID {
    /// Creates a SwiftNetwork connection ID from a NIOQUIC connection ID.
    init(_ connectionID: NIOQUIC.QUICConnectionID) {
        self.init(
            storage: connectionID.bytes,
            size: connectionID.length
        )
    }
}
