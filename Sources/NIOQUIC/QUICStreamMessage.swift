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

/// A data structure for processing QUIC stream messages.
struct QUICStreamMessage: Hashable {
    /// The actual data.
    var data: ByteBuffer
    /// Indicates if the stream should be closed.
    var fin: Bool

    /// Initializes a new ``QUICStreamMessage``.
    ///
    /// - Parameters:
    ///   - data: The actual data.
    ///   - fin: Indicates if the stream should be closed.
    init(data: ByteBuffer, fin: Bool) {
        self.data = data
        self.fin = fin
    }
}
