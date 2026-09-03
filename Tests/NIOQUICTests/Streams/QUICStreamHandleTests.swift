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

import Testing

@testable import NIOQUIC

@Suite
struct QUICStreamHandleTests {
    @Test
    func indexAndGenerationRoundTrip() {
        let handle = QUICStreamHandle(index: QUICStreamHandle.Index(3), generation: 7)
        #expect(handle.index == QUICStreamHandle.Index(3))
        #expect(handle.generation == 7)
    }

    @Test
    func handlesWithDifferentGenerationsDiffer() {
        let index = QUICStreamHandle.Index(1)
        let lhs = QUICStreamHandle(index: index, generation: 1)
        let rhs = QUICStreamHandle(index: index, generation: 2)
        #expect(lhs != rhs)
    }
}
