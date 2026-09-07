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

struct QUICStreamEventsTests {
    @available(anyAppleOS 26, *)
    @Test func distinctFlagsCombine() {
        var events = QUICStreamEvents.readable
        events.formUnion(.writable)
        #expect(events.contains(.readable))
        #expect(events.contains(.writable))
        #expect(!events.contains(.closed))
    }

    @available(anyAppleOS 26, *)
    @Test func streamReadyEventsHaveDistinctBits() {
        let all: [QUICStreamEvents] = [
            .opened, .readable, .writable, .reset, .stopSending, .closed,
        ]
        let combined = all.reduce(into: QUICStreamEvents()) { $0.formUnion($1) }
        #expect(combined.rawValue.nonzeroBitCount == all.count)
    }
}
