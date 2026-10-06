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

struct InitialPacketSizeTests {
    @Test(arguments: [(Int?.none, 1350), (1200, 1350), (1500, 1350)])
    func fixedIgnoresTheClientInitial(clientInitialSize: Int?, expected: Int) {
        #expect(InitialPacketSize.fixed(1350).size(forClientInitial: clientInitialSize) == expected)
    }

    @Test(arguments: [(Int?.none, 1200), (1350, 1350), (9000, 1400)])
    func matchingFollowsTheClientInitialUpToTheMaximum(clientInitialSize: Int?, expected: Int) {
        let initialPacketSize = InitialPacketSize.matchingClientInitial(upTo: 1400)
        #expect(initialPacketSize.size(forClientInitial: clientInitialSize) == expected)
    }
}
