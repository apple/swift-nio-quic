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

import Logging
import NIOEmbedded
import Testing

@testable import NIOQUIC

struct QUICConfigurationTests {
    @available(anyAppleOS 26, *)
    @Test func handlerRejectsAConfigurationMissingARequiredMember() throws {
        func makeHandler(_ configuration: QUICConfiguration) throws {
            _ = try QUICHandler<QUICStreamChannels>.makeHandlerAndConnectionMultiplexer(
                channel: EmbeddedChannel(),
                quicConfiguration: configuration,
                logger: Logger(label: "test"),
                inboundStreamChannelInitializer: { $0.eventLoop.makeSucceededVoidFuture() }
            )
        }

        var configuration = QUICConfiguration()
        configuration.applicationProtocols = ["swift_nio_quic"]
        // Raw public keys are only loaded once a connection starts, so the paths don't need to exist.
        configuration.authenticationConfiguration = .rawPublicKeys(publicKeyFilePath: "", privateKeyFilePath: "")
        // Each rejected configuration misses exactly one required member.
        #expect(throws: QUICError.invalidConfiguration) { try makeHandler(configuration) }  // no serverName
        configuration.serverName = "quic-test.local"
        try makeHandler(configuration)
        configuration.authenticationConfiguration = nil
        #expect(throws: QUICError.invalidConfiguration) { try makeHandler(configuration) }  // no authentication

        configuration.role = .client
        #expect(throws: QUICError.invalidConfiguration) { try makeHandler(configuration) }  // no verification
        configuration.verificationConfiguration = .rawPublicKeys(publicKeyFilePath: "")
        try makeHandler(configuration)
        configuration.applicationProtocols = []
        #expect(throws: QUICError.invalidConfiguration) { try makeHandler(configuration) }  // no applicationProtocols
    }
}
