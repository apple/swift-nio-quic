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

import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOQUICHelpers
import XCTest

@testable import NIOQUIC

/// Marker handler installed on the QUIC connection channel. It does nothing.
private final class ConnectionMarker: ChannelInboundHandler, Sendable {
    typealias InboundIn = NIOAny
}

/// Marker handler installed on the UDP datagram channel. It does nothing.
private final class UDPMarker: ChannelInboundHandler, Sendable {
    typealias InboundIn = NIOAny
}

@available(anyAppleOS 26, *)
final class ChannelHierarchyTests: XCTestCase {
    /// End-to-end test that asserts the channel hierarchy from the perspective
    /// of a QUIC stream channel:
    ///
    ///   stream.parent        == connection channel  (has ConnectionMarker, no UDPMarker)
    ///   stream.parent.parent == UDP channel         (has UDPMarker, no ConnectionMarker)
    ///
    /// The assertions run inside the stream channel initializer on both the
    /// server side (inbound stream) and the client side (outbound stream).
    func testStreamChannelHasCorrectParentHierarchy() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup.singleton
        let host = "127.0.0.1"

        let serverVerified = eventLoopGroup.any().makePromise(of: Void.self)
        let clientVerified = eventLoopGroup.any().makePromise(of: Void.self)

        let serverChannel = try await createServerChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: Logger(label: "Server"),
            udpChannelInitializer: { udpChannel in
                try udpChannel.pipeline.syncOperations.addHandler(UDPMarker())
            },
            inboundConnectionInitializer: { connectionChannel, _ in
                connectionChannel.eventLoop.makeCompletedFuture {
                    try connectionChannel.pipeline.syncOperations.addHandler(ConnectionMarker())
                }
            },
            inboundStreamInitializer: { streamChannel in
                streamChannel.eventLoop.makeCompletedFuture {
                    Self.assertHierarchy(forStream: streamChannel, promise: serverVerified)
                }
            },
            noMoreConnections: {}
        ).get()
        let serverPort = serverChannel.localAddress!.port!

        let clientChannel = try await createClientChannel(
            eventLoopGroup: eventLoopGroup,
            host: host,
            port: 0,
            logger: Logger(label: "Client"),
            udpChannelInitializer: { udpChannel in
                try udpChannel.pipeline.syncOperations.addHandler(UDPMarker())
            }
        ).get()

        // Open an outbound connection so we can hook the connection initializer
        // and add ConnectionMarker before any stream is created.
        let (connectionChannel, streamCreator) = try await clientChannel.pipeline.handler(type: QUICHandler.self)
            .flatMap {
                quicHandler in
                quicHandler.createOutboundConnection(
                    serverName: "\(host):\(serverPort)",
                    remoteAddress: try! .init(ipAddress: host, port: serverPort),
                    connectionInitializer: { connectionChannel, _ in
                        connectionChannel.eventLoop.makeCompletedFuture {
                            try connectionChannel.pipeline.syncOperations.addHandler(ConnectionMarker())
                        }
                    },
                    inboundStreamInitializer: { streamChannel in
                        streamChannel.eventLoop.makeSucceededVoidFuture()
                    }
                )
            }.get()

        // Open an outbound stream and verify hierarchy in its initializer.
        let streamChannel = try await streamCreator.createBidirectionalStream { parameters in
            parameters.channel.eventLoop.makeCompletedFuture {
                Self.assertHierarchy(forStream: parameters.channel, promise: clientVerified)
                return parameters.channel
            }
        }.get()

        // Send a byte so the server materializes the inbound stream and runs
        // its inboundStreamInitializer (which performs the server-side assertion).
        try await streamChannel.writeAndFlush(ByteBuffer(string: "x"))

        try await serverVerified.futureResult.get()
        try await clientVerified.futureResult.get()

        // Tidy up.
        try? await streamChannel.close()
        try? await connectionChannel.close()
        try? await clientChannel.close()
        try? await serverChannel.close()
    }

    /// Asserts the parent/grandparent hierarchy of a stream channel and succeeds
    /// or fails the supplied promise.
    private static func assertHierarchy(
        forStream streamChannel: any Channel,
        promise: EventLoopPromise<Void>
    ) {
        do {
            guard let connectionChannel = streamChannel.parent else {
                throw HierarchyError.missingParent("stream channel has no parent")
            }
            guard let udpChannel = connectionChannel.parent else {
                throw HierarchyError.missingParent("connection channel has no parent")
            }

            // Connection channel: must contain ConnectionMarker, must not contain UDPMarker.
            _ = try connectionChannel.pipeline.syncOperations.handler(type: ConnectionMarker.self)
            XCTAssertThrowsError(
                try connectionChannel.pipeline.syncOperations.handler(type: UDPMarker.self),
                "Connection channel unexpectedly contains UDPMarker"
            )

            // UDP channel: must contain UDPMarker, must not contain ConnectionMarker.
            _ = try udpChannel.pipeline.syncOperations.handler(type: UDPMarker.self)
            XCTAssertThrowsError(
                try udpChannel.pipeline.syncOperations.handler(type: ConnectionMarker.self),
                "UDP channel unexpectedly contains ConnectionMarker"
            )

            promise.succeed(())
        } catch {
            XCTFail("Channel hierarchy assertion failed: \(error)")
            promise.fail(error)
        }
    }

    private enum HierarchyError: Error {
        case missingParent(String)
    }
}
