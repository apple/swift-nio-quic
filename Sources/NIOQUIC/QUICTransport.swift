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

/// The channel's view of its UDP outbound side.
///
/// ``QUICConnectionChannel`` writes finalized datagrams, flushes, and requests
/// reads through this seam rather than holding a concrete
/// ``QUICHandler/ChildView`` directly. This lets the outbound side be
/// substituted (for example, a recording double) when testing the channel in
/// isolation. The production conformer is ``QUICHandler/ChildView``.
@available(anyAppleOS 26, *)
protocol QUICTransport {
    /// Write a single addressed datagram to the UDP channel.
    func writeDatagram(_ envelope: AddressedEnvelope<ByteBuffer>, promise: EventLoopPromise<Void>?)

    /// Flush any datagrams written but not yet sent.
    func flush()

    /// Request a read from the UDP channel.
    func read()
}

@available(anyAppleOS 26, *)
extension QUICConnectionChannel {
    enum Transport: QUICTransport {
        case test(any QUICTransport)

        func writeDatagram(_ envelope: AddressedEnvelope<ByteBuffer>, promise: EventLoopPromise<Void>?) {
            switch self {
            case .test(let transport):
                transport.writeDatagram(envelope, promise: promise)
            }
        }

        func flush() {
            switch self {
            case .test(let transport):
                transport.flush()
            }
        }

        func read() {
            switch self {
            case .test(let transport):
                transport.read()
            }
        }
    }
}
