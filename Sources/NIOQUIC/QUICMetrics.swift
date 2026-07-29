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

import Metrics

/// This type holds optional metrics to be recorded at the QUIC layer.
public struct QUICMetrics: Sendable {
    // `@unchecked Sendable` is safe here because `Storage` is never mutated while shared: `QUICMetrics` has
    // copy-on-write semantics.
    private final class Storage: @unchecked Sendable {
        var connectionMetrics: Connection?

        var connectionCloseMetrics: ConnectionClose?

        var quicConnectionHandlerMetrics: QUICConnectionHandler?

        init(
            connectionMetrics: Connection?,
            connectionCloseMetrics: ConnectionClose?,
            quicConnectionHandlerMetrics: QUICConnectionHandler?
        ) {
            self.connectionMetrics = connectionMetrics
            self.connectionCloseMetrics = connectionCloseMetrics
            self.quicConnectionHandlerMetrics = quicConnectionHandlerMetrics
        }

        func copy() -> Self {
            Self(
                connectionMetrics: self.connectionMetrics,
                connectionCloseMetrics: self.connectionCloseMetrics,
                quicConnectionHandlerMetrics: self.quicConnectionHandlerMetrics
            )
        }
    }

    /// The backing storage.
    private var storage: Storage

    /// The identity of the backing storage. Only intended for use in tests.
    var _storageID: ObjectIdentifier {
        get {
            ObjectIdentifier(self.storage)
        }
    }

    /// Metrics to be recorded at the connection level.
    public struct Connection: Sendable {
        /// The total time the stream was open.
        public var streamDuration: Timer?

        /// Initializes a new instance of ``QUICMetrics.Connection``.
        public init(
            streamDuration: Timer? = nil
        ) {
            self.streamDuration = streamDuration
        }
    }

    /// Metrics to be reported at connection close.
    public struct ConnectionClose: Sendable {
        /// The number of QUIC packets received on the connection.
        public var receivedPackets: Recorder?

        /// The number of QUIC packets sent on the connection.
        public var sentPackets: Recorder?

        /// The number of QUIC packets lost on the connection.
        public var lostPackets: Recorder?

        /// The estimated round-trip time of the connection.
        public var roundTripTimeInNanoseconds: Timer?

        /// The size of the connection's congestion window in bytes.
        public var congestionWindowInBytes: Recorder?

        /// The most recent data delivery rate estimate in bytes/s.
        public var deliveryRateInBytesPerSecond: Recorder?

        /// The total time the connection was open.
        public var connectionDuration: Timer?

        /// Initializes a new instance of ``QUICMetrics.ConnectionClose``.
        public init(
            receivedPackets: Recorder? = nil,
            sentPackets: Recorder? = nil,
            lostPackets: Recorder? = nil,
            roundTripTimeInNanoseconds: Timer? = nil,
            congestionWindowInBytes: Recorder? = nil,
            deliveryRateInBytesPerSecond: Recorder? = nil,
            connectionDuration: Timer? = nil
        ) {
            self.receivedPackets = receivedPackets
            self.sentPackets = sentPackets
            self.lostPackets = lostPackets
            self.roundTripTimeInNanoseconds = roundTripTimeInNanoseconds
            self.congestionWindowInBytes = congestionWindowInBytes
            self.deliveryRateInBytesPerSecond = deliveryRateInBytesPerSecond
            self.connectionDuration = connectionDuration
        }
    }

    /// Metrics to be recorded at the QUIC Handler level.
    public struct QUICConnectionHandler: Sendable {
        /// The number of currently open connections.
        public var openConnections: Meter?

        /// Initializes a new instance of ``QUICMetrics.QUICConnectionHandler``.
        public init(
            openConnections: Meter? = nil
        ) {
            self.openConnections = openConnections
        }
    }

    /// Copies the backing storage if it is shared with another `QUICMetrics` instance.
    private mutating func copyStorageIfNotUniquelyReferenced() {
        if !isKnownUniquelyReferenced(&self.storage) {
            self.storage = self.storage.copy()
        }
    }

    /// Optional connection-level metrics to be recorded.
    public var connectionMetrics: Connection? {
        get {
            self.storage.connectionMetrics
        }

        set {
            self.copyStorageIfNotUniquelyReferenced()
            self.storage.connectionMetrics = newValue
        }
    }

    /// Optional metrics to be reported on connection close.
    public var connectionCloseMetrics: ConnectionClose? {
        get {
            self.storage.connectionCloseMetrics
        }

        set {
            self.copyStorageIfNotUniquelyReferenced()
            self.storage.connectionCloseMetrics = newValue
        }
    }

    /// Optional handler-level metrics to be recorded.
    public var quicConnectionHandlerMetrics: QUICConnectionHandler? {
        get {
            self.storage.quicConnectionHandlerMetrics
        }

        set {
            self.copyStorageIfNotUniquelyReferenced()
            self.storage.quicConnectionHandlerMetrics = newValue
        }
    }

    /// Initializes a new instance of ``QUICMetrics``.
    public init(
        connectionMetrics: Connection? = nil,
        connectionCloseMetrics: ConnectionClose? = nil,
        quicConnectionHandlerMetrics: QUICConnectionHandler? = nil
    ) {
        self.storage = Storage(
            connectionMetrics: connectionMetrics,
            connectionCloseMetrics: connectionCloseMetrics,
            quicConnectionHandlerMetrics: quicConnectionHandlerMetrics
        )
    }
}
