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

/// The routing table for a QUIC handler's connections.
///
/// Connections are stored under a ``ConnectionHandle``, and any number of ``QUICConnectionID``s
/// route to that handle. Retiring a connection ID drops one route; it never moves a connection or
/// invalidates a handle, so a connection with no routes left is unreachable by the peer but still
/// identifiable, and is removed with ``remove(_:)`` when it closes.
@available(anyAppleOS 27, *)
struct ConnectionRegistry<Value> {
    // Note: 'connections' and 'connectionIDs' are separate: this avoids the  `values` iterator
    // retaining the IDs array each time it's called (which can be frequent, e.g. on writability
    // changes).

    /// Connections, in the slots their routes point to. Removing a connection moves the last one
    /// into its slot.
    ///
    /// Slots rather than a dictionary keyed by handle, so that routing a datagram hashes once.
    private var connections: [Value]
    /// The handle of the connection in each slot.
    private var handles: [ConnectionHandle]
    /// The slot of each connection, by handle.
    private var slots: [ConnectionHandle: Int]
    /// The IDs for a given connection handle.
    private var connectionIDs: [ConnectionHandle: [QUICConnectionID]]
    /// The slot of the connection each connection ID routes to.
    private var routes: [QUICConnectionID: Int]

    init() {
        self.connections = []
        self.handles = []
        self.slots = [:]
        self.connectionIDs = [:]
        self.routes = [:]
    }

    /// The number of connections in the registry.
    ///
    /// This counts connections, not routes: a connection with `n` connection IDs contributes
    /// `1`, not `n`.
    var count: Int {
        self.slots.count
    }

    /// Every connection in the registry, in no particular order.
    var values: [Value] {
        self.connections
    }

    /// Returns the connection `connectionID` routes to, or `nil` if it routes nowhere.
    subscript(connectionID: QUICConnectionID) -> Value? {
        if let slot = self.routes[connectionID] {
            return self.connections[slot]
        } else {
            return nil
        }
    }

    /// Adds a connection, routable by `connectionID`.
    ///
    /// - Parameters:
    ///   - value: The connection to store.
    ///   - handle: The connection's handle. Must not already be in the registry.
    ///   - connectionID: The connection ID the connection is initially routable by. Must not
    ///     already route to a connection.
    mutating func insert(_ value: Value, forHandle handle: ConnectionHandle, connectionID: QUICConnectionID) {
        precondition(self.slots[handle] == nil, "Handle \(handle) is already registered")
        precondition(self.routes[connectionID] == nil, "Connection ID \(connectionID) already routes to a connection")

        let slot = self.connections.count
        self.connections.append(value)
        self.handles.append(handle)

        self.slots[handle] = slot
        self.connectionIDs[handle] = [connectionID]
        self.routes[connectionID] = slot
    }

    /// Adds `connectionID` as another route to the connection identified by `handle`.
    ///
    /// - Parameters:
    ///   - connectionID: The connection ID to route.
    ///   - handle: The connection to route it to.
    /// - Returns: `true` if the route was added; `false` if `handle` is unknown or
    ///   `connectionID` already routes somewhere.
    @discardableResult
    mutating func associate(_ connectionID: QUICConnectionID, with handle: ConnectionHandle) -> Bool {
        guard let slot = self.slots[handle] else { return false }
        guard self.routes[connectionID] == nil else { return false }

        self.routes[connectionID] = slot
        // '!' okay: the guard above checked the connection is registered.
        self.connectionIDs[handle]!.append(connectionID)

        return true
    }

    /// Removes the route for `connectionID`, leaving its connection in place.
    ///
    /// Only the connection a route points at may retire it, so one connection can't make another
    /// unreachable.
    ///
    /// - Parameters:
    ///   - connectionID: The connection ID to stop routing.
    ///   - handle: The connection retiring it.
    /// - Returns: `true` if a route was removed, `false` if `connectionID` routed nowhere or
    ///   routed to a connection other than `handle`.
    /// - Complexity: O(*c*) where *c* is the number of connection IDs routing to the connection.
    @discardableResult
    mutating func retire(_ connectionID: QUICConnectionID, from handle: ConnectionHandle) -> Bool {
        guard let slot = self.slots[handle], self.routes[connectionID] == slot else { return false }

        self.routes.removeValue(forKey: connectionID)
        // '!' okay: a registered connection always has a list of connection IDs.
        self.connectionIDs[handle]!.removeAll { $0 == connectionID }

        return true
    }

    /// Removes a connection and every route to it.
    ///
    /// - Parameter handle: The connection to remove.
    /// - Returns: The removed connection, or `nil` if `handle` is unknown.
    /// - Complexity: O(*c*) where *c* is the number of connection IDs routing to the connection,
    ///   and to the connection that moves into its slot.
    @discardableResult
    mutating func remove(_ handle: ConnectionHandle) -> Value? {
        guard let slot = self.slots.removeValue(forKey: handle) else { return nil }

        // '!' okay: a registered connection always has a list of connection IDs.
        for connectionID in self.connectionIDs.removeValue(forKey: handle)! {
            self.routes.removeValue(forKey: connectionID)
        }

        // Move the last connection into the slot, and its routes with it.
        let lastSlot = self.connections.count - 1
        if slot != lastSlot {
            self.connections.swapAt(slot, lastSlot)
            self.handles.swapAt(slot, lastSlot)

            let moved = self.handles[slot]
            self.slots[moved] = slot
            // '!' okay: a registered connection always has a list of connection IDs.
            for connectionID in self.connectionIDs[moved]! {
                self.routes[connectionID] = slot
            }
        }
        self.handles.removeLast()
        return self.connections.removeLast()
    }
}

/// What the registry needs from a connection, and what the QUIC handler delivers packets to.
///
/// The handler stores its connections as `any QUICConnectionRegistryView`, whose type is concrete:
/// `QUICConnectionChannel`, which conforms, is generic over its consumer, and storing it under a type
/// of its own made the handler's per-packet bookkeeping look up metadata at run time. Class-bound,
/// so that the existential is two words and copying it is a retain.
///
/// The requirements must be called on the connection's event loop, hence their leading underscores:
/// see `QUICConnectionChannel`.
@available(anyAppleOS 27, *)
protocol QUICConnectionRegistryView: AnyObject {
    /// Delivers a packet. Returns whether the read caused the connection to enter a read loop.
    func _parentChannelRead(_ envelope: AddressedEnvelope<ByteBuffer>) -> Bool
    /// Ends the read loop the connection entered.
    func _parentChannelReadComplete()
    func _parentInvokedShutdown(promise: EventLoopPromise<Void>?)
    /// Tears the connection down immediately.
    ///
    /// Skips waiting for the QUIC close handshake or for per-stream close futures; used when a
    /// graceful shutdown overran its deadline.
    func _forceClose()
    func _parentChannelBecameInactive()
    func _parentChannelWritabilityChanged(to isWritable: Bool)
    func _parentChannelUserInboundEvent(_ event: Any)
}

/// A class because a local `var` trips up CMO.
private final class ShutdownRace {
    var finished = false
}

@available(anyAppleOS 27, *)
extension ConnectionRegistry where Value == any QUICConnectionRegistryView {
    /// Shut down every registered connection, racing graceful completion against `deadline`.
    ///
    /// If all connections drain before the deadline, `promise` succeeds. If the deadline wins,
    /// any connections still open are torn down abruptly via `parentChannelInactive` and the
    /// promise still succeeds — the caller is told the shutdown completed even if it had to
    /// be forced.
    ///
    /// When `promise` is `nil` shutdown is best-effort and the deadline is ignored.
    func shutdownConnections(deadline: NIODeadline, promise: EventLoopPromise<Void>.Isolated?) {
        guard let promise = promise else {
            for view in self.values {
                view._parentInvokedShutdown(promise: nil)
            }
            return
        }

        let eventLoop = promise.nonisolated().futureResult.eventLoop
        var futures = [EventLoopFuture<Void>]()
        futures.reserveCapacity(self.count)

        for view in self.values {
            let promise = eventLoop.makePromise(of: Void.self)
            view._parentInvokedShutdown(promise: promise)
            futures.append(promise.futureResult)
        }

        let allCompleteFuture = EventLoopFuture.andAllComplete(futures, on: eventLoop)
        let connectionsSnapshot = self.values
        let race = ShutdownRace()

        // Start a timeout which races with the connections closing before the deadline.
        let timeoutTask = eventLoop.assumeIsolated().scheduleTask(deadline: deadline) {
            if race.finished { return }
            race.finished = true

            for view in connectionsSnapshot {
                view._forceClose()
            }

            promise.succeed(())
        }

        allCompleteFuture.assumeIsolated().whenComplete { _ in
            if race.finished { return }
            race.finished = true

            timeoutTask.cancel()
            promise.succeed(())
        }
    }

    func notifyParentChannelInactive() {
        for view in self.values {
            view._parentChannelBecameInactive()
        }
    }

    func notifyParentChannelWritabilityChanged(_ writable: Bool) {
        for view in self.values {
            view._parentChannelWritabilityChanged(to: writable)
        }
    }

    func notifyParentChannelUserInboundEventTriggered(_ event: Any) {
        for view in self.values {
            view._parentChannelUserInboundEvent(event)
        }
    }
}
