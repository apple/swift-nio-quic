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
@available(anyAppleOS 26, *)
struct ConnectionRegistry<Value> {
    // Note: 'connections' and 'connectionIDs' are separate: this avoids the  `values` iterator
    // retaining the IDs array each time it's called (which can be frequent, e.g. on writability
    // changes).

    /// Connections, keyed by their handle.
    private var connections: [ConnectionHandle: Value]
    /// The IDs for a given connection handle.
    private var connectionIDs: [ConnectionHandle: [QUICConnectionID]]
    /// The handle each connection ID routes to.
    private var handles: [QUICConnectionID: ConnectionHandle]

    init() {
        self.connections = [:]
        self.connectionIDs = [:]
        self.handles = [:]
    }

    /// The number of connections in the registry.
    ///
    /// This counts connections, not routes: a connection with `n` connection IDs contributes
    /// `1`, not `n`.
    var count: Int {
        self.connections.count
    }

    /// Every connection in the registry, in no particular order.
    var values: [ConnectionHandle: Value].Values {
        self.connections.values
    }

    /// Returns the connection `connectionID` routes to, or `nil` if it routes nowhere.
    subscript(connectionID: QUICConnectionID) -> Value? {
        if let handle = self.handles[connectionID] {
            return self.connections[handle]
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
        precondition(!self.connections.keys.contains(handle), "Handle \(handle) is already registered")
        precondition(self.handles[connectionID] == nil, "Connection ID \(connectionID) already routes to a connection")
        self.connections[handle] = value
        self.connectionIDs[handle] = [connectionID]
        self.handles[connectionID] = handle
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
        guard self.connections.keys.contains(handle) else { return false }
        guard self.handles[connectionID] == nil else { return false }

        self.handles[connectionID] = handle
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
        guard self.handles[connectionID] == handle else { return false }

        self.handles.removeValue(forKey: connectionID)
        // '!' okay: a route always points at a registered connection.
        self.connectionIDs[handle]!.removeAll { $0 == connectionID }

        return true
    }

    /// Removes a connection and every route to it.
    ///
    /// - Parameter handle: The connection to remove.
    /// - Returns: The removed connection, or `nil` if `handle` is unknown.
    /// - Complexity: O(*c*) where *c* is the number of connection IDs routing to the connection.
    @discardableResult
    mutating func remove(_ handle: ConnectionHandle) -> Value? {
        guard let value = self.connections.removeValue(forKey: handle) else { return nil }

        // '!' okay: a registered connection always has a list of connection IDs.
        for connectionID in self.connectionIDs.removeValue(forKey: handle)! {
            self.handles.removeValue(forKey: connectionID)
        }

        return value
    }
}

/// What the registry needs from a connection.
@available(anyAppleOS 26, *)
protocol QUICConnectionRegistryView {
    func shutdown(promise: EventLoopPromise<Void>?)
    func forceClose()
    func parentChannelInactive()
    func parentChannelWritabilityChanged(to writable: Bool)
    func parentChannelUserInboundEventTriggered(_ event: Any)
}

/// A class because a local `var` trips up CMO.
private final class ShutdownRace {
    var finished = false
}

@available(anyAppleOS 26, *)
extension ConnectionRegistry where Value: QUICConnectionRegistryView {
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
                view.shutdown(promise: nil)
            }
            return
        }

        let eventLoop = promise.nonisolated().futureResult.eventLoop
        var futures = [EventLoopFuture<Void>]()
        futures.reserveCapacity(self.count)

        for view in self.values {
            let promise = eventLoop.makePromise(of: Void.self)
            view.shutdown(promise: promise)
            futures.append(promise.futureResult)
        }

        let allCompleteFuture = EventLoopFuture.andAllComplete(futures, on: eventLoop)
        let connectionsSnapshot = Array(self.values)
        let race = ShutdownRace()

        // Start a timeout which races with the connections closing before the deadline.
        let timeoutTask = eventLoop.assumeIsolated().scheduleTask(deadline: deadline) {
            if race.finished { return }
            race.finished = true

            for view in connectionsSnapshot {
                view.forceClose()
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
            view.parentChannelInactive()
        }
    }

    func notifyParentChannelWritabilityChanged(_ writable: Bool) {
        for view in self.values {
            view.parentChannelWritabilityChanged(to: writable)
        }
    }

    func notifyParentChannelUserInboundEventTriggered(_ event: Any) {
        for view in self.values {
            view.parentChannelUserInboundEventTriggered(event)
        }
    }
}
