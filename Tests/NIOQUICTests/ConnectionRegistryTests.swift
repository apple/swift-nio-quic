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

struct ConnectionRegistryTests {
    /// Makes `count` distinct connection IDs.
    @available(anyAppleOS 26, *)
    private func makeConnectionIDs(_ count: Int) -> [QUICConnectionID] {
        var generator = RandomQUICConnectionIDGenerator()
        return (0..<count).map { _ in generator.next() }
    }

    @available(anyAppleOS 26, *)
    @Test
    func empty() {
        let registry = ConnectionRegistry<String>()
        #expect(registry.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func insertAndLookUpByCreationConnectionID() {
        var registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(1)
        let handle = ConnectionHandle.initial

        registry.insert("connection", forHandle: handle, connectionID: ids[0])

        #expect(registry.count == 1)
        #expect(registry[ids[0]] == "connection")
    }

    @available(anyAppleOS 26, *)
    @Test
    func lookUpByAssociatedConnectionID() {
        var registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(2)
        let handle = ConnectionHandle.initial

        registry.insert("connection", forHandle: handle, connectionID: ids[0])
        #expect(registry.associate(ids[1], with: handle) == true)

        #expect(registry[ids[0]] == "connection")
        #expect(registry[ids[1]] == "connection")
        // Associating a CID doesn't add a connection.
        #expect(registry.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test
    func lookUpUnknownConnectionID() {
        let registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(1)
        #expect(registry[ids[0]] == nil)
    }

    @available(anyAppleOS 26, *)
    @Test
    func associateRejectsConnectionIDRoutedElsewhere() {
        var registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(2)
        let first = ConnectionHandle.initial
        let second = first.next()

        registry.insert("first", forHandle: first, connectionID: ids[0])
        registry.insert("second", forHandle: second, connectionID: ids[1])

        #expect(registry.associate(ids[1], with: first) == false)
        #expect(registry[ids[1]] == "second")
    }

    @available(anyAppleOS 26, *)
    @Test
    func associateRejectsUnknownHandle() {
        var registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(1)
        #expect(registry.associate(ids[0], with: ConnectionHandle.initial) == false)
        #expect(registry[ids[0]] == nil)
    }

    @available(anyAppleOS 26, *)
    @Test
    func retireLeavesTheConnectionReachableByItsOtherConnectionIDs() {
        var registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(2)
        let handle = ConnectionHandle.initial

        registry.insert("connection", forHandle: handle, connectionID: ids[0])
        #expect(registry.associate(ids[1], with: handle) == true)

        // Retire the ID the connection was created with: nothing is promoted and
        // nothing is re-keyed, the connection is simply reachable by one fewer ID.
        #expect(registry.retire(ids[0], from: handle) == true)
        #expect(registry[ids[0]] == nil)
        #expect(registry[ids[1]] == "connection")
        #expect(registry.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test
    func retireLastConnectionIDKeepsTheConnectionRemovableByHandle() {
        var registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(1)
        let handle = ConnectionHandle.initial

        registry.insert("connection", forHandle: handle, connectionID: ids[0])
        #expect(registry.retire(ids[0], from: handle) == true)

        // Unroutable, but still a live connection the handler can close.
        #expect(registry[ids[0]] == nil)
        #expect(registry.count == 1)
        #expect(registry.remove(handle) == "connection")
        #expect(registry.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func retireUnknownConnectionID() {
        var registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(1)
        #expect(registry.retire(ids[0], from: ConnectionHandle.initial) == false)
    }

    @available(anyAppleOS 26, *)
    @Test
    func retireRejectsAnotherConnectionsConnectionID() {
        var registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(1)
        let owner = ConnectionHandle.initial
        let other = owner.next()

        registry.insert("owner", forHandle: owner, connectionID: ids[0])

        // A connection may only retire its own routes, otherwise it could make another
        // connection unreachable by the peer.
        #expect(registry.retire(ids[0], from: other) == false)
        #expect(registry[ids[0]] == "owner")
        #expect(registry.retire(ids[0], from: owner) == true)
        #expect(registry[ids[0]] == nil)
    }

    @available(anyAppleOS 26, *)
    @Test
    func retireStopsAConnectionIDBeingDroppedWhenItsOldConnectionIsRemoved() {
        var registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(1)
        let first = ConnectionHandle.initial
        let second = first.next()

        registry.insert("first", forHandle: first, connectionID: ids[0])
        #expect(registry.retire(ids[0], from: first) == true)

        // The ID now routes to a different connection. Removing the connection which used to
        // own it must not take the new route with it.
        registry.insert("second", forHandle: second, connectionID: ids[0])
        #expect(registry.remove(first) == "first")
        #expect(registry[ids[0]] == "second")
    }

    @available(anyAppleOS 26, *)
    @Test
    func removeDropsEveryRoute() {
        var registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(3)
        let handle = ConnectionHandle.initial

        registry.insert("connection", forHandle: handle, connectionID: ids[0])
        #expect(registry.associate(ids[1], with: handle) == true)
        #expect(registry.associate(ids[2], with: handle) == true)

        #expect(registry.remove(handle) == "connection")

        for id in ids {
            #expect(registry[id] == nil)
        }
        #expect(registry.count == 0)
    }

    @available(anyAppleOS 26, *)
    @Test
    func removeUnknownHandle() {
        var registry = ConnectionRegistry<String>()
        #expect(registry.remove(ConnectionHandle.initial) == nil)
    }

    @available(anyAppleOS 26, *)
    @Test
    func removeOneConnectionLeavesOthersIntact() {
        var registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(2)
        let first = ConnectionHandle.initial
        let second = first.next()

        registry.insert("first", forHandle: first, connectionID: ids[0])
        registry.insert("second", forHandle: second, connectionID: ids[1])

        #expect(registry.remove(first) == "first")
        #expect(registry[ids[0]] == nil)
        #expect(registry[ids[1]] == "second")
        #expect(registry.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test
    func values() {
        var registry = ConnectionRegistry<String>()
        let ids = self.makeConnectionIDs(2)

        let first = ConnectionHandle.initial
        registry.insert("first", forHandle: first, connectionID: ids[0])
        registry.insert("second", forHandle: first.next(), connectionID: ids[1])

        #expect(Set(registry.values) == ["first", "second"])
    }
}
