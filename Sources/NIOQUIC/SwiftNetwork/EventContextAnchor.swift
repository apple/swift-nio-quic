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

@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork

/// A protocol instance that exists only to enter the stack on behalf of code with no instance of its
/// own: the stream table's operations, which act for streams that aren't `ProtocolInstance`s.
@available(anyAppleOS 26, *)
final class EventContextAnchor: ProtocolInstance {
    let context: NetworkContext
    var identifier: InstanceIdentifier
    var eventManager: ProtocolEventManager

    /// Registers with `context`, which acquires the event context: must be called with none held.
    init(context: NetworkContext) {
        self.context = context
        var eventManager = ProtocolEventManager()
        self.identifier = InstanceIdentifier(context: context, eventManager: &eventManager)
        self.eventManager = eventManager
    }

    /// Unregisters, so its event manager doesn't trap on deinit.
    func retire(through callouts: QUICCallouts) {
        if self.identifier.isNone { return }
        callouts.enter(as: self) { eventContext in
            self.unregisterEventManager(in: &eventContext)
        }
    }
}
