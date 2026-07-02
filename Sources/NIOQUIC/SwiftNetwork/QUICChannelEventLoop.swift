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
@_spi(ProtocolProvider) @_spi(Essentials) import SwiftNetwork

// TODO: Make ScheduledEntry ~Copyable with UniqueArray / PriorityQueue
@available(anyAppleOS 26, *)
struct ScheduledEntry {
    var milliseconds: Int64
    var reference: SwiftNetwork.TimerReference
    var scheduledTask: Scheduled<Void>??
}

@available(anyAppleOS 26, *)
final class QUICChannelEventLoop: NetworkContext.Scheduler, CustomStringConvertible {

    internal let description = "QUICChannelEventLoop"
    internal var runningInScheduler: Bool {
        self.eventLoop.inEventLoop
    }

    private var scheduledTasks: [SwiftNetwork.TimerReference: ScheduledEntry] = [:]
    private let eventLoop: any EventLoop
    internal init(eventLoop: any EventLoop) {
        self.eventLoop = eventLoop
    }

    private struct UnsafeTransfer<Wrapped>: @unchecked Sendable {
        var wrappedValue: Wrapped
        init(_ wrappedValue: Wrapped) {
            self.wrappedValue = wrappedValue
        }
    }

    func runImmediate(_ task: @escaping (() -> Void)) {
        if self.eventLoop.inEventLoop {
            self.eventLoop.assumeIsolated().execute(task)
        } else {
            // Remove once this has landed: https://github.com/apple/swift-network-evolution/pull/36
            let transfer = UnsafeTransfer(task)
            self.eventLoop.execute {
                let value = transfer.wrappedValue
                value()
            }
        }
    }

    func schedule(
        _ task: @escaping (() -> Void),
        milliseconds: Int64,
        reference: SwiftNetwork.TimerReference
    ) {
        // First, wipe out any previously scheduled tasks with this handle
        self.unschedule(reference: reference)
        // Create the entry and schedule it
        var scheduledEntry = ScheduledEntry(milliseconds: milliseconds, reference: reference)
        scheduledEntry.scheduledTask = self.eventLoop.assumeIsolated().scheduleTask(
            in: .milliseconds(scheduledEntry.milliseconds)
        ) {
            task()
        }
        self.scheduledTasks[reference] = scheduledEntry
    }

    func unschedule(reference: SwiftNetwork.TimerReference) {
        if self.scheduledTasks.isEmpty {
            return
        }
        if let removedEntry = self.scheduledTasks.removeValue(forKey: reference) {
            removedEntry.scheduledTask??.cancel()
        }
    }
}
