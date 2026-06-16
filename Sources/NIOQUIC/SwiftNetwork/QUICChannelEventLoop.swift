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
struct ScheduledEntry {
    var milliseconds: Int64
    var reference: SwiftNetwork.TimerReference
    var scheduledTask: Scheduled<Void>??
}

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

    func runImmediate(_ task: @escaping (() -> Void)) {
        // Swift Network expects this to run asynchronously, i.e., the task should be scheduled
        // even when called from the same event loop.
        self.eventLoop.assumeIsolated().execute(task)
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
