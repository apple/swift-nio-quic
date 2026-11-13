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

/// A connection's `NetworkContext.Scheduler`: it reads the event loop's clock, runs immediate tasks on the loop, and
/// keeps the connection's timers in a ``QUICTimerQueue`` shared with the handler's other connections.
///
/// The stack acquires the event context itself only inside the tasks it schedules here, so the connection's queued
/// callouts run after each one.
@available(anyAppleOS 27, *)
final class EventLoopBackedScheduler: NetworkContext.Scheduler {
    internal var runningInScheduler: Bool {
        self.eventLoop.inEventLoop
    }

    /// The connection's timers, kept to reuse them each time a reference is scheduled again.
    private var timers: [TimerReference: QUICTimer] = [:]
    /// The queue that runs the timers.
    private let timerQueue: QUICTimerQueue
    private let eventLoop: any EventLoop
    /// The connection's queued callouts.
    private let callouts: QUICCallouts

    var now: NetworkClock.Instant {
        .zero + .nanoseconds(self.eventLoop.now.uptimeNanoseconds)
    }

    /// NIO has a single clock, so there is no offset between the continuous and absolute clocks.
    var nowAbsolute: NetworkClock.Instant {
        self.now
    }

    /// - Parameter timerQueue: The queue for the connection's timers, shared with the handler's other connections.
    internal init(eventLoop: any EventLoop, callouts: QUICCallouts, timerQueue: QUICTimerQueue) {
        self.eventLoop = eventLoop
        self.callouts = callouts
        self.timerQueue = timerQueue
    }

    /// A scheduler with a timer queue of its own.
    internal convenience init(eventLoop: any EventLoop, callouts: QUICCallouts) {
        self.init(eventLoop: eventLoop, callouts: callouts, timerQueue: QUICTimerQueue(eventLoop: eventLoop))
    }

    private struct UnsafeTransfer<Wrapped>: @unchecked Sendable {
        var wrappedValue: Wrapped
        init(_ wrappedValue: Wrapped) {
            self.wrappedValue = wrappedValue
        }
    }

    func runImmediate(_ task: @escaping (() -> Void)) {
        let callouts = self.callouts
        let stackTask = { callouts.runStackTask(task) }
        if self.eventLoop.inEventLoop {
            self.eventLoop.assumeIsolatedUnsafeUnchecked().execute(stackTask)
        } else {
            // `Scheduler.runImmediate` hands over a non-Sendable task. It only runs on the event loop, like all
            // other stack work; SwiftNetwork's own scheduler rests on the same guarantee.
            let transfer = UnsafeTransfer(stackTask)
            self.eventLoop.execute {
                let value = transfer.wrappedValue
                value()
            }
        }
    }

    /// Schedules `task` to run no sooner than `milliseconds` from now, replacing whatever `reference`
    /// was last scheduled with.
    func schedule(
        _ task: @escaping (() -> Void),
        after milliseconds: SwiftNetwork.NetworkDuration,
        reference: SwiftNetwork.TimerReference
    ) {
        let deadline = self.eventLoop.now + .milliseconds(milliseconds.roundedUpMilliseconds)
        let timer: QUICTimer
        if let existing = self.timers[reference] {
            timer = existing
        } else {
            timer = QUICTimer(callouts: self.callouts)
            self.timers[reference] = timer
        }
        self.timerQueue.schedule(timer, at: deadline, task: task)
    }

    func unschedule(reference: SwiftNetwork.TimerReference) {
        if let timer = self.timers.removeValue(forKey: reference) {
            self.timerQueue.remove(timer)
        }
    }
}
