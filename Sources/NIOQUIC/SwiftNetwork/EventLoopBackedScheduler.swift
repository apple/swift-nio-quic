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

@available(anyAppleOS 26, *)
final class EventLoopBackedScheduler: NetworkContext.Scheduler {
    struct Wakeup {
        /// When the task is due.
        var deadline: NIODeadline

        /// The task to run.
        var task: () -> Void

        struct Scheduled {
            /// A handle on the scheduled callback.
            var callback: NIOScheduledCallback
            /// The deadline when the callback is scheduled to fire.
            ///
            /// Must be at or before the deadline stored directly on `Wakeup`.
            var deadline: NIODeadline
        }

        /// Scheduled callback and deadline.
        var scheduled: Scheduled
    }

    /// Identifies which timer an event loop wakeup belongs to.
    private struct Handler: NIOScheduledCallbackHandler {
        let scheduler: EventLoopBackedScheduler
        let reference: TimerReference

        func handleScheduledCallback(eventLoop: some EventLoop) {
            self.scheduler.wakeupFired(reference: self.reference)
        }
    }

    internal var runningInScheduler: Bool {
        self.eventLoop.inEventLoop
    }

    private var wakeups: [TimerReference: Wakeup] = [:]
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
            self.eventLoop.assumeIsolatedUnsafeUnchecked().execute(task)
        } else {
            // Remove once this has landed: https://github.com/apple/swift-network-evolution/pull/36
            let transfer = UnsafeTransfer(task)
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
        milliseconds: Int64,
        reference: SwiftNetwork.TimerReference
    ) {
        let deadline = self.eventLoop.now + .milliseconds(milliseconds)

        if let index = self.wakeups.index(forKey: reference) {
            let keepWakeup = self._reschedule(
                &self.wakeups.values[index],
                to: deadline,
                task: task,
                reference: reference
            )

            if !keepWakeup {
                self.wakeups.remove(at: index)
            }
        } else if let scheduled = self.schedule(at: deadline, reference: reference) {
            self.wakeups[reference] = Wakeup(deadline: deadline, task: task, scheduled: scheduled)
        } //  else: event loop is shutdown.
    }

    func _reschedule(
        _ wakeup: inout Wakeup,
        to deadline: NIODeadline,
        task: @escaping (() -> Void),
        reference: SwiftNetwork.TimerReference
    ) -> Bool {
        wakeup.deadline = deadline
        wakeup.task = task

        let keepWakeup: Bool

        // Only cancel the scheduled task if the new deadline is earlier. If the new deadline is
        // the same as or later than the scheduled deadline then let the original callback fire as
        // cancellation can be expsneive (it's O(log N) where N is the number of tasks currently
        // scheduled) when there are connections. When the original callback fires it will
        // reschedule itself if the deadline hasn't passed yet.
        if wakeup.scheduled.deadline > deadline {
            wakeup.scheduled.callback.cancel()

            if let scheduled = self.schedule(at: deadline, reference: reference) {
                wakeup.scheduled = scheduled
                keepWakeup = true
            } else {
                keepWakeup = false
            }
        } else {
            keepWakeup = true
        }

        return keepWakeup
    }

    func unschedule(reference: SwiftNetwork.TimerReference) {
        let wakeup = self.wakeups.removeValue(forKey: reference)
        wakeup?.scheduled.callback.cancel()
    }

    private func wakeupFired(reference: TimerReference) {
        guard let index = self.wakeups.index(forKey: reference) else { return }

        if self.eventLoop.now < self.wakeups.values[index].deadline {
            // The deadline hasn't passed yet: the deadline must have moved. Rescheduled the wakeup.
            let scheduled = self.schedule(
                at: self.wakeups.values[index].deadline,
                reference: reference
            )

            if let scheduled {
                self.wakeups.values[index].scheduled = scheduled
            } else {
                // Event loop has shutdown; remove the entry.
                self.wakeups.remove(at: index)
            }
        } else {
            // Deadline has been passed: run the task.
            let (_, wakeup) = self.wakeups.remove(at: index)
            wakeup.task()
        }
    }

    private func schedule(
        at deadline: NIODeadline,
        reference: SwiftNetwork.TimerReference
    ) -> Wakeup.Scheduled? {
        do {
            let callback = try self.eventLoop.assumeIsolatedUnsafeUnchecked().scheduleCallback(
                at: deadline,
                handler: Handler(scheduler: self, reference: reference)
            )
            return Wakeup.Scheduled(callback: callback, deadline: deadline)
        } catch {
            // The loop is shutting down.
            return nil
        }
    }
}
