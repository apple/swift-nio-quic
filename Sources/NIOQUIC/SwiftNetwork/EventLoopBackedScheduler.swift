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

@available(anyAppleOS 27, *)
final class EventLoopBackedScheduler: NetworkContext.Scheduler {
    /// A timer: its task, while one is due, and the callbacks armed on the event loop for it.
    ///
    /// Moving the deadline later keeps the armed callback, which fires early and arms another for what
    /// is left. Moving it earlier arms another callback rather than cancelling the armed one:
    /// `SelectableEventLoop` cancels by searching every task scheduled on the loop, which with a
    /// thousand connections, each with a timer, costs more than the rest of the timer's work. The later
    /// callbacks serve the deadline again once it moves back out, as the idle timer's does after every
    /// earlier one. A timer keeps at most three callbacks armed: QUIC moves its deadline earlier twice
    /// before a callback fires, for a probe and then an acknowledgement, and back out to the idle timeout.
    struct Wakeup {
        struct Due {
            /// When the task is due.
            var deadline: NIODeadline
            /// The task to run.
            var task: () -> Void
        }

        struct Scheduled {
            /// A handle on the scheduled callback.
            var callback: NIOScheduledCallback
            /// The deadline when the callback is scheduled to fire.
            var deadline: NIODeadline
            /// Tells the callback apart from the timer's others when it fires.
            var id: UInt64
        }

        /// The task and its deadline, or `nil` once it ran.
        var due: Due?

        /// The earliest armed callback.
        var first: Scheduled?

        /// A later armed callback. Only set if `first` is.
        var second: Scheduled?

        /// The latest armed callback. Only set if `second` is.
        var third: Scheduled?

        /// Whether an armed callback fires no later than `deadline`. It then serves the deadline: if
        /// the deadline moved out by the time it fires, it arms another for what is left.
        func isCovered(by deadline: NIODeadline) -> Bool {
            if let first = self.first {
                return first.deadline <= deadline
            }
            return false
        }

        /// Adds a callback armed for a deadline no armed callback covers, so it's the earliest.
        ///
        /// - Returns: The latest callback, if every slot was taken: the caller cancels it.
        mutating func arm(_ scheduled: Scheduled) -> Scheduled? {
            let displaced = self.third
            self.third = self.second
            self.second = self.first
            self.first = scheduled
            return displaced
        }

        /// Forgets the callback that fired: the earliest, on a loop that fires them in deadline order.
        ///
        /// - Returns: Whether it was armed. A callback cancelled after the loop picked it up still fires,
        ///   and isn't any more.
        mutating func disarm(id: UInt64) -> Bool {
            if self.first?.id == id {
                self.first = self.second
                self.second = self.third
                self.third = nil
                return true
            }
            if self.second?.id == id {
                self.second = self.third
                self.third = nil
                return true
            }
            if self.third?.id == id {
                self.third = nil
                return true
            }
            return false
        }
    }

    /// Identifies which timer, and which of its callbacks, an event loop wakeup belongs to.
    private struct Handler: NIOScheduledCallbackHandler {
        let scheduler: EventLoopBackedScheduler
        let reference: TimerReference
        let id: UInt64

        func handleScheduledCallback(eventLoop: some EventLoop) {
            self.scheduler.wakeupFired(reference: self.reference, id: self.id)
        }
    }

    internal var runningInScheduler: Bool {
        self.eventLoop.inEventLoop
    }

    private var wakeups: [TimerReference: Wakeup] = [:]
    /// The ID of the next callback armed.
    private var nextCallbackID: UInt64 = 0
    private let eventLoop: any EventLoop
    /// The connection's queued callouts. The stack acquires the event context itself only inside the
    /// tasks it schedules here, so they run after each one.
    private let callouts: QUICCallouts

    var now: NetworkClock.Instant {
        .zero + .nanoseconds(self.eventLoop.now.uptimeNanoseconds)
    }

    /// NIO has a single clock, so there is no offset between the continuous and absolute clocks.
    var nowAbsolute: NetworkClock.Instant {
        self.now
    }

    internal init(eventLoop: any EventLoop, callouts: QUICCallouts) {
        self.eventLoop = eventLoop
        self.callouts = callouts
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
        let due = Wakeup.Due(deadline: deadline, task: task)

        if let index = self.wakeups.index(forKey: reference) {
            self.wakeups.values[index].due = due
            if self.wakeups.values[index].isCovered(by: deadline) {
                return
            }
            if let scheduled = self.schedule(at: deadline, reference: reference) {
                if let displaced = self.wakeups.values[index].arm(scheduled) {
                    // A fourth deadline earlier than every armed callback before any fired, which only
                    // the handshake does, so cancelling the latest is fine.
                    displaced.callback.cancel()
                }
            } else {
                // The event loop is shutting down.
                self.wakeups.remove(at: index)
            }
        } else if let scheduled = self.schedule(at: deadline, reference: reference) {
            self.wakeups[reference] = Wakeup(due: due, first: scheduled, second: nil, third: nil)
        }  //  else: event loop is shutdown.
    }

    func unschedule(reference: SwiftNetwork.TimerReference) {
        // Unlike moving a deadline, which happens all the time, this happens when the timer stops, so
        // cancelling is fine, and releases the callbacks now rather than when they would have fired.
        let wakeup = self.wakeups.removeValue(forKey: reference)
        wakeup?.first?.callback.cancel()
        wakeup?.second?.callback.cancel()
        wakeup?.third?.callback.cancel()
    }

    private func wakeupFired(reference: TimerReference, id: UInt64) {
        guard let index = self.wakeups.index(forKey: reference) else { return }

        if !self.wakeups.values[index].disarm(id: id) {
            // Cancelled after the loop picked it up, by unscheduling the timer, which may have been
            // scheduled again since: its own callbacks serve it.
            return
        }

        guard let due = self.wakeups.values[index].due else {
            // Left armed after the task ran. The timer is done once no callback is left.
            if self.wakeups.values[index].first == nil {
                self.wakeups.remove(at: index)
            }
            return
        }

        if self.eventLoop.now < due.deadline {
            // The deadline moved out since this callback was armed. Another armed callback may serve it;
            // otherwise arm one for what is left.
            if self.wakeups.values[index].isCovered(by: due.deadline) {
                return
            }
            if let scheduled = self.schedule(at: due.deadline, reference: reference) {
                let displaced = self.wakeups.values[index].arm(scheduled)
                assert(displaced == nil, "Nothing is displaced: this callback's slot is free")
            } else {
                // Event loop has shutdown; remove the entry.
                self.wakeups.remove(at: index)
            }
        } else {
            // Deadline has been passed: run the task, keeping the timer only for a callback still armed.
            self.wakeups.values[index].due = nil
            if self.wakeups.values[index].first == nil {
                self.wakeups.remove(at: index)
            }
            self.callouts.runStackTask(due.task)
        }
    }

    private func schedule(
        at deadline: NIODeadline,
        reference: SwiftNetwork.TimerReference
    ) -> Wakeup.Scheduled? {
        let id = self.nextCallbackID
        self.nextCallbackID += 1
        do {
            let callback = try self.eventLoop.assumeIsolatedUnsafeUnchecked().scheduleCallback(
                at: deadline,
                handler: Handler(scheduler: self, reference: reference, id: id)
            )
            return Wakeup.Scheduled(callback: callback, deadline: deadline, id: id)
        } catch {
            // The loop is shutting down.
            return nil
        }
    }
}
