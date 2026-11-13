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

/// The timers of a handler's connections, behind one event loop callback.
///
/// SwiftNetwork's QUIC moves each connection's timer on nearly every packet. With a callback per timer, the event
/// loop's task heap would hold one per connection, and `SelectableEventLoop` cancels a callback by searching every
/// task in that heap. So the queue keeps the timers in a heap of its own, where a timer moves in O(log n), and keeps
/// one callback armed, for the earliest deadline.
///
/// Must only be used on its event loop.
@available(anyAppleOS 27, *)
final class QUICTimerQueue {
    private struct Armed {
        /// A handle on the scheduled callback.
        var callback: NIOScheduledCallback
        /// When it fires.
        var deadline: NIODeadline
        /// Tells it apart from a callback cancelled after the loop picked it up, which still fires.
        var id: UInt64
    }

    private struct Wakeup: NIOScheduledCallbackHandler {
        let queue: QUICTimerQueue
        let id: UInt64

        func handleScheduledCallback(eventLoop: some EventLoop) {
            self.queue.wakeupFired(id: self.id)
        }
    }

    private let eventLoop: any EventLoop

    /// The queued timers, as a binary min-heap by deadline. Each knows its index, so it can be moved or removed.
    private var heap: [QUICTimer]

    /// The callback armed for the earliest deadline, if any. It may be earlier: it then fires early, and arms
    /// another for what's left.
    private var armed: Armed?

    /// The ID of the next callback armed.
    private var nextCallbackID: UInt64

    /// Whether a wakeup is running due timers. The timers their tasks schedule only get a callback once it's done.
    private var isFiring: Bool

    /// The timers a wakeup found due, kept to reuse the storage.
    private var dueTimers: [QUICTimer]

    init(eventLoop: any EventLoop) {
        self.eventLoop = eventLoop
        self.heap = []
        self.armed = nil
        self.nextCallbackID = 0
        self.isFiring = false
        self.dueTimers = []
    }

    /// Queues `timer` to run `task` at `deadline`, or moves it there, replacing its task, if it's queued.
    func schedule(_ timer: QUICTimer, at deadline: NIODeadline, task: @escaping () -> Void) {
        self.eventLoop.assertInEventLoop()
        timer.deadline = deadline
        timer.task = task
        if let index = timer.heapIndex {
            self.restoreOrder(at: index)
        } else {
            self.heap.append(timer)
            self.siftUp(from: self.heap.count - 1)
        }
        self.armForEarliest()
    }

    /// Takes `timer` out of the queue: its task doesn't run.
    func remove(_ timer: QUICTimer) {
        self.eventLoop.assertInEventLoop()
        timer.task = nil
        if let index = timer.heapIndex {
            self.removeTimer(at: index)
        }
        if self.heap.isEmpty, let armed = self.armed {
            // Nothing left to wake up for. Cancelling is cheap: the loop holds a callback per queue, not per timer.
            armed.callback.cancel()
            self.armed = nil
        }
    }

    // MARK: - Wakeups

    /// Arms a callback for the earliest deadline, unless one fires no later than that.
    private func armForEarliest() {
        if self.isFiring {
            return
        }
        guard let earliest = self.heap.first?.deadline else { return }
        if let armed = self.armed {
            if armed.deadline <= earliest {
                return
            }
            armed.callback.cancel()
            self.armed = nil
        }

        let id = self.nextCallbackID
        self.nextCallbackID += 1
        do {
            let callback = try self.eventLoop.assumeIsolatedUnsafeUnchecked().scheduleCallback(
                at: earliest,
                handler: Wakeup(queue: self, id: id)
            )
            self.armed = Armed(callback: callback, deadline: earliest, id: id)
        } catch {
            // The loop is shutting down.
        }
    }

    private func wakeupFired(id: UInt64) {
        if self.armed?.id != id {
            // Cancelled after the loop picked it up.
            return
        }
        self.armed = nil

        // Take every due timer before running any, so that a timer a task schedules again, even for now, waits for
        // the next wakeup rather than running again in this one.
        let now = self.eventLoop.now
        var due = self.dueTimers
        self.dueTimers = []
        while let first = self.heap.first, first.deadline <= now {
            self.removeTimer(at: 0)
            due.append(first)
        }

        self.isFiring = true
        for timer in due {
            // A task earlier in this wakeup may have removed the timer, or scheduled it again.
            if timer.heapIndex == nil, let task = timer.task {
                timer.task = nil
                timer.callouts.runStackTask(task)
            }
        }
        self.isFiring = false

        due.removeAll(keepingCapacity: true)
        self.dueTimers = due
        self.armForEarliest()
    }

    // MARK: - Heap

    /// Restores the heap order around `index`, whose timer's deadline changed.
    private func restoreOrder(at index: Int) {
        if index > 0, self.heap[index].deadline < self.heap[(index - 1) / 2].deadline {
            self.siftUp(from: index)
        } else {
            self.siftDown(from: index)
        }
    }

    private func siftUp(from start: Int) {
        let timer = self.heap[start]
        var index = start
        while index > 0 {
            let parent = (index - 1) / 2
            if self.heap[parent].deadline <= timer.deadline {
                break
            }
            self.heap[index] = self.heap[parent]
            self.heap[index].heapIndex = index
            index = parent
        }
        self.heap[index] = timer
        timer.heapIndex = index
    }

    private func siftDown(from start: Int) {
        let timer = self.heap[start]
        let count = self.heap.count
        var index = start
        while true {
            let left = 2 * index + 1
            if left >= count {
                break
            }
            let right = left + 1
            let child = right < count && self.heap[right].deadline < self.heap[left].deadline ? right : left
            if timer.deadline <= self.heap[child].deadline {
                break
            }
            self.heap[index] = self.heap[child]
            self.heap[index].heapIndex = index
            index = child
        }
        self.heap[index] = timer
        timer.heapIndex = index
    }

    private func removeTimer(at index: Int) {
        self.heap[index].heapIndex = nil
        let last = self.heap.removeLast()
        if index < self.heap.count {
            self.heap[index] = last
            last.heapIndex = index
            self.restoreOrder(at: index)
        }
    }

    #if DEBUG
    /// How many timers are queued, for tests.
    var _forTesting_count: Int {
        self.heap.count
    }
    #endif
}

/// One of a connection's timers: the task SwiftNetwork scheduled for one of its `TimerReference`s, and when it's due.
@available(anyAppleOS 27, *)
final class QUICTimer {
    /// When the task is due, while the timer is queued.
    fileprivate var deadline: NIODeadline

    /// The task to run, until it ran or the timer was removed.
    fileprivate var task: (() -> Void)?

    /// The timer's index in its queue's heap, or `nil` if it isn't queued.
    fileprivate var heapIndex: Int?

    /// The connection's callouts, which run the task.
    let callouts: QUICCallouts

    init(callouts: QUICCallouts) {
        self.deadline = .uptimeNanoseconds(0)
        self.task = nil
        self.heapIndex = nil
        self.callouts = callouts
    }
}
