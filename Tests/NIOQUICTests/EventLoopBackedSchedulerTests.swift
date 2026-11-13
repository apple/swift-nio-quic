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

import Dispatch
import NIOCore
import NIOEmbedded
@_spi(ProtocolProvider) @_spi(Essentials) import SwiftNetwork
import Testing

@testable import NIOQUIC

/// Counts how often a scheduled task ran.
private final class Fires {
    var count = 0
}

/// An ``EmbeddedEventLoop`` which counts what is scheduled on it.
///
/// `@unchecked Sendable` because the counters are plain storage: an embedded loop only ever runs on the
/// thread driving it, which for these tests is the test's own.
private final class CountingScheduler: EventLoop, @unchecked Sendable {
    let embedded = EmbeddedEventLoop()
    /// How many tasks have been scheduled, which for the scheduler under test is how many wakeups it
    /// has armed.
    private(set) var scheduled = 0
    /// How many of them ran.
    private(set) var ran = 0
    /// How many of them were cancelled.
    private(set) var cancelled = 0

    /// How many are still pending.
    var pending: Int {
        self.scheduled - self.ran - self.cancelled
    }

    var inEventLoop: Bool {
        self.embedded.inEventLoop
    }

    var now: NIODeadline {
        self.embedded.now
    }

    func execute(_ task: @escaping @Sendable () -> Void) {
        self.embedded.execute(task)
    }

    func submit<T>(_ task: @escaping @Sendable () throws -> T) -> EventLoopFuture<T> {
        self.embedded.submit(task)
    }

    func scheduleTask<T>(
        deadline: NIODeadline,
        _ task: @escaping @Sendable () throws -> T
    ) -> Scheduled<T> {
        self.count(self.embedded.scheduleTask(deadline: deadline, self.counting(task)))
    }

    func scheduleTask<T>(
        in amount: TimeAmount,
        _ task: @escaping @Sendable () throws -> T
    ) -> Scheduled<T> {
        self.count(self.embedded.scheduleTask(in: amount, self.counting(task)))
    }

    private func counting<T>(_ task: @escaping @Sendable () throws -> T) -> @Sendable () throws -> T {
        {
            self.ran &+= 1
            return try task()
        }
    }

    private func count<T>(_ scheduled: Scheduled<T>) -> Scheduled<T> {
        self.scheduled &+= 1
        scheduled.futureResult.whenFailure { error in
            if case EventLoopError.cancelled = error {
                self.cancelled &+= 1
            }
        }
        return scheduled
    }

    func shutdownGracefully(queue: DispatchQueue, _ callback: @escaping (((any Error)?) -> Void)) {
        self.embedded.shutdownGracefully(queue: queue, callback)
    }

    func advanceTime(by amount: TimeAmount) {
        self.embedded.advanceTime(by: amount)
    }
}

@Suite
struct EventLoopBackedSchedulerTests {
    @available(anyAppleOS 27, *)
    @Test func timerFiresAtItsDeadline() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts())
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(100), reference: reference)

        loop.advanceTime(by: .milliseconds(99))
        #expect(fires.count == 0)

        loop.advanceTime(by: .milliseconds(1))
        #expect(fires.count == 1)

        // One-shot: nothing runs it again.
        loop.advanceTime(by: .seconds(1))
        #expect(fires.count == 1)
    }

    /// The reschedule the loss recovery timer makes on nearly every packet, which pushes the deadline
    /// further out and must not fire at the deadline it replaced.
    @available(anyAppleOS 27, *)
    @Test func rescheduleFurtherOutFiresAtTheNewDeadline() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts())
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(100), reference: reference)
        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(300), reference: reference)

        loop.advanceTime(by: .milliseconds(100))
        #expect(fires.count == 0)

        loop.advanceTime(by: .milliseconds(199))
        #expect(fires.count == 0)

        loop.advanceTime(by: .milliseconds(1))
        #expect(fires.count == 1)
    }

    @available(anyAppleOS 27, *)
    @Test func rescheduleEarlierFiresAtTheEarlierDeadline() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts())
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(500), reference: reference)
        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(50), reference: reference)

        loop.advanceTime(by: .milliseconds(50))
        #expect(fires.count == 1)

        // The deadline it replaced must not fire a second time.
        loop.advanceTime(by: .seconds(1))
        #expect(fires.count == 1)
    }

    @available(anyAppleOS 27, *)
    @Test func unscheduledTimerNeverFires() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts())
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(100), reference: reference)
        scheduler.unschedule(reference: reference)

        loop.advanceTime(by: .seconds(1))
        #expect(fires.count == 0)

        // Unscheduling something which was never scheduled is allowed.
        scheduler.unschedule(reference: SwiftNetwork.TimerReference())
    }

    /// A timer pushed out and then unscheduled leaves a wakeup armed for the deadline it no longer
    /// has, which must find nothing to run rather than running the task early.
    @available(anyAppleOS 27, *)
    @Test func unscheduleAfterPushingTheDeadlineOutNeverFires() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts())
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(100), reference: reference)
        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(300), reference: reference)
        scheduler.unschedule(reference: reference)

        loop.advanceTime(by: .seconds(1))
        #expect(fires.count == 0)
    }

    /// The stack schedules the next timer from inside the one which just fired, which is the ordering
    /// the one-shot bookkeeping has to survive.
    @available(anyAppleOS 27, *)
    @Test func taskWhichReschedulesItselfRunsAgain() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts())
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        func arm() {
            scheduler.schedule(
                {
                    fires.count &+= 1
                    if fires.count < 3 {
                        arm()
                    }
                },
                after: .milliseconds(100),
                reference: reference
            )
        }
        arm()

        loop.advanceTime(by: .milliseconds(100))
        #expect(fires.count == 1)

        loop.advanceTime(by: .milliseconds(100))
        #expect(fires.count == 2)

        loop.advanceTime(by: .milliseconds(100))
        #expect(fires.count == 3)

        loop.advanceTime(by: .seconds(1))
        #expect(fires.count == 3)
    }

    @available(anyAppleOS 27, *)
    @Test func timersAreIndependentPerReference() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts())
        let first = Fires()
        let second = Fires()
        let firstReference = SwiftNetwork.TimerReference()
        let secondReference = SwiftNetwork.TimerReference()

        scheduler.schedule({ first.count &+= 1 }, after: .milliseconds(100), reference: firstReference)
        scheduler.schedule({ second.count &+= 1 }, after: .milliseconds(200), reference: secondReference)
        // Pushing the first one out must not disturb the second.
        scheduler.schedule({ first.count &+= 1 }, after: .milliseconds(300), reference: firstReference)

        loop.advanceTime(by: .milliseconds(200))
        #expect(first.count == 0)
        #expect(second.count == 1)

        loop.advanceTime(by: .milliseconds(100))
        #expect(first.count == 1)
        #expect(second.count == 1)
    }

    /// The point of the whole thing: rescheduling a timer further out costs nothing on the event loop,
    /// so the loss recovery timer's per-packet reset stops arming and cancelling a wakeup each time.
    @available(anyAppleOS 27, *)
    @Test func pushingTheDeadlineOutDoesNotArmTheLoopAgain() {
        let loop = CountingScheduler()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts())
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        for step in 1...20 {
            scheduler.schedule(
                { fires.count &+= 1 },
                after: .milliseconds(Int64(100 &+ step &* 10)),
                reference: reference
            )
        }

        #expect(loop.scheduled == 1)

        // The one armed wakeup fires early, so it arms once more for what is left of the last
        // deadline, and the task runs there and not before.
        loop.advanceTime(by: .milliseconds(110))
        #expect(fires.count == 0)
        #expect(loop.scheduled == 2)

        loop.advanceTime(by: .milliseconds(190))
        #expect(fires.count == 1)
        #expect(loop.scheduled == 2)
    }
}

/// The timers of several connections, in one ``QUICTimerQueue``.
@Suite
struct QUICTimerQueueTests {
    /// A connection's scheduler on `queue`, with callouts of its own.
    @available(anyAppleOS 27, *)
    private static func connection(on loop: any EventLoop, queue: QUICTimerQueue) -> EventLoopBackedScheduler {
        EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts(), timerQueue: queue)
    }

    /// The point of the queue: however many connections have timers, the loop holds one wakeup for them.
    @available(anyAppleOS 27, *)
    @Test func connectionsShareOneWakeup() {
        let loop = CountingScheduler()
        let queue = QUICTimerQueue(eventLoop: loop)
        var runs: [Int] = []
        let reference = SwiftNetwork.TimerReference()

        let connections = (0..<3).map { _ in Self.connection(on: loop, queue: queue) }
        for (index, connection) in connections.enumerated().reversed() {
            connection.schedule(
                { runs.append(index) },
                after: .milliseconds(Int64(100 &* (index &+ 1))),
                reference: reference
            )
        }
        // Armed for 300 ms first, then moved earlier twice.
        #expect(loop.pending == 1)

        loop.advanceTime(by: .milliseconds(100))
        #expect(runs == [0])
        loop.advanceTime(by: .milliseconds(200))
        #expect(runs == [0, 1, 2])
        #expect(loop.pending == 0)
    }

    /// Moving a connection's deadline costs the loop nothing unless it becomes the earliest of all.
    @available(anyAppleOS 27, *)
    @Test func movingADeadlineThatStaysLaterLeavesTheLoopAlone() {
        let loop = CountingScheduler()
        let queue = QUICTimerQueue(eventLoop: loop)
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()
        let first = Self.connection(on: loop, queue: queue)
        let second = Self.connection(on: loop, queue: queue)

        first.schedule({ fires.count &+= 1 }, after: .milliseconds(100), reference: reference)
        second.schedule({ fires.count &+= 1 }, after: .milliseconds(300), reference: reference)
        second.schedule({ fires.count &+= 1 }, after: .milliseconds(200), reference: reference)
        second.schedule({ fires.count &+= 1 }, after: .seconds(30), reference: reference)
        #expect(loop.scheduled == 1)
        #expect(loop.cancelled == 0)

        // Earlier than every other deadline: the wakeup moves too.
        second.schedule({ fires.count &+= 1 }, after: .milliseconds(50), reference: reference)
        #expect(loop.cancelled == 1)
        #expect(loop.pending == 1)

        loop.advanceTime(by: .milliseconds(50))
        #expect(fires.count == 1)
        loop.advanceTime(by: .milliseconds(50))
        #expect(fires.count == 2)
        #expect(loop.pending == 0)
    }

    @available(anyAppleOS 27, *)
    @Test func timersDueTogetherRunInOneWakeup() {
        let loop = CountingScheduler()
        let queue = QUICTimerQueue(eventLoop: loop)
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        for _ in 0..<5 {
            Self.connection(on: loop, queue: queue).schedule(
                { fires.count &+= 1 },
                after: .milliseconds(100),
                reference: reference
            )
        }
        loop.advanceTime(by: .milliseconds(100))
        #expect(fires.count == 5)
        #expect(loop.scheduled == 1)
        #expect(loop.pending == 0)
    }

    /// A task can close another connection whose timer is due in the same wakeup: that timer doesn't run.
    @available(anyAppleOS 27, *)
    @Test func aTaskUnschedulingAnotherDueTimerKeepsItFromRunning() {
        let loop = EmbeddedEventLoop()
        let queue = QUICTimerQueue(eventLoop: loop)
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()
        let first = Self.connection(on: loop, queue: queue)
        let second = Self.connection(on: loop, queue: queue)

        // Whichever runs first unschedules the other.
        first.schedule(
            {
                fires.count &+= 1
                second.unschedule(reference: reference)
            },
            after: .milliseconds(100),
            reference: reference
        )
        second.schedule(
            {
                fires.count &+= 1
                first.unschedule(reference: reference)
            },
            after: .milliseconds(100),
            reference: reference
        )

        loop.advanceTime(by: .seconds(1))
        #expect(fires.count == 1)
    }

    /// A task can schedule another connection's due timer again: it runs its new task at its new deadline, and
    /// not its old one in this wakeup.
    @available(anyAppleOS 27, *)
    @Test func aTaskSchedulingAnotherDueTimerAgainMovesIt() {
        let loop = EmbeddedEventLoop()
        let queue = QUICTimerQueue(eventLoop: loop)
        let firstFires = Fires()
        let oldFires = Fires()
        let newFires = Fires()
        let reference = SwiftNetwork.TimerReference()
        let first = Self.connection(on: loop, queue: queue)
        let second = Self.connection(on: loop, queue: queue)

        first.schedule(
            {
                firstFires.count &+= 1
                second.schedule({ newFires.count &+= 1 }, after: .milliseconds(50), reference: reference)
            },
            after: .milliseconds(100),
            reference: reference
        )
        second.schedule({ oldFires.count &+= 1 }, after: .milliseconds(100), reference: reference)

        loop.advanceTime(by: .milliseconds(100))
        #expect(firstFires.count == 1)
        #expect(oldFires.count == 0)
        #expect(newFires.count == 0)

        loop.advanceTime(by: .milliseconds(50))
        #expect(newFires.count == 1)
        #expect(oldFires.count == 0)
    }

    /// A task that schedules its own timer again, even for now, runs again in the next wakeup, not in this one.
    @available(anyAppleOS 27, *)
    @Test func aTimerScheduledForNowInsideAWakeupWaitsForTheNext() {
        let loop = CountingScheduler()
        let queue = QUICTimerQueue(eventLoop: loop)
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()
        let connection = Self.connection(on: loop, queue: queue)

        func arm(after delay: SwiftNetwork.NetworkDuration) {
            connection.schedule(
                {
                    fires.count &+= 1
                    if fires.count < 3 {
                        arm(after: .zero)
                    }
                },
                after: delay,
                reference: reference
            )
        }
        arm(after: .milliseconds(100))

        // Each wakeup runs the task once. The embedded loop runs the wakeups armed for now right away, one each.
        loop.advanceTime(by: .milliseconds(100))
        #expect(fires.count == 3)
        #expect(loop.ran == 3)
        #expect(queue._forTesting_count == 0)
    }

    @available(anyAppleOS 27, *)
    @Test func unschedulingTheLastTimerCancelsTheWakeup() {
        let loop = CountingScheduler()
        let queue = QUICTimerQueue(eventLoop: loop)
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()
        let first = Self.connection(on: loop, queue: queue)
        let second = Self.connection(on: loop, queue: queue)

        first.schedule({ fires.count &+= 1 }, after: .milliseconds(100), reference: reference)
        second.schedule({ fires.count &+= 1 }, after: .milliseconds(200), reference: reference)
        // The earliest of several: the wakeup stays, fires early, and arms one for the next.
        first.unschedule(reference: reference)
        #expect(loop.cancelled == 0)
        loop.advanceTime(by: .milliseconds(100))
        #expect(fires.count == 0)
        #expect(loop.scheduled == 2)

        second.unschedule(reference: reference)
        #expect(loop.cancelled == 1)
        #expect(loop.pending == 0)

        loop.advanceTime(by: .seconds(1))
        #expect(fires.count == 0)
    }

    /// A wakeup the loop picked up before it was cancelled still fires. Here a task in the same tick unschedules
    /// the only timer, which cancels the wakeup, and schedules it again, which arms another.
    @available(anyAppleOS 27, *)
    @Test func aWakeupCancelledAfterTheLoopPickedItUpDoesNothing() {
        let loop = CountingScheduler()
        let queue = QUICTimerQueue(eventLoop: loop)
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()
        let connection = Self.connection(on: loop, queue: queue)

        // Due in the same tick as the wakeup, and scheduled first, so it runs first.
        loop.assumeIsolated().scheduleTask(in: .milliseconds(50)) {
            connection.unschedule(reference: reference)
            connection.schedule({ fires.count &+= 1 }, after: .milliseconds(10), reference: reference)
        }
        connection.schedule({ fires.count &+= 1 }, after: .milliseconds(50), reference: reference)

        loop.advanceTime(by: .milliseconds(50))
        #expect(fires.count == 0)

        loop.advanceTime(by: .milliseconds(10))
        #expect(fires.count == 1)
        // That task and two wakeups. Honoring the stale one would have armed a third.
        #expect(loop.scheduled == 3)
    }

    /// Timers keep their order through many moves and removals.
    @available(anyAppleOS 27, *)
    @Test func manyTimersRunInDeadlineOrder() {
        let loop = EmbeddedEventLoop()
        let queue = QUICTimerQueue(eventLoop: loop)
        var runs: [Int] = []
        let reference = SwiftNetwork.TimerReference()
        let connections = (0..<50).map { _ in Self.connection(on: loop, queue: queue) }

        // Each connection's deadline moves a few times; every fifth is unscheduled.
        for round in 0..<3 {
            for (index, connection) in connections.enumerated() {
                let delay = Int64((index &* 37 &+ round &* 11) % 50 &+ 1)
                connection.schedule({ runs.append(index) }, after: .milliseconds(delay), reference: reference)
            }
        }
        for (index, connection) in connections.enumerated() where index % 5 == 0 {
            connection.unschedule(reference: reference)
        }

        loop.advanceTime(by: .milliseconds(100))
        // The last round's deadlines are distinct: 37 and 50 are coprime.
        let expected = connections.indices
            .filter { $0 % 5 != 0 }
            .sorted { ($0 &* 37 &+ 22) % 50 < ($1 &* 37 &+ 22) % 50 }
        #expect(runs == expected)
        #expect(queue._forTesting_count == 0)
    }
}
