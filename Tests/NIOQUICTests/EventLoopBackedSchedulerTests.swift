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

    /// QUIC's timer moves earlier for an acknowledgement or a probe, then back out to the idle timeout,
    /// on nearly every request. Each earlier deadline arms a wakeup instead of cancelling one, which on a
    /// `SelectableEventLoop` searches every scheduled task, and the idle timeout's wakeup serves the
    /// deadline whenever it moves back out.
    @available(anyAppleOS 27, *)
    @Test func movingTheDeadlineEarlierArmsRatherThanCancels() {
        let loop = CountingScheduler()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts())
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        // Like QUIC's, the task arms the timer again when it runs: here for the idle timeout.
        func idleTimeout() {
            scheduler.schedule({ fires.count &+= 1 }, after: .seconds(30), reference: reference)
        }
        let task = {
            fires.count &+= 1
            idleTimeout()
        }

        for request in 1...10 {
            idleTimeout()
            scheduler.schedule(task, after: .milliseconds(28), reference: reference)
            scheduler.schedule(task, after: .milliseconds(25), reference: reference)
            #expect(loop.pending == 3)

            // The acknowledgement's wakeup runs the task; the probe's then finds the deadline moved out
            // to the idle timeout, which the idle timeout's wakeup covers.
            loop.advanceTime(by: .milliseconds(25))
            #expect(fires.count == request)
            loop.advanceTime(by: .milliseconds(3))
            #expect(fires.count == request)
            #expect(loop.pending == 1)
        }
        // The first idle timeout's wakeup, and one per earlier deadline.
        #expect(loop.scheduled == 21)
        #expect(loop.cancelled == 0)

        // Back out to the idle timeout, which the first idle wakeup covers: it fires early, arms one
        // more for what is left, and the task runs there.
        scheduler.schedule({ fires.count &+= 1 }, after: .seconds(30), reference: reference)
        #expect(loop.scheduled == 21)
        loop.advanceTime(by: .milliseconds(29_720))
        #expect(fires.count == 10)
        #expect(loop.scheduled == 22)
        loop.advanceTime(by: .milliseconds(280))
        #expect(fires.count == 11)
        #expect(loop.pending == 0)
        #expect(loop.cancelled == 0)
    }

    /// A timer keeps at most three wakeups armed: a fourth deadline, earlier than all of them, cancels
    /// the latest.
    @available(anyAppleOS 27, *)
    @Test func aFourthEarlierDeadlineCancelsTheLatestWakeup() {
        let loop = CountingScheduler()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts())
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(500), reference: reference)
        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(300), reference: reference)
        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(200), reference: reference)
        #expect(loop.cancelled == 0)
        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(100), reference: reference)
        #expect(loop.cancelled == 1)
        #expect(loop.pending == 3)

        loop.advanceTime(by: .milliseconds(100))
        #expect(fires.count == 1)

        // The others find nothing due, and the timer is done.
        loop.advanceTime(by: .seconds(1))
        #expect(fires.count == 1)
        #expect(loop.pending == 0)
    }

    @available(anyAppleOS 27, *)
    @Test func unschedulingCancelsBothWakeups() {
        let loop = CountingScheduler()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts())
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(500), reference: reference)
        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(50), reference: reference)
        scheduler.unschedule(reference: reference)
        #expect(loop.cancelled == 2)

        loop.advanceTime(by: .seconds(1))
        #expect(fires.count == 0)
    }

    /// Once the task ran, a wakeup left armed keeps the timer, so the next deadline it covers arms
    /// nothing.
    @available(anyAppleOS 27, *)
    @Test func aWakeupLeftArmedServesTheNextDeadline() {
        let loop = CountingScheduler()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: QUICCallouts())
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(500), reference: reference)
        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(50), reference: reference)
        loop.advanceTime(by: .milliseconds(50))
        #expect(fires.count == 1)

        scheduler.schedule({ fires.count &+= 1 }, after: .milliseconds(450), reference: reference)
        #expect(loop.scheduled == 2)
        loop.advanceTime(by: .milliseconds(450))
        #expect(fires.count == 2)
        #expect(loop.pending == 0)
    }
}
