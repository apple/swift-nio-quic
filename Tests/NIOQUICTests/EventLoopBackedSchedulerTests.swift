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
/// `@unchecked Sendable` because the counter is plain storage: an embedded loop only ever runs on the
/// thread driving it, which for these tests is the test's own.
private final class CountingScheduler: EventLoop, @unchecked Sendable {
    let embedded = EmbeddedEventLoop()
    /// How many tasks have been scheduled, which for the scheduler under test is how many wakeups it
    /// has armed.
    private(set) var scheduled = 0

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
        self.scheduled &+= 1
        return self.embedded.scheduleTask(deadline: deadline, task)
    }

    func scheduleTask<T>(
        in amount: TimeAmount,
        _ task: @escaping @Sendable () throws -> T
    ) -> Scheduled<T> {
        self.scheduled &+= 1
        return self.embedded.scheduleTask(in: amount, task)
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
    @available(anyAppleOS 26, *)
    @Test func timerFiresAtItsDeadline() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop)
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, milliseconds: 100, reference: reference)

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
    @available(anyAppleOS 26, *)
    @Test func rescheduleFurtherOutFiresAtTheNewDeadline() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop)
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, milliseconds: 100, reference: reference)
        scheduler.schedule({ fires.count &+= 1 }, milliseconds: 300, reference: reference)

        loop.advanceTime(by: .milliseconds(100))
        #expect(fires.count == 0)

        loop.advanceTime(by: .milliseconds(199))
        #expect(fires.count == 0)

        loop.advanceTime(by: .milliseconds(1))
        #expect(fires.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test func rescheduleEarlierFiresAtTheEarlierDeadline() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop)
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, milliseconds: 500, reference: reference)
        scheduler.schedule({ fires.count &+= 1 }, milliseconds: 50, reference: reference)

        loop.advanceTime(by: .milliseconds(50))
        #expect(fires.count == 1)

        // The deadline it replaced must not fire a second time.
        loop.advanceTime(by: .seconds(1))
        #expect(fires.count == 1)
    }

    @available(anyAppleOS 26, *)
    @Test func unscheduledTimerNeverFires() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop)
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, milliseconds: 100, reference: reference)
        scheduler.unschedule(reference: reference)

        loop.advanceTime(by: .seconds(1))
        #expect(fires.count == 0)

        // Unscheduling something which was never scheduled is allowed.
        scheduler.unschedule(reference: SwiftNetwork.TimerReference())
    }

    /// A timer pushed out and then unscheduled leaves a wakeup armed for the deadline it no longer
    /// has, which must find nothing to run rather than running the task early.
    @available(anyAppleOS 26, *)
    @Test func unscheduleAfterPushingTheDeadlineOutNeverFires() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop)
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        scheduler.schedule({ fires.count &+= 1 }, milliseconds: 100, reference: reference)
        scheduler.schedule({ fires.count &+= 1 }, milliseconds: 300, reference: reference)
        scheduler.unschedule(reference: reference)

        loop.advanceTime(by: .seconds(1))
        #expect(fires.count == 0)
    }

    /// The stack schedules the next timer from inside the one which just fired, which is the ordering
    /// the one-shot bookkeeping has to survive.
    @available(anyAppleOS 26, *)
    @Test func taskWhichReschedulesItselfRunsAgain() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop)
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
                milliseconds: 100,
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

    @available(anyAppleOS 26, *)
    @Test func timersAreIndependentPerReference() {
        let loop = EmbeddedEventLoop()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop)
        let first = Fires()
        let second = Fires()
        let firstReference = SwiftNetwork.TimerReference()
        let secondReference = SwiftNetwork.TimerReference()

        scheduler.schedule({ first.count &+= 1 }, milliseconds: 100, reference: firstReference)
        scheduler.schedule({ second.count &+= 1 }, milliseconds: 200, reference: secondReference)
        // Pushing the first one out must not disturb the second.
        scheduler.schedule({ first.count &+= 1 }, milliseconds: 300, reference: firstReference)

        loop.advanceTime(by: .milliseconds(200))
        #expect(first.count == 0)
        #expect(second.count == 1)

        loop.advanceTime(by: .milliseconds(100))
        #expect(first.count == 1)
        #expect(second.count == 1)
    }

    /// The point of the whole thing: rescheduling a timer further out costs nothing on the event loop,
    /// so the loss recovery timer's per-packet reset stops arming and cancelling a wakeup each time.
    @available(anyAppleOS 26, *)
    @Test func pushingTheDeadlineOutDoesNotArmTheLoopAgain() {
        let loop = CountingScheduler()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop)
        let fires = Fires()
        let reference = SwiftNetwork.TimerReference()

        for step in 1...20 {
            scheduler.schedule(
                { fires.count &+= 1 },
                milliseconds: Int64(100 &+ step &* 10),
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
