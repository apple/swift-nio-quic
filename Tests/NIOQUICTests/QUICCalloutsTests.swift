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
import NIOEmbedded
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
import Testing

@testable import NIOQUIC

struct QUICCalloutsTests {
    /// A registered protocol instance to enter the stack as.
    @available(anyAppleOS 26, *)
    private final class TestInstance: ProtocolInstance {
        let context: NetworkContext
        var identifier: InstanceIdentifier
        var eventManager: ProtocolEventManager

        init(context: NetworkContext) {
            self.context = context
            var eventManager = ProtocolEventManager()
            self.identifier = InstanceIdentifier(context: context, eventManager: &eventManager)
            self.eventManager = eventManager
        }
    }

    /// Records the datagrams of the callouts it runs, as strings.
    @available(anyAppleOS 26, *)
    private final class RecordingTarget: QUICConnectionCalloutTarget {
        var log: [String] = []
        var onRun: ((String) -> Void)?

        func run(_ callout: QUICConnectionCallout) {
            switch callout {
            case .datagramRead(let datagram):
                let tag = String(buffer: datagram)
                self.log.append(tag)
                self.onRun?(tag)
            case .handshakeCompleted, .closed, .connectionIDAssociated, .connectionIDRetired, .datagramError:
                Issue.record("Unexpected callout \(callout)")
            }
        }
    }

    private struct Boom: Error {}

    /// Runs `body` with a queue, its scheduler's event loop and a registered instance, and unregisters
    /// the instance afterwards (its event manager traps on deinit otherwise).
    @available(anyAppleOS 26, *)
    private func withCallouts(
        _ body: (QUICCallouts, EmbeddedEventLoop, EventLoopBackedScheduler, TestInstance) throws -> Void
    ) rethrows {
        let loop = EmbeddedEventLoop()
        let callouts = QUICCallouts()
        let scheduler = EventLoopBackedScheduler(eventLoop: loop, callouts: callouts)
        let context = NetworkContext(identifier: "QUICCalloutsTests", externalScheduler: scheduler)
        let instance = TestInstance(context: context)
        defer {
            callouts.enter(as: instance) { eventContext in
                instance.unregisterEventManager(in: &eventContext)
            }
        }
        try body(callouts, loop, scheduler, instance)
    }

    @available(anyAppleOS 26, *)
    private static func datagram(_ tag: String) -> QUICConnectionCallout {
        .datagramRead(ByteBuffer(string: tag))
    }

    @available(anyAppleOS 26, *)
    @Test
    func queuedCalloutsRunInOrderOnceTheAcquisitionEnds() {
        self.withCallouts { callouts, _, _, instance in
            let target = RecordingTarget()
            callouts.enter(as: instance) { eventContext in
                callouts.add(.connection(target, Self.datagram("a")), in: &eventContext)
                callouts.add(.connection(target, Self.datagram("b")), in: &eventContext)
                #expect(target.log.isEmpty)
            }
            #expect(target.log == ["a", "b"])
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func anEntryFromACalloutRunsItsOwnCalloutsBeforeReturning() {
        // A handler that flushes from a `channelRead`: its entry's callouts run before it returns,
        // and the rest of the outer batch runs after it.
        self.withCallouts { callouts, _, _, instance in
            let target = RecordingTarget()
            target.onRun = { tag in
                if tag == "a" {
                    callouts.enter(as: instance) { eventContext in
                        callouts.add(.connection(target, Self.datagram("nested")), in: &eventContext)
                    }
                    #expect(target.log == ["a", "nested"])
                }
            }
            callouts.enter(as: instance) { eventContext in
                callouts.add(.connection(target, Self.datagram("a")), in: &eventContext)
                callouts.add(.connection(target, Self.datagram("b")), in: &eventContext)
            }
            #expect(target.log == ["a", "nested", "b"])
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func aThrowingAcquisitionStillRunsItsCallouts() {
        self.withCallouts { callouts, _, _, instance in
            let target = RecordingTarget()
            #expect(throws: Boom.self) {
                try callouts.enter(as: instance) { (eventContext) throws(Boom) -> Void in
                    callouts.add(.connection(target, Self.datagram("a")), in: &eventContext)
                    throw Boom()
                }
            }
            #expect(target.log == ["a"])
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func calloutsQueuedInAStackTaskRunWhenItEnds() {
        // The stack acquires the event context itself inside the tasks it schedules: a timer, or
        // asynchronous work.
        self.withCallouts { callouts, loop, scheduler, instance in
            let target = RecordingTarget()
            scheduler.runImmediate {
                instance.fromExternal { eventContext in
                    callouts.add(.connection(target, Self.datagram("task")), in: &eventContext)
                }
                #expect(target.log.isEmpty)
            }
            #expect(target.log.isEmpty)
            loop.run()
            #expect(target.log == ["task"])
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func calloutsQueuedInATimerWakeupRunWhenItEnds() {
        self.withCallouts { callouts, loop, scheduler, instance in
            let target = RecordingTarget()
            scheduler.schedule(
                {
                    instance.fromExternal { eventContext in
                        callouts.add(.connection(target, Self.datagram("wakeup")), in: &eventContext)
                    }
                    #expect(target.log.isEmpty)
                },
                after: .milliseconds(1),
                reference: SwiftNetwork.TimerReference()
            )
            loop.advanceTime(by: .milliseconds(1))
            #expect(target.log == ["wakeup"])
        }
    }

    @available(anyAppleOS 26, *)
    @Test
    func calloutsQueuedByAFrameworkCallInACalloutRunBeforeTheRestOfTheBatch() {
        // A queued call that calls a framework entry point which acquires the event context itself,
        // such as the keep-alive setter: what that entry point's witnesses queue runs first.
        self.withCallouts { callouts, _, _, instance in
            let target = RecordingTarget()
            target.onRun = { tag in
                if tag == "a" {
                    callouts.callFramework {
                        instance.fromExternal { eventContext in
                            callouts.add(.connection(target, Self.datagram("framework")), in: &eventContext)
                        }
                    }
                }
            }
            callouts.enter(as: instance) { eventContext in
                callouts.add(.connection(target, Self.datagram("a")), in: &eventContext)
                callouts.add(.connection(target, Self.datagram("b")), in: &eventContext)
            }
            #expect(target.log == ["a", "framework", "b"])
        }
    }

    #if DEBUG
    @available(anyAppleOS 26, *)
    @Test
    func queueingWhereNothingRunsTheQueueTraps() async throws {
        #if os(Android)
        try Test.cancel("Exit tests cannot be run on Android yet")
        #else
        // A framework entry point called directly, rather than through `callFramework(_:)`: nothing
        // would run what its witnesses queue.
        await #expect(processExitsWith: .failure) {
            QUICCalloutsTests().withCallouts { callouts, _, _, instance in
                instance.fromExternal { eventContext in
                    callouts.add(.connection(RecordingTarget(), QUICCalloutsTests.datagram("lost")), in: &eventContext)
                }
                // Leave nothing queued or registered, so that only the queueing above can trap.
                callouts.run()
            }
        }
        #endif
    }
    #endif

    #if DEBUG
    @available(anyAppleOS 26, *)
    @Test
    func theQueueKnowsWhenTheStackMayHoldTheEventContext() {
        self.withCallouts { callouts, loop, scheduler, instance in
            #expect(!callouts.isInsideStack)
            callouts.enter(as: instance) { _ in
                #expect(callouts.isInsideStack)
            }
            scheduler.runImmediate {
                #expect(callouts.isInsideStack)
            }
            loop.run()
            #expect(!callouts.isInsideStack)
        }
    }
    #endif

    @available(anyAppleOS 26, *)
    @Test
    func aRetiredAnchorLeavesNothingRegistered() {
        // Its event manager traps on deinit if still registered, so completing is the check.
        self.withCallouts { callouts, _, _, instance in
            let anchor = EventContextAnchor(context: instance.context)
            #expect(!anchor.identifier.isNone)
            anchor.retire(through: callouts)
            #expect(anchor.identifier.isNone)
            anchor.retire(through: callouts)
            #expect(anchor.identifier.isNone)
        }
    }
}
