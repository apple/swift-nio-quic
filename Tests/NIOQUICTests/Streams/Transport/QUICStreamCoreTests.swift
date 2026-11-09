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

#if DEBUG  // These tests rely on debug only API.

import NIOCore
import NIOEmbedded
import NIOQUICHelpers
@_spi(Essentials) @_spi(ProtocolProvider) import SwiftNetwork
import Testing

@_spi(Essentials) @_spi(ProtocolProvider) @testable import NIOQUIC

@available(anyAppleOS 26, *)
extension QUICStreamCore {
    fileprivate mutating func hold(_ chunks: [String]) {
        var frames = FrameArray()

        for chunk in chunks {
            frames.add(frame: Frame(copyBuffer: Array(chunk.utf8)))
        }

        self._forTesting_addUndeliveredReads(frames)
    }

    fileprivate var holdsNothing: Bool {
        !self._forTesting_hasUndeliveredReads
    }

    fileprivate mutating func expectFlushRefused(fin: Bool) {
        do {
            try withTestEventContext { eventContext in try self.flush(fin: fin, in: &eventContext) }
            Issue.record("Expected flush to be refused")
        } catch {
            ()  // Refused, as expected.
        }
    }

    /// Reads, acquiring an event context for each pull from the stack.
    fileprivate mutating func read(
        minContiguous: Int,
        _ body: (_ span: borrowing RawSpan) -> Int
    ) -> QUICStreamReadOutcome {
        self.read(minContiguous: minContiguous, withEventContext: { pull in withTestEventContext(pull) }, body)
    }

    fileprivate mutating func close(error: NetworkError?) {
        withTestEventContext { eventContext in self.close(error: error, in: &eventContext) }
    }
}

/// Runs `body` with an event context, entering the stack as an anchor on a fresh event-loop-backed
/// context. These cores never attach, so nothing reaches the stack through it.
@available(anyAppleOS 26, *)
private func withTestEventContext<R, E: Error>(
    _ body: (inout NetworkContext.EventContext) throws(E) -> R
) throws(E) -> R {
    let callouts = QUICCallouts()
    let context = NetworkContext(
        identifier: "QUICStreamCoreTests",
        externalScheduler: EventLoopBackedScheduler(eventLoop: EmbeddedEventLoop(), callouts: callouts)
    )
    let anchor = EventContextAnchor(context: context)
    defer { anchor.retire(through: callouts) }
    return try callouts.enter(as: anchor, body)
}

@available(anyAppleOS 26, *)
extension RawSpan {
    fileprivate var utf8String: String {
        self.withUnsafeBytes { String(decoding: $0, as: UTF8.self) }
    }
}

@Suite
struct QUICStreamCoreTests {
    @Test
    @available(anyAppleOS 26, *)
    func partialReadResumesAtOffset() {
        var core = QUICStreamCore(id: nil)
        core.hold(["abcdef"])

        var seen = [String]()
        let outcome = core.read(minContiguous: 1) { span in
            seen.append(span.utf8String)
            return 2  // consume 2 bytes
        }

        #expect(outcome == .read(6))
        #expect(seen == ["abcdef", "cdef", "ef"])

        core.close(error: nil)
    }

    @Test
    @available(anyAppleOS 26, *)
    func declinedBytesAreHeldForTheNextRead() {
        var core = QUICStreamCore(id: nil)
        core.hold(["ab", "cd", "ef"])

        var seen = [String]()
        var calls = 0

        let first = core.read(minContiguous: 1) { span in
            seen.append(span.utf8String)
            calls += 1
            return calls == 1 ? 1 : 0
        }

        #expect(first == .read(1))

        let second = core.read(minContiguous: 1) { span in
            seen.append(span.utf8String)
            return span.byteCount
        }

        #expect(second == .read(5))
        #expect(seen == ["ab", "b", "b", "cd", "ef"])

        let nothingHeld = core.holdsNothing
        #expect(nothingHeld)

        core.close(error: nil)
    }

    @Test
    @available(anyAppleOS 26, *)
    func coalescesAcrossFrames() {
        var core = QUICStreamCore(id: nil)
        core.hold(["ab", "cd", "ef"])

        var seen = [String]()
        let first = core.read(minContiguous: 4) { span in
            seen.append(span.utf8String)
            return 3
        }

        #expect(first == .read(3))

        let needsAnotherVisit = core.needsReadVisit
        #expect(needsAnotherVisit)

        let second = core.read(minContiguous: 1) { span in
            seen.append(span.utf8String)
            return span.byteCount
        }

        #expect(second == .read(3))
        #expect(seen == ["abcd", "d", "ef"])

        core.close(error: nil)
    }

    @Test
    @available(anyAppleOS 26, *)
    func readWithNothingHeld() {
        var core = QUICStreamCore(id: nil)

        let outcome = core.read(minContiguous: 1) { _ in
            Issue.record("read a stream with nothing held")
            return 0
        }

        #expect(outcome == .nothingAvailable)

        core.close(error: nil)
    }

    @Test
    @available(anyAppleOS 26, *)
    func closeFinalizesHeldFrames() {
        var core = QUICStreamCore(id: nil)
        core.hold(["ab", "cd"])
        core.close(error: nil)

        let nothingHeld = core.holdsNothing
        #expect(nothingHeld)
    }

    @Test(arguments: [true, false])
    @available(anyAppleOS 26, *)
    func flushOnStreamWIthoutIDIsRefused(fin: Bool) {
        var core = QUICStreamCore(id: nil)
        core.write(ByteBuffer(string: "hello"))
        core.expectFlushRefused(fin: fin)

        let stillQueued = core._forTesting_hasPendingWrites
        #expect(stillQueued)

        core.close(error: nil)
    }
}

#endif  // DEBUG
