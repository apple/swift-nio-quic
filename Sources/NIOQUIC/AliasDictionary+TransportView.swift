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
import NIOQUICHelpers
import Synchronization

@available(anyAppleOS 26, *)
extension AliasDictionary where Key == QUICConnectionID, Value == QUICConnectionChannel.TransportView {
    /// Shut down every registered connection, racing graceful completion against `deadline`.
    ///
    /// If all connections drain before the deadline, `promise` succeeds. If the deadline wins,
    /// any connections still open are torn down abruptly via `parentChannelInactive` and the
    /// promise still succeeds — the caller is told the shutdown completed even if it had to
    /// be forced.
    ///
    /// When `promise` is `nil` shutdown is best-effort and the deadline is ignored.
    func shutdownConnections(deadline: NIODeadline, promise: EventLoopPromise<Void>.Isolated?) {
        guard let promise = promise else {
            for view in self.values {
                view.shutdown(promise: nil)
            }
            return
        }

        let eventLoop = promise.nonisolated().futureResult.eventLoop
        var futures = [EventLoopFuture<Void>]()
        futures.reserveCapacity(self.count)

        for view in self.values {
            let promise = eventLoop.makePromise(of: Void.self)
            view.shutdown(promise: promise)
            futures.append(promise.futureResult)
        }

        let allCompleteFuture = EventLoopFuture.andAllComplete(futures, on: eventLoop)
        let connectionsSnapshot = Array(self.values)
        var raceFinished = false

        // Start a timeout which races with the connections closing before the deadline.
        let timeoutTask = eventLoop.assumeIsolated().scheduleTask(deadline: deadline) {
            if raceFinished { return }
            raceFinished = true

            for view in connectionsSnapshot {
                view.forceClose()
            }

            promise.succeed(())
        }

        allCompleteFuture.assumeIsolated().whenComplete { _ in
            if raceFinished { return }
            raceFinished = true

            timeoutTask.cancel()
            promise.succeed(())
        }
    }

    func notifyParentChannelInactive() {
        for view in self.values {
            view.parentChannelInactive()
        }
    }

    func notifyParentChannelReadComplete() {
        for view in self.values {
            view.parentChannelReadComplete()
        }
    }

    func notifyParentChannelWritabilityChanged(_ writable: Bool) {
        for view in self.values {
            view.parentChannelWritabilityChanged(to: writable)
        }
    }

    func notifyParentChannelUserInboundEventTriggered(_ event: Any) {
        for view in self.values {
            view.parentChannelUserInboundEventTriggered(event)
        }
    }
}
