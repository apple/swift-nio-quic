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

import Logging
import NIOCore

/// The delegate of a ``ChildChannelMultiplexer``.
public protocol ChildChannelMultiplexerDelegate: AnyObject {
    /// The type of the IDs of the child channels.
    associatedtype ChildChannelID
    /// The type of the ID properties of the child channels.
    associatedtype ChildChannelIDProperties
    /// The type of the messages of the parent channel.
    associatedtype ParentChannelOutboundMessage

    /// The parent channel.
    var parent: Channel { get }

    /// Called when a child channel wants to write a message to
    /// the parent channel.
    ///
    /// - Parameters:
    ///   - childChannelID: The id of the child channel that wrote the message.
    ///   - message: The message to write to the parent channel.
    ///   - promise: The ``EventLoopPromise`` which should be notified once the write completes, or nil if no notification should take place.
    func writeFromChildChannel(
        childChannelID: ChildChannelID,
        message: ParentChannelOutboundMessage,
        promise: EventLoopPromise<Void>?
    )

    /// Called when a child channel wants to flush the
    /// parent channel.
    ///
    /// - Parameters:
    ///   - childChannelID: The id of the child channel that issued the flush.
    func flushFromChildChannel(childChannelID: ChildChannelID)

    /// Called when the child channel wants to read more data from the parent channel.
    ///
    /// - Parameters:
    ///   - childChannelID: The id of the child channel that issued the read.
    func readFromChildChannel(childChannelID: ChildChannelID)

    /// Called when a pending child channel wants to write the first message.
    ///
    /// The delegate is responsible for generating the proper ID.
    /// - Parameter properties: The properties which can be used to influence the generation of the next ID.
    /// - Returns: The new ID.
    func generateChildChannelID(for properties: ChildChannelIDProperties) -> ChildChannelID

    /// Called when the child channel is closed.
    func childChannelClosed(
        childChannelIDOrProperties: ChildChannelIDOrProperties<ChildChannelID, ChildChannelIDProperties>
    )
}

public enum ChildChannelIDOrProperties<ChildChannelID, ChildChannelIDProperties> {
    case childChannelID(ChildChannelID)
    case childChannelIDProperties(ChildChannelIDProperties)
}

extension ChildChannelIDOrProperties: Sendable where ChildChannelID: Sendable, ChildChannelIDProperties: Sendable {}

extension ChildChannelMultiplexerDelegate where ChildChannelIDProperties == Never {
    public func generateChildChannelID(for properties: ChildChannelIDProperties) -> ChildChannelID {}
}

/// A generic multiplexer that is capable of multiplexing a single channel onto multiple child channels.
@preconcurrency
@available(macOS 15, iOS 18, tvOS 18, watchOS 11, visionOS 2, *)
public final class ChildChannelMultiplexer<
    ChildChannelID: Hashable & _ChildChannelMultiplexerSendableMetatype,
    ChildChannelIDProperties: _ChildChannelMultiplexerSendableMetatype,
    StateMachine: ChildChannelStateMachine & _ChildChannelMultiplexerSendableMetatype,
    WritabilityStrategy: ChildChannelWritabilityStrategy & _ChildChannelMultiplexerSendableMetatype,
    ParentChannelInboundMessage,
    ParentChannelOutboundMessage,
    ChildChannelInboundMessage,
    ChildChannelOutboundMessage: _ChildChannelMultiplexerSendableMetatype,
    ChildChannelTask: _ChildChannelMultiplexerSendableMetatype,
    Delegate: ChildChannelMultiplexerDelegate & _ChildChannelMultiplexerSendableMetatype
>
where
    StateMachine.ChildChannelID == ChildChannelID,
    StateMachine.ParentChannelInboundMessage == ParentChannelInboundMessage,
    StateMachine.ParentChannelOutboundMessage == ParentChannelOutboundMessage,
    StateMachine.ChildChannelInboundMessage == ChildChannelInboundMessage,
    StateMachine.ChildChannelOutboundMessage == ChildChannelOutboundMessage,
    StateMachine.Task == ChildChannelTask,
    WritabilityStrategy.Message == ChildChannelOutboundMessage,
    Delegate.ChildChannelID == ChildChannelID,
    Delegate.ChildChannelIDProperties == ChildChannelIDProperties,
    Delegate.ParentChannelOutboundMessage == ParentChannelOutboundMessage
{
    @usableFromInline
    enum State {
        /// Initial state of the multiplexer.
        case idle
        /// Indicates that the multiplexer was started and we have a delegate.
        case started(
            delegate: Delegate,
            channels: ChildChannelMap,
            pendingChannels: [ObjectIdentifier: (channel: Child, idProperties: ChildChannelIDProperties)]
        )
        /// Indicates that the multiplexer is currently shutting down.
        case shuttingDown(
            delegate: Delegate,
            channels: ChildChannelMap,
            pendingChannels: [ObjectIdentifier: (channel: Child, idProperties: ChildChannelIDProperties)],
            canCreateChannels: Bool,
            shutdownPromise: EventLoopPromise<Void>,
            forceShutdownScheduledTask: Scheduled<Void>?
        )
        /// Indicates that the the multiplexer successfully shutdown.
        case shutdown

        case modify
    }

    @usableFromInline
    typealias Child = ChildChannel<
        ChildChannelID,
        StateMachine,
        WritabilityStrategy,
        ParentChannelInboundMessage,
        ParentChannelOutboundMessage,
        ChildChannelInboundMessage,
        ChildChannelOutboundMessage,
        ChildChannelTask,
        ChildChannelMultiplexer
    >

    @usableFromInline
    let _eventLoop: EventLoop
    /// The current state of the multiplexer.
    @usableFromInline
    var state: State = .idle
    /// The logger.
    @usableFromInline
    var logger: Logger

    /// Initializes a new ``ChildChannelMultiplexer``.
    ///
    /// - Parameters:
    ///   - eventLoop: The ``EventLoop`` of the parent channel.
    ///   - logger: The logger.
    public init(
        eventLoop: EventLoop,
        logger: Logger
    ) {
        self._eventLoop = eventLoop
        self.logger = logger
    }

    /// Starts the multiplexer.
    ///
    /// - Important: This must be called before forwarding any calls to the multiplexer and it should only ever be called once.
    /// - Parameter delegate: The delegate of the multiplexer. This will be strongly retained until the multiplexer is shutdown.
    @inlinable
    public func start(with delegate: Delegate) {
        self._eventLoop.assertInEventLoop()

        self.logger.trace("ChildChannelMultiplexer starting")
        switch self.state {
        case .idle:
            self.state = .started(
                delegate: delegate,
                channels: ChildChannelMap(),
                pendingChannels: [:]
            )
        case .started:
            fatalError("Tried to start a multiplexer that is already started.")
        case .shuttingDown, .shutdown:
            fatalError("Tried to start a multiplexer that is shutting down or has shutdown.")
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    /// Tries to shutdown the multiplexer gracefully.
    ///
    /// - Note: This will result in ``ChannelShouldQuiesceEvent``s being send to the child channels which
    /// should be handled by the state machine.
    ///
    /// - Parameters:
    ///   - deadline: Deadline by which the child channels must have closed gracefully. Afterwards, they will be forcibly closed.
    ///   - promise: A promise that is fulfilled once all child channels are closed.
    @inlinable
    public func shutdownGracefully(deadline: NIODeadline, promise: EventLoopPromise<Void>?) {
        self._eventLoop.assertInEventLoop()

        self.logger.trace("ChildChannelMultiplexer shutting down gracefully")
        let promise = promise ?? self._eventLoop.makePromise(of: Void.self)

        switch self.state {
        case .idle:
            self.logger.trace("ChildChannelMultiplexer shutdown")
            self.state = .shutdown
            promise.succeed(())
        case .started(let delegate, let channels, let pendingChannels):
            if !channels.isEmpty || !pendingChannels.isEmpty {
                let shutdownPromise = self._eventLoop.makePromise(of: Void.self)
                shutdownPromise.futureResult.cascade(to: promise)

                let boundedSelf = NIOLoopBound(self, eventLoop: self._eventLoop)
                let scheduled = self._eventLoop.scheduleTask(deadline: deadline) {
                    // We need to forcibly close all channels here otherwise this might on forever.
                    boundedSelf.value.shutdownForcefully()
                }

                self.state = .shuttingDown(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels,
                    canCreateChannels: true,
                    shutdownPromise: shutdownPromise,
                    forceShutdownScheduledTask: scheduled
                )

                self.logger.trace("ChildChannelMultiplexer sending quiescing event to child channels")
                for channel in channels {
                    channel.parentChannelUserInboundEventTriggered(ChannelShouldQuiesceEvent())
                }
                for (channel, _) in pendingChannels.values {
                    channel.parentChannelUserInboundEventTriggered(ChannelShouldQuiesceEvent())
                }
            } else {
                // No channels to shutdown so we can transition to `shutdown` right away.
                self.logger.debug("ChildChannelMultiplexer shutdown")
                self.state = .shutdown
                promise.succeed(())
            }
        case .shuttingDown(_, _, _, _, let shutdownPromise, _):
            shutdownPromise.futureResult.cascade(to: promise)
        case .shutdown:
            promise.succeed(())
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    @inlinable
    func shutdownForcefully() {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle, .started:
            fatalError("Unreachable")
        case .shuttingDown(let delegate, let channels, let pendingChannels, _, let shutdownPromise, _):
            if !channels.isEmpty || !pendingChannels.isEmpty {
                // We have to close the remaining channels forcibly.
                self.logger.trace("ChildChannelMultiplexer shutting down forcefully")
                self.state = .shuttingDown(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels,
                    canCreateChannels: false,
                    shutdownPromise: shutdownPromise,
                    forceShutdownScheduledTask: nil
                )

                for value in channels {
                    value.close(promise: nil)
                }
                for pendingChannel in pendingChannels.values {
                    pendingChannel.0.close(promise: nil)
                }
            } else {
                fatalError(
                    "This state should be unreachable: if all channels have been closed, multiplexer should already be shutdown."
                )
            }
        case .shutdown:
            // No op
            break
        case .modify:
            fatalError("Internal inconsistency")
        }
    }
}

// This is a restriction that could conceptually be lifted, but right now it reflects the implementation strategy.
@available(*, unavailable)
extension ChildChannelMultiplexer: Sendable {}

// MARK: Calls from parent

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, visionOS 2, *)
extension ChildChannelMultiplexer {
    /// Method to check if there is a child channel for a given ID.
    ///
    /// - Parameters:
    ///     - with: The ID of the child channel.
    /// - Returns: A boolean wether a channel with the given ID exists.
    @inlinable
    public func hasChannel(with id: ChildChannelID) -> Bool {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .started(_, let channels, _), .shuttingDown(_, let channels, _, _, _, _):
            return channels.channelIDs.contains(id)
        case .shutdown:
            return false
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    /// Creates a new child channel.
    ///
    /// - Note: The `localAddress` and `remoteAddress` passed to the child channel
    /// do not need to be parent's addresses. For example when having a protocol that is multiplexed over UDP,
    /// it is common that the remote address is derived from the ``AddressedEnvelope`` instead of the
    /// parent channel.
    ///
    /// - Parameters:
    ///   - promise: An ``EventLoopPromise`` that will be fulfilled with the channel when it becomes active.
    ///   - channelID: The channel ID. This must be unique across all open child channels. If you pass in `nil` then the multiplexer
    ///   will treat this as a pending child channel. Once a pending child channel tries to write outbound messages, the multiplexer will
    ///   call ``ChildChannelMultiplexerDelegate/generateChildChannelID()``.
    ///   - stateMachine: The state machine used by the created child channel.
    ///   - writabilityStrategy: The writabilityStrategy that is used by the created child channel.
    ///   - localAddress: The child channel's local address.
    ///   - remoteAddress: The child channel's remote address.
    ///   - channelInitializer: A callback that will be invoked to initialize the channel.
    @inlinable
    public func createChildChannel(
        promise: EventLoopPromise<Channel>?,
        newChannelID: NewChildChannelID<ChildChannelID, ChildChannelIDProperties>,
        stateMachine: StateMachine,
        writabilityStrategy: WritabilityStrategy,
        localAddress: SocketAddress?,
        remoteAddress: SocketAddress?,
        channelInitializer: ((Channel, StateMachine) -> EventLoopFuture<Void>)?
    ) {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .shuttingDown(_, _, _, canCreateChannels: false, _, _):
            promise?.fail(ChildChannelMultiplexerError.multiplexerShutdown)
        case .shuttingDown(
            let delegate,
            var channels,
            var pendingChannels,
            canCreateChannels: true,
            let shutdownPromise,
            let forceShutdownScheduledTask
        ):
            self.state = .modify

            let channel = self._createChildChannel(
                delegate: delegate,
                newChannelID: newChannelID,
                stateMachine: stateMachine,
                writabilityStrategy: writabilityStrategy,
                localAddress: localAddress,
                remoteAddress: remoteAddress
            )

            switch newChannelID {
            case .channelID(let childChannelID):
                channels.insertNewValue(channel, forKey: childChannelID)

                self.state = .shuttingDown(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels,
                    canCreateChannels: true,
                    shutdownPromise: shutdownPromise,
                    forceShutdownScheduledTask: forceShutdownScheduledTask
                )

            case .pending(let childChannelIDProperties):
                pendingChannels[ObjectIdentifier(channel)] = (channel: channel, idProperties: childChannelIDProperties)

                self.state = .shuttingDown(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels,
                    canCreateChannels: true,
                    shutdownPromise: shutdownPromise,
                    forceShutdownScheduledTask: forceShutdownScheduledTask
                )
            }

            channel.configure(promise: promise, initializer: channelInitializer)

            // We have to send the ChannelShouldQuiesceEvent right away to inform the channel
            // that it should close.
            channel.parentChannelUserInboundEventTriggered(ChannelShouldQuiesceEvent())
        case .started(let delegate, var channels, var pendingChannels):
            self.state = .modify

            let channel = self._createChildChannel(
                delegate: delegate,
                newChannelID: newChannelID,
                stateMachine: stateMachine,
                writabilityStrategy: writabilityStrategy,
                localAddress: localAddress,
                remoteAddress: remoteAddress
            )

            switch newChannelID {
            case .channelID(let childChannelID):
                channels.insertNewValue(channel, forKey: childChannelID)

                self.state = .started(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels
                )

            case .pending(let childChannelIDProperties):
                pendingChannels[ObjectIdentifier(channel)] = (channel: channel, idProperties: childChannelIDProperties)

                self.state = .started(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels
                )
            }

            channel.configure(promise: promise, initializer: channelInitializer)
        case .shutdown:
            promise?.fail(ChildChannelMultiplexerError.multiplexerShutdown)
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    @discardableResult
    @inlinable
    func _createChildChannel(
        delegate: Delegate,
        newChannelID: NewChildChannelID<ChildChannelID, ChildChannelIDProperties>,
        stateMachine: StateMachine,
        writabilityStrategy: WritabilityStrategy,
        localAddress: SocketAddress?,
        remoteAddress: SocketAddress?
    ) -> Child {
        let channel = Child(
            id: newChannelID.childChannelID,
            stateMachine: stateMachine,
            parent: delegate.parent,
            writabilityStrategy: writabilityStrategy,
            localAddress: localAddress,
            remoteAddress: remoteAddress,
            logger: self.logger
        )
        // This is creating a retain cycle which we are going to break
        // when the channel closes.
        channel.delegate = self

        return channel
    }

    /// Closes a child channel.
    ///
    /// - Parameters:
    /// - channelID: The channel ID.
    @inlinable
    public func closeChildChannel(withId id: ChildChannelID) {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .started(_, let channels, _):
            let childChannel = channels[channelID: id]
            childChannel?.close(mode: .all, promise: nil)
        case .shuttingDown, .shutdown:
            // No op, because the channel will be or already has been closed.
            break
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    /// Closes all child channels that satisfy the given predicate.
    ///
    /// - Parameters:
    /// - shouldBeClosed: A closure that takes a child channel ID as its argument and returns a boolean indicating wether the
    /// channel should be closed.
    @inlinable
    public func closeChildChannel(where shouldBeClosed: (ChildChannelID) -> Bool) {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .started(_, let channels, _):
            for (channelID, childChannel) in channels.channelsAndIDs where shouldBeClosed(channelID) {
                childChannel.close(mode: .all, promise: nil)
            }
        case .shuttingDown, .shutdown:
            // No op, because the channel will be or already has been closed.
            break
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    /// Adds a new child channel ID as an "extra" ID for an existing channel.
    ///
    /// In some protocol contexts it may be possible for the same channel to have multiple IDs. For example,
    /// in QUIC a connection may have multiple connection IDs. In this case, we can associate "extra" IDs with
    /// an existing channel.
    ///
    /// - parameters:
    ///     - existingChannelID: The channel ID already in use for a given child channel.
    ///     - extraChannelID: The new channel ID to associate with this child channel.
    /// - Throws: Throws if no channel for the existing channel ID exists.
    @inlinable
    public func addExtraChannelID(existingChannelID: ChildChannelID, extraChannelID: ChildChannelID) throws {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .shuttingDown(
            let delegate,
            var channels,
            let pendingChannels,
            let canCreateChannels,
            let shutdownPromise,
            let forceShutdownScheduledTask
        ):
            self.state = .modify

            defer {
                self.state = .shuttingDown(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels,
                    canCreateChannels: canCreateChannels,
                    shutdownPromise: shutdownPromise,
                    forceShutdownScheduledTask: forceShutdownScheduledTask
                )
            }

            guard let channel = channels[channelID: existingChannelID] else {
                throw ChildChannelMultiplexerError.unknownChannelID
            }

            channels.registerExtraChannelID(channelID: extraChannelID, child: channel)

            channel.addExtraChannelID(extraChannelID)

        case .started(let delegate, var channels, let pendingChannels):
            self.state = .modify

            defer {
                self.state = .started(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels
                )
            }

            guard let channel = channels[channelID: existingChannelID] else {
                throw ChildChannelMultiplexerError.unknownChannelID
            }

            channels.registerExtraChannelID(channelID: extraChannelID, child: channel)

            channel.addExtraChannelID(extraChannelID)
        case .shutdown:
            throw ChildChannelMultiplexerError.multiplexerShutdown
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    /// Removes a retired child channel ID from an existing channel.
    ///
    /// In some protocol contexts it may be possible for the same channel to have multiple IDs. For example,
    /// in QUIC a connection may have multiple connection IDs. These IDs might be transient and change
    /// during the lifetime of the connection.
    ///
    /// - parameters:
    ///     - retiredChannelID: The new channel ID to associate with this child channel.
    /// - Throws: Throws if no channel for the retired channel ID exists.
    @inlinable
    public func removeChannelID(_ retiredChannelID: ChildChannelID) throws {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .shuttingDown(
            let delegate,
            var channels,
            let pendingChannels,
            let canCreateChannels,
            let shutdownPromise,
            let forceShutdownScheduledTask
        ):
            self.state = .modify

            defer {
                self.state = .shuttingDown(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels,
                    canCreateChannels: canCreateChannels,
                    shutdownPromise: shutdownPromise,
                    forceShutdownScheduledTask: forceShutdownScheduledTask
                )
            }

            guard let channel = channels[channelID: retiredChannelID] else {
                throw ChildChannelMultiplexerError.unknownChannelID
            }

            // Removing an ID from the channel will fail if it would be the last ID.
            try channel.removeChannelID(retiredChannelID)

            channels.retireChannelID(channelID: retiredChannelID, child: channel)

        case .started(let delegate, var channels, let pendingChannels):
            self.state = .modify

            defer {
                self.state = .started(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels
                )
            }

            guard let channel = channels[channelID: retiredChannelID] else {
                throw ChildChannelMultiplexerError.unknownChannelID
            }

            // Removing an ID from the channel will fail if it would be the last ID. We should do it before other modifications.
            try channel.removeChannelID(retiredChannelID)

            channels.retireChannelID(channelID: retiredChannelID, child: channel)

        case .shutdown:
            throw ChildChannelMultiplexerError.multiplexerShutdown
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    /// This method needs to be called when the parent channel becomes inactive.
    @inlinable
    public func parentChannelInactive() {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .started(let delegate, let channels, let pendingChannels):
            if !channels.isEmpty || !pendingChannels.isEmpty {
                self.state = .shuttingDown(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels,
                    canCreateChannels: false,
                    shutdownPromise: self._eventLoop.makePromise(),
                    forceShutdownScheduledTask: nil
                )

                for channel in channels {
                    channel.parentChannelInactive()
                }

                for (channel, _) in pendingChannels.values {
                    channel.parentChannelInactive()
                }
            } else {
                self.state = .shutdown
            }
        case .shuttingDown(_, let channels, let pendingChannels, _, _, _):
            for channel in channels {
                channel.parentChannelInactive()
            }

            for (channel, _) in pendingChannels.values {
                channel.parentChannelInactive()
            }
        case .shutdown:
            // No op
            break
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    /// This method needs to be called when a new message was read in the parent channel.
    ///
    /// - Important: A channel with the given ID must be created before via ``createChildChannel(promise:stateMachine:channelInitializer:)``
    ///
    /// - Parameters:
    ///   - message: The message read in the parent channel.
    ///   - id: The ID of the child channel.
    /// - Throws: Throws if no channel for the given channel ID exists.
    @inlinable
    public func parentChannelRead(_ message: ParentChannelInboundMessage, for id: ChildChannelID) throws {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .started(_, let channels, _), .shuttingDown(_, let channels, _, _, _, _):
            guard let channel = channels[channelID: id] else {
                throw ChildChannelMultiplexerError.unknownChannelID
            }
            channel.parentChannelReadMessage(message)
        case .shutdown:
            // No op
            break
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    /// This method needs to be called when the parent channel completed reading.
    @inlinable
    public func parentChannelReadComplete() {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .started(_, let channels, _), .shuttingDown(_, let channels, _, _, _, _):
            for channel in channels {
                channel.parentChannelReadComplete()
            }
        case .shutdown:
            // No op
            break
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    /// This method needs to be called when a new user inbound event was triggered on the parent channel
    /// that is important for the child channel as well.
    ///
    /// - Note: Not all parent channel user inbound events need to be forwarded to the child channels necessarily.
    ///
    /// - Parameter event: The inbound event.
    @inlinable
    public func parentChannelUserInboundEventTriggered(_ event: Any) {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .started(_, let channels, let pendingChannels),
            .shuttingDown(_, let channels, let pendingChannels, _, _, _):
            for channel in channels {
                channel.parentChannelUserInboundEventTriggered(event)
            }

            for (channel, _) in pendingChannels.values {
                channel.parentChannelUserInboundEventTriggered(event)
            }
        case .shutdown:
            // No op
            break
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    /// This method needs to be called when the writability of the parent channel changed.
    ///
    /// - Parameter newValue: The new writability state of the parent.
    @inlinable
    public func parentChannelWritabilityChanged(newValue: Bool) {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .started(_, let channels, let pendingChannels),
            .shuttingDown(_, let channels, let pendingChannels, _, _, _):
            for channel in channels {
                channel.parentChannelWritabilityChanged(newValue: newValue)
            }
            for (channel, _) in pendingChannels.values {
                channel.parentChannelWritabilityChanged(newValue: newValue)
            }
        case .shutdown:
            // No op
            break
        case .modify:
            fatalError("Internal inconsistency")
        }
    }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, visionOS 2, *)
extension ChildChannelMultiplexer: ChildChannelDelegate {
    @inlinable
    func writeFromChildChannel(
        channelID: ChildChannelID?,
        channelObjectIdentifier: ObjectIdentifier,
        message: ParentChannelOutboundMessage,
        promise: EventLoopPromise<Void>?
    ) {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")

        case .started(let delegate, var channels, var pendingChannels):
            if let channelID = channelID {
                delegate.writeFromChildChannel(childChannelID: channelID, message: message, promise: promise)
            } else {
                self.state = .modify

                // This is the first write of a pending channel. We need to generate an ID for it
                // and then forward the write

                // This force-unwrap is safe since we MUST have a pending channel if we get a write from it.
                let (channel, idProperties) = pendingChannels.removeValue(forKey: channelObjectIdentifier)!
                let channelID = delegate.generateChildChannelID(for: idProperties)
                channel.setID(channelID)
                channels.insertNewValue(channel, forKey: channelID)

                self.state = .started(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels
                )

                delegate.writeFromChildChannel(childChannelID: channelID, message: message, promise: promise)
            }

        case .shuttingDown(
            let delegate,
            var channels,
            var pendingChannels,
            let canCreateChannels,
            let shutdownPromise,
            let forceShutdownScheduledTask
        ):
            if let channelID = channelID {
                delegate.writeFromChildChannel(childChannelID: channelID, message: message, promise: promise)
            } else {
                self.state = .modify

                // This is the first write of a pending channel. We need to generate an ID for it
                // and then forward the write

                // This force-unwrap is safe since we MUST have a pending channel if we get a write from it.
                let (channel, idProperties) = pendingChannels.removeValue(forKey: channelObjectIdentifier)!
                let channelID = delegate.generateChildChannelID(for: idProperties)
                channel.setID(channelID)
                channels.insertNewValue(channel, forKey: channelID)

                self.state = .shuttingDown(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels,
                    canCreateChannels: canCreateChannels,
                    shutdownPromise: shutdownPromise,
                    forceShutdownScheduledTask: forceShutdownScheduledTask
                )

                delegate.writeFromChildChannel(childChannelID: channelID, message: message, promise: promise)
            }

        case .shutdown:
            promise?.fail(ChannelError.alreadyClosed)
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    @inlinable
    func flushFromChildChannel(channelID: ChildChannelID?) {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .started(let delegate, _, _), .shuttingDown(let delegate, _, _, _, _, _):
            if let channelID = channelID {
                delegate.flushFromChildChannel(childChannelID: channelID)
            } else {
                self.logger.trace("ChildChannelMultiplexer dropped a flush since the channel is still pending")
            }
        case .shutdown:
            // No op
            break
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    @inlinable
    func readFromChildChannel(
        channelID: ChildChannelID?,
        channelObjectIdentifier: ObjectIdentifier
    ) {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .started(let delegate, _, _), .shuttingDown(let delegate, _, _, _, _, _):
            if let channelID = channelID {
                delegate.readFromChildChannel(childChannelID: channelID)
            } else {
                self.logger.trace("ChildChannelMultiplexer dropped a read since the channel is still pending")
            }
        case .shutdown:
            // No op
            break
        case .modify:
            fatalError("Internal inconsistency")
        }
    }

    @inlinable
    func closeFromChildChannel(
        channelID: ChildChannelID?,
        channelObjectIdentifier: ObjectIdentifier
    ) {
        self._eventLoop.assertInEventLoop()

        switch self.state {
        case .idle:
            fatalError("The multiplexer needs to be started before calling other methods.")
        case .started(let delegate, var channels, var pendingChannels):
            self.state = .modify

            let childChannelIDOrProperties: ChildChannelIDOrProperties<ChildChannelID, ChildChannelIDProperties>
            if let channelID = channelID {
                channels.removeChannel(withIdentifier: channelID)
                childChannelIDOrProperties = .childChannelID(channelID)
            } else {
                if let channel = pendingChannels.removeValue(forKey: channelObjectIdentifier) {
                    childChannelIDOrProperties = .childChannelIDProperties(channel.idProperties)
                } else {
                    fatalError("Received a close from a pending child channel that we don't know")
                }
            }

            self.state = .started(
                delegate: delegate,
                channels: channels,
                pendingChannels: pendingChannels
            )
            delegate.childChannelClosed(childChannelIDOrProperties: childChannelIDOrProperties)

        case .shuttingDown(
            let delegate,
            var channels,
            var pendingChannels,
            let canCreateChannels,
            let shutdownPromise,
            let forceShutdownScheduledTask
        ):
            self.state = .modify

            let childChannelIDOrProperties: ChildChannelIDOrProperties<ChildChannelID, ChildChannelIDProperties>
            if let channelID = channelID {
                channels.removeChannel(withIdentifier: channelID)
                childChannelIDOrProperties = .childChannelID(channelID)
            } else {
                if let channel = pendingChannels.removeValue(forKey: channelObjectIdentifier) {
                    childChannelIDOrProperties = .childChannelIDProperties(channel.idProperties)
                } else {
                    fatalError("Received a close from a pending child channel that we don't know")
                }
            }

            if !channels.isEmpty || !pendingChannels.isEmpty {
                self.state = .shuttingDown(
                    delegate: delegate,
                    channels: channels,
                    pendingChannels: pendingChannels,
                    canCreateChannels: canCreateChannels,
                    shutdownPromise: shutdownPromise,
                    forceShutdownScheduledTask: forceShutdownScheduledTask
                )
            } else {
                self.state = .shutdown
                forceShutdownScheduledTask?.cancel()
                shutdownPromise.succeed(())
            }
            delegate.childChannelClosed(childChannelIDOrProperties: childChannelIDOrProperties)
        case .shutdown:
            fatalError("All channels should have been closed before multiplexer is shutdown.")
        case .modify:
            fatalError("Internal inconsistency")
        }
    }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, visionOS 2, *)
extension ChildChannelMultiplexer {
    @usableFromInline
    struct ChildChannelMap: Collection {
        @usableFromInline
        var _channelByID: [ChildChannelID: Child]

        @usableFromInline
        var _channelsByIdentity: [ObjectIdentifier: Child]

        @inlinable
        init() {
            self._channelByID = [:]
            self._channelsByIdentity = [:]
        }

        @inlinable
        var channelIDs: [ChildChannelID: Child].Keys {
            self._channelByID.keys
        }

        @inlinable
        subscript(channelID channelID: ChildChannelID) -> Child? {
            self._channelByID[channelID]
        }

        @usableFromInline
        typealias Index = [ObjectIdentifier: Child].Index

        @inlinable
        var startIndex: Index {
            self._channelsByIdentity.startIndex
        }

        @inlinable
        var endIndex: Index {
            self._channelsByIdentity.endIndex
        }

        @inlinable
        func index(after index: Index) -> Index {
            self._channelsByIdentity.index(after: index)
        }

        @inlinable
        subscript(index: Index) -> Child {
            self._channelsByIdentity[index].value
        }

        @inlinable
        mutating func insertNewValue(_ value: Child, forKey key: ChildChannelID) {
            let existingValue = self._channelByID.updateValue(value, forKey: key)
            precondition(existingValue == nil, "The child channel ID is already in use.")

            self._channelsByIdentity[ObjectIdentifier(value)] = value
        }

        @inlinable
        var channelsAndIDs: [ChildChannelID: Child] {
            self._channelByID
        }

        @inlinable
        mutating func removeChannel(withIdentifier channelID: ChildChannelID) {
            guard let channel = self._channelByID.removeValue(forKey: channelID) else {
                // Weird, but let's tolerate it
                return
            }

            self._channelsByIdentity.removeValue(forKey: ObjectIdentifier(channel))

            for id in channel._multiplexedChannelIDs where id != channelID {
                self._channelByID.removeValue(forKey: id)
            }
        }

        @inlinable
        mutating func registerExtraChannelID(channelID: ChildChannelID, child: Child) {
            assert(self._channelsByIdentity[ObjectIdentifier(child)] != nil)
            let existingValue = self._channelByID.updateValue(child, forKey: channelID)
            precondition(existingValue == nil, "The child channel ID is already in use.")
        }

        @inlinable
        mutating func retireChannelID(channelID: ChildChannelID, child: Child) {
            assert(self._channelsByIdentity[ObjectIdentifier(child)] != nil)
            let existingValue = self._channelByID.removeValue(forKey: channelID)
            precondition(existingValue != nil, "The child channel ID was not in use.")
        }
    }
}

@available(*, unavailable)
extension ChildChannelMultiplexer.State: Sendable {}
