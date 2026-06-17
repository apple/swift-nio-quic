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

import HeapModule
import Logging
import NIOConcurrencyHelpers
import NIOCore
import Synchronization

/// The delegate of a ``ChildChannel``.
///
/// This is implemented by the multiplexer and allows communication from the child channel to the multiplexer.
@usableFromInline
protocol ChildChannelDelegate: AnyObject {
    /// The type of the child channel identifiers.
    associatedtype ChildChannelID
    /// The type of the outbound messages of the parent channel.
    associatedtype ParentChannelOutboundMessage

    /// Informs the delegate about a message from the child to the parent.
    ///
    /// - Parameters:
    ///   - channelID: The channel ID of the child channel.
    ///   - channelObjectIdentifier: The channel's ObjectIdentifier.
    ///   - message: The message to write to the parent.
    ///   - promise: The ``EventLoopPromise`` which should be notified once the write completes, or nil if no notification should take place.
    func writeFromChildChannel(
        channelID: ChildChannelID?,
        channelObjectIdentifier: ObjectIdentifier,
        message: ParentChannelOutboundMessage,
        promise: EventLoopPromise<Void>?
    )

    /// Informs the delegate that the child channel flushed.
    ///
    /// - Parameters:
    ///   - channelID: The channel ID of the child channel.
    func flushFromChildChannel(channelID: ChildChannelID?)

    /// Informs the delegate that the child channel read and
    /// has no messages buffered to satisfy the read.
    ///
    /// - Parameters:
    ///   - channelID: The channel ID of the child channel.
    ///   - channelObjectIdentifier: The channel's ObjectIdentifier.
    func readFromChildChannel(
        channelID: ChildChannelID?,
        channelObjectIdentifier: ObjectIdentifier
    )

    /// Informs the delegate that the child channel closed.
    ///
    /// - Parameters:
    ///   - channelID: The channel ID of the child channel.
    ///   - channelObjectIdentifier: The channel's ObjectIdentifier.
    func closeFromChildChannel(
        channelID: ChildChannelID?,
        channelObjectIdentifier: ObjectIdentifier
    )
}

/// A generic child channel that implements
/// the necessary plumbing like message ordering, reentrancy protection, etc..
///
/// It reaches out to its state machine for customization.
@usableFromInline
final class ChildChannel<
    ID: Hashable & _ChildChannelMultiplexerSendableMetatype,
    StateMachine: ChildChannelStateMachine & _ChildChannelMultiplexerSendableMetatype,
    WritabilityStrategy: ChildChannelWritabilityStrategy & _ChildChannelMultiplexerSendableMetatype,
    ParentChannelInboundMessage,
    ParentChannelOutboundMessage,
    ChildChannelInboundMessage,
    ChildChannelOutboundMessage: _ChildChannelMultiplexerSendableMetatype,
    Task: _ChildChannelMultiplexerSendableMetatype,
    Delegate: ChildChannelDelegate & _ChildChannelMultiplexerSendableMetatype
>
where
    ID == StateMachine.ChildChannelID,
    ParentChannelInboundMessage == StateMachine.ParentChannelInboundMessage,
    ParentChannelOutboundMessage == StateMachine.ParentChannelOutboundMessage,
    ChildChannelInboundMessage == StateMachine.ChildChannelInboundMessage,
    ChildChannelOutboundMessage == StateMachine.ChildChannelOutboundMessage,
    Task == StateMachine.Task,
    WritabilityStrategy.Message == ChildChannelOutboundMessage,
    Delegate.ParentChannelOutboundMessage == ParentChannelOutboundMessage,
    Delegate.ChildChannelID == ID
{
    @usableFromInline
    typealias _Actions = ChildChannelActions<
        ParentChannelInboundMessage, ParentChannelOutboundMessage, ChildChannelInboundMessage,
        ChildChannelOutboundMessage, Task
    >

    /// An action which depends on the completion of another action.
    @usableFromInline
    struct DependentAction {
        /// The action to complete.
        @usableFromInline
        var action:
            ChildChannelAction<
                ParentChannelInboundMessage, ParentChannelOutboundMessage, ChildChannelInboundMessage,
                ChildChannelOutboundMessage, Task
            >
        /// The ID of the action which must be completed before `action` may be executed.
        @usableFromInline
        var id: UInt64

        @inlinable
        init(
            action: ChildChannelAction<
                ParentChannelInboundMessage, ParentChannelOutboundMessage, ChildChannelInboundMessage,
                ChildChannelOutboundMessage, Task
            >,
            id: UInt64
        ) {
            self.action = action
            self.id = id
        }
    }

    /// This is a simple wrapper struct that contains both the user scheduled task and the deadline passed with it.
    @usableFromInline
    struct TaskWithDeadline: Comparable {
        @usableFromInline
        static func < (lhs: TaskWithDeadline, rhs: TaskWithDeadline) -> Bool {
            guard lhs.deadline == rhs.deadline else {
                return lhs.deadline < rhs.deadline
            }
            return lhs.id < rhs.id
        }

        @usableFromInline
        var task: Task

        @usableFromInline
        var deadline: NIODeadline

        @usableFromInline
        var id: UInt64

        @inlinable
        init(task: Task, deadline: NIODeadline, id: UInt64) {
            self.task = task
            self.deadline = deadline
            self.id = id
        }
    }

    /// This is a simple wrapper struct for the currently scheduled task.
    @usableFromInline
    struct ScheduledTask {
        @usableFromInline
        var taskWithDeadline: TaskWithDeadline

        @usableFromInline
        var scheduled: Scheduled<Void>

        @inlinable
        init(taskWithDeadline: TaskWithDeadline, scheduled: Scheduled<Void>) {
            self.taskWithDeadline = taskWithDeadline
            self.scheduled = scheduled
        }
    }

    /// A simple enum that we use for buffering our reads or input closes. This is used since we have to make sure that
    /// any `inputClosed` is happening after all the reads have been fired down the channel pipeline.
    @usableFromInline
    enum ReadOrInputClose {
        case read(ChildChannelInboundMessage)
        case inputClose
    }

    @usableFromInline
    enum WriteOrClose {
        case write(ChildChannelOutboundMessage, EventLoopPromise<Void>?)
        case close(Error, CloseMode, EventLoopPromise<Void>?)
    }

    /// A buffer of actions from the state machine.
    @usableFromInline
    var _actionsBuffer = CircularBuffer<
        ChildChannelAction<
            ParentChannelInboundMessage, ParentChannelOutboundMessage, ChildChannelInboundMessage,
            ChildChannelOutboundMessage, Task
        >
    >()
    /// A buffer of actions which have dependencies on actions in the actions buffer.
    @usableFromInline
    var _dependentActionsBuffer = CircularBuffer<DependentAction>()
    /// The current dependent action ID. Do not modify directly, instead use `_nextDependentActionID()`.
    @usableFromInline
    var _dependentActionID: UInt64 = 0

    /// Returns the next ID to use for a dependent action and prepares the next ID to use.
    @inlinable
    func _nextDependentActionID() -> UInt64 {
        self.eventLoop.assertInEventLoop()

        let id = self._dependentActionID
        self._dependentActionID &+= 1
        return id
    }

    /// Boolean indicating if we are currently processing actions. Used to avoid reentrancy.
    @usableFromInline
    var _isProcessingActions = false

    /// A buffer of pending inbound reads delivered from the parent channel.
    @usableFromInline
    var _pendingReads = CircularBuffer<ReadOrInputClose>()
    /// Whether a call to `read` has happened without any messages available to read (that is, whether newly
    /// received messages should be immediately delivered to the pipeline).
    @usableFromInline
    var _unsatisfiedRead: Bool = false
    /// Whether `autoRead` is enabled. By default, all ``ChildChannel``s objects inherit their `autoRead`
    /// state from their parent.
    @usableFromInline
    var _autoRead: Bool

    /// A buffer of pending outbound writes from the user.
    ///
    /// To correctly respect flushes, we deliberately withhold data from the parent channel until this
    /// stream is flushed, at which time we deliver them all. This buffer holds the pending ones.
    @usableFromInline
    var _pendingWritesFromChannel = MarkedCircularBuffer<WriteOrClose>(initialCapacity: 8)
    /// A buffer of pending outbound messages for the parent channel.
    ///
    /// This buffer exists to avoid message re-ordering issues when we make outcalls. Some messages
    /// trigger multiple outcalls, any of which could interrupt message delivery or event ordering.
    /// To avoid difficulty here, we make sure we enqueue the writes for the multiplexer here.
    @usableFromInline
    var _pendingWritesForMultiplexer = CircularBuffer<(ParentChannelOutboundMessage, EventLoopPromise<Void>?)>()
    /// An object that controls whether this channel should be writable.
    @usableFromInline
    var _writabilityManager: ChildChannelWritabilityManager<WritabilityStrategy, ChildChannelOutboundMessage>

    /// The current activation state of this channel.
    @usableFromInline
    var _activationState: _ActivationState
    /// The promise passed by the user which needs to be fulfilled after the activation is done.
    @usableFromInline
    var _userActivationPromise: EventLoopPromise<Channel>?
    /// Indicates if the activation can be completed.
    ///
    /// We validate that the parent channel is active and that we haven't activated before.
    @usableFromInline
    var _isAllowedToCompleteActivation: Bool {
        (self.parent?.isActive ?? false) && (self._activationState == .neverActivated)
    }

    /// This promise needs to be fulfilled when the channel closed.
    @usableFromInline
    let _closePromise: EventLoopPromise<Void>
    /// Boolean to keep track wether we closed our input already.
    @usableFromInline
    var isInputClosed = false
    /// Boolean to keep track wether we closed already.
    @usableFromInline
    var _didClose = false

    /// A Heap of pending tasks that need to be scheduled after the current on fired.
    @usableFromInline
    var _pendingTasks = Heap<TaskWithDeadline>()
    /// The currently scheduled task. We are only ever scheduling one task to reduce the number of `Scheduled`s we have to create.
    @usableFromInline
    var _currentScheduledTask: ScheduledTask?
    /// The next task ID.
    @usableFromInline
    var _nextTaskID: UInt64 = 0

    /// The id of the child channel.
    @usableFromInline
    var _id: ID?

    @usableFromInline
    var _multiplexedChannelIDs: [ID]

    /// The state machine of the child channel.
    @usableFromInline
    var _stateMachine: StateMachine

    // MARK: - Stored properties for `Channel`/`ChannelCore` conformance

    /// The parent channel.
    @usableFromInline
    let parent: Channel?
    /// The event loop of the parent.
    @usableFromInline
    let eventLoop: EventLoop
    /// The allocator of the parent.
    @usableFromInline
    let allocator: ByteBufferAllocator
    /// Atomic that stores if this channel is currently active.
    @usableFromInline
    let _isActive: Synchronization.Atomic<Bool>
    /// Atomic that stores if this channel is currently writable.
    @usableFromInline
    let _isWritable: Synchronization.Atomic<Bool>
    /// The actual channel pipeline.
    ///
    /// We don't have to `nil` this out since the ``ChannelPipeline`` will break the retain cycles
    /// once the ``ChildChannel`` closed.
    @usableFromInline
    var _pipeline: ChannelPipeline!
    /// The local ``SocketAddress``.
    @usableFromInline
    let _localAddress: SocketAddress?
    /// The remote peer’s ``SocketAddress``.
    @usableFromInline
    let _remoteAddress: SocketAddress?

    /// The delegate of the ``ChildChannel``.
    @usableFromInline
    var delegate: Delegate?
    /// The logger.
    @usableFromInline
    var logger: Logger

    /// Initializes a new ``ChildChannel``.
    ///
    /// - Parameters:
    ///   - id: The id of the child channel.
    ///   - stateMachine: The state machine of the child channel.
    ///   - parent: The parent channel.
    ///   - writabilityStrategy: The writability strategy.
    ///   - localAddress: The child channel's local address.
    ///   - remoteAddress: The child channel's remote address.
    ///   - logger: The self.logger.
    @inlinable
    init(
        id: ID?,
        stateMachine: StateMachine,
        parent: Channel,
        writabilityStrategy: WritabilityStrategy,
        localAddress: SocketAddress?,
        remoteAddress: SocketAddress?,
        logger: Logger
    ) {
        self._id = id
        self._stateMachine = stateMachine
        self.parent = parent
        var logger = logger
        let idString = id.flatMap { "\($0)" } ?? "pending"
        logger[metadataKey: LoggingKeys.childChannelID] = "\(ID.self)(\(idString))"
        self.logger = logger
        self.eventLoop = parent.eventLoop
        self.allocator = parent.allocator
        self._localAddress = localAddress
        self._remoteAddress = remoteAddress
        self._closePromise = parent.eventLoop.makePromise(of: Void.self)
        self._isActive = Atomic(false)
        self._isWritable = Atomic(true)
        self._activationState = .neverActivated
        self._multiplexedChannelIDs = []
        self._writabilityManager = ChildChannelWritabilityManager(
            strategy: writabilityStrategy,
            parentIsWritable: parent.isWritable,
            logger: self.logger
        )

        // To begin with we initialize autoRead to false, but we are going to fetch it from our parent before we
        // go much further.
        self._autoRead = false
        // We are wrapping self in an AnyChannel here to make sure all method calls
        // get specialized properly.
        self._pipeline = ChannelPipeline(channel: AnyChannel(channel: self))

        if let id {
            self._multiplexedChannelIDs.append(id)
        }
    }

    @inlinable
    func setID(_ id: ID) {
        self.eventLoop.preconditionInEventLoop()

        precondition(self._id == nil, "The child channel ID is getting updated more than once.")
        self._id = id
        self._multiplexedChannelIDs.append(id)
        self._processActions(self._stateMachine.childChannelIDGenerated(childChannelID: id))
        var logger = self.logger
        logger[metadataKey: LoggingKeys.childChannelID] = "\(ID.self)(\(id))"
        self.logger = logger
    }

    @inlinable
    func addExtraChannelID(_ id: ID) {
        self.eventLoop.preconditionInEventLoop()

        precondition(self._id != nil, "Channels can only have extra IDs after they have their first one")
        self._multiplexedChannelIDs.append(id)

        self.logger.trace("Extra channel ID assigned")
        self._processActions(self._stateMachine.extraChannelIDAssigned(id))
    }

    @inlinable
    func removeChannelID(_ id: ID) throws {
        self.eventLoop.preconditionInEventLoop()

        precondition(self._id != nil, "Channels can only interact with IDs after they have their first one")

        if self._multiplexedChannelIDs.count == 1 {
            throw ChildChannelMultiplexerError.cannotRemoveLastChannelID
        }

        // We cannot remove the inital ID. But the extra channel IDs should be here.
        let index = self._multiplexedChannelIDs.firstIndex(of: id)
        precondition(index != nil, "Cannot remove unassociated ID from the channel")
        self._multiplexedChannelIDs.remove(at: index!)

        self.logger.trace("Channel ID retired")
        self._processActions(self._stateMachine.channelIDRetired(id))
    }
}

@available(*, unavailable)
extension ChildChannel.DependentAction: Sendable {}

@available(*, unavailable)
extension ChildChannel.ScheduledTask: Sendable {}

@available(*, unavailable)
extension ChildChannel.ReadOrInputClose: Sendable {}

@available(*, unavailable)
extension ChildChannel.WriteOrClose: Sendable {}

@available(*, unavailable)
extension ChildChannel.TaskWithDeadline: Sendable {}

// MARK: - Channel action processing

extension ChildChannel {
    /// This method processed the generated actions by the state machine. It is protected against reentrancy.
    ///
    /// - Parameters:
    ///     actions: The actions to process from the state machine.
    @inlinable
    func _processActions(
        _ actions: ChildChannelActions<
            ParentChannelInboundMessage, ParentChannelOutboundMessage, ChildChannelInboundMessage,
            ChildChannelOutboundMessage, Task
        >
    ) {
        self.eventLoop.assertInEventLoop()

        self.logger.trace(
            "ChildChannel appending new actions to process",
            metadata: [
                LoggingKeys.childChannelActionsCount: "\(actions.count)",
                LoggingKeys.childChannelActions: "\(actions)",
            ]
        )

        self._actionsBuffer.append(contentsOf: actions)
        if self._isProcessingActions {
            return
        }
        self.logger.trace("ChildChannel processing actions")
        self._isProcessingActions = true
        do {
            // We are making sure we are unwinding the re-entrency protection.
            defer {
                self._isProcessingActions = false
            }

            while let action = self._actionsBuffer.popFirst() {
                switch action.action {
                // Public actions
                case .childChannelCompleteActivation:
                    self._completeActivation()
                case .childChannelBufferRead(let message):
                    self._bufferRead(message: message)
                case .childChannelFireChannelRead(let message):
                    self._fireChannelRead(message: message)
                case .childChannelBufferInputClosed:
                    self._bufferInputClosed()
                case .childChannelFireInputClosed:
                    self._fireInputClosed()
                case .childChannelCloseCleanly(let promise):
                    self._close(error: nil, promise: promise)
                case .childChannelFireUserInboundEventTriggered(let event):
                    self._fireUserInboundEventTriggered(event: event)
                case .childChannelFailClose(let error, let promise):
                    self._failClose(error: error, promise: promise)
                case .succeedPromise(let promise):
                    self._succeedPromise(promise)
                case .childChannelEncounterError(let error, let promise):
                    self._close(error: error, promise: promise)
                case .childChannelFlush:
                    self.flush()
                case .failPromise(let promise, let error):
                    self._failPromise(promise, with: error)
                case .childChannelScheduleTask(let task, let deadline):
                    self._scheduleTask(task, deadline: deadline)
                case .childChannelCancelTask(let task):
                    self._cancelTask(task)
                case .parentChannelWrite(let message, let promise):
                    self._writeToParent(message: message, promise: promise)
                case .parentChannelRead:
                    self._readFromParent()

                // Internal actions
                case .deliverPendingWritesToStateMachine:
                    self._deliverPendingWritesToStateMachine()

                case .writePendingToMultiplexer:
                    self._writePendingToMultiplexer()

                case .deliverPendingReads:
                    self._deliverPendingReads()

                case .fireChannelReadComplete:
                    self._fireChannelReadComplete()

                case .fireErrorCaught(let error):
                    self._fireErrorCaught(error)

                case .failChannelPromise(let promise, let error):
                    self._failPromise(promise, with: error)

                case .notifyChannelInactive:
                    self._notifyChannelInactive()

                case .failPendingWrites(let error):
                    self._failPendingWrites(error: error)
                }

                if let dependentActionID = action.dependentActionID {
                    while let nextID = self._dependentActionsBuffer.first?.id {
                        guard nextID == dependentActionID else {
                            assert(
                                nextID > dependentActionID,
                                "Dependent action reordering bug: action with dependency ID (\(dependentActionID)) was greater than the next buffered action ID (\(nextID))"
                            )
                            break
                        }
                        let dependentAction = self._dependentActionsBuffer.removeFirst()
                        self._actionsBuffer.append(dependentAction.action)
                    }
                }
            }
        }
    }

    @inlinable
    func _completeActivation() {
        self.eventLoop.assertInEventLoop()

        guard self._isAllowedToCompleteActivation else {
            self.logger.trace("ChildChannel not allowed to complete activation")
            return
        }
        self.logger.trace("ChildChannel completing activation")

        self._notifyChannelActive()

        if self._writabilityManager.isWritable != self.isWritable {
            self._changeWritability(to: self._writabilityManager.isWritable)
        }

        if let promise = self._userActivationPromise {
            self._userActivationPromise = nil
            promise.succeed(self)
        }

        self._tryToAutoRead()
    }

    @inlinable
    func _bufferRead(message: ChildChannelInboundMessage) {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel buffering read")
        self._pendingReads.append(.read(message))
    }

    @inlinable
    func _fireChannelRead(message: ChildChannelInboundMessage) {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel firing channel read")
        self.pipeline.syncOperations.fireChannelRead(NIOAny(message))
    }

    @inlinable
    func _bufferInputClosed() {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel buffering inputClosed")
        self._pendingReads.append(.inputClose)
    }

    @inlinable
    func _fireInputClosed() {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel firing inputClosed")
        self.pipeline.syncOperations.fireUserInboundEventTriggered(ChannelEvent.inputClosed)
    }

    @inlinable
    func _scheduleTask(_ task: Task, deadline: NIODeadline) {
        self.eventLoop.assertInEventLoop()

        self.logger.trace(
            "ChildChannel scheduling task",
            metadata: [
                LoggingKeys.childChannelTask: "\(task)",
                LoggingKeys.childChannelTaskDeadline: "\(deadline)",
            ]
        )

        if deadline <= .now() {
            // This should already happen we can execute it right away

            self.logger.trace(
                "ChildChannel task is ready. Executing now",
                metadata: [
                    LoggingKeys.childChannelTask: "\(task)"
                ]
            )
            self._processActions(self._stateMachine.childChannelExecuteTask(task))
        } else if let currentScheduledTask = self._currentScheduledTask {
            if currentScheduledTask.taskWithDeadline.deadline <= deadline {
                // We have something scheduled already that happens before us so we can just queue up behind it

                self.logger.trace(
                    "ChildChannel queuing task",
                    metadata: [
                        LoggingKeys.childChannelTask: "\(task)"
                    ]
                )
                self._pendingTasks.insert(.init(task: task, deadline: deadline, id: self._nextTaskID))
                self._nextTaskID += 1
            } else {
                // We need to run before the current scheduled task so let's cancel the current one
                // and schedule a new one

                self.logger.trace(
                    "ChildChannel cancelling current scheduled task and scheduling new one",
                    metadata: [
                        LoggingKeys.childChannelTask: "\(task)"
                    ]
                )

                currentScheduledTask.scheduled.cancel()
                self._pendingTasks.insert(currentScheduledTask.taskWithDeadline)
                self._scheduleTaskWithDeadline(.init(task: task, deadline: deadline, id: self._nextTaskID))
                self._nextTaskID += 1
            }
        } else {
            // We don't have a scheduled task yet so let's go ahead and create a fresh one
            self._scheduleTaskWithDeadline(.init(task: task, deadline: deadline, id: self._nextTaskID))
            self._nextTaskID += 1
        }
    }

    @inlinable
    func _scheduledTaskFired() {
        self.eventLoop.assertInEventLoop()

        self.logger.trace(
            "ChildChannel scheduled task fired"
        )

        guard let currentScheduledTask = self._currentScheduledTask else {
            // It might be that we have cancelled the task but it still fired since it is not guaranteed
            // that cancellation is effective if the task is already executing.
            self.logger.trace(
                "ChildChannel no current scheduled task"
            )

            return
        }

        guard currentScheduledTask.taskWithDeadline.deadline <= .now() else {
            // Since cancellation is not guaranteed it might be that we already scheduled the next one
            // but the previous one is firing. We must make sure that the previous one is not
            // executing the next one.
            self.logger.trace(
                "ChildChannel current scheduled task not ready yet",
                metadata: [
                    LoggingKeys.childChannelTask: "\(currentScheduledTask.taskWithDeadline.task)",
                    LoggingKeys.childChannelTaskDeadline: "\(currentScheduledTask.taskWithDeadline.deadline)",
                ]
            )

            return
        }

        self._currentScheduledTask = nil

        self.logger.trace(
            "ChildChannel task is ready. Executing now",
            metadata: [
                LoggingKeys.childChannelTask: "\(currentScheduledTask.taskWithDeadline.task)"
            ]
        )

        self._processActions(self._stateMachine.childChannelExecuteTask(currentScheduledTask.taskWithDeadline.task))

        while let nextTaskWithDeadline = self._pendingTasks.popMin() {
            guard nextTaskWithDeadline.deadline <= .now() else {
                // We have to schedule the next task now
                self._scheduleTaskWithDeadline(nextTaskWithDeadline)
                // We are breaking the loop here since we have to wait until the next schedule is fired
                return
            }
            // The next task is also ready to be executed

            self.logger.trace(
                "ChildChannel task is ready. Executing now",
                metadata: [
                    LoggingKeys.childChannelTask: "\(nextTaskWithDeadline.task)"
                ]
            )

            self._processActions(self._stateMachine.childChannelExecuteTask(nextTaskWithDeadline.task))
        }
    }

    @inlinable
    func _cancelTask(_ task: Task) {
        self.eventLoop.assertInEventLoop()

        if let currentScheduledTask = self._currentScheduledTask, currentScheduledTask.taskWithDeadline.task == task {
            self.logger.trace(
                "ChildChannel cancelling task",
                metadata: [
                    LoggingKeys.childChannelTask: "\(task)"
                ]
            )

            currentScheduledTask.scheduled.cancel()

            if let nextTaskWithDeadline = self._pendingTasks.popMin() {
                self._scheduleTaskWithDeadline(nextTaskWithDeadline)
            } else {
                self.logger.trace("ChildChannel no more tasks to schedule")

                self._currentScheduledTask = nil
            }
        } else {
            if let index = self._pendingTasks.unordered.firstIndex(where: { $0.task == task }) {
                self.logger.trace(
                    "ChildChannel removing scheduled task from queue",
                    metadata: [
                        LoggingKeys.childChannelTask: "\(task)"
                    ]
                )

                // Temporarily assigning an empty Heap to avoid CoWing
                var items = self._pendingTasks.unordered
                self._pendingTasks = Heap()
                items.remove(at: index)
                self._pendingTasks = Heap(items)
            }
        }
    }

    @usableFromInline
    func _scheduleTaskWithDeadline(_ taskWithDeadline: TaskWithDeadline) {
        self.eventLoop.assertInEventLoop()

        self.logger.trace(
            "ChildChannel scheduling task",
            metadata: [
                LoggingKeys.childChannelTask: "\(taskWithDeadline.task)",
                LoggingKeys.childChannelTaskDeadline: "\(taskWithDeadline.deadline)",
            ]
        )

        let scheduled = self.eventLoop.scheduleTask(deadline: taskWithDeadline.deadline) {
            self._scheduledTaskFired()
        }
        self._currentScheduledTask = .init(
            taskWithDeadline: taskWithDeadline,
            scheduled: scheduled
        )
    }

    @inlinable
    func _succeedPromise(_ promise: EventLoopPromise<Void>) {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel succeeding promise")
        promise.succeed(())
    }

    @inlinable
    func _failPromise(_ promise: EventLoopPromise<Void>, with error: Error) {
        self.eventLoop.assertInEventLoop()
        self.logger.trace(
            "ChildChannel failing promise",
            metadata: [
                LoggingKeys.error: "\(error)"
            ]
        )
        promise.fail(error)
    }

    @inlinable
    func _fireUserInboundEventTriggered(event: Any) {
        self.eventLoop.assertInEventLoop()

        self.logger.trace(
            "ChildChannel fire user inbound event",
            metadata: [
                LoggingKeys.childChannelUserInboundEvent: "\(event)"
            ]
        )

        if case ChannelEvent.inputClosed = event, !self.isInputClosed {
            self._pendingReads.append(.inputClose)
        } else {
            self.pipeline.syncOperations.fireUserInboundEventTriggered(event)
        }
    }

    @inlinable
    func _writeToParent(message: ParentChannelOutboundMessage, promise: EventLoopPromise<Void>?) {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel write to parent")
        self._pendingWritesForMultiplexer.append((message, promise))
    }

    @inlinable
    func _readFromParent() {
        self.eventLoop.assertInEventLoop()

        self.logger.trace("ChildChannel reading from parent")
        guard let delegate = self.delegate else {
            // Nothing to do for us
            return
        }

        delegate.readFromChildChannel(channelID: self._id, channelObjectIdentifier: ObjectIdentifier(self))
    }

    @inlinable
    func _appendDependentAction(
        _ action: ChildChannelAction<
            ParentChannelInboundMessage, ParentChannelOutboundMessage, ChildChannelInboundMessage,
            ChildChannelOutboundMessage, Task
        >,
        withID id: UInt64
    ) {
        self.eventLoop.assertInEventLoop()
        self._dependentActionsBuffer.append(.init(action: action, id: id))
    }

    @inlinable
    func _failClose(error: Error, promise: EventLoopPromise<Void>?) {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel fail to close")
        promise?.fail(error)
    }

    @inlinable
    func _close(error: Error?, promise: EventLoopPromise<Void>?) {
        self.eventLoop.assertInEventLoop()

        self.logger.trace(
            "Closing ChildChannel",
            metadata: [
                LoggingKeys.error: "\(error.flatMap { "\($0)" } ?? "No error")"
            ]
        )

        // We use didClose as a gating mechanism: only one of closedCleanly
        // or errorEncountered ever gets to run. This is important, as errorEncountered
        // can actually trigger closedCleanly.
        if self._didClose {
            self.logger.trace("ChildChannel already closed")
            return
        }
        self._didClose = true

        // We need to make sure that all dependent actions are run after delivering the pending
        // reads, since they might generate more writes and we need to fail all of them.
        let dependentActionID = self._nextDependentActionID()
        self._appendDependentAction(
            .failPendingWrites(error ?? ChannelError.eof),
            withID: dependentActionID
        )

        if let promise = promise {
            if let error = error {
                self._appendDependentAction(.failPromise(promise, withError: error), withID: dependentActionID)
            } else {
                self._appendDependentAction(.succeedPromise(promise), withID: dependentActionID)
            }
        }

        if let promise = self._userActivationPromise {
            self._userActivationPromise = nil
            self._appendDependentAction(
                .failChannelPromise(promise, withError: error ?? ChannelError.eof),
                withID: dependentActionID
            )
        }

        self._appendDependentAction(.notifyChannelInactive, withID: dependentActionID)

        if let error = error {
            self._processActions(
                _Actions(
                    .fireErrorCaught(error),
                    .init(action: .deliverPendingReads, dependentActionID: dependentActionID)
                )
            )
        } else {
            self._processActions(_Actions(.init(action: .deliverPendingReads, dependentActionID: dependentActionID)))
        }

        // We are executing this on the next tick since there might be scheduled tasks
        // on the event loop that expect the channel to be alive. This gives everything
        // an opportunity to settle, and it reduces the call stack depth to avoid blowing
        // the stack.
        self.eventLoop.execute {
            self.logger.trace("ChildChannel removing handlers from pipeline")
            self.removeHandlers(pipeline: self.pipeline)

            // We must not fail the close future, so we always succeed it regardless of any errors.
            // See docs on Channel protocol.
            self._closePromise.succeed()

            guard let delegate = self.delegate else {
                // Nothing we can do
                return
            }

            delegate.closeFromChildChannel(channelID: self._id, channelObjectIdentifier: ObjectIdentifier(self))
            self.delegate = nil
        }
    }

    // MARK: - Internal actions

    /// Delivers all pending messages from the channel to the state machine.
    @inlinable
    func _deliverPendingWritesToStateMachine() {
        self.eventLoop.assertInEventLoop()

        self._pendingWritesFromChannel.mark()

        if self.isActive {
            self.logger.trace("ChildChannel delivering pending buffered writes to state machine")
            while self._pendingWritesFromChannel.hasMark, let writeOrClose = self._pendingWritesFromChannel.popFirst() {
                switch writeOrClose {
                case .write(let message, let writePromise):
                    if let next = self._pendingWritesFromChannel.first,
                        case .close(let closeError, .output, let closePromise) = next
                    {
                        _ = self._pendingWritesFromChannel.popFirst()
                        self._processActions(
                            self._stateMachine.childChannelWriteAndCloseOutput(
                                message,
                                writePromise: writePromise,
                                closeError: closeError,
                                closePromise: closePromise
                            )
                        )
                    } else {
                        self._processActions(
                            self._stateMachine.childChannelWriteMessage(message, promise: writePromise)
                        )
                    }

                    if case .changed(let newValue) = self._writabilityManager.wroteMessage(message) {
                        self._changeWritability(to: newValue)
                    }

                case .close(let error, let mode, let promise):
                    // We only consider mode output as a write and no other close mode should
                    // get buffered.
                    assert(mode == .output)
                    self._processActions(
                        self._stateMachine.childChannelClose(error: error, mode: mode, promise: promise)
                    )
                }
            }
        } else {
            self.logger.trace(
                "ChildChannel NOT delivering pending buffered writes to state machine because channel is inactive"
            )
        }
    }

    /// Flush any pending messages to the multiplexer. By definition all pending messages are flushed.
    @inlinable
    func _writePendingToMultiplexer() {
        self.eventLoop.assertInEventLoop()

        self.logger.trace("ChildChannel flushing pending writes to the multiplexer")
        guard let delegate = self.delegate else {
            self._failPendingWrites(error: ChannelError.alreadyClosed)
            return
        }

        var didWrite = false
        while let (message, promise) = self._pendingWritesForMultiplexer.popFirst() {
            didWrite = true
            delegate.writeFromChildChannel(
                channelID: self._id,
                channelObjectIdentifier: ObjectIdentifier(self),
                message: message,
                promise: promise
            )
        }

        if didWrite {
            delegate.flushFromChildChannel(channelID: self._id)
        }
    }

    /// Deliver reads to the channel.
    ///
    /// This is sometimes done when the channel itself is closed, because data loss in these circumstances is unacceptable.
    @inlinable
    func _deliverPendingReads() {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel delivering buffered pending reads")
        while let readOrClose = self._pendingReads.popFirst() {
            switch readOrClose {
            case .read(let message):
                // If our input is already closed we are trying to deliver a read afterwards now.
                // This is not correct and we should fail those
                if !self.isInputClosed {
                    self._processActions(self._stateMachine.childChannelReadMessage(message))
                } else {
                    self._processActions(.init(.fireErrorCaught(ChannelError.inputClosed)))
                }

            case .inputClose:
                self.isInputClosed = true
                // Tell the state machine that we want to unbuffer the input closed. State machine will decide what to do. Typically, it'll just fire it.
                self._processActions(self._stateMachine.childChannelReceivedInputClosed())
            }
        }
    }

    @inlinable
    func _fireChannelReadComplete() {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel fire channel read complete")
        self.pipeline.fireChannelReadComplete()

        // Once we send the read complete we should try to auto read again
        self._tryToAutoRead()
    }

    @inlinable
    func _fireErrorCaught(_ error: Error) {
        self.eventLoop.assertInEventLoop()
        self.logger.trace(
            "ChildChannel fire error caught",
            metadata: [
                LoggingKeys.error: "\(error)"
            ]
        )
        self.pipeline.fireErrorCaught(error)
    }

    @inlinable
    func _failPromise(_ promise: EventLoopPromise<Channel>, with error: Error) {
        self.eventLoop.assertInEventLoop()
        self.logger.trace(
            "ChildChannel failing channel promise",
            metadata: [
                LoggingKeys.error: "\(error)"
            ]
        )
        promise.fail(error)
    }

    /// Fails all pending writes with the given error.
    ///
    /// - Parameter error: The error to fail the pending writes with.
    @inlinable
    func _failPendingWrites(error: Error) {
        self.eventLoop.assertInEventLoop()

        self.logger.trace(
            "ChildChannel failing pending writes",
            metadata: [
                LoggingKeys.error: "\(error)"
            ]
        )
        while let writeOrClose = self._pendingWritesFromChannel.popFirst() {
            switch writeOrClose {
            case .write(_, let promise), .close(_, _, let promise):
                promise?.fail(error)
            }
        }

        while let (_, promise) = self._pendingWritesForMultiplexer.popFirst() {
            promise?.fail(error)
        }
    }
}

// MARK: - Channel initialization

extension ChildChannel {
    @inlinable
    func configure(
        promise: EventLoopPromise<Channel>?,
        initializer: ((Channel, StateMachine) -> EventLoopFuture<Void>)?
    ) {
        self.eventLoop.preconditionInEventLoop()

        // We need to configure this channel. This involves doing three things:
        // 1. Setting our autoRead state from the parent
        // 2. Calling the initializer, if provided.
        // 3. Call out to the state machine.
        self._getAutoReadFromParent { autoReadResult in
            switch autoReadResult {
            case .success(let autoRead):
                self._autoRead = autoRead
                guard let initializer = initializer else {
                    self._userActivationPromise = promise
                    self._processActions(self._stateMachine.childChannelInitializationSucceeded())
                    return
                }
                initializer(self, self._stateMachine)
                    .whenComplete { result in
                        switch result {
                        case .success:
                            self._userActivationPromise = promise
                            self._processActions(self._stateMachine.childChannelInitializationSucceeded())
                        case .failure(let error):
                            self._userActivationPromise = promise
                            self._processActions(self._stateMachine.childChannelInitializationFailed(error: error))
                        }
                    }
            case .failure(let error):
                self._userActivationPromise = promise
                self._processActions(self._stateMachine.childChannelInitializationFailed(error: error))
            }
        }
    }

    /// Gets the 'autoRead' option from the parent channel and invokes the `body` closure with the
    /// result. This may be done synchronously if the parent `Channel` supports synchronous options.
    @inlinable
    func _getAutoReadFromParent(_ body: @escaping (Result<Bool, Error>) -> Void) {
        self.eventLoop.assertInEventLoop()

        // This force unwrap is safe as parent is assigned in the initializer, and never unassigned.
        if let syncOptions = self.parent!.syncOptions {
            let autoRead = Result(catching: { try syncOptions.getOption(ChannelOptions.autoRead) })
            body(autoRead)
        } else {
            let boundedBody = NIOLoopBound(body, eventLoop: self.parent!.eventLoop)
            self.parent!.getOption(ChannelOptions.autoRead)
                .whenComplete { autoRead in
                    boundedBody.value(autoRead)
                }
        }
    }
}

// MARK: Activation state management

extension ChildChannel {
    /// An enum to keep track of whether we've notified the channel of activation or not.
    @usableFromInline
    enum _ActivationState: Sendable {
        case neverActivated
        case activated
        case deactivated
    }

    /// This function handles the state required to notify the channel of activity. It can safely
    /// be called repeatedly, and will only activate the channel once.
    @inlinable
    func _notifyChannelActive() {
        self.eventLoop.assertInEventLoop()
        switch self._activationState {
        case .neverActivated:
            self._activationState = .activated
            self._isActive.store(true, ordering: .sequentiallyConsistent)
            self.logger.trace("ChildChannel fire channel active")
            self.pipeline.fireChannelActive()

        case .activated:
            assert(self.isActive)

        case .deactivated:
            assert(!self.isActive)
        }
    }

    /// This function handles the state required to notify the channel of inactivity. It can safely
    /// be called repeatedly, and will only deactivate the channel once.
    @inlinable
    func _notifyChannelInactive() {
        self.eventLoop.assertInEventLoop()
        switch self._activationState {
        case .neverActivated:
            // Do nothing, transition to inactive.
            self._activationState = .deactivated
            assert(!self.isActive)

        case .activated:
            self._activationState = .deactivated
            self._isActive.store(false, ordering: .sequentiallyConsistent)
            self.logger.trace("ChildChannel fire channel inactive")
            self.pipeline.fireChannelInactive()

        case .deactivated:
            assert(!self.isActive)
        }
    }
}

// MARK: - Calls from the multiplexer

extension ChildChannel {
    @inlinable
    func parentChannelInactive() {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel parent channel inactive")
        self._processActions(self._stateMachine.parentChannelInactive())
    }

    @inlinable
    func parentChannelReadMessage(_ message: ParentChannelInboundMessage) {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel parent channel read message")
        self._processActions(self._stateMachine.parentChannelReadMessage(message))
    }

    @inlinable
    func parentChannelReadComplete() {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel parent channel read complete")
        self._processActions(self._stateMachine.parentChannelReadComplete())
        self._tryToRead()
    }

    @inlinable
    func parentChannelWritabilityChanged(newValue: Bool) {
        self.eventLoop.assertInEventLoop()

        self.logger.trace(
            "ChildChannel parent channel writability changed",
            metadata: [
                LoggingKeys.parentChannelWritability: "\(newValue)"
            ]
        )
        guard case .changed(newValue: let newValue) = self._writabilityManager.parentWritabilityChanged(newValue) else {
            return
        }

        self._changeWritability(to: newValue)
    }

    @inlinable
    func parentChannelUserInboundEventTriggered(_ event: Any) {
        self.eventLoop.assertInEventLoop()

        self.logger.trace(
            "ChildChannel parent channel user inbound event",
            metadata: [
                LoggingKeys.parentChannelUserInboundEvent: "\(event)"
            ]
        )
        self._processActions(self._stateMachine.parentChannelUserInboundEventTriggered(event))
    }
}

// MARK: - `Channel` & `ChannelCore` conformance

extension ChildChannel: Channel, ChannelCore {
    @usableFromInline
    struct SynchronousOptions: NIOSynchronousChannelOptions {
        @usableFromInline
        let channel: ChildChannel

        fileprivate init(channel: ChildChannel) {
            self.channel = channel
        }

        @inlinable
        func setOption<Option: ChannelOption>(_ option: Option, value: Option.Value) throws {
            try self.channel._setOption0(option, value: value)
        }

        @inlinable
        func getOption<Option: ChannelOption>(_ option: Option) throws -> Option.Value {
            try self.channel._getOption0(option)
        }
    }

    @usableFromInline
    var syncOptions: NIOSynchronousChannelOptions? {
        SynchronousOptions(channel: self)
    }

    @inlinable
    func getOption<Option: ChannelOption>(_ option: Option) -> EventLoopFuture<Option.Value> {
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.submit { try self._getOption0(option) }
        }
        do {
            return self.eventLoop.makeSucceededFuture(try self._getOption0(option))
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    @inlinable
    func _getOption0<Option: ChannelOption>(_ option: Option) throws -> Option.Value {
        self.eventLoop.preconditionInEventLoop()

        switch option {
        case _ as ChannelOptions.Types.AutoReadOption:
            return self._autoRead as! Option.Value
        default:
            return try self._stateMachine.childChannelGetOption(option)
        }
    }

    @inlinable
    func setOption<Option: ChannelOption>(_ option: Option, value: Option.Value) -> EventLoopFuture<Void>
    where Option.Value: Sendable {
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.submit { try self._setOption0(option, value: value) }
        }
        do {
            return self.eventLoop.makeSucceededFuture(try self._setOption0(option, value: value))
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    @inlinable
    func _setOption0<Option: ChannelOption>(_ option: Option, value: Option.Value) throws {
        self.eventLoop.preconditionInEventLoop()

        switch option {
        case _ as ChannelOptions.Types.AutoReadOption:
            self._autoRead = value as! Bool
        default:
            try self._stateMachine.childChannelSetOption(option, value: value)
        }
    }

    @usableFromInline
    var closeFuture: EventLoopFuture<Void> {
        self._closePromise.futureResult
    }

    @usableFromInline
    var pipeline: ChannelPipeline {
        self._pipeline
    }

    @usableFromInline
    var isWritable: Bool {
        self._isWritable.load(ordering: .sequentiallyConsistent)
    }

    @usableFromInline
    var isActive: Bool {
        self._isActive.load(ordering: .sequentiallyConsistent)
    }

    @usableFromInline
    var _channelCore: ChannelCore {
        self.eventLoop.preconditionInEventLoop()
        return self
    }

    @usableFromInline
    var localAddress: SocketAddress? {
        self._localAddress
    }

    @usableFromInline
    var remoteAddress: SocketAddress? {
        self._remoteAddress
    }

    @inlinable
    func localAddress0() throws -> SocketAddress {
        self.eventLoop.preconditionInEventLoop()

        guard let localAddress = self.localAddress else {
            throw ChannelError.unknownLocalAddress
        }

        return localAddress
    }

    @inlinable
    func remoteAddress0() throws -> SocketAddress {
        self.eventLoop.preconditionInEventLoop()

        guard let remoteAddress = self.remoteAddress else {
            throw ChannelError.operationUnsupported
        }

        return remoteAddress
    }

    @inlinable
    func write0(_ data: NIOAny, promise: EventLoopPromise<Void>?) {
        self.eventLoop.preconditionInEventLoop()

        self.logger.trace("ChildChannel write0")
        // First we have to check if we can write.
        do {
            try self._stateMachine.childChannelCanWrite()
        } catch {
            self.logger.trace("ChildChannel cannot write")
            promise?.fail(error)
            return
        }

        let message = self.unwrapData(data, as: ChildChannelOutboundMessage.self)

        self._pendingWritesFromChannel.append(.write(message, promise))

        // Ok, we can make an outcall now, which means we can safely deal with the flow control.
        if case .changed(newValue: let value) = self._writabilityManager.bufferedMessage(message) {
            self._changeWritability(to: value)
        }
    }

    @inlinable
    func flush0() {
        self.eventLoop.preconditionInEventLoop()

        self.logger.trace("ChildChannel flush0")

        // Since we might be in a processAction loop already we have to enqueue the flush.
        // Flushing consists of two steps
        // 1. We need to deliver all the pending writes to the state machine
        // 2. Once all pending writes have been delivered we need to write them to the multiplexer
        //
        // It is important that the second part is only enqueued after the first finished. Otherwise,
        // there are ordering problems.
        let id = self._nextDependentActionID()
        self._appendDependentAction(.writePendingToMultiplexer, withID: id)
        self._processActions(
            _Actions(
                .init(
                    action: .deliverPendingWritesToStateMachine,
                    dependentActionID: id
                )
            )
        )
    }

    @inlinable
    func read0() {
        self.eventLoop.preconditionInEventLoop()

        self.logger.trace("ChildChannel read0")
        if self._unsatisfiedRead {
            // We already have an unsatisfied read, let's do nothing.
            self.logger.trace("ChildChannel has an outstanding unsatisfied read")
            return
        }

        // At this stage, we have an unsatisfied read.
        self._unsatisfiedRead = true

        // If there is no pending data to read, we're going to call read() on the parent channel.
        if self._pendingReads.count == 0 {
            self.logger.trace("ChildChannel read from parent")
            var actions = self._stateMachine.childChannelReadFromParent()

            // We need to make sure to add the flush as a dependent action
            // since we could be in a process action loop
            self._addDependentActionsToLastAction(
                actions: &actions,
                dependentActions: _Actions(.writePendingToMultiplexer)
            )

            self._processActions(actions)
        }

        // If there *is* pending data, we're going to succeed the read out of it. Note that the call to
        // `self._processActions()` above may have added some pending reads; we want to use them here.
        if self._pendingReads.count > 0 {
            self._tryToRead()
        }
    }

    @inlinable
    func close0(error: Error, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        self.eventLoop.preconditionInEventLoop()

        self.logger.trace("ChildChannel close0")

        switch mode {
        case .input:
            // Input is weird and arguably shouldn't exist. We just end it on.
            self._processActions(self._stateMachine.childChannelClose(error: error, mode: mode, promise: promise))

        case .output:
            // We are treating output as being a write since it often translates to a
            // frame being sent out to the remote. That frame should be ordered correctly w.r.t.
            // the write issued in the channel before.
            self._pendingWritesFromChannel.append(.close(error, mode, promise))
            self.flush0()

        case .all:
            // All is different since it means close as fast as possible and the user
            // must expect that buffered writes might get lost.
            self._processActions(self._stateMachine.childChannelClose(error: error, mode: mode, promise: promise))
        }
    }

    @inlinable
    func triggerUserOutboundEvent0(_ event: Any, promise: EventLoopPromise<Void>?) {
        self.eventLoop.preconditionInEventLoop()

        self.logger.trace(
            "ChildChannel triggerUserOutboundEvent0",
            metadata: [
                LoggingKeys.childChannelUserOutboundEvent: "\(event)"
            ]
        )
        self._processActions(self._stateMachine.childChannelTriggerUserOutboundEvent(event, promise: promise))
    }

    @inlinable
    func channelRead0(_: NIOAny) {
        // do nothing
    }

    @inlinable
    func errorCaught0(error: Error) {
        // do nothing
    }

    @inlinable
    func register0(promise: EventLoopPromise<Void>?) {
        fatalError("not implemented \(#function)")
    }

    @inlinable
    func bind0(to: SocketAddress, promise: EventLoopPromise<Void>?) {
        fatalError("not implemented \(#function)")
    }

    @inlinable
    func connect0(to: SocketAddress, promise: EventLoopPromise<Void>?) {
        fatalError("not implemented \(#function)")
    }
}

@available(*, unavailable)
extension ChildChannel.SynchronousOptions: Sendable {}

// MARK: - Internal read and write handling

extension ChildChannel {
    @inlinable
    func _tryToRead() {
        self.eventLoop.assertInEventLoop()
        self.logger.trace("ChildChannel try to read")
        // If there's no read to satisfy, no worries about it.
        guard self._unsatisfiedRead else {
            self.logger.trace("ChildChannel no unsatisfied read")
            return
        }

        // If we're not active, we will hold on to those reads.
        guard self.isActive else {
            self.logger.trace("ChildChannel not active")
            return
        }

        // Check with the state machine if we can read.
        guard self._stateMachine.childChannelCanRead() else {
            self.logger.trace("ChildChannel cannot read")
            return
        }

        // If there are no pending reads, do nothing.
        guard self._pendingReads.count > 0 else {
            self.logger.trace("ChildChannel no buffered pending reads")
            return
        }

        // Ok, we're satisfying a read here.
        self._unsatisfiedRead = false

        let id = self._nextDependentActionID()
        self._appendDependentAction(.fireChannelReadComplete, withID: id)
        self._appendDependentAction(.writePendingToMultiplexer, withID: id)
        self._processActions(
            ChildChannelActions(
                .init(
                    action: .deliverPendingReads,
                    dependentActionID: id
                )
            )
        )
    }

    @inlinable
    func _tryToAutoRead() {
        self.eventLoop.assertInEventLoop()
        if !self._unsatisfiedRead && self._autoRead {
            self.logger.trace("ChildChannel auto read")
            // If auto-read is turned on, recurse into channelPipeline.read().
            // This cannot recurse indefinitely unless frames are being delivered
            // by the read stacks, which is generally fairly unlikely to continue unbounded.
            self.pipeline.read()
        }
    }

    @inlinable
    func _changeWritability(to newWritability: Bool) {
        self.eventLoop.assertInEventLoop()
        // We are reaching out to the state machine here so that it can prevent us from changing
        // the writability. This is most often the case before the channel becomes active.
        guard self._stateMachine.childChannelShouldChangeWritability() else {
            self.logger.trace("ChildChannel should not change writability")
            return
        }

        self.logger.trace(
            "ChildChannel changing writability",
            metadata: [
                LoggingKeys.childChannelWritability: "\(newWritability)"
            ]
        )
        self._isWritable.store(newWritability, ordering: .sequentiallyConsistent)
        self.pipeline.fireChannelWritabilityChanged()
    }
}

extension ChildChannel {
    /// Appends the passed dependent actions to the last action in the passed actions collection.
    /// If the actions collection is empty all dependent actions will be just appended to the collection.
    @inlinable
    func _addDependentActionsToLastAction(
        actions:
            inout ChildChannelActions<
                ParentChannelInboundMessage, ParentChannelOutboundMessage, ChildChannelInboundMessage,
                ChildChannelOutboundMessage, Task
            >,
        dependentActions: ChildChannelActions<
            ParentChannelInboundMessage, ParentChannelOutboundMessage, ChildChannelInboundMessage,
            ChildChannelOutboundMessage, Task
        >
    ) {
        self.eventLoop.assertInEventLoop()
        if actions.isEmpty {
            for dependentAction in dependentActions {
                actions.append(dependentAction)
            }
        } else {
            precondition(actions[actions.endIndex - 1].dependentActionID == nil, "Dependent action ID was not nil")
            let id = self._nextDependentActionID()
            for dependentAction in dependentActions {
                self._appendDependentAction(dependentAction, withID: id)
            }
            actions[actions.endIndex - 1].dependentActionID = id
        }
    }
}

/// It's okay to mark ``ChildChannel`` as `@unchecked Sendable` because all operations are properly
/// isolated to `EventLoop`s.
extension ChildChannel: @unchecked Sendable {}
