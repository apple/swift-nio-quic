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

import XCTest

@testable import ChildChannelMultiplexer

@available(anyAppleOS 26, *)
extension ChildChannelAction.Action {
    func assertIsChildChannelCompleteActivation(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .childChannelCompleteActivation:
            break
        default:
            XCTFail("Expected action 'childChannelCompleteActivation'. Actual \(self)", file: file, line: line)
        }
    }

    func assertIsParentChannelWrite(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .parentChannelWrite:
            break
        default:
            XCTFail("Expected action 'parentChannelWrite'. Actual \(self)", file: file, line: line)
        }
    }

    func assertIsChildChannelFlush(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .childChannelFlush:
            break
        default:
            XCTFail("Expected action 'childChannelFlush'. Actual \(self)", file: file, line: line)
        }
    }

    func assertIsChildChannelEncounterError(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .childChannelEncounterError:
            break
        default:
            XCTFail("Expected action 'childChannelEncounterError'. Actual \(self)", file: file, line: line)
        }
    }

    func assertIsChildChannelClosedCleanly(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .childChannelCloseCleanly:
            break
        default:
            XCTFail("Expected action 'childChannelCloseCleanly'. Actual \(self)", file: file, line: line)
        }
    }

    func assertIsChildChannelFireUserInboundEventTriggered(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .childChannelFireUserInboundEventTriggered:
            break
        default:
            XCTFail(
                "Expected action 'childChannelFireUserInboundEventTriggered'. Actual \(self)",
                file: file,
                line: line
            )
        }
    }

    func assertIsChildChannelFireUserInboundEventTriggered<T>(
        _ type: T.Type,
        file: StaticString = #filePath,
        line: UInt = #line
    ) -> T? {
        switch self {
        case .childChannelFireUserInboundEventTriggered(let event):
            guard let typed = event as? T else {
                XCTFail(
                    "Expected event of type '\(T.self)'. Actual: \(Swift.type(of: event))",
                    file: file,
                    line: line
                )
                return nil
            }
            return typed
        default:
            XCTFail(
                "Expected action 'childChannelFireUserInboundEventTriggered'. Actual \(self)",
                file: file,
                line: line
            )
            return nil
        }
    }

    func assertIsChildChannelFireChannelRead(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .childChannelFireChannelRead:
            break
        default:
            XCTFail("Expected action 'childChannelFireChannelRead'. Actual \(self)", file: file, line: line)
        }
    }

    func assertIsChildChannelBufferRead(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .childChannelBufferRead:
            break
        default:
            XCTFail("Expected action 'childChannelBufferRead'. Actual \(self)", file: file, line: line)
        }
    }

    func assertIsChildChannelBufferInputClosed(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .childChannelBufferInputClosed:
            break
        default:
            XCTFail("Expected action 'childChannelBufferInputClosed'. Actual \(self)", file: file, line: line)
        }
    }

    func assertIsFailPromise(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .failPromise:
            break
        default:
            XCTFail("Expected action 'failPromise'. Actual \(self)", file: file, line: line)
        }
    }

    func assertIsSucceedPromise(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .succeedPromise:
            break
        default:
            XCTFail("Expected action 'succeedPromise'. Actual \(self)", file: file, line: line)
        }
    }

    func assertIsChildChannelCancelTask(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .childChannelCancelTask:
            break
        default:
            XCTFail("Expected action 'childChannelCancelTask'. Actual \(self)", file: file, line: line)
        }
    }

    func assertIsChildChannelScheduleTask(file: StaticString = #filePath, line: UInt = #line) {
        switch self {
        case .childChannelScheduleTask:
            break
        default:
            XCTFail("Expected action 'childChannelScheduleTask'. Actual \(self)", file: file, line: line)
        }
    }
}
