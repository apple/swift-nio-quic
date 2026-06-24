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

import Foundation
import SwiftASN1
import X509

@available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
func loadCertificates(fromPEMFile path: String) throws -> [Certificate] {
    let pemData = try Data(contentsOf: URL(fileURLWithPath: path))
    let pemString = String(decoding: pemData, as: UTF8.self)
    let documents = try PEMDocument.parseMultiple(pemString: pemString)
    do {
        let certificates = try documents.map { try Certificate(pemDocument: $0) }
        return certificates
    } catch {
        throw QUICError.unableToLoadCertificates
    }
}

@available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
func loadPrivateKey(fromPEMFile path: String) throws -> Certificate.PrivateKey {
    let pemData = try Data(contentsOf: URL(fileURLWithPath: path))
    let pemString = String(decoding: pemData, as: UTF8.self)
    let documents = try PEMDocument.parseMultiple(pemString: pemString)
    for document in documents {
        if document.discriminator.hasSuffix("PRIVATE KEY") {
            return try Certificate.PrivateKey(pemDocument: document)
        }
    }
    throw QUICError.unableToLoadPrivateKey
}
