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

import Crypto
import Foundation
import Testing

@testable import NIOQUIC

let p256PrivateKeyPath: String = Bundle.module.path(forResource: "privateKey", ofType: "der")!
let p256PublicKeyPath: String = Bundle.module.path(forResource: "publicKey", ofType: "der")!

let privateKeyB64 =
    "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgAwx2URy4NAHA77bWmUT6k5cplHkFwelMaFOWHN1qyeyhRANCAARVi0dP1kVNh4FvNibyhsbOkFFFXy9GFzuS/e4c2eG9BwiQ3SS48L7J+ekRfoPi9AIDoElOCi7l2jwjy3sBbVxN"
let publicKeyB64 =
    "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEVYtHT9ZFTYeBbzYm8obGzpBRRV8vRhc7kv3uHNnhvQcIkN0kuPC+yfnpEX6D4vQCA6BJTgou5do8I8t7AW1cTQ=="

let privateKeyECWithParameters = """
    -----BEGIN EC PARAMETERS-----
    BggqhkjOPQMBBw==
    -----END EC PARAMETERS-----
    -----BEGIN EC PRIVATE KEY-----
    MHcCAQEEIMjwv1Lzz5YbUudNqXNDbzgcd+vNpWNfZ+KaAHmVzc/3oAoGCCqGSM49
    AwEHoUQDQgAEdznamnYxRIz1cDMAr+lBhSlGfWXxDKmXv2NP1pTCxi75l+E0R4V8
    JtPzVkSCP0PXDsqO6/TLgU0zXCx1J9YZcw==
    -----END EC PRIVATE KEY-----
    """

let privateKeyEC = """
    -----BEGIN PRIVATE KEY-----
    MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgO6fz+J/sZqbCki3h
    chsrVb69KW8q24pLDwotAtwz/gahRANCAASkBqszxMCGrd8l+xZitPto300blCWk
    wRCdoar3UeEEfuH5LsJ3kNjN+oMZmHAmnhHE6cqLHFem/ujsGgrqJ3E8
    -----END PRIVATE KEY-----
    """

let privateKeyRSA = """
    -----BEGIN RSA PRIVATE KEY-----
    MIIEpQIBAAKCAQEAxeBrlW1i+gXmqC/4XlTppLCXPpksZbxsc0AufsWzpcIxBAQL
    jr4LxXyOenZcyhrdXxksEsCJE/8stJuUZgiFyDMesWjoL5bYyOpWrwPyvCMNaC8F
    15cKe5n7OSVNU312X0ZSxTZAOrCEH0kGrsXoQn5JgHygVXejSPlHw8F1Pwps3pnc
    cRYE9vsHZegspUI3DaqWgmewFWz6jSMY0v1DcZv2Xw+pMLeXEpKKMf+eo90mHShh
    n8FijsI1tFuYD++LmM/e1TV1z+W2sPL2CaosBO890WeCyL/bFl4j1lmdnXdBNX+l
    ub/5HTqI7hxAm/qonzDs3iV5KK1ZWZTVsPyqtwIDAQABAoIBAQCGLBQG8HMKgXHT
    XSOWIxGCIFONmKMoIMmQpFZik3+qx7AgvvVvRqIIuNqLYzKrv+eXEiR2WqMYMhCI
    Lm5DeUftZexL84xsqGY6Zdt9NLoko8f1et0FQF9VTCWyq/5wvEPFepOpMY3/vaz4
    4bVsULmaTLNeMiMtkL/hPVZSAB2WLjI7EgOq7JamRBCMY+ivtdtqi12kO8vaA2Ns
    dSKuU+e8tvAP4o6cMvuLtcqLy2UeoZzYTI998up0tqn+mGHl0DHx6MSi/TbVv3v8
    gSQGlBvWzUx85vz+1GyjRn+o4hO/tibtP6aKfWYztIyLgkaU8JuDZzT+CwwmCnEH
    ge/JmGuRAoGBAPypZ9MuWi1Ms57nVcpHRSHUc+cO/CfoaC4kRzgh99ApDbL+rj78
    9eXkS10Z8hTLQprcznQr+WZhnhNw13PrvzaGhtOXd/xLBnWlqZ6aIGYrJgKu+JLl
    yGVBZByG70yXel8IZt9l4CYctlS4D8iYAtdR+CCn8XGrc8JILnwBbYDNAoGBAMh9
    tiC8hyIc/wBV/WcgtpY6tPkrN4uw6WREH+UkVpvNM2kMMCgn1YLHbYq1vSKN3rKZ
    L9Ep4+8umRMg5yTgq9iURMoNjq+qU8NDF0nRljmyJRvgA5Ez2iQErm3y5dYxK+8w
    BRvCkDeOiiu1Mnd9chgu0tQhpfo7o+T3XeXB5omTAoGBAKl3IqlVnKxnls6NEVC0
    Tt0q93ZR6bUGv+G6+X3f4qxe7M5S3iJnXrMMVbQjc+iYkJr4YQ0wdX5DGVimxgv9
    Ymo6/vGq1ZKF69Y7ADLd479DT6JbI2S79JZdrr0nkBfKPgzBwOY0GYzWk0Dtl8CO
    nNE5LHkSy/HW8rSr32nTN1Q9AoGAOdl8GcoMO92d/pzRN1aLGKHr4hGEP3xWe6Xk
    hhuMGfyFnwPzSULlKo0coG98GWJSJbppv7KUoEkTxh8yUsO5Eg8GIj7zMuC0tpy/
    NX+SFye96WMj5FvPz6DCK9twUfNyN9vlPXNQZZdtatsnqq65oxyvnKHw4FkhG0n8
    //SI7p0CgYEA9CoA6/3rRIVKKEOgeCQHDVKIJIauTUskwdrBHLVU7SH+cfQ6VNy6
    zp/M54MpUP5jviSL61HmRoEqqcMWLALJHyZ1yQAZXSpthyMw0ahqTUZ71j1ukIO0
    UUjK3drJJd2jGQ0LfhlDCX7VmURIqJ6kaQ0WBNAJLFhTW4AS8HGYRZk=
    -----END RSA PRIVATE KEY-----
    """

struct SingingKeysTests {
    @available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
    @Test("Reading private key from disk")
    func readPrivateKey() throws {
        let privateKey = try P256.Signing.PrivateKey.fromDERFile(p256PrivateKeyPath)
        let privateKeyData = Data(base64Encoded: privateKeyB64)

        #expect(privateKey.derRepresentation == privateKeyData)
    }

    @available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
    @Test("Reading public key from disk")
    func readPublicKey() throws {
        let publicKey = try P256.Signing.PublicKey.fromDERFile(p256PublicKeyPath)
        let publicKeyData = Data(base64Encoded: publicKeyB64)

        #expect(publicKey.derRepresentation == publicKeyData)
    }

    @available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
    @Test("Reading EC private key")
    func readPrivateECKey() throws {
        // Write key.
        let fm = FileManager.default
        let directory = fm.temporaryDirectory
        let url = directory.appendingPathComponent("private-ec.key.pem")
        try privateKeyEC.write(to: url, atomically: true, encoding: .utf8)

        // Read key.
        #expect(throws: Never.self) {
            try loadPrivateKey(fromPEMFile: url.path())
        }
    }

    @available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
    @Test("Reading EC private key with parameters")
    func readPrivateECKeyWithParameters() throws {
        // Write key.
        let fm = FileManager.default
        let directory = fm.temporaryDirectory
        let url = directory.appendingPathComponent("private-ec-with-parameters.key.pem")
        try privateKeyECWithParameters.write(to: url, atomically: true, encoding: .utf8)

        // Read key.
        #expect(throws: Never.self) {
            try loadPrivateKey(fromPEMFile: url.path())
        }
    }

    @available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
    @Test("Reading RSA private key")
    func readPrivateRSAKey() throws {
        // Write key.
        let fm = FileManager.default
        let directory = fm.temporaryDirectory
        let url = directory.appendingPathComponent("private-rsa.key.pem")
        try privateKeyRSA.write(to: url, atomically: true, encoding: .utf8)

        // Read key.
        #expect(throws: Never.self) {
            try loadPrivateKey(fromPEMFile: url.path())
        }
    }
}
