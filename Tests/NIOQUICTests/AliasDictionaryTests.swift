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

import Testing

@testable import NIOQUIC

struct AliasDictionaryTests {
    @Test
    func empty() {
        let dictionary = AliasDictionary<String, Int>()
        #expect(dictionary.isEmpty)
        #expect(dictionary.count == 0)
        #expect(dictionary.values.isEmpty)
        #expect(dictionary.values.count == 0)
    }

    @Test
    func insert() {
        var dictionary = AliasDictionary<String, Int>()
        let previous = dictionary.updateValue(42, forKey: "key-1")
        #expect(previous == nil)
        #expect(dictionary.count == 1)
    }

    @Test
    func updateExistingCanonical() {
        var dictionary: AliasDictionary<String, Int> = ["key-1": 42]
        let previous = dictionary.updateValue(99, forKey: "key-1")
        #expect(previous == 42)
        #expect(dictionary["key-1"] == 99)
        #expect(dictionary.count == 1)
    }

    @Test
    func updateViaAlias() {
        var dictionary: AliasDictionary<String, Int> = ["key-1": 42]
        dictionary.addAlias("key-1-alt", for: "key-1")

        let previous = dictionary.updateValue(99, forKey: "key-1-alt")
        #expect(previous == 42)
        #expect(dictionary["key-1"] == 99)
        #expect(dictionary["key-1-alt"] == 99)
        #expect(dictionary.count == 1)
    }

    @Test
    func remove() {
        var dictionary = AliasDictionary<String, Int>()
        #expect(dictionary.removeValue(forKey: "key-1") == nil)

        dictionary.updateValue(42, forKey: "key-1")
        #expect(dictionary.removeValue(forKey: "key-1") == 42)
        #expect(dictionary.count == 0)
    }

    @Test
    func lookup() {
        var dictionary = AliasDictionary<String, Int>()
        #expect(dictionary["key-1"] == nil)
        dictionary.updateValue(42, forKey: "key-1")
        #expect(dictionary["key-1"] == 42)
    }

    @Test
    func canonicalKey() {
        var dictionary = AliasDictionary<String, Int>()

        dictionary.updateValue(42, forKey: "key-1")
        #expect(dictionary._canonicalKey(forKey: "key-1") == "key-1")

        dictionary.addAlias("key-1a", for: "key-1")
        #expect(dictionary._canonicalKey(forKey: "key-1a") == "key-1")

        dictionary.addAlias("key-1b", for: "key-1")
        #expect(dictionary._canonicalKey(forKey: "key-1b") == "key-1")

        dictionary.addAlias("key-1c", for: "key-1a")
        #expect(dictionary._canonicalKey(forKey: "key-1c") == "key-1")

        dictionary.addAlias("key-1d", for: "key-1c")
        #expect(dictionary._canonicalKey(forKey: "key-1d") == "key-1")
    }

    @Test
    func dictionaryLiteral() {
        let dictionary: AliasDictionary<String, Int> = ["k1": 1, "k2": 2, "k3": 3]
        #expect(dictionary.count == 3)
        #expect(dictionary["k1"] == 1)
        #expect(dictionary["k2"] == 2)
        #expect(dictionary["k3"] == 3)
    }

    @Test
    func values() {
        let dictionary: AliasDictionary<String, Int> = ["k1": 1, "k2": 2, "k3": 3]
        // Order isn't guaranteed so use a set for comparison.
        let values = Set(dictionary.values)
        #expect(values == [1, 2, 3])
    }

    @Test
    func valuesAfterMutations() {
        var dictionary: AliasDictionary<String, Int> = ["k1": 1, "k2": 2]
        dictionary.addAlias("k1-alt", for: "k1")
        #expect(Set(dictionary.values) == [1, 2])

        dictionary.removeValue(forKey: "k1-alt")
        #expect(Set(dictionary.values) == [2])
    }

    @Test
    func canonicalKeyForUnknownKey() {
        let dictionary: AliasDictionary<String, Int> = ["key-1": 42]
        #expect(dictionary._canonicalKey(forKey: "missing") == nil)
    }

    @Test
    func subscriptInsert() {
        var dictionary = AliasDictionary<String, Int>()
        dictionary["key-1"] = 42
        #expect(dictionary["key-1"] == 42)
        #expect(dictionary.count == 1)
    }

    @Test
    func subscriptRemoveViaKey() {
        var dictionary: AliasDictionary<String, Int> = ["key-1": 42]
        dictionary.addAlias("key-1-alt", for: "key-1")

        // Setting nil via the canonical key tears the whole entry down, including aliases.
        dictionary["key-1"] = nil
        #expect(dictionary.count == 0)
        #expect(dictionary["key-1"] == nil)
        #expect(dictionary["key-1-alt"] == nil)
    }

    @Test
    func subscriptRemoveViaAlias() {
        var dictionary: AliasDictionary<String, Int> = ["key-1": 42]
        dictionary.addAlias("key-1-alt", for: "key-1")

        dictionary["key-1-alt"] = nil
        #expect(dictionary.count == 0)
        #expect(dictionary["key-1"] == nil)
        #expect(dictionary["key-1-alt"] == nil)
    }

    @Test
    func subscriptRemoveUnknownKeyIsNoOp() {
        var dictionary: AliasDictionary<String, Int> = ["key-1": 42]
        dictionary["unknown"] = nil
        #expect(dictionary["key-1"] == 42)
        #expect(dictionary.count == 1)
    }

    @Test
    func reinsertAfterRemove() {
        var dictionary: AliasDictionary<String, Int> = ["key-1": 42]
        #expect(dictionary.removeValue(forKey: "key-1") == 42)

        dictionary.updateValue(99, forKey: "key-1")
        #expect(dictionary["key-1"] == 99)
        #expect(dictionary.count == 1)
    }

    @Test
    func reinsertedKeyDoesNotInheritStaleAlias() {
        // After promoting an alias to canonical and then removing it, the formerly-promoted key
        // must not still resolve to the original canonical key's slot.
        var dictionary: AliasDictionary<String, Int> = ["key-1": 42]
        dictionary.addAlias("key-1-alt", for: "key-1")
        dictionary.removeKey("key-1")
        dictionary.removeValue(forKey: "key-1-alt")

        dictionary.updateValue(99, forKey: "key-1")
        #expect(dictionary["key-1"] == 99)
        #expect(dictionary["key-1-alt"] == nil)
    }

    @Suite
    struct AddAlias {
        var dictionary: AliasDictionary<String, Int>

        init() {
            self.dictionary = ["key-1": 42]
        }

        @Test
        mutating func viaKey() {
            self.dictionary.addAlias("key-1-alt", for: "key-1")
            #expect(self.dictionary.count == 1)

            #expect(self.dictionary["key-1"] == 42)
            #expect(self.dictionary["key-1-alt"] == 42)
        }

        @Test
        mutating func viaAlias() {
            self.dictionary.addAlias("key-1-alt", for: "key-1")
            self.dictionary.addAlias("key-1-alt-prime", for: "key-1-alt")
            #expect(self.dictionary.count == 1)

            #expect(self.dictionary["key-1"] == 42)
            #expect(self.dictionary["key-1-alt"] == 42)
            #expect(self.dictionary["key-1-alt-prime"] == 42)
        }

        @Test
        mutating func viaChainedAliases() {
            self.dictionary.addAlias("key-1-alt", for: "key-1")
            self.dictionary.addAlias("key-1-alt-prime", for: "key-1-alt")
            self.dictionary.addAlias("key-1-alt-prime-prime", for: "key-1-alt-prime")
            #expect(self.dictionary.count == 1)

            #expect(self.dictionary["key-1"] == 42)
            #expect(self.dictionary["key-1-alt"] == 42)
            #expect(self.dictionary["key-1-alt-prime"] == 42)
            #expect(self.dictionary["key-1-alt-prime-prime"] == 42)
        }

        @Test
        mutating func withUnknownKey() {
            let added = self.dictionary.addAlias("new", for: "missing")
            #expect(!added)
        }
    }

    @Suite
    struct RemoveKey {
        var dictionary: AliasDictionary<String, Int>

        init() {
            self.dictionary = ["key-1": 42]
            self.dictionary.addAlias("key-1-alt", for: "key-1")
        }

        @Test
        mutating func keyWithAlias() {
            self.dictionary.removeKey("key-1")
            #expect(self.dictionary.count == 1)  // still 1

            #expect(self.dictionary["key-1"] == nil)
            #expect(self.dictionary["key-1-alt"] == 42)
            #expect(self.dictionary._canonicalKey(forKey: "key-1-alt") == "key-1-alt")
        }

        @Test
        mutating func keyWithNoAlias() {
            self.dictionary = ["key-1": 42]
            self.dictionary.removeKey("key-1")

            #expect(self.dictionary.count == 0)
            #expect(self.dictionary.isEmpty)
            #expect(self.dictionary["key-1"] == nil)
        }

        @Test
        mutating func keyWithMultipleAliases() {
            self.dictionary.addAlias("key-2-alt", for: "key-1")

            self.dictionary.removeKey("key-1")
            #expect(self.dictionary.count == 1)  // still 1

            #expect(self.dictionary["key-1"] == nil)
            #expect(self.dictionary["key-1-alt"] == 42)
            #expect(self.dictionary["key-2-alt"] == 42)

            let promotedFor1 = self.dictionary._canonicalKey(forKey: "key-1-alt")
            let promotedFor2 = self.dictionary._canonicalKey(forKey: "key-2-alt")
            #expect(promotedFor1 == promotedFor2)
            #expect(promotedFor1 == "key-1-alt" || promotedFor1 == "key-2-alt")
        }

        @Test
        mutating func alias() {
            self.dictionary.removeKey("key-1-alt")
            #expect(self.dictionary.count == 1)  // still 1
            #expect(self.dictionary["key-1"] == 42)
            #expect(self.dictionary["key-1-alt"] == nil)
        }

        @Test
        mutating func oneOfSeveralAliases() {
            self.dictionary.addAlias("key-1-alt-2", for: "key-1")

            self.dictionary.removeKey("key-1-alt")
            #expect(self.dictionary.count == 1)
            #expect(self.dictionary["key-1"] == 42)
            #expect(self.dictionary["key-1-alt"] == nil)
            #expect(self.dictionary["key-1-alt-2"] == 42)
            #expect(self.dictionary._canonicalKey(forKey: "key-1-alt-2") == "key-1")
        }

        @Test
        mutating func unknownKey() {
            let removed = self.dictionary.removeKey("unknown")
            #expect(!removed)
        }

        @Test
        mutating func all() {
            self.dictionary.removeKey("key-1")
            #expect(self.dictionary.count == 1)  // still 1
            self.dictionary.removeKey("key-1-alt")
            #expect(self.dictionary.count == 0)
            #expect(self.dictionary["key-1"] == nil)
            #expect(self.dictionary["key-1-alt"] == nil)
        }
    }

    @Suite
    struct RemoveValue {
        var dictionary: AliasDictionary<String, Int>

        init() {
            self.dictionary = ["key-1": 42]
            self.dictionary.addAlias("key-1a", for: "key-1")
        }

        @Test
        mutating func key() {
            let value = self.dictionary.removeValue(forKey: "key-1")
            #expect(value == 42)
            #expect(self.dictionary.isEmpty)
            #expect(self.dictionary._canonicalKey(forKey: "key-1") == nil)
            #expect(self.dictionary._canonicalKey(forKey: "key-1a") == nil)
        }

        @Test
        mutating func alias() {
            let value = self.dictionary.removeValue(forKey: "key-1a")
            #expect(value == 42)
            #expect(self.dictionary.isEmpty)
            #expect(self.dictionary._canonicalKey(forKey: "key-1") == nil)
            #expect(self.dictionary._canonicalKey(forKey: "key-1a") == nil)
        }

        @Test
        mutating func keyWithMultipleAliases() {
            self.dictionary.addAlias("key-1b", for: "key-1")

            let value = self.dictionary.removeValue(forKey: "key-1")
            #expect(value == 42)
            #expect(self.dictionary.isEmpty)
            #expect(self.dictionary["key-1"] == nil)
            #expect(self.dictionary["key-1a"] == nil)
            #expect(self.dictionary["key-1b"] == nil)
            #expect(self.dictionary._canonicalKey(forKey: "key-1a") == nil)
            #expect(self.dictionary._canonicalKey(forKey: "key-1b") == nil)
        }
    }
}
