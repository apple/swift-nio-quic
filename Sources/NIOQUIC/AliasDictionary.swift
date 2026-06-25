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

/// A dictionary where multiple keys can resolve to the same value.
///
/// Each value is stored under a canonical key. Additional keys may be registered as aliases that
/// resolve to the same value via subscript lookup. The canonical key is the one used to insert
/// the value via ``insertValue(_:forKey:)``; if that key is later removed via ``removeKey(_:)``
/// while aliases exist, one of the aliases is promoted to become the new canonical key and the
/// value is preserved. From a caller's perspective, canonical keys and aliases lookup
/// identically.
struct AliasDictionary<Key: Hashable, Value> {
    /// Values keyed by their canonical key.
    private var storage: [Key: Value]
    /// The canonical key each alias points to.
    private var keyForAlias: [Key: Key]
    /// The aliases registered for each canonical key.
    private var aliasesForKey: [Key: [Key]]

    init() {
        self.storage = [:]
        self.keyForAlias = [:]
        self.aliasesForKey = [:]
    }

    /// The number of values stored in the dictionary.
    ///
    /// This counts values, not keys: a value with `n` aliases contributes `1`, not `n + 1`.
    ///
    /// - Complexity: O(1).
    var count: Int {
        self.storage.count
    }

    /// Whether the dictionary contains no values.
    ///
    /// - Complexity: O(1).
    var isEmpty: Bool {
        self.storage.isEmpty
    }

    /// Looks up the value for `key`, or inserts/removes one.
    ///
    /// Reading: returns the value associated with `key` (which may be the canonical key or an
    /// alias), or `nil` if `key` is unknown.
    ///
    /// Writing a non-`nil` value: equivalent to ``updateValue(_:forKey:)`` — inserts a new value
    /// if `key` is unknown, or replaces the existing value (looked up via canonical key or alias).
    ///
    /// Writing `nil`: equivalent to ``removeValue(forKey:)`` — removes the value and all keys
    /// associated with it.
    ///
    /// - Complexity: O(1) for read and for non-`nil` write. O(*a*) for `nil` write, where *a*
    ///   is the number of aliases registered for the value.
    subscript(key: Key) -> Value? {
        get {
            if let value = self.storage[key] {
                return value
            } else if let canonical = self.keyForAlias[key] {
                return self.storage[canonical]
            } else {
                return nil
            }
        }
        set {
            if let newValue {
                self.updateValue(newValue, forKey: key)
            } else {
                self.removeValue(forKey: key)
            }
        }
    }

    /// All values in the dictionary, in no particular order.
    ///
    /// - Complexity: O(1) to obtain the collection. Iterating it is O(*n*) where *n* is `count`.
    var values: Dictionary<Key, Value>.Values {
        self.storage.values
    }

    /// Returns the canonical key for `key`. Test-only.
    ///
    /// If `key` is itself a canonical key, returns it. If `key` is an alias, returns the canonical
    /// key it points to. Returns `nil` if `key` is unknown.
    ///
    /// - Complexity: O(1).
    func _canonicalKey(forKey key: Key) -> Key? {
        if self.storage.keys.contains(key) {
            return key
        } else {
            return self.keyForAlias[key]
        }
    }

    /// Inserts a new value or updates an existing one.
    ///
    /// If `key` is the canonical key for an existing value, or an alias of one, the value is
    /// replaced and the previous value is returned. Otherwise `key` becomes the canonical key
    /// for a newly inserted value.
    ///
    /// - Parameters:
    ///   - value: The value to store.
    ///   - key: The canonical key, alias, or new key under which to store `value`.
    /// - Returns: The previous value, or `nil` if `key` did not already exist.
    /// - Complexity: O(1).
    @discardableResult
    mutating func updateValue(_ value: Value, forKey key: Key) -> Value? {
        if let canonical = self._canonicalKey(forKey: key) {
            return self.storage.updateValue(value, forKey: canonical)
        } else {
            self.storage[key] = value
            return nil
        }
    }

    /// Adds `alias` as another way to look up the value stored under `key`.
    ///
    /// `key` may be either the canonical key or an existing alias; in either case the new alias
    /// is registered against the canonical key.
    ///
    /// - Parameters:
    ///   - alias: The alias to register.
    ///   - key: A canonical key or existing alias whose value `alias` should also resolve to.
    /// - Returns: `true` if the alias was added; `false` if `key` is unknown (in which case the
    ///   alias is silently dropped) or the alias is already a key in the dictionary.
    /// - Complexity: O(1).
    @discardableResult
    mutating func addAlias(_ alias: Key, for key: Key) -> Bool {
        guard let canonical = self._canonicalKey(forKey: key) else { return false }
        guard self._canonicalKey(forKey: alias) == nil else { return false }

        self.keyForAlias[alias] = canonical
        self.aliasesForKey[canonical, default: []].append(alias)

        return true
    }

    /// Removes a single key from the dictionary, preserving the value where possible.
    ///
    /// Behavior depends on what `key` is:
    /// - If `key` is the canonical key and one or more aliases exist, an unspecified alias is
    ///   promoted to be the new canonical key and the value is preserved.
    /// - If `key` is the canonical key and no aliases exist, the value is removed.
    /// - If `key` is an alias, only the alias is removed; the value and other keys remain.
    ///
    /// To remove the value outright, regardless of how many keys reference it, use
    /// ``removeValue(forKey:)``.
    ///
    /// - Parameter key: The key to remove. May be the canonical key or an alias.
    /// - Returns: `true` if the key was removed; `false` if `key` was not found.
    /// - Complexity: O(*a*) where *a* is the number of aliases registered for the affected
    ///   value. O(1) if `key` is unknown.
    @discardableResult
    mutating func removeKey(_ key: Key) -> Bool {
        let removed: Bool

        if let value = self.storage.removeValue(forKey: key) {
            removed = true

            // Retiring the canonical key. Promote an alias if one exists.
            if var aliases = self.aliasesForKey.removeValue(forKey: key) {
                if let promoted = aliases.popLast() {
                    // 'key' was canonical and a replacement alias exists:
                    // - store the value under the promoted alias
                    // - drop the promoted alias's now-stale entry in the alias lookup
                    // - point the remaining aliases at the promoted key
                    // - record the remaining aliases under the promoted key
                    self.storage[promoted] = value
                    self.keyForAlias.removeValue(forKey: promoted)
                    for alias in aliases {
                        self.keyForAlias[alias] = promoted
                    }
                    self.aliasesForKey[promoted] = aliases
                }
            }
        } else if let canonical = self.keyForAlias[key] {
            assert(self.storage.keys.contains(canonical))
            // 'key' is an alias: remove just the alias.
            if let index = self.aliasesForKey[canonical]?.firstIndex(of: key) {
                // '!' okay: index isn't 'nil' so the array must exist.
                self.aliasesForKey[canonical]!.removeWithoutPreservingOrder(at: index)
                self.keyForAlias.removeValue(forKey: key)
                removed = true
            } else {
                removed = false
                assertionFailure(
                    """
                    Alias (\(key)) maps to canonical key (\(canonical)) but the canonical key has \
                    no record of the inverse mapping. This is a bug in \(#file).
                    """
                )
            }
        } else {
            // Unknown key.
            removed = false
        }

        return removed
    }

    /// Removes the value for `key`, along with all keys that reference it.
    ///
    /// Both the canonical key and all aliases for the value are removed. To remove a single key
    /// while preserving the value (with an alias promoted if needed), use ``removeKey(_:)``.
    ///
    /// - Parameter key: A key for the value to remove. May be the canonical key or an alias.
    /// - Returns: The removed value, or `nil` if `key` was not found.
    /// - Complexity: O(*a*) where *a* is the number of aliases registered for the value.
    ///   O(1) if `key` is unknown.
    @discardableResult
    mutating func removeValue(forKey key: Key) -> Value? {
        let removed: Value?

        if let value = self.storage.removeValue(forKey: key) {
            // 'key' is the canonical key.
            self.removeAliases(of: key)
            removed = value
        } else if let canonical = self.keyForAlias[key] {
            // 'key' is an alias.
            self.removeAliases(of: canonical)
            removed = self.storage.removeValue(forKey: canonical)
        } else {
            // Unknown key.
            removed = nil
        }

        return removed
    }

    private mutating func removeAliases(of key: Key) {
        guard let aliases = self.aliasesForKey.removeValue(forKey: key) else {
            return
        }

        for alias in aliases {
            self.keyForAlias.removeValue(forKey: alias)
        }
    }
}

extension AliasDictionary: ExpressibleByDictionaryLiteral {
    init(dictionaryLiteral elements: (Key, Value)...) {
        self.storage = Dictionary(uniqueKeysWithValues: elements)
        self.keyForAlias = [:]
        self.aliasesForKey = [:]
    }
}

extension Array {
    fileprivate mutating func removeWithoutPreservingOrder(at index: Int) {
        if self.isEmpty { return }
        self.swapAt(index, self.index(before: self.endIndex))
        self.removeLast()
    }
}
