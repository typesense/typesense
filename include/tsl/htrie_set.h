/**
 * MIT License
 *
 * Copyright (c) 2017 Thibaut Goetghebuer-Planchon <tessil@gmx.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#ifndef TSL_HTRIE_SET_H
#define TSL_HTRIE_SET_H

#include <cstddef>
#include <cstring>
#include <initializer_list>
#include <string>
#include <utility>

#include "htrie_hash.h"

namespace tsl {

/**
 * Implementation of a hat-trie set.
 *
 * The size of a key string is limited to std::numeric_limits<KeySizeT>::max()
 * - 1. That is 65 535 characters by default, but can be raised with the
 * KeySizeT template parameter. See max_key_size() for an easy access to this
 * limit.
 *
 * Iterators invalidation:
 *  - clear, operator=: always invalidate the iterators.
 *  - insert: always invalidate the iterators.
 *  - erase: always invalidate the iterators.
 */
template <class CharT, class Hash = tsl::ah::str_hash<CharT>,
          class KeySizeT = std::uint16_t>
class htrie_set {
 private:
  template <typename U>
  using is_iterator = tsl::detail_array_hash::is_iterator<U>;

  using ht = tsl::detail_htrie_hash::htrie_hash<CharT, void, Hash, KeySizeT>;

 public:
  using char_type = typename ht::char_type;
  using key_size_type = typename ht::key_size_type;
  using size_type = typename ht::size_type;
  using hasher = typename ht::hasher;
  using iterator = typename ht::iterator;
  using const_iterator = typename ht::const_iterator;
  using prefix_iterator = typename ht::prefix_iterator;
  using const_prefix_iterator = typename ht::const_prefix_iterator;

 public:
  explicit htrie_set(const Hash& hash = Hash())
      : m_ht(hash, ht::HASH_NODE_DEFAULT_MAX_LOAD_FACTOR,
             ht::DEFAULT_BURST_THRESHOLD) {}

  explicit htrie_set(size_type burst_threshold, const Hash& hash = Hash())
      : m_ht(hash, ht::HASH_NODE_DEFAULT_MAX_LOAD_FACTOR, burst_threshold) {}

  template <class InputIt, typename std::enable_if<
                               is_iterator<InputIt>::value>::type* = nullptr>
  htrie_set(InputIt first, InputIt last, const Hash& hash = Hash())
      : htrie_set(hash) {
    insert(first, last);
  }

#ifdef TSL_HT_HAS_STRING_VIEW
  htrie_set(std::initializer_list<std::basic_string_view<CharT>> init,
            const Hash& hash = Hash())
      : htrie_set(hash) {
    insert(init);
  }
#else
  htrie_set(std::initializer_list<const CharT*> init, const Hash& hash = Hash())
      : htrie_set(hash) {
    insert(init);
  }
#endif

#ifdef TSL_HT_HAS_STRING_VIEW
  htrie_set& operator=(
      std::initializer_list<std::basic_string_view<CharT>> ilist) {
    clear();
    insert(ilist);

    return *this;
  }
#else
  htrie_set& operator=(std::initializer_list<const CharT*> ilist) {
    clear();
    insert(ilist);

    return *this;
  }
#endif

  /*
   * Iterators
   */
  iterator begin() noexcept { return m_ht.begin(); }
  const_iterator begin() const noexcept { return m_ht.begin(); }
  const_iterator cbegin() const noexcept { return m_ht.cbegin(); }

  iterator end() noexcept { return m_ht.end(); }
  const_iterator end() const noexcept { return m_ht.end(); }
  const_iterator cend() const noexcept { return m_ht.cend(); }

  /*
   * Capacity
   */
  bool empty() const noexcept { return m_ht.empty(); }
  size_type size() const noexcept { return m_ht.size(); }
  size_type max_size() const noexcept { return m_ht.max_size(); }
  size_type max_key_size() const noexcept { return m_ht.max_key_size(); }

  /**
   * Call shrink_to_fit() on each hash node of the hat-trie to reduce its size.
   */
  void shrink_to_fit() { m_ht.shrink_to_fit(); }

  /*
   * Modifiers
   */
  void clear() noexcept { m_ht.clear(); }

  std::pair<iterator, bool> insert_ks(const CharT* key, size_type key_size) {
    return m_ht.insert(key, key_size);
  }
#ifdef TSL_HT_HAS_STRING_VIEW
  std::pair<iterator, bool> insert(const std::basic_string_view<CharT>& key) {
    return m_ht.insert(key.data(), key.size());
  }
#else
  std::pair<iterator, bool> insert(const CharT* key) {
    return m_ht.insert(key, std::strlen(key));
  }

  std::pair<iterator, bool> insert(const std::basic_string<CharT>& key) {
    return m_ht.insert(key.data(), key.size());
  }
#endif

  template <class InputIt, typename std::enable_if<
                               is_iterator<InputIt>::value>::type* = nullptr>
  void insert(InputIt first, InputIt last) {
    for (auto it = first; it != last; ++it) {
      insert(*it);
    }
  }

#ifdef TSL_HT_HAS_STRING_VIEW
  void insert(std::initializer_list<std::basic_string_view<CharT>> ilist) {
    insert(ilist.begin(), ilist.end());
  }
#else
  void insert(std::initializer_list<const CharT*> ilist) {
    insert(ilist.begin(), ilist.end());
  }
#endif

  std::pair<iterator, bool> emplace_ks(const CharT* key, size_type key_size) {
    return m_ht.insert(key, key_size);
  }
#ifdef TSL_HT_HAS_STRING_VIEW
  std::pair<iterator, bool> emplace(const std::basic_string_view<CharT>& key) {
    return m_ht.insert(key.data(), key.size());
  }
#else
  std::pair<iterator, bool> emplace(const CharT* key) {
    return m_ht.insert(key, std::strlen(key));
  }

  std::pair<iterator, bool> emplace(const std::basic_string<CharT>& key) {
    return m_ht.insert(key.data(), key.size());
  }
#endif

  iterator erase(const_iterator pos) { return m_ht.erase(pos); }
  iterator erase(const_iterator first, const_iterator last) {
    return m_ht.erase(first, last);
  }

  size_type erase_ks(const CharT* key, size_type key_size) {
    return m_ht.erase(key, key_size);
  }
#ifdef TSL_HT_HAS_STRING_VIEW
  size_type erase(const std::basic_string_view<CharT>& key) {
    return m_ht.erase(key.data(), key.size());
  }
#else
  size_type erase(const CharT* key) {
    return m_ht.erase(key, std::strlen(key));
  }

  size_type erase(const std::basic_string<CharT>& key) {
    return m_ht.erase(key.data(), key.size());
  }
#endif

  /**
   * Erase all the elements which have 'prefix' as prefix. Return the number of
   * erase elements.
   */
  size_type erase_prefix_ks(const CharT* prefix, size_type prefix_size) {
    return m_ht.erase_prefix(prefix, prefix_size);
  }
#ifdef TSL_HT_HAS_STRING_VIEW
  /**
   * @copydoc erase_prefix_ks(const CharT* prefix, size_type prefix_size)
   */
  size_type erase_prefix(const std::basic_string_view<CharT>& prefix) {
    return m_ht.erase_prefix(prefix.data(), prefix.size());
  }
#else
  /**
   * @copydoc erase_prefix_ks(const CharT* prefix, size_type prefix_size)
   */
  size_type erase_prefix(const CharT* prefix) {
    return m_ht.erase_prefix(prefix, std::strlen(prefix));
  }

  /**
   * @copydoc erase_prefix_ks(const CharT* prefix, size_type prefix_size)
   */
  size_type erase_prefix(const std::basic_string<CharT>& prefix) {
    return m_ht.erase_prefix(prefix.data(), prefix.size());
  }
#endif

  void swap(htrie_set& other) { other.m_ht.swap(m_ht); }

  /*
   * Lookup
   */
  size_type count_ks(const CharT* key, size_type key_size) const {
    return m_ht.count(key, key_size);
  }
#ifdef TSL_HT_HAS_STRING_VIEW
  size_type count(const std::basic_string_view<CharT>& key) const {
    return m_ht.count(key.data(), key.size());
  }
#else
  size_type count(const CharT* key) const {
    return m_ht.count(key, std::strlen(key));
  }
  size_type count(const std::basic_string<CharT>& key) const {
    return m_ht.count(key.data(), key.size());
  }
#endif

  iterator find_ks(const CharT* key, size_type key_size) {
    return m_ht.find(key, key_size);
  }

  const_iterator find_ks(const CharT* key, size_type key_size) const {
    return m_ht.find(key, key_size);
  }
#ifdef TSL_HT_HAS_STRING_VIEW
  iterator find(const std::basic_string_view<CharT>& key) {
    return m_ht.find(key.data(), key.size());
  }

  const_iterator find(const std::basic_string_view<CharT>& key) const {
    return m_ht.find(key.data(), key.size());
  }
#else
  iterator find(const CharT* key) { return m_ht.find(key, std::strlen(key)); }

  const_iterator find(const CharT* key) const {
    return m_ht.find(key, std::strlen(key));
  }

  iterator find(const std::basic_string<CharT>& key) {
    return m_ht.find(key.data(), key.size());
  }

  const_iterator find(const std::basic_string<CharT>& key) const {
    return m_ht.find(key.data(), key.size());
  }
#endif

  std::pair<iterator, iterator> equal_range_ks(const CharT* key,
                                               size_type key_size) {
    return m_ht.equal_range(key, key_size);
  }

  std::pair<const_iterator, const_iterator> equal_range_ks(
      const CharT* key, size_type key_size) const {
    return m_ht.equal_range(key, key_size);
  }
#ifdef TSL_HT_HAS_STRING_VIEW
  std::pair<iterator, iterator> equal_range(
      const std::basic_string_view<CharT>& key) {
    return m_ht.equal_range(key.data(), key.size());
  }

  std::pair<const_iterator, const_iterator> equal_range(
      const std::basic_string_view<CharT>& key) const {
    return m_ht.equal_range(key.data(), key.size());
  }
#else
  std::pair<iterator, iterator> equal_range(const CharT* key) {
    return m_ht.equal_range(key, std::strlen(key));
  }

  std::pair<const_iterator, const_iterator> equal_range(
      const CharT* key) const {
    return m_ht.equal_range(key, std::strlen(key));
  }

  std::pair<iterator, iterator> equal_range(
      const std::basic_string<CharT>& key) {
    return m_ht.equal_range(key.data(), key.size());
  }

  std::pair<const_iterator, const_iterator> equal_range(
      const std::basic_string<CharT>& key) const {
    return m_ht.equal_range(key.data(), key.size());
  }
#endif

  /**
   * Return a range containing all the elements which have 'prefix' as prefix.
   * The range is defined by a pair of iterator, the first being the begin
   * iterator and the second being the end iterator.
   */
  std::pair<prefix_iterator, prefix_iterator> equal_prefix_range_ks(
      const CharT* prefix, size_type prefix_size) {
    return m_ht.equal_prefix_range(prefix, prefix_size);
  }

  /**
   * @copydoc equal_prefix_range_ks(const CharT* prefix, size_type prefix_size)
   */
  std::pair<const_prefix_iterator, const_prefix_iterator> equal_prefix_range_ks(
      const CharT* prefix, size_type prefix_size) const {
    return m_ht.equal_prefix_range(prefix, prefix_size);
  }
#ifdef TSL_HT_HAS_STRING_VIEW
  /**
   * @copydoc equal_prefix_range_ks(const CharT* prefix, size_type prefix_size)
   */
  std::pair<prefix_iterator, prefix_iterator> equal_prefix_range(
      const std::basic_string_view<CharT>& prefix) {
    return m_ht.equal_prefix_range(prefix.data(), prefix.size());
  }

  /**
   * @copydoc equal_prefix_range_ks(const CharT* prefix, size_type prefix_size)
   */
  std::pair<const_prefix_iterator, const_prefix_iterator> equal_prefix_range(
      const std::basic_string_view<CharT>& prefix) const {
    return m_ht.equal_prefix_range(prefix.data(), prefix.size());
  }
#else
  /**
   * @copydoc equal_prefix_range_ks(const CharT* prefix, size_type prefix_size)
   */
  std::pair<prefix_iterator, prefix_iterator> equal_prefix_range(
      const CharT* prefix) {
    return m_ht.equal_prefix_range(prefix, std::strlen(prefix));
  }

  /**
   * @copydoc equal_prefix_range_ks(const CharT* prefix, size_type prefix_size)
   */
  std::pair<const_prefix_iterator, const_prefix_iterator> equal_prefix_range(
      const CharT* prefix) const {
    return m_ht.equal_prefix_range(prefix, std::strlen(prefix));
  }

  /**
   * @copydoc equal_prefix_range_ks(const CharT* prefix, size_type prefix_size)
   */
  std::pair<prefix_iterator, prefix_iterator> equal_prefix_range(
      const std::basic_string<CharT>& prefix) {
    return m_ht.equal_prefix_range(prefix.data(), prefix.size());
  }

  /**
   * @copydoc equal_prefix_range_ks(const CharT* prefix, size_type prefix_size)
   */
  std::pair<const_prefix_iterator, const_prefix_iterator> equal_prefix_range(
      const std::basic_string<CharT>& prefix) const {
    return m_ht.equal_prefix_range(prefix.data(), prefix.size());
  }
#endif

  /**
   * Return the element in the trie which is the longest prefix of `key`. If no
   * element in the trie is a prefix of `key`, the end iterator is returned.
   *
   * Example:
   *
   *     tsl::htrie_set<char> set = {"/foo", "/foo/bar"};
   *
   *     set.longest_prefix("/foo"); // returns "/foo"
   *     set.longest_prefix("/foo/baz"); // returns "/foo"
   *     set.longest_prefix("/foo/bar/baz"); // returns "/foo/bar"
   *     set.longest_prefix("/foo/bar/"); // returns "/foo/bar"
   *     set.longest_prefix("/bar"); // returns end()
   *     set.longest_prefix(""); // returns end()
   */
  iterator longest_prefix_ks(const CharT* key, size_type key_size) {
    return m_ht.longest_prefix(key, key_size);
  }

  /**
   * @copydoc longest_prefix_ks(const CharT* key, size_type key_size)
   */
  const_iterator longest_prefix_ks(const CharT* key, size_type key_size) const {
    return m_ht.longest_prefix(key, key_size);
  }
#ifdef TSL_HT_HAS_STRING_VIEW
  /**
   * @copydoc longest_prefix_ks(const CharT* key, size_type key_size)
   */
  iterator longest_prefix(const std::basic_string_view<CharT>& key) {
    return m_ht.longest_prefix(key.data(), key.size());
  }

  /**
   * @copydoc longest_prefix_ks(const CharT* key, size_type key_size)
   */
  const_iterator longest_prefix(
      const std::basic_string_view<CharT>& key) const {
    return m_ht.longest_prefix(key.data(), key.size());
  }
#else
  /**
   * @copydoc longest_prefix_ks(const CharT* key, size_type key_size)
   */
  iterator longest_prefix(const CharT* key) {
    return m_ht.longest_prefix(key, std::strlen(key));
  }

  /**
   * @copydoc longest_prefix_ks(const CharT* key, size_type key_size)
   */
  const_iterator longest_prefix(const CharT* key) const {
    return m_ht.longest_prefix(key, std::strlen(key));
  }

  /**
   * @copydoc longest_prefix_ks(const CharT* key, size_type key_size)
   */
  iterator longest_prefix(const std::basic_string<CharT>& key) {
    return m_ht.longest_prefix(key.data(), key.size());
  }

  /**
   * @copydoc longest_prefix_ks(const CharT* key, size_type key_size)
   */
  const_iterator longest_prefix(const std::basic_string<CharT>& key) const {
    return m_ht.longest_prefix(key.data(), key.size());
  }
#endif

  /*
   *  Hash policy
   */
  float max_load_factor() const { return m_ht.max_load_factor(); }
  void max_load_factor(float ml) { m_ht.max_load_factor(ml); }

  /*
   * Burst policy
   */
  size_type burst_threshold() const { return m_ht.burst_threshold(); }
  void burst_threshold(size_type threshold) { m_ht.burst_threshold(threshold); }

  /*
   * Observers
   */
  hasher hash_function() const { return m_ht.hash_function(); }

  /*
   * Other
   */

  /**
   * Serialize the set through the `serializer` parameter.
   *
   * The `serializer` parameter must be a function object that supports the
   * following calls:
   *  - `void operator()(const U& value);` where the types `std::uint64_t` and
   * `float` must be supported for U.
   *  - `void operator()(const CharT* value, std::size_t value_size);`
   *
   * The implementation leaves binary compatibility (endianness, IEEE 754 for
   * floats, ...) of the types it serializes in the hands of the `Serializer`
   * function object if compatibility is required.
   */
  template <class Serializer>
  void serialize(Serializer& serializer) const {
    m_ht.serialize(serializer);
  }

  /**
   * Deserialize a previously serialized set through the `deserializer`
   * parameter.
   *
   * The `deserializer` parameter must be a function object that supports the
   * following calls:
   *  - `template<typename U> U operator()();` where the types `std::uint64_t`
   * and `float` must be supported for U.
   *  - `void operator()(CharT* value_out, std::size_t value_size);`
   *
   * If the deserialized hash set part of the hat-trie is hash compatible with
   * the serialized set, the deserialization process can be sped up by setting
   * `hash_compatible` to true. To be hash compatible, the Hash (take care of
   * the 32-bits vs 64 bits), and KeySizeT must behave the same than the ones
   * used in the serialized set. Otherwise the behaviour is undefined with
   * `hash_compatible` sets to true.
   *
   * The behaviour is undefined if the type `CharT` of the `htrie_set` is not
   * the same as the type used during serialization.
   *
   * The implementation leaves binary compatibility (endianness, IEEE 754 for
   * floats, size of int, ...) of the types it deserializes in the hands of the
   * `Deserializer` function object if compatibility is required.
   */
  template <class Deserializer>
  static htrie_set deserialize(Deserializer& deserializer,
                               bool hash_compatible = false) {
    htrie_set set;
    set.m_ht.deserialize(deserializer, hash_compatible);

    return set;
  }

  friend bool operator==(const htrie_set& lhs, const htrie_set& rhs) {
    if (lhs.size() != rhs.size()) {
      return false;
    }

    std::string key_buffer;
    for (auto it = lhs.cbegin(); it != lhs.cend(); ++it) {
      it.key(key_buffer);

      const auto it_element_rhs = rhs.find(key_buffer);
      if (it_element_rhs == rhs.cend()) {
        return false;
      }
    }

    return true;
  }

  friend bool operator!=(const htrie_set& lhs, const htrie_set& rhs) {
    return !operator==(lhs, rhs);
  }

  friend void swap(htrie_set& lhs, htrie_set& rhs) { lhs.swap(rhs); }

 private:
  ht m_ht;
};

}  // end namespace tsl

#endif
