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
#ifndef TSL_ARRAY_SET_H
#define TSL_ARRAY_SET_H

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <iterator>
#include <string>
#include <type_traits>
#include <utility>

#include "array_hash.h"

namespace tsl {

/**
 * Implementation of a cache-conscious string hash set.
 *
 * The set stores the strings as `const CharT*`. If `StoreNullTerminator` is
 * true, the strings are stored with the a null-terminator (the `key()` method
 * of the iterators will return a pointer to this null-terminated string).
 * Otherwise the null character is not stored (which allow an economy of 1 byte
 * per string).
 *
 * The size of a key string is limited to `std::numeric_limits<KeySizeT>::max()
 * - 1`. That is 65 535 characters by default, but can be raised with the
 * `KeySizeT` template parameter. See `max_key_size()` for an easy access to
 * this limit.
 *
 * The number of elements in the set is limited to
 * `std::numeric_limits<IndexSizeT>::max()`. That is 4 294 967 296 elements, but
 * can be raised with the `IndexSizeT` template parameter. See `max_size()` for
 * an easy access to this limit.
 *
 * Iterators invalidation:
 *  - clear, operator=: always invalidate the iterators.
 *  - insert, emplace, operator[]: always invalidate the iterators.
 *  - erase: always invalidate the iterators.
 *  - shrink_to_fit: always invalidate the iterators.
 */
template <class CharT, class Hash = tsl::ah::str_hash<CharT>,
          class KeyEqual = tsl::ah::str_equal<CharT>,
          bool StoreNullTerminator = true, class KeySizeT = std::uint16_t,
          class IndexSizeT = std::uint32_t,
          class GrowthPolicy = tsl::ah::power_of_two_growth_policy<2>>
class array_set {
 private:
  template <typename U>
  using is_iterator = tsl::detail_array_hash::is_iterator<U>;

  using ht = tsl::detail_array_hash::array_hash<CharT, void, Hash, KeyEqual,
                                                StoreNullTerminator, KeySizeT,
                                                IndexSizeT, GrowthPolicy>;

 public:
  using char_type = typename ht::char_type;
  using key_size_type = typename ht::key_size_type;
  using index_size_type = typename ht::index_size_type;
  using size_type = typename ht::size_type;
  using hasher = typename ht::hasher;
  using key_equal = typename ht::key_equal;
  using iterator = typename ht::iterator;
  using const_iterator = typename ht::const_iterator;

  array_set() : array_set(ht::DEFAULT_INIT_BUCKET_COUNT) {}

  explicit array_set(size_type bucket_count, const Hash& hash = Hash())
      : m_ht(bucket_count, hash, ht::DEFAULT_MAX_LOAD_FACTOR) {}

  template <class InputIt, typename std::enable_if<
                               is_iterator<InputIt>::value>::type* = nullptr>
  array_set(InputIt first, InputIt last,
            size_type bucket_count = ht::DEFAULT_INIT_BUCKET_COUNT,
            const Hash& hash = Hash())
      : array_set(bucket_count, hash) {
    insert(first, last);
  }

#ifdef TSL_AH_HAS_STRING_VIEW
  array_set(std::initializer_list<std::basic_string_view<CharT>> init,
            size_type bucket_count = ht::DEFAULT_INIT_BUCKET_COUNT,
            const Hash& hash = Hash())
      : array_set(bucket_count, hash) {
    insert(init);
  }
#else
  array_set(std::initializer_list<const CharT*> init,
            size_type bucket_count = ht::DEFAULT_INIT_BUCKET_COUNT,
            const Hash& hash = Hash())
      : array_set(bucket_count, hash) {
    insert(init);
  }
#endif

#ifdef TSL_AH_HAS_STRING_VIEW
  array_set& operator=(
      std::initializer_list<std::basic_string_view<CharT>> ilist) {
    clear();

    reserve(ilist.size());
    insert(ilist);

    return *this;
  }
#else
  array_set& operator=(std::initializer_list<const CharT*> ilist) {
    clear();

    reserve(ilist.size());
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
  void shrink_to_fit() { m_ht.shrink_to_fit(); }

  /*
   * Modifiers
   */
  void clear() noexcept { m_ht.clear(); }

#ifdef TSL_AH_HAS_STRING_VIEW
  std::pair<iterator, bool> insert(const std::basic_string_view<CharT>& key) {
    return m_ht.emplace(key.data(), key.size());
  }
#else
  std::pair<iterator, bool> insert(const CharT* key) {
    return m_ht.emplace(key, std::char_traits<CharT>::length(key));
  }

  std::pair<iterator, bool> insert(const std::basic_string<CharT>& key) {
    return m_ht.emplace(key.data(), key.size());
  }
#endif
  std::pair<iterator, bool> insert_ks(const CharT* key, size_type key_size) {
    return m_ht.emplace(key, key_size);
  }

  template <class InputIt, typename std::enable_if<
                               is_iterator<InputIt>::value>::type* = nullptr>
  void insert(InputIt first, InputIt last) {
    if (std::is_base_of<
            std::forward_iterator_tag,
            typename std::iterator_traits<InputIt>::iterator_category>::value) {
      const auto nb_elements_insert = std::distance(first, last);
      const std::size_t nb_free_buckets =
          std::size_t(float(bucket_count()) * max_load_factor()) - size();

      if (nb_elements_insert > 0 &&
          nb_free_buckets < std::size_t(nb_elements_insert)) {
        reserve(size() + std::size_t(nb_elements_insert));
      }
    }

    for (auto it = first; it != last; ++it) {
      insert(*it);
    }
  }

#ifdef TSL_AH_HAS_STRING_VIEW
  void insert(std::initializer_list<std::basic_string_view<CharT>> ilist) {
    insert(ilist.begin(), ilist.end());
  }
#else
  void insert(std::initializer_list<const CharT*> ilist) {
    insert(ilist.begin(), ilist.end());
  }
#endif

#ifdef TSL_AH_HAS_STRING_VIEW
  /**
   * @copydoc emplace_ks(const CharT* key, size_type key_size)
   */
  std::pair<iterator, bool> emplace(const std::basic_string_view<CharT>& key) {
    return m_ht.emplace(key.data(), key.size());
  }
#else
  /**
   * @copydoc emplace_ks(const CharT* key, size_type key_size)
   */
  std::pair<iterator, bool> emplace(const CharT* key) {
    return m_ht.emplace(key, std::char_traits<CharT>::length(key));
  }

  /**
   * @copydoc emplace_ks(const CharT* key, size_type key_size)
   */
  std::pair<iterator, bool> emplace(const std::basic_string<CharT>& key) {
    return m_ht.emplace(key.data(), key.size());
  }
#endif
  /**
   * No difference compared to the insert method. Mainly here for coherence with
   * array_map.
   */
  std::pair<iterator, bool> emplace_ks(const CharT* key, size_type key_size) {
    return m_ht.emplace(key, key_size);
  }

  iterator erase(const_iterator pos) { return m_ht.erase(pos); }
  iterator erase(const_iterator first, const_iterator last) {
    return m_ht.erase(first, last);
  }

#ifdef TSL_AH_HAS_STRING_VIEW
  size_type erase(const std::basic_string_view<CharT>& key) {
    return m_ht.erase(key.data(), key.size());
  }
#else
  size_type erase(const CharT* key) {
    return m_ht.erase(key, std::char_traits<CharT>::length(key));
  }

  size_type erase(const std::basic_string<CharT>& key) {
    return m_ht.erase(key.data(), key.size());
  }
#endif
  size_type erase_ks(const CharT* key, size_type key_size) {
    return m_ht.erase(key, key_size);
  }

#ifdef TSL_AH_HAS_STRING_VIEW
  /**
   * @copydoc erase_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  size_type erase(const std::basic_string_view<CharT>& key,
                  std::size_t precalculated_hash) {
    return m_ht.erase(key.data(), key.size(), precalculated_hash);
  }
#else
  /**
   * @copydoc erase_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  size_type erase(const CharT* key, std::size_t precalculated_hash) {
    return m_ht.erase(key, std::char_traits<CharT>::length(key),
                      precalculated_hash);
  }

  /**
   * @copydoc erase_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  size_type erase(const std::basic_string<CharT>& key,
                  std::size_t precalculated_hash) {
    return m_ht.erase(key.data(), key.size(), precalculated_hash);
  }
#endif
  /**
   * Use the hash value 'precalculated_hash' instead of hashing the key. The
   * hash value should be the same as hash_function()(key). Useful to speed-up
   * the lookup to the value if you already have the hash.
   */
  size_type erase_ks(const CharT* key, size_type key_size,
                     std::size_t precalculated_hash) {
    return m_ht.erase(key, key_size, precalculated_hash);
  }

  void swap(array_set& other) { other.m_ht.swap(m_ht); }

  /*
   * Lookup
   */
#ifdef TSL_AH_HAS_STRING_VIEW
  size_type count(const std::basic_string_view<CharT>& key) const {
    return m_ht.count(key.data(), key.size());
  }
#else
  size_type count(const CharT* key) const {
    return m_ht.count(key, std::char_traits<CharT>::length(key));
  }
  size_type count(const std::basic_string<CharT>& key) const {
    return m_ht.count(key.data(), key.size());
  }
#endif
  size_type count_ks(const CharT* key, size_type key_size) const {
    return m_ht.count(key, key_size);
  }

#ifdef TSL_AH_HAS_STRING_VIEW
  /**
   * @copydoc count_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash) const
   */
  size_type count(const std::basic_string_view<CharT>& key,
                  std::size_t precalculated_hash) const {
    return m_ht.count(key.data(), key.size(), precalculated_hash);
  }
#else
  /**
   * @copydoc count_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash) const
   */
  size_type count(const CharT* key, std::size_t precalculated_hash) const {
    return m_ht.count(key, std::char_traits<CharT>::length(key),
                      precalculated_hash);
  }

  /**
   * @copydoc count_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash) const
   */
  size_type count(const std::basic_string<CharT>& key,
                  std::size_t precalculated_hash) const {
    return m_ht.count(key.data(), key.size(), precalculated_hash);
  }
#endif
  /**
   * Use the hash value 'precalculated_hash' instead of hashing the key. The
   * hash value should be the same as hash_function()(key). Useful to speed-up
   * the lookup to the value if you already have the hash.
   */
  size_type count_ks(const CharT* key, size_type key_size,
                     std::size_t precalculated_hash) const {
    return m_ht.count(key, key_size, precalculated_hash);
  }

#ifdef TSL_AH_HAS_STRING_VIEW
  iterator find(const std::basic_string_view<CharT>& key) {
    return m_ht.find(key.data(), key.size());
  }

  const_iterator find(const std::basic_string_view<CharT>& key) const {
    return m_ht.find(key.data(), key.size());
  }
#else
  iterator find(const CharT* key) {
    return m_ht.find(key, std::char_traits<CharT>::length(key));
  }

  const_iterator find(const CharT* key) const {
    return m_ht.find(key, std::char_traits<CharT>::length(key));
  }

  iterator find(const std::basic_string<CharT>& key) {
    return m_ht.find(key.data(), key.size());
  }

  const_iterator find(const std::basic_string<CharT>& key) const {
    return m_ht.find(key.data(), key.size());
  }
#endif
  iterator find_ks(const CharT* key, size_type key_size) {
    return m_ht.find(key, key_size);
  }

  const_iterator find_ks(const CharT* key, size_type key_size) const {
    return m_ht.find(key, key_size);
  }

#ifdef TSL_AH_HAS_STRING_VIEW
  /**
   * @copydoc find_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  iterator find(const std::basic_string_view<CharT>& key,
                std::size_t precalculated_hash) {
    return m_ht.find(key.data(), key.size(), precalculated_hash);
  }

  /**
   * @copydoc find_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  const_iterator find(const std::basic_string_view<CharT>& key,
                      std::size_t precalculated_hash) const {
    return m_ht.find(key.data(), key.size(), precalculated_hash);
  }
#else
  /**
   * @copydoc find_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  iterator find(const CharT* key, std::size_t precalculated_hash) {
    return m_ht.find(key, std::char_traits<CharT>::length(key),
                     precalculated_hash);
  }

  /**
   * @copydoc find_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  const_iterator find(const CharT* key, std::size_t precalculated_hash) const {
    return m_ht.find(key, std::char_traits<CharT>::length(key),
                     precalculated_hash);
  }

  /**
   * @copydoc find_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  iterator find(const std::basic_string<CharT>& key,
                std::size_t precalculated_hash) {
    return m_ht.find(key.data(), key.size(), precalculated_hash);
  }

  /**
   * @copydoc find_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  const_iterator find(const std::basic_string<CharT>& key,
                      std::size_t precalculated_hash) const {
    return m_ht.find(key.data(), key.size(), precalculated_hash);
  }
#endif
  /**
   * Use the hash value 'precalculated_hash' instead of hashing the key. The
   * hash value should be the same as hash_function()(key). Useful to speed-up
   * the lookup to the value if you already have the hash.
   */
  iterator find_ks(const CharT* key, size_type key_size,
                   std::size_t precalculated_hash) {
    return m_ht.find(key, key_size, precalculated_hash);
  }

  /**
   * @copydoc find_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  const_iterator find_ks(const CharT* key, size_type key_size,
                         std::size_t precalculated_hash) const {
    return m_ht.find(key, key_size, precalculated_hash);
  }

#ifdef TSL_AH_HAS_STRING_VIEW
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
    return m_ht.equal_range(key, std::char_traits<CharT>::length(key));
  }

  std::pair<const_iterator, const_iterator> equal_range(
      const CharT* key) const {
    return m_ht.equal_range(key, std::char_traits<CharT>::length(key));
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
  std::pair<iterator, iterator> equal_range_ks(const CharT* key,
                                               size_type key_size) {
    return m_ht.equal_range(key, key_size);
  }

  std::pair<const_iterator, const_iterator> equal_range_ks(
      const CharT* key, size_type key_size) const {
    return m_ht.equal_range(key, key_size);
  }

#ifdef TSL_AH_HAS_STRING_VIEW
  /**
   * @copydoc equal_range_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  std::pair<iterator, iterator> equal_range(
      const std::basic_string_view<CharT>& key,
      std::size_t precalculated_hash) {
    return m_ht.equal_range(key.data(), key.size(), precalculated_hash);
  }

  /**
   * @copydoc equal_range_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  std::pair<const_iterator, const_iterator> equal_range(
      const std::basic_string_view<CharT>& key,
      std::size_t precalculated_hash) const {
    return m_ht.equal_range(key.data(), key.size(), precalculated_hash);
  }
#else
  /**
   * @copydoc equal_range_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  std::pair<iterator, iterator> equal_range(const CharT* key,
                                            std::size_t precalculated_hash) {
    return m_ht.equal_range(key, std::char_traits<CharT>::length(key),
                            precalculated_hash);
  }

  /**
   * @copydoc equal_range_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  std::pair<const_iterator, const_iterator> equal_range(
      const CharT* key, std::size_t precalculated_hash) const {
    return m_ht.equal_range(key, std::char_traits<CharT>::length(key),
                            precalculated_hash);
  }

  /**
   * @copydoc equal_range_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  std::pair<iterator, iterator> equal_range(const std::basic_string<CharT>& key,
                                            std::size_t precalculated_hash) {
    return m_ht.equal_range(key.data(), key.size(), precalculated_hash);
  }

  /**
   * @copydoc equal_range_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  std::pair<const_iterator, const_iterator> equal_range(
      const std::basic_string<CharT>& key,
      std::size_t precalculated_hash) const {
    return m_ht.equal_range(key.data(), key.size(), precalculated_hash);
  }
#endif
  /**
   * Use the hash value 'precalculated_hash' instead of hashing the key. The
   * hash value should be the same as hash_function()(key). Useful to speed-up
   * the lookup to the value if you already have the hash.
   */
  std::pair<iterator, iterator> equal_range_ks(const CharT* key,
                                               size_type key_size,
                                               std::size_t precalculated_hash) {
    return m_ht.equal_range(key, key_size, precalculated_hash);
  }

  /**
   * @copydoc equal_range_ks(const CharT* key, size_type key_size, std::size_t
   * precalculated_hash)
   */
  std::pair<const_iterator, const_iterator> equal_range_ks(
      const CharT* key, size_type key_size,
      std::size_t precalculated_hash) const {
    return m_ht.equal_range(key, key_size, precalculated_hash);
  }

  /*
   * Bucket interface
   */
  size_type bucket_count() const { return m_ht.bucket_count(); }
  size_type max_bucket_count() const { return m_ht.max_bucket_count(); }

  /*
   *  Hash policy
   */
  float load_factor() const { return m_ht.load_factor(); }
  float max_load_factor() const { return m_ht.max_load_factor(); }
  void max_load_factor(float ml) { m_ht.max_load_factor(ml); }

  void rehash(size_type count) { m_ht.rehash(count); }
  void reserve(size_type count) { m_ht.reserve(count); }

  /*
   * Observers
   */
  hasher hash_function() const { return m_ht.hash_function(); }
  key_equal key_eq() const { return m_ht.key_eq(); }

  /*
   * Other
   */
  /**
   * Return the `const_iterator it` as an `iterator`.
   */
  iterator mutable_iterator(const_iterator it) noexcept {
    return m_ht.mutable_iterator(it);
  }

  /**
   * Serialize the set through the `serializer` parameter.
   *
   * The `serializer` parameter must be a function object that supports the
   * following calls:
   *  - `template<typename U> void operator()(const U& value);` where the types
   * `std::uint64_t` and `float` must be supported for U.
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
   * If the deserialized hash set type is hash compatible with the serialized
   * set, the deserialization process can be sped up by setting
   * `hash_compatible` to true. To be hash compatible, the Hash (take care of
   * the 32-bits vs 64 bits), KeyEqual, GrowthPolicy, StoreNullTerminator,
   * KeySizeT and IndexSizeT must behave the same than the ones used on the
   * serialized set. Otherwise the behaviour is undefined with `hash_compatible`
   * sets to true.
   *
   * The behaviour is undefined if the type `CharT` of the `array_set` is not
   * the same as the type used during serialization.
   *
   * The implementation leaves binary compatibility (endianness, IEEE 754 for
   * floats, size of int, ...) of the types it deserializes in the hands of the
   * `Deserializer` function object if compatibility is required.
   */
  template <class Deserializer>
  static array_set deserialize(Deserializer& deserializer,
                               bool hash_compatible = false) {
    array_set set(0);
    set.m_ht.deserialize(deserializer, hash_compatible);

    return set;
  }

  friend bool operator==(const array_set& lhs, const array_set& rhs) {
    if (lhs.size() != rhs.size()) {
      return false;
    }

    for (auto it = lhs.cbegin(); it != lhs.cend(); ++it) {
      const auto it_element_rhs = rhs.find_ks(it.key(), it.key_size());
      if (it_element_rhs == rhs.cend()) {
        return false;
      }
    }

    return true;
  }

  friend bool operator!=(const array_set& lhs, const array_set& rhs) {
    return !operator==(lhs, rhs);
  }

  friend void swap(array_set& lhs, array_set& rhs) { lhs.swap(rhs); }

 public:
  static const size_type MAX_KEY_SIZE = ht::MAX_KEY_SIZE;

 private:
  ht m_ht;
};

/**
 * Same as
 * `tsl::array_set<CharT, Hash, KeyEqual, StoreNullTerminator, KeySizeT,
 * IndexSizeT, tsl::ah::prime_growth_policy>`.
 */
template <class CharT, class Hash = tsl::ah::str_hash<CharT>,
          class KeyEqual = tsl::ah::str_equal<CharT>,
          bool StoreNullTerminator = true, class KeySizeT = std::uint16_t,
          class IndexSizeT = std::uint32_t>
using array_pg_set =
    array_set<CharT, Hash, KeyEqual, StoreNullTerminator, KeySizeT, IndexSizeT,
              tsl::ah::prime_growth_policy>;

}  // end namespace tsl

#endif
