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
#ifndef TSL_HTRIE_HASH_H
#define TSL_HTRIE_HASH_H

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "array-hash/array_map.h"
#include "array-hash/array_set.h"

/*
 * __has_include is a bit useless
 * (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=79433), check also __cplusplus
 * version.
 */
#ifdef __has_include
#if __has_include(<string_view>) && (__cplusplus >= 201703L || _MSVC_LANG >= 201703L)
#define TSL_HT_HAS_STRING_VIEW
#endif
#endif

#ifdef TSL_HT_HAS_STRING_VIEW
#include <string_view>
#endif

#ifdef TSL_DEBUG
#define tsl_ht_assert(expr) assert(expr)
#else
#define tsl_ht_assert(expr) (static_cast<void>(0))
#endif

namespace tsl {

namespace detail_htrie_hash {

template <typename T, typename = void>
struct is_iterator : std::false_type {};

template <typename T>
struct is_iterator<T, typename std::enable_if<!std::is_same<
                          typename std::iterator_traits<T>::iterator_category,
                          void>::value>::type> : std::true_type {};

template <typename T, typename... U>
struct is_related : std::false_type {};

template <typename T, typename U>
struct is_related<T, U>
    : std::is_same<typename std::remove_cv<
                       typename std::remove_reference<T>::type>::type,
                   typename std::remove_cv<
                       typename std::remove_reference<U>::type>::type> {};

template <typename T, typename U>
static T numeric_cast(U value,
                      const char* error_message = "numeric_cast() failed.") {
  T ret = static_cast<T>(value);
  if (static_cast<U>(ret) != value) {
    throw std::runtime_error(error_message);
  }

  const bool is_same_signedness =
      (std::is_unsigned<T>::value && std::is_unsigned<U>::value) ||
      (std::is_signed<T>::value && std::is_signed<U>::value);
  if (!is_same_signedness && (ret < T{}) != (value < U{})) {
    throw std::runtime_error(error_message);
  }

  return ret;
}

template <class T>
struct value_node {
  /*
   * Avoid conflict with copy constructor 'value_node(const value_node&)'. If we
   * call the copy constructor with a mutable reference
   * 'value_node(value_node&)', we don't want the forward constructor to be
   * called.
   */
  template <class... Args, typename std::enable_if<!is_related<
                               value_node, Args...>::value>::type* = nullptr>
  value_node(Args&&... args) : m_value(std::forward<Args>(args)...) {}

  T m_value;
};

template <>
struct value_node<void> {};

/**
 * T should be void if there is no value associated to a key (in a set for
 * example).
 */
template <class CharT, class T, class Hash, class KeySizeT>
class htrie_hash {
 private:
  template <typename U>
  using has_value =
      typename std::integral_constant<bool, !std::is_same<U, void>::value>;

  static_assert(std::is_same<CharT, char>::value,
                "char is the only supported CharT type for now.");

  static const std::size_t ALPHABET_SIZE =
      std::numeric_limits<typename std::make_unsigned<CharT>::type>::max() + 1;

 public:
  template <bool IsConst, bool IsPrefixIterator>
  class htrie_hash_iterator;

  using char_type = CharT;
  using key_size_type = KeySizeT;
  using size_type = std::size_t;
  using hasher = Hash;
  using iterator = htrie_hash_iterator<false, false>;
  using const_iterator = htrie_hash_iterator<true, false>;
  using prefix_iterator = htrie_hash_iterator<false, true>;
  using const_prefix_iterator = htrie_hash_iterator<true, true>;

 private:
  using array_hash_type = typename std::conditional<
      has_value<T>::value,
      tsl::array_map<CharT, T, Hash, tsl::ah::str_equal<CharT>, false, KeySizeT,
                     std::uint16_t, tsl::ah::power_of_two_growth_policy<4>>,
      tsl::array_set<CharT, Hash, tsl::ah::str_equal<CharT>, false, KeySizeT,
                     std::uint16_t,
                     tsl::ah::power_of_two_growth_policy<4>>>::type;

 private:
  /*
   * The tree is mainly composed of two nodes types: trie_node and hash_node
   * which both have anode as base class. Each child is either a hash_node or a
   * trie_node.
   *
   * A hash_node is always a leaf node, it doesn't have any child.
   *
   * Example:
   *      | ... | a |.. ..................... | f | ... | trie_node_1
   *               \                             \
   * hash_node_1 |array_hash = {"dd"}|     |...| a | ... | trie_node_2
   *                                             /
   *                     |array_hash = {"ble", "bric", "lse"}| hash_node_2
   *
   *
   * Each trie_node may also have a value node, which contains a value T, if the
   * trie_node marks the end of a string value.
   *
   * A trie node should at least have one child or a value node. There can't be
   * a trie node without any child and no value node.
   */

  using value_node = tsl::detail_htrie_hash::value_node<T>;

  class trie_node;
  class hash_node;

  // TODO better encapsulate operations modifying the tree.
  class anode {
    friend class trie_node;

   public:
    /*
     * TODO Avoid the virtual to economize 8 bytes. We could use a custom
     * deleter in the std::unique_ptr<anode> we use (as we know if an anode is a
     * trie_node or hash_node).
     */
    virtual ~anode() = default;

    bool is_trie_node() const noexcept {
      return m_node_type == node_type::TRIE_NODE;
    }

    bool is_hash_node() const noexcept {
      return m_node_type == node_type::HASH_NODE;
    }

    trie_node& as_trie_node() noexcept {
      tsl_ht_assert(is_trie_node());
      return static_cast<trie_node&>(*this);
    }

    hash_node& as_hash_node() noexcept {
      tsl_ht_assert(is_hash_node());
      return static_cast<hash_node&>(*this);
    }

    const trie_node& as_trie_node() const noexcept {
      tsl_ht_assert(is_trie_node());
      return static_cast<const trie_node&>(*this);
    }

    const hash_node& as_hash_node() const noexcept {
      tsl_ht_assert(is_hash_node());
      return static_cast<const hash_node&>(*this);
    }

    /**
     * @see m_child_of_char
     */
    CharT child_of_char() const noexcept {
      tsl_ht_assert(parent() != nullptr);
      return m_child_of_char;
    }

    /**
     * Return nullptr if none.
     */
    trie_node* parent() noexcept { return m_parent_node; }

    const trie_node* parent() const noexcept { return m_parent_node; }

   protected:
    enum class node_type : unsigned char { HASH_NODE, TRIE_NODE };

    anode(node_type node_type_)
        : m_node_type(node_type_), m_child_of_char(0), m_parent_node(nullptr) {}

    anode(node_type node_type_, CharT child_of_char)
        : m_node_type(node_type_),
          m_child_of_char(child_of_char),
          m_parent_node(nullptr) {}

   protected:
    node_type m_node_type;

    /**
     * If the node has a parent, then it's a descendant of some char.
     *
     * Example:
     *      | ... | a | b | ... | trie_node_1
     *                   \
     *              |...| a | ... | trie_node_2
     *                   /
     *            |array_hash| hash_node_1
     *
     * trie_node_2 is a child of trie_node_1 through 'b', it will have 'b' as
     * m_child_of_char. hash_node_1 is a child of trie_node_2 through 'a', it
     * will have 'a' as m_child_of_char.
     *
     * trie_node_1 has no parent, its m_child_of_char is undefined.
     */
    CharT m_child_of_char;
    trie_node* m_parent_node;
  };

  // Give the position in trie_node::m_children corresponding to the character c
  static std::size_t as_position(CharT c) noexcept {
    return static_cast<std::size_t>(
        static_cast<typename std::make_unsigned<CharT>::type>(c));
  }

  class trie_node : public anode {
   public:
    trie_node()
        : anode(anode::node_type::TRIE_NODE),
          m_value_node(nullptr),
          m_children() {}

    trie_node(const trie_node& other)
        : anode(anode::node_type::TRIE_NODE, other.m_child_of_char),
          m_value_node(nullptr),
          m_children() {
      if (other.m_value_node != nullptr) {
        m_value_node = make_unique<value_node>(*other.m_value_node);
      }

      // TODO avoid recursion
      for (std::size_t ichild = 0; ichild < other.m_children.size(); ichild++) {
        if (other.m_children[ichild] != nullptr) {
          if (other.m_children[ichild]->is_hash_node()) {
            m_children[ichild] = make_unique<hash_node>(
                other.m_children[ichild]->as_hash_node());
          } else {
            m_children[ichild] = make_unique<trie_node>(
                other.m_children[ichild]->as_trie_node());
          }

          m_children[ichild]->m_parent_node = this;
        }
      }
    }

    trie_node(trie_node&& other) = delete;
    trie_node& operator=(const trie_node& other) = delete;
    trie_node& operator=(trie_node&& other) = delete;

    /**
     * Return nullptr if none.
     */
    anode* first_child() noexcept {
      return const_cast<anode*>(
          static_cast<const trie_node*>(this)->first_child());
    }

    const anode* first_child() const noexcept {
      for (std::size_t ichild = 0; ichild < m_children.size(); ichild++) {
        if (m_children[ichild] != nullptr) {
          return m_children[ichild].get();
        }
      }

      return nullptr;
    }

    /**
     * Get the next_child that come after current_child. Return nullptr if no
     * next child.
     */
    anode* next_child(const anode& current_child) noexcept {
      return const_cast<anode*>(
          static_cast<const trie_node*>(this)->next_child(current_child));
    }

    const anode* next_child(const anode& current_child) const noexcept {
      tsl_ht_assert(current_child.parent() == this);

      for (std::size_t ichild = as_position(current_child.child_of_char()) + 1;
           ichild < m_children.size(); ichild++) {
        if (m_children[ichild] != nullptr) {
          return m_children[ichild].get();
        }
      }

      return nullptr;
    }

    /**
     * Return the first left-descendant trie node with an m_value_node. If none
     * return the most left trie node.
     */
    trie_node& most_left_descendant_value_trie_node() noexcept {
      return const_cast<trie_node&>(
          static_cast<const trie_node*>(this)
              ->most_left_descendant_value_trie_node());
    }

    const trie_node& most_left_descendant_value_trie_node() const noexcept {
      const trie_node* current_node = this;
      while (true) {
        if (current_node->m_value_node != nullptr) {
          return *current_node;
        }

        const anode* first_child = current_node->first_child();
        tsl_ht_assert(first_child !=
                      nullptr);  // a trie_node must either have a value_node or
                                 // at least one child.
        if (first_child->is_hash_node()) {
          return *current_node;
        }

        current_node = &first_child->as_trie_node();
      }
    }

    size_type nb_children() const noexcept {
      return std::count_if(
          m_children.cbegin(), m_children.cend(),
          [](const std::unique_ptr<anode>& n) { return n != nullptr; });
    }

    bool empty() const noexcept {
      return std::all_of(
          m_children.cbegin(), m_children.cend(),
          [](const std::unique_ptr<anode>& n) { return n == nullptr; });
    }

    std::unique_ptr<anode>& child(CharT for_char) noexcept {
      return m_children[as_position(for_char)];
    }

    const std::unique_ptr<anode>& child(CharT for_char) const noexcept {
      return m_children[as_position(for_char)];
    }

    typename std::array<std::unique_ptr<anode>, ALPHABET_SIZE>::iterator
    begin() noexcept {
      return m_children.begin();
    }

    typename std::array<std::unique_ptr<anode>, ALPHABET_SIZE>::iterator
    end() noexcept {
      return m_children.end();
    }

    void set_child(CharT for_char, std::unique_ptr<anode> child) noexcept {
      if (child != nullptr) {
        child->m_child_of_char = for_char;
        child->m_parent_node = this;
      }

      m_children[as_position(for_char)] = std::move(child);
    }

    std::unique_ptr<value_node>& val_node() noexcept { return m_value_node; }

    const std::unique_ptr<value_node>& val_node() const noexcept {
      return m_value_node;
    }

   private:
    // TODO Avoid storing a value_node when has_value<T>::value is false
    std::unique_ptr<value_node> m_value_node;

    /**
     * Each character CharT corresponds to one position in the array. To convert
     * a character to a position use the as_position method.
     *
     * TODO Try to reduce the size of m_children with a hash map, linear/binary
     * search on array, ...
     * TODO Store number of non-null values in m_children. Check if we can store
     * this value in the alignment space as we don't want the node to get bigger
     * (empty() and nb_children() are rarely used so it is not an important
     * variable).
     */
    std::array<std::unique_ptr<anode>, ALPHABET_SIZE> m_children;
  };

  class hash_node : public anode {
   public:
    hash_node(const Hash& hash, float max_load_factor)
        : hash_node(HASH_NODE_DEFAULT_INIT_BUCKETS_COUNT, hash,
                    max_load_factor) {}

    hash_node(size_type bucket_count, const Hash& hash, float max_load_factor)
        : anode(anode::node_type::HASH_NODE), m_array_hash(bucket_count, hash) {
      m_array_hash.max_load_factor(max_load_factor);
    }

    hash_node(array_hash_type&& array_hash) noexcept(
        std::is_nothrow_move_constructible<array_hash_type>::value)
        : anode(anode::node_type::HASH_NODE),
          m_array_hash(std::move(array_hash)) {}

    hash_node(const hash_node& other) = default;

    hash_node(hash_node&& other) = delete;
    hash_node& operator=(const hash_node& other) = delete;
    hash_node& operator=(hash_node&& other) = delete;

    array_hash_type& array_hash() noexcept { return m_array_hash; }

    const array_hash_type& array_hash() const noexcept { return m_array_hash; }

   private:
    array_hash_type m_array_hash;
  };

 public:
  template <bool IsConst, bool IsPrefixIterator>
  class htrie_hash_iterator {
    friend class htrie_hash;

   private:
    using anode_type =
        typename std::conditional<IsConst, const anode, anode>::type;
    using trie_node_type =
        typename std::conditional<IsConst, const trie_node, trie_node>::type;
    using hash_node_type =
        typename std::conditional<IsConst, const hash_node, hash_node>::type;

    using array_hash_iterator_type =
        typename std::conditional<IsConst,
                                  typename array_hash_type::const_iterator,
                                  typename array_hash_type::iterator>::type;

   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type =
        typename std::conditional<has_value<T>::value, T, void>::type;
    using difference_type = std::ptrdiff_t;
    using reference = typename std::conditional<
        has_value<T>::value,
        typename std::conditional<
            IsConst, typename std::add_lvalue_reference<const T>::type,
            typename std::add_lvalue_reference<T>::type>::type,
        void>::type;
    using pointer = typename std::conditional<
        has_value<T>::value,
        typename std::conditional<IsConst, const T*, T*>::type, void>::type;

   private:
    /**
     * Start reading from start_hash_node->array_hash().begin().
     */
    htrie_hash_iterator(hash_node_type& start_hash_node) noexcept
        : htrie_hash_iterator(start_hash_node,
                              start_hash_node.array_hash().begin()) {}

    /**
     * Start reading from iterator begin in start_hash_node->array_hash().
     */
    htrie_hash_iterator(hash_node_type& start_hash_node,
                        array_hash_iterator_type begin) noexcept
        : m_current_trie_node(start_hash_node.parent()),
          m_current_hash_node(&start_hash_node),
          m_array_hash_iterator(begin),
          m_array_hash_end_iterator(start_hash_node.array_hash().end()),
          m_read_trie_node_value(false) {
      tsl_ht_assert(!m_current_hash_node->array_hash().empty());
    }

    /**
     * Start reading from the value in start_trie_node.
     * start_trie_node->val_node() should be non-null.
     */
    htrie_hash_iterator(trie_node_type& start_trie_node) noexcept
        : m_current_trie_node(&start_trie_node),
          m_current_hash_node(nullptr),
          m_read_trie_node_value(true) {
      tsl_ht_assert(m_current_trie_node->val_node() != nullptr);
    }

    template <bool P = IsPrefixIterator,
              typename std::enable_if<!P>::type* = nullptr>
    htrie_hash_iterator(trie_node_type* tnode, hash_node_type* hnode,
                        array_hash_iterator_type begin,
                        array_hash_iterator_type end,
                        bool read_trie_node_value) noexcept
        : m_current_trie_node(tnode),
          m_current_hash_node(hnode),
          m_array_hash_iterator(begin),
          m_array_hash_end_iterator(end),
          m_read_trie_node_value(read_trie_node_value) {}

    template <bool P = IsPrefixIterator,
              typename std::enable_if<P>::type* = nullptr>
    htrie_hash_iterator(trie_node_type* tnode, hash_node_type* hnode,
                        array_hash_iterator_type begin,
                        array_hash_iterator_type end, bool read_trie_node_value,
                        std::basic_string<CharT> prefix_filter) noexcept
        : m_current_trie_node(tnode),
          m_current_hash_node(hnode),
          m_array_hash_iterator(begin),
          m_array_hash_end_iterator(end),
          m_read_trie_node_value(read_trie_node_value),
          m_prefix_filter(std::move(prefix_filter)) {}

   public:
    htrie_hash_iterator() noexcept {}

    // Copy constructor from iterator to const_iterator.
    template <bool TIsConst = IsConst,
              bool TIsPrefixIterator = IsPrefixIterator,
              typename std::enable_if<TIsConst && !TIsPrefixIterator>::type* =
                  nullptr>
    htrie_hash_iterator(
        const htrie_hash_iterator<!TIsConst, TIsPrefixIterator>& other) noexcept
        : m_current_trie_node(other.m_current_trie_node),
          m_current_hash_node(other.m_current_hash_node),
          m_array_hash_iterator(other.m_array_hash_iterator),
          m_array_hash_end_iterator(other.m_array_hash_end_iterator),
          m_read_trie_node_value(other.m_read_trie_node_value) {}

    // Copy constructor from iterator to const_iterator.
    template <
        bool TIsConst = IsConst, bool TIsPrefixIterator = IsPrefixIterator,
        typename std::enable_if<TIsConst && TIsPrefixIterator>::type* = nullptr>
    htrie_hash_iterator(
        const htrie_hash_iterator<!TIsConst, TIsPrefixIterator>& other) noexcept
        : m_current_trie_node(other.m_current_trie_node),
          m_current_hash_node(other.m_current_hash_node),
          m_array_hash_iterator(other.m_array_hash_iterator),
          m_array_hash_end_iterator(other.m_array_hash_end_iterator),
          m_read_trie_node_value(other.m_read_trie_node_value),
          m_prefix_filter(other.m_prefix_filter) {}

    htrie_hash_iterator(const htrie_hash_iterator& other) = default;
    htrie_hash_iterator(htrie_hash_iterator&& other) = default;
    htrie_hash_iterator& operator=(const htrie_hash_iterator& other) = default;
    htrie_hash_iterator& operator=(htrie_hash_iterator&& other) = default;

    void key(std::basic_string<CharT>& key_buffer_out) const {
      key_buffer_out.clear();

      trie_node_type* tnode = m_current_trie_node;
      while (tnode != nullptr && tnode->parent() != nullptr) {
        key_buffer_out.push_back(tnode->child_of_char());
        tnode = tnode->parent();
      }

      std::reverse(key_buffer_out.begin(), key_buffer_out.end());

      if (!m_read_trie_node_value) {
        tsl_ht_assert(m_current_hash_node != nullptr);
        if (m_current_hash_node->parent() != nullptr) {
          key_buffer_out.push_back(m_current_hash_node->child_of_char());
        }

        key_buffer_out.append(m_array_hash_iterator.key(),
                              m_array_hash_iterator.key_size());
      }
    }

    std::basic_string<CharT> key() const {
      std::basic_string<CharT> key_buffer;
      key(key_buffer);

      return key_buffer;
    }

    template <class U = T,
              typename std::enable_if<has_value<U>::value>::type* = nullptr>
    reference value() const {
      if (this->m_read_trie_node_value) {
        tsl_ht_assert(this->m_current_trie_node != nullptr);
        tsl_ht_assert(this->m_current_trie_node->val_node() != nullptr);

        return this->m_current_trie_node->val_node()->m_value;
      } else {
        return this->m_array_hash_iterator.value();
      }
    }

    template <class U = T,
              typename std::enable_if<has_value<U>::value>::type* = nullptr>
    reference operator*() const {
      return value();
    }

    template <class U = T,
              typename std::enable_if<has_value<U>::value>::type* = nullptr>
    pointer operator->() const {
      return std::addressof(value());
    }

    htrie_hash_iterator& operator++() {
      if (m_read_trie_node_value) {
        tsl_ht_assert(m_current_trie_node != nullptr);

        m_read_trie_node_value = false;

        anode_type* child = m_current_trie_node->first_child();
        if (child != nullptr) {
          set_most_left_descendant_as_next_node(*child);
        } else if (m_current_trie_node->parent() != nullptr) {
          trie_node_type* current_node_child = m_current_trie_node;
          m_current_trie_node = m_current_trie_node->parent();

          set_next_node_ascending(*current_node_child);
        } else {
          set_as_end_iterator();
        }
      } else {
        ++m_array_hash_iterator;
        if (m_array_hash_iterator != m_array_hash_end_iterator) {
          filter_prefix();
        }
        // End of the road, set the iterator as an end node.
        else if (m_current_trie_node == nullptr) {
          set_as_end_iterator();
        } else {
          tsl_ht_assert(m_current_hash_node != nullptr);
          set_next_node_ascending(*m_current_hash_node);
        }
      }

      return *this;
    }

    htrie_hash_iterator operator++(int) {
      htrie_hash_iterator tmp(*this);
      ++*this;

      return tmp;
    }

    friend bool operator==(const htrie_hash_iterator& lhs,
                           const htrie_hash_iterator& rhs) {
      if (lhs.m_current_trie_node != rhs.m_current_trie_node ||
          lhs.m_read_trie_node_value != rhs.m_read_trie_node_value) {
        return false;
      } else if (lhs.m_read_trie_node_value) {
        return true;
      } else {
        if (lhs.m_current_hash_node != rhs.m_current_hash_node) {
          return false;
        } else if (lhs.m_current_hash_node == nullptr) {
          return true;
        } else {
          return lhs.m_array_hash_iterator == rhs.m_array_hash_iterator &&
                 lhs.m_array_hash_end_iterator == rhs.m_array_hash_end_iterator;
        }
      }
    }

    friend bool operator!=(const htrie_hash_iterator& lhs,
                           const htrie_hash_iterator& rhs) {
      return !(lhs == rhs);
    }

   private:
    void hash_node_prefix(std::basic_string<CharT>& key_buffer_out) const {
      tsl_ht_assert(!m_read_trie_node_value);
      key_buffer_out.clear();

      trie_node_type* tnode = m_current_trie_node;
      while (tnode != nullptr && tnode->parent() != nullptr) {
        key_buffer_out.push_back(tnode->child_of_char());
        tnode = tnode->parent();
      }

      std::reverse(key_buffer_out.begin(), key_buffer_out.end());

      tsl_ht_assert(m_current_hash_node != nullptr);
      if (m_current_hash_node->parent() != nullptr) {
        key_buffer_out.push_back(m_current_hash_node->child_of_char());
      }
    }

    template <bool P = IsPrefixIterator,
              typename std::enable_if<!P>::type* = nullptr>
    void filter_prefix() {}

    template <bool P = IsPrefixIterator,
              typename std::enable_if<P>::type* = nullptr>
    void filter_prefix() {
      tsl_ht_assert(m_array_hash_iterator != m_array_hash_end_iterator);
      tsl_ht_assert(!m_read_trie_node_value && m_current_hash_node != nullptr);

      if (m_prefix_filter.empty()) {
        return;
      }

      while ((m_prefix_filter.size() > m_array_hash_iterator.key_size() ||
              m_prefix_filter.compare(0, m_prefix_filter.size(),
                                      m_array_hash_iterator.key(),
                                      m_prefix_filter.size()) != 0)) {
        ++m_array_hash_iterator;
        if (m_array_hash_iterator == m_array_hash_end_iterator) {
          if (m_current_trie_node == nullptr) {
            set_as_end_iterator();
          } else {
            tsl_ht_assert(m_current_hash_node != nullptr);
            set_next_node_ascending(*m_current_hash_node);
          }

          return;
        }
      }
    }

    /**
     * Go back up in the tree to get the current_trie_node_child sibling.
     * If none, try to go back up more in the tree to check the siblings of the
     * ancestors.
     */
    void set_next_node_ascending(anode_type& current_trie_node_child) {
      tsl_ht_assert(m_current_trie_node != nullptr);
      tsl_ht_assert(current_trie_node_child.parent() == m_current_trie_node);

      anode_type* next_node =
          m_current_trie_node->next_child(current_trie_node_child);
      while (next_node == nullptr && m_current_trie_node->parent() != nullptr) {
        anode_type* current_child = m_current_trie_node;
        m_current_trie_node = m_current_trie_node->parent();
        next_node = m_current_trie_node->next_child(*current_child);
      }

      // End of the road, set the iterator as an end node.
      if (next_node == nullptr) {
        set_as_end_iterator();
      } else {
        set_most_left_descendant_as_next_node(*next_node);
      }
    }

    void set_most_left_descendant_as_next_node(anode_type& search_start) {
      if (search_start.is_hash_node()) {
        set_current_hash_node(search_start.as_hash_node());
      } else {
        m_current_trie_node =
            &search_start.as_trie_node().most_left_descendant_value_trie_node();
        if (m_current_trie_node->val_node() != nullptr) {
          m_read_trie_node_value = true;
        } else {
          anode_type* first_child = m_current_trie_node->first_child();
          // a trie_node must either have a value_node or at least one child.
          tsl_ht_assert(first_child != nullptr);

          set_current_hash_node(first_child->as_hash_node());
        }
      }
    }

    void set_current_hash_node(hash_node_type& hnode) {
      tsl_ht_assert(!hnode.array_hash().empty());

      m_current_hash_node = &hnode;
      m_array_hash_iterator = m_current_hash_node->array_hash().begin();
      m_array_hash_end_iterator = m_current_hash_node->array_hash().end();
    }

    void set_as_end_iterator() {
      m_current_trie_node = nullptr;
      m_current_hash_node = nullptr;
      m_read_trie_node_value = false;
    }

    void skip_hash_node() {
      tsl_ht_assert(!m_read_trie_node_value && m_current_hash_node != nullptr);
      if (m_current_trie_node == nullptr) {
        set_as_end_iterator();
      } else {
        tsl_ht_assert(m_current_hash_node != nullptr);
        set_next_node_ascending(*m_current_hash_node);
      }
    }

   private:
    trie_node_type* m_current_trie_node;
    hash_node_type* m_current_hash_node;

    array_hash_iterator_type m_array_hash_iterator;
    array_hash_iterator_type m_array_hash_end_iterator;

    bool m_read_trie_node_value;
    // TODO can't have void if !IsPrefixIterator, use inheritance
    typename std::conditional<IsPrefixIterator, std::basic_string<CharT>,
                              bool>::type m_prefix_filter;
  };

 public:
  htrie_hash(const Hash& hash, float max_load_factor, size_type burst_threshold)
      : m_root(nullptr),
        m_nb_elements(0),
        m_hash(hash),
        m_max_load_factor(max_load_factor) {
    this->burst_threshold(burst_threshold);
  }

  htrie_hash(const htrie_hash& other)
      : m_root(nullptr),
        m_nb_elements(other.m_nb_elements),
        m_hash(other.m_hash),
        m_max_load_factor(other.m_max_load_factor),
        m_burst_threshold(other.m_burst_threshold) {
    if (other.m_root != nullptr) {
      if (other.m_root->is_hash_node()) {
        m_root = make_unique<hash_node>(other.m_root->as_hash_node());
      } else {
        m_root = make_unique<trie_node>(other.m_root->as_trie_node());
      }
    }
  }

  htrie_hash(htrie_hash&& other) noexcept(
      std::is_nothrow_move_constructible<Hash>::value)
      : m_root(std::move(other.m_root)),
        m_nb_elements(other.m_nb_elements),
        m_hash(std::move(other.m_hash)),
        m_max_load_factor(other.m_max_load_factor),
        m_burst_threshold(other.m_burst_threshold) {
    other.clear();
  }

  htrie_hash& operator=(const htrie_hash& other) {
    if (&other != this) {
      std::unique_ptr<anode> new_root = nullptr;
      if (other.m_root != nullptr) {
        if (other.m_root->is_hash_node()) {
          new_root = make_unique<hash_node>(other.m_root->as_hash_node());
        } else {
          new_root = make_unique<trie_node>(other.m_root->as_trie_node());
        }
      }

      m_hash = other.m_hash;
      m_root = std::move(new_root);
      m_nb_elements = other.m_nb_elements;
      m_max_load_factor = other.m_max_load_factor;
      m_burst_threshold = other.m_burst_threshold;
    }

    return *this;
  }

  htrie_hash& operator=(htrie_hash&& other) {
    other.swap(*this);
    other.clear();

    return *this;
  }

  /*
   * Iterators
   */
  iterator begin() noexcept { return mutable_iterator(cbegin()); }

  const_iterator begin() const noexcept { return cbegin(); }

  const_iterator cbegin() const noexcept {
    if (empty()) {
      return cend();
    }

    return cbegin<const_iterator>(*m_root);
  }

  iterator end() noexcept {
    iterator it;
    it.set_as_end_iterator();

    return it;
  }

  const_iterator end() const noexcept { return cend(); }

  const_iterator cend() const noexcept {
    const_iterator it;
    it.set_as_end_iterator();

    return it;
  }

  /*
   * Capacity
   */
  bool empty() const noexcept { return m_nb_elements == 0; }

  size_type size() const noexcept { return m_nb_elements; }

  size_type max_size() const noexcept {
    return std::numeric_limits<size_type>::max();
  }

  size_type max_key_size() const noexcept {
    return array_hash_type::MAX_KEY_SIZE;
  }

  void shrink_to_fit() {
    auto first = begin();
    auto last = end();

    while (first != last) {
      if (first.m_read_trie_node_value) {
        ++first;
      } else {
        /*
         * shrink_to_fit on array_hash will invalidate the iterators of
         * array_hash. Save pointer to array_hash, skip the array_hash_node and
         * then call shrink_to_fit on the saved pointer.
         */
        hash_node* hnode = first.m_current_hash_node;
        first.skip_hash_node();

        tsl_ht_assert(hnode != nullptr);
        hnode->array_hash().shrink_to_fit();
      }
    }
  }

  /*
   * Modifiers
   */
  void clear() noexcept {
    m_root.reset(nullptr);
    m_nb_elements = 0;
  }

  template <class... ValueArgs>
  std::pair<iterator, bool> insert(const CharT* key, size_type key_size,
                                   ValueArgs&&... value_args) {
    if (key_size > max_key_size()) {
      throw std::length_error("Key is too long.");
    }

    if (m_root == nullptr) {
      m_root = make_unique<hash_node>(m_hash, m_max_load_factor);
    }

    return insert_impl(*m_root, key, key_size,
                       std::forward<ValueArgs>(value_args)...);
  }

  iterator erase(const_iterator pos) { return erase(mutable_iterator(pos)); }

  iterator erase(const_iterator first, const_iterator last) {
    // TODO Optimize, could avoid the call to std::distance
    const std::size_t nb_to_erase = std::size_t(std::distance(first, last));
    auto to_delete = mutable_iterator(first);
    for (std::size_t i = 0; i < nb_to_erase; i++) {
      to_delete = erase(to_delete);
    }

    return to_delete;
  }

  size_type erase(const CharT* key, size_type key_size) {
    auto it = find(key, key_size);
    if (it != end()) {
      erase(it);
      return 1;
    } else {
      return 0;
    }
  }

  size_type erase_prefix(const CharT* prefix, size_type prefix_size) {
    if (m_root == nullptr) {
      return 0;
    }

    anode* current_node = m_root.get();
    for (size_type iprefix = 0; iprefix < prefix_size; iprefix++) {
      if (current_node->is_trie_node()) {
        trie_node* tnode = &current_node->as_trie_node();

        if (tnode->child(prefix[iprefix]) == nullptr) {
          return 0;
        } else {
          current_node = tnode->child(prefix[iprefix]).get();
        }
      } else {
        hash_node& hnode = current_node->as_hash_node();
        return erase_prefix_hash_node(hnode, prefix + iprefix,
                                      prefix_size - iprefix);
      }
    }

    if (current_node->is_trie_node()) {
      trie_node* parent = current_node->parent();

      if (parent != nullptr) {
        const size_type nb_erased =
            size_descendants(current_node->as_trie_node());

        parent->set_child(current_node->child_of_char(), nullptr);
        m_nb_elements -= nb_erased;

        if (parent->empty()) {
          clear_empty_nodes(*parent);
        }

        return nb_erased;
      } else {
        const size_type nb_erased = m_nb_elements;
        m_root.reset(nullptr);
        m_nb_elements = 0;

        return nb_erased;
      }
    } else {
      const size_type nb_erased =
          current_node->as_hash_node().array_hash().size();

      current_node->as_hash_node().array_hash().clear();
      m_nb_elements -= nb_erased;

      clear_empty_nodes(current_node->as_hash_node());

      return nb_erased;
    }
  }

  void swap(htrie_hash& other) {
    using std::swap;

    swap(m_hash, other.m_hash);
    swap(m_root, other.m_root);
    swap(m_nb_elements, other.m_nb_elements);
    swap(m_max_load_factor, other.m_max_load_factor);
    swap(m_burst_threshold, other.m_burst_threshold);
  }

  /*
   * Lookup
   */
  template <class U = T,
            typename std::enable_if<has_value<U>::value>::type* = nullptr>
  U& at(const CharT* key, size_type key_size) {
    return const_cast<U&>(
        static_cast<const htrie_hash*>(this)->at(key, key_size));
  }

  template <class U = T,
            typename std::enable_if<has_value<U>::value>::type* = nullptr>
  const U& at(const CharT* key, size_type key_size) const {
    auto it_find = find(key, key_size);
    if (it_find != cend()) {
      return it_find.value();
    } else {
      throw std::out_of_range("Couldn't find key.");
    }
  }

  // TODO optimize
  template <class U = T,
            typename std::enable_if<has_value<U>::value>::type* = nullptr>
  U& access_operator(const CharT* key, size_type key_size) {
    auto it_find = find(key, key_size);
    if (it_find != cend()) {
      return it_find.value();
    } else {
      return insert(key, key_size, U{}).first.value();
    }
  }

  size_type count(const CharT* key, size_type key_size) const {
    if (find(key, key_size) != cend()) {
      return 1;
    } else {
      return 0;
    }
  }

  iterator find(const CharT* key, size_type key_size) {
    if (m_root == nullptr) {
      return end();
    }

    return find_impl(*m_root, key, key_size);
  }

  const_iterator find(const CharT* key, size_type key_size) const {
    if (m_root == nullptr) {
      return cend();
    }

    return find_impl(*m_root, key, key_size);
  }

  std::pair<iterator, iterator> equal_range(const CharT* key,
                                            size_type key_size) {
    iterator it = find(key, key_size);
    return std::make_pair(it, (it == end()) ? it : std::next(it));
  }

  std::pair<const_iterator, const_iterator> equal_range(
      const CharT* key, size_type key_size) const {
    const_iterator it = find(key, key_size);
    return std::make_pair(it, (it == cend()) ? it : std::next(it));
  }

  std::pair<prefix_iterator, prefix_iterator> equal_prefix_range(
      const CharT* prefix, size_type prefix_size) {
    if (m_root == nullptr) {
      return std::make_pair(prefix_end(), prefix_end());
    }

    return equal_prefix_range_impl(*m_root, prefix, prefix_size);
  }

  std::pair<const_prefix_iterator, const_prefix_iterator> equal_prefix_range(
      const CharT* prefix, size_type prefix_size) const {
    if (m_root == nullptr) {
      return std::make_pair(prefix_cend(), prefix_cend());
    }

    return equal_prefix_range_impl(*m_root, prefix, prefix_size);
  }

  iterator longest_prefix(const CharT* key, size_type key_size) {
    if (m_root == nullptr) {
      return end();
    }

    return longest_prefix_impl(*m_root, key, key_size);
  }

  const_iterator longest_prefix(const CharT* key, size_type key_size) const {
    if (m_root == nullptr) {
      return cend();
    }

    return longest_prefix_impl(*m_root, key, key_size);
  }

  /*
   * Hash policy
   */
  float max_load_factor() const { return m_max_load_factor; }

  void max_load_factor(float ml) { m_max_load_factor = ml; }

  /*
   * Burst policy
   */
  size_type burst_threshold() const { return m_burst_threshold; }

  void burst_threshold(size_type threshold) {
    const size_type min_burst_threshold = MIN_BURST_THRESHOLD;
    m_burst_threshold = std::max(min_burst_threshold, threshold);
  }

  /*
   * Observers
   */
  hasher hash_function() const { return m_hash; }

  /*
   * Other
   */
  template <class Serializer>
  void serialize(Serializer& serializer) const {
    serialize_impl(serializer);
  }

  template <class Deserializer>
  void deserialize(Deserializer& deserializer, bool hash_compatible) {
    deserialize_impl(deserializer, hash_compatible);
  }

 private:
  /**
   * Get the begin iterator by searching for the most left descendant node
   * starting at search_start_node.
   */
  template <typename Iterator>
  Iterator cbegin(const anode& search_start_node) const noexcept {
    if (search_start_node.is_hash_node()) {
      return Iterator(search_start_node.as_hash_node());
    }

    const trie_node& tnode =
        search_start_node.as_trie_node().most_left_descendant_value_trie_node();
    if (tnode.val_node() != nullptr) {
      return Iterator(tnode);
    } else {
      const anode* first_child = tnode.first_child();
      tsl_ht_assert(first_child != nullptr);

      return Iterator(first_child->as_hash_node());
    }
  }

  /**
   * Get an iterator to the node that come just after the last descendant of
   * search_start_node.
   */
  template <typename Iterator>
  Iterator cend(const anode& search_start_node) const noexcept {
    if (search_start_node.parent() == nullptr) {
      Iterator it;
      it.set_as_end_iterator();

      return it;
    }

    const trie_node* current_trie_node = search_start_node.parent();
    const anode* next_node = current_trie_node->next_child(search_start_node);

    while (next_node == nullptr && current_trie_node->parent() != nullptr) {
      const anode* current_child = current_trie_node;
      current_trie_node = current_trie_node->parent();
      next_node = current_trie_node->next_child(*current_child);
    }

    if (next_node == nullptr) {
      Iterator it;
      it.set_as_end_iterator();

      return it;
    } else {
      return cbegin<Iterator>(*next_node);
    }
  }

  prefix_iterator prefix_end() noexcept {
    prefix_iterator it;
    it.set_as_end_iterator();

    return it;
  }

  const_prefix_iterator prefix_cend() const noexcept {
    const_prefix_iterator it;
    it.set_as_end_iterator();

    return it;
  }

  size_type size_descendants(const anode& start_node) const {
    auto first = cbegin<const_iterator>(start_node);
    auto last = cend<const_iterator>(start_node);

    size_type nb_elements = 0;
    while (first != last) {
      if (first.m_read_trie_node_value) {
        nb_elements++;
        ++first;
      } else {
        nb_elements += first.m_current_hash_node->array_hash().size();
        first.skip_hash_node();
      }
    }

    return nb_elements;
  }

  template <class... ValueArgs>
  std::pair<iterator, bool> insert_impl(anode& search_start_node,
                                        const CharT* key, size_type key_size,
                                        ValueArgs&&... value_args) {
    anode* current_node = &search_start_node;

    for (size_type ikey = 0; ikey < key_size; ikey++) {
      if (current_node->is_trie_node()) {
        trie_node& tnode = current_node->as_trie_node();

        if (tnode.child(key[ikey]) != nullptr) {
          current_node = tnode.child(key[ikey]).get();
        } else {
          auto hnode = make_unique<hash_node>(m_hash, m_max_load_factor);
          auto insert_it = hnode->array_hash().emplace_ks(
              key + ikey + 1, key_size - ikey - 1,
              std::forward<ValueArgs>(value_args)...);

          tnode.set_child(key[ikey], std::move(hnode));
          m_nb_elements++;

          return std::make_pair(
              iterator(tnode.child(key[ikey])->as_hash_node(), insert_it.first),
              true);
        }
      } else {
        return insert_in_hash_node(current_node->as_hash_node(), key + ikey,
                                   key_size - ikey,
                                   std::forward<ValueArgs>(value_args)...);
      }
    }

    if (current_node->is_trie_node()) {
      trie_node& tnode = current_node->as_trie_node();
      if (tnode.val_node() != nullptr) {
        return std::make_pair(iterator(tnode), false);
      } else {
        tnode.val_node() =
            make_unique<value_node>(std::forward<ValueArgs>(value_args)...);
        m_nb_elements++;

        return std::make_pair(iterator(tnode), true);
      }
    } else {
      return insert_in_hash_node(current_node->as_hash_node(), "", 0,
                                 std::forward<ValueArgs>(value_args)...);
    }
  }

  template <class... ValueArgs>
  std::pair<iterator, bool> insert_in_hash_node(hash_node& hnode,
                                                const CharT* key,
                                                size_type key_size,
                                                ValueArgs&&... value_args) {
    if (need_burst(hnode)) {
      std::unique_ptr<trie_node> new_node = burst(hnode);
      if (hnode.parent() == nullptr) {
        tsl_ht_assert(m_root.get() == &hnode);

        m_root = std::move(new_node);
        return insert_impl(*m_root, key, key_size,
                           std::forward<ValueArgs>(value_args)...);
      } else {
        trie_node* parent = hnode.parent();
        const CharT child_of_char = hnode.child_of_char();

        parent->set_child(child_of_char, std::move(new_node));

        return insert_impl(*parent->child(child_of_char), key, key_size,
                           std::forward<ValueArgs>(value_args)...);
      }
    } else {
      auto it_insert = hnode.array_hash().emplace_ks(
          key, key_size, std::forward<ValueArgs>(value_args)...);
      if (it_insert.second) {
        m_nb_elements++;
      }

      return std::make_pair(iterator(hnode, it_insert.first), it_insert.second);
    }
  }

  iterator erase(iterator pos) {
    iterator next_pos = std::next(pos);

    if (pos.m_read_trie_node_value) {
      tsl_ht_assert(pos.m_current_trie_node != nullptr &&
                    pos.m_current_trie_node->val_node() != nullptr);

      pos.m_current_trie_node->val_node().reset(nullptr);
      m_nb_elements--;

      if (pos.m_current_trie_node->empty()) {
        clear_empty_nodes(*pos.m_current_trie_node);
      }

      return next_pos;
    } else {
      tsl_ht_assert(pos.m_current_hash_node != nullptr);
      auto next_array_hash_it = pos.m_current_hash_node->array_hash().erase(
          pos.m_array_hash_iterator);
      m_nb_elements--;

      if (next_array_hash_it != pos.m_current_hash_node->array_hash().end()) {
        // The erase on array_hash invalidated the next_pos iterator, return the
        // right one.
        return iterator(*pos.m_current_hash_node, next_array_hash_it);
      } else {
        if (pos.m_current_hash_node->array_hash().empty()) {
          clear_empty_nodes(*pos.m_current_hash_node);
        }

        return next_pos;
      }
    }
  }

  /**
   * Clear all the empty nodes from the tree starting from empty_node (empty for
   * a hash_node means that the array hash is empty, for a trie_node it means
   * the node doesn't have any child or value_node associated to it).
   */
  void clear_empty_nodes(anode& empty_node) noexcept {
    tsl_ht_assert(!empty_node.is_trie_node() ||
                  (empty_node.as_trie_node().empty() &&
                   empty_node.as_trie_node().val_node() == nullptr));
    tsl_ht_assert(!empty_node.is_hash_node() ||
                  empty_node.as_hash_node().array_hash().empty());

    trie_node* parent = empty_node.parent();
    if (parent == nullptr) {
      tsl_ht_assert(m_root.get() == &empty_node);
      tsl_ht_assert(m_nb_elements == 0);
      m_root.reset(nullptr);
    } else if (parent->val_node() != nullptr || parent->nb_children() > 1) {
      parent->child(empty_node.child_of_char()).reset(nullptr);
    } else if (parent->parent() == nullptr) {
      tsl_ht_assert(m_root.get() == empty_node.parent());
      tsl_ht_assert(m_nb_elements == 0);
      m_root.reset(nullptr);
    } else {
      /**
       * Parent is empty if we remove its empty_node child.
       * Put empty_node as new child of the grand parent instead of parent (move
       * hnode up, and delete the parent). And recurse.
       *
       * We can't just set grand_parent->child(parent->child_of_char()) to
       * nullptr as the grand_parent may also become empty. We don't want empty
       * trie_node with no value_node in the tree.
       */
      trie_node* grand_parent = parent->parent();
      grand_parent->set_child(
          parent->child_of_char(),
          std::move(parent->child(empty_node.child_of_char())));

      clear_empty_nodes(empty_node);
    }
  }

  iterator find_impl(const anode& search_start_node, const CharT* key,
                     size_type key_size) {
    return mutable_iterator(static_cast<const htrie_hash*>(this)->find_impl(
        search_start_node, key, key_size));
  }

  const_iterator find_impl(const anode& search_start_node, const CharT* key,
                           size_type key_size) const {
    const anode* current_node = &search_start_node;

    for (size_type ikey = 0; ikey < key_size; ikey++) {
      if (current_node->is_trie_node()) {
        const trie_node* tnode = &current_node->as_trie_node();

        if (tnode->child(key[ikey]) == nullptr) {
          return cend();
        } else {
          current_node = tnode->child(key[ikey]).get();
        }
      } else {
        return find_in_hash_node(current_node->as_hash_node(), key + ikey,
                                 key_size - ikey);
      }
    }

    if (current_node->is_trie_node()) {
      const trie_node& tnode = current_node->as_trie_node();
      return (tnode.val_node() != nullptr) ? const_iterator(tnode) : cend();
    } else {
      return find_in_hash_node(current_node->as_hash_node(), "", 0);
    }
  }

  const_iterator find_in_hash_node(const hash_node& hnode, const CharT* key,
                                   size_type key_size) const {
    auto it = hnode.array_hash().find_ks(key, key_size);
    if (it != hnode.array_hash().end()) {
      return const_iterator(hnode, it);
    } else {
      return cend();
    }
  }

  iterator longest_prefix_impl(const anode& search_start_node,
                               const CharT* value, size_type value_size) {
    return mutable_iterator(
        static_cast<const htrie_hash*>(this)->longest_prefix_impl(
            search_start_node, value, value_size));
  }

  const_iterator longest_prefix_impl(const anode& search_start_node,
                                     const CharT* value,
                                     size_type value_size) const {
    const anode* current_node = &search_start_node;
    const_iterator longest_found_prefix = cend();

    for (size_type ivalue = 0; ivalue < value_size; ivalue++) {
      if (current_node->is_trie_node()) {
        const trie_node& tnode = current_node->as_trie_node();

        if (tnode.val_node() != nullptr) {
          longest_found_prefix = const_iterator(tnode);
        }

        if (tnode.child(value[ivalue]) == nullptr) {
          return longest_found_prefix;
        } else {
          current_node = tnode.child(value[ivalue]).get();
        }
      } else {
        const hash_node& hnode = current_node->as_hash_node();

        /**
         * Test the presence in the hash node of each substring from the
         * remaining [ivalue, value_size) string starting from the longest.
         * Also test the empty string.
         */
        for (std::size_t i = ivalue; i <= value_size; i++) {
          auto it =
              hnode.array_hash().find_ks(value + ivalue, (value_size - i));
          if (it != hnode.array_hash().end()) {
            return const_iterator(hnode, it);
          }
        }

        return longest_found_prefix;
      }
    }

    if (current_node->is_trie_node()) {
      const trie_node& tnode = current_node->as_trie_node();

      if (tnode.val_node() != nullptr) {
        longest_found_prefix = const_iterator(tnode);
      }
    } else {
      const hash_node& hnode = current_node->as_hash_node();

      auto it = hnode.array_hash().find_ks("", 0);
      if (it != hnode.array_hash().end()) {
        longest_found_prefix = const_iterator(hnode, it);
      }
    }

    return longest_found_prefix;
  }

  std::pair<prefix_iterator, prefix_iterator> equal_prefix_range_impl(
      anode& search_start_node, const CharT* prefix, size_type prefix_size) {
    auto range = static_cast<const htrie_hash*>(this)->equal_prefix_range_impl(
        search_start_node, prefix, prefix_size);
    return std::make_pair(mutable_iterator(range.first),
                          mutable_iterator(range.second));
  }

  std::pair<const_prefix_iterator, const_prefix_iterator>
  equal_prefix_range_impl(const anode& search_start_node, const CharT* prefix,
                          size_type prefix_size) const {
    const anode* current_node = &search_start_node;

    for (size_type iprefix = 0; iprefix < prefix_size; iprefix++) {
      if (current_node->is_trie_node()) {
        const trie_node* tnode = &current_node->as_trie_node();

        if (tnode->child(prefix[iprefix]) == nullptr) {
          return std::make_pair(prefix_cend(), prefix_cend());
        } else {
          current_node = tnode->child(prefix[iprefix]).get();
        }
      } else {
        const hash_node& hnode = current_node->as_hash_node();
        const_prefix_iterator begin(
            hnode.parent(), &hnode, hnode.array_hash().begin(),
            hnode.array_hash().end(), false,
            std::basic_string<CharT>(prefix + iprefix, prefix_size - iprefix));
        begin.filter_prefix();

        const_prefix_iterator end = cend<const_prefix_iterator>(*current_node);

        return std::make_pair(begin, end);
      }
    }

    const_prefix_iterator begin = cbegin<const_prefix_iterator>(*current_node);
    const_prefix_iterator end = cend<const_prefix_iterator>(*current_node);

    return std::make_pair(begin, end);
  }

  size_type erase_prefix_hash_node(hash_node& hnode, const CharT* prefix,
                                   size_type prefix_size) {
    size_type nb_erased = 0;

    auto it = hnode.array_hash().begin();
    while (it != hnode.array_hash().end()) {
      if (it.key_size() >= prefix_size &&
          std::memcmp(prefix, it.key(), prefix_size * sizeof(CharT)) == 0) {
        it = hnode.array_hash().erase(it);
        ++nb_erased;
        --m_nb_elements;
      } else {
        ++it;
      }
    }

    return nb_erased;
  }

  /*
   * Burst
   */
  bool need_burst(hash_node& node) const {
    return node.array_hash().size() >= m_burst_threshold;
  }

  /**
   * Burst the node and use the copy constructor instead of move constructor for
   * the values. Also use this method for trivial value types like int, int*,
   * ... as it requires less book-keeping (thus faster) than the burst using
   * move constructors.
   */
  template <class U = T,
            typename std::enable_if<
                has_value<U>::value && std::is_copy_constructible<U>::value &&
                (!std::is_nothrow_move_constructible<U>::value ||
                 !std::is_nothrow_move_assignable<U>::value ||
                 std::is_arithmetic<U>::value ||
                 std::is_pointer<U>::value)>::type* = nullptr>
  std::unique_ptr<trie_node> burst(hash_node& node) {
    const std::array<size_type, ALPHABET_SIZE> first_char_count =
        get_first_char_count(node.array_hash().cbegin(),
                             node.array_hash().cend());

    auto new_node = make_unique<trie_node>();
    for (auto it = node.array_hash().cbegin(); it != node.array_hash().cend();
         ++it) {
      if (it.key_size() == 0) {
        new_node->val_node() = make_unique<value_node>(it.value());
      } else {
        hash_node& hnode =
            get_hash_node_for_char(first_char_count, *new_node, it.key()[0]);
        hnode.array_hash().insert_ks(it.key() + 1, it.key_size() - 1,
                                     it.value());
      }
    }

    tsl_ht_assert(new_node->val_node() != nullptr || !new_node->empty());
    return new_node;
  }

  /**
   * Burst the node and use the move constructor and move assign operator if
   * they don't throw.
   */
  template <class U = T, typename std::enable_if<
                             has_value<U>::value &&
                             std::is_nothrow_move_constructible<U>::value &&
                             std::is_nothrow_move_assignable<U>::value &&
                             !std::is_arithmetic<U>::value &&
                             !std::is_pointer<U>::value>::type* = nullptr>
  std::unique_ptr<trie_node> burst(hash_node& node) {
    /**
     * We burst the node->array_hash() into multiple arrays hash. While doing
     * so, we move each value in the node->array_hash() into the new arrays
     * hash. After each move, we save a pointer to where the value has been
     * moved. In case of exception, we rollback these values into the original
     * node->array_hash().
     */
    std::vector<T*> moved_values_rollback;
    moved_values_rollback.reserve(node.array_hash().size());

    try {
      const std::array<size_type, ALPHABET_SIZE> first_char_count =
          get_first_char_count(node.array_hash().cbegin(),
                               node.array_hash().cend());

      auto new_node = make_unique<trie_node>();
      for (auto it = node.array_hash().begin(); it != node.array_hash().end();
           ++it) {
        if (it.key_size() == 0) {
          new_node->val_node() = make_unique<value_node>(std::move(it.value()));
          moved_values_rollback.push_back(
              std::addressof(new_node->val_node()->m_value));
        } else {
          hash_node& hnode =
              get_hash_node_for_char(first_char_count, *new_node, it.key()[0]);
          auto it_insert = hnode.array_hash().insert_ks(
              it.key() + 1, it.key_size() - 1, std::move(it.value()));
          moved_values_rollback.push_back(
              std::addressof(it_insert.first.value()));
        }
      }

      tsl_ht_assert(new_node->val_node() != nullptr || !new_node->empty());
      return new_node;
    } catch (...) {
      // Rollback the values
      auto it = node.array_hash().begin();
      for (std::size_t ivalue = 0; ivalue < moved_values_rollback.size();
           ivalue++) {
        it.value() = std::move(*moved_values_rollback[ivalue]);

        ++it;
      }

      throw;
    }
  }

  template <class U = T,
            typename std::enable_if<!has_value<U>::value>::type* = nullptr>
  std::unique_ptr<trie_node> burst(hash_node& node) {
    const std::array<size_type, ALPHABET_SIZE> first_char_count =
        get_first_char_count(node.array_hash().begin(),
                             node.array_hash().end());

    auto new_node = make_unique<trie_node>();
    for (auto it = node.array_hash().cbegin(); it != node.array_hash().cend();
         ++it) {
      if (it.key_size() == 0) {
        new_node->val_node() = make_unique<value_node>();
      } else {
        hash_node& hnode =
            get_hash_node_for_char(first_char_count, *new_node, it.key()[0]);
        hnode.array_hash().insert_ks(it.key() + 1, it.key_size() - 1);
      }
    }

    tsl_ht_assert(new_node->val_node() != nullptr || !new_node->empty());
    return new_node;
  }

  std::array<size_type, ALPHABET_SIZE> get_first_char_count(
      typename array_hash_type::const_iterator begin,
      typename array_hash_type::const_iterator end) const {
    std::array<size_type, ALPHABET_SIZE> count{{}};
    for (auto it = begin; it != end; ++it) {
      if (it.key_size() == 0) {
        continue;
      }

      count[as_position(it.key()[0])]++;
    }

    return count;
  }

  hash_node& get_hash_node_for_char(
      const std::array<size_type, ALPHABET_SIZE>& first_char_count,
      trie_node& tnode, CharT for_char) {
    if (tnode.child(for_char) == nullptr) {
      const size_type nb_buckets =
          size_type(std::ceil(float(first_char_count[as_position(for_char)] +
                                    HASH_NODE_DEFAULT_INIT_BUCKETS_COUNT / 2) /
                              m_max_load_factor));

      tnode.set_child(for_char, make_unique<hash_node>(nb_buckets, m_hash,
                                                       m_max_load_factor));
    }

    return tnode.child(for_char)->as_hash_node();
  }

  iterator mutable_iterator(const_iterator it) noexcept {
    // end iterator or reading from a trie node value
    if (it.m_current_hash_node == nullptr || it.m_read_trie_node_value) {
      typename array_hash_type::iterator default_it;

      return iterator(const_cast<trie_node*>(it.m_current_trie_node), nullptr,
                      default_it, default_it, it.m_read_trie_node_value);
    } else {
      hash_node* hnode = const_cast<hash_node*>(it.m_current_hash_node);
      return iterator(
          const_cast<trie_node*>(it.m_current_trie_node), hnode,
          hnode->array_hash().mutable_iterator(it.m_array_hash_iterator),
          hnode->array_hash().mutable_iterator(it.m_array_hash_end_iterator),
          it.m_read_trie_node_value);
    }
  }

  prefix_iterator mutable_iterator(const_prefix_iterator it) noexcept {
    // end iterator or reading from a trie node value
    if (it.m_current_hash_node == nullptr || it.m_read_trie_node_value) {
      typename array_hash_type::iterator default_it;

      return prefix_iterator(const_cast<trie_node*>(it.m_current_trie_node),
                             nullptr, default_it, default_it,
                             it.m_read_trie_node_value, "");
    } else {
      hash_node* hnode = const_cast<hash_node*>(it.m_current_hash_node);
      return prefix_iterator(
          const_cast<trie_node*>(it.m_current_trie_node), hnode,
          hnode->array_hash().mutable_iterator(it.m_array_hash_iterator),
          hnode->array_hash().mutable_iterator(it.m_array_hash_end_iterator),
          it.m_read_trie_node_value, it.m_prefix_filter);
    }
  }

  template <class Serializer>
  void serialize_impl(Serializer& serializer) const {
    const slz_size_type version = SERIALIZATION_PROTOCOL_VERSION;
    serializer(version);

    const slz_size_type nb_elements = m_nb_elements;
    serializer(nb_elements);

    const float max_load_factor = m_max_load_factor;
    serializer(max_load_factor);

    const slz_size_type burst_threshold = m_burst_threshold;
    serializer(burst_threshold);

    std::basic_string<CharT> str_buffer;

    auto it = begin();
    auto last = end();

    while (it != last) {
      // Serialize trie node value
      if (it.m_read_trie_node_value) {
        const CharT node_type =
            static_cast<typename std::underlying_type<slz_node_type>::type>(
                slz_node_type::TRIE_NODE);
        serializer(&node_type, 1);

        it.key(str_buffer);

        const slz_size_type str_size = str_buffer.size();
        serializer(str_size);
        serializer(str_buffer.data(), str_buffer.size());
        serialize_value(serializer, it);

        ++it;
      }
      // Serialize hash node values
      else {
        const CharT node_type =
            static_cast<typename std::underlying_type<slz_node_type>::type>(
                slz_node_type::HASH_NODE);
        serializer(&node_type, 1);

        it.hash_node_prefix(str_buffer);

        const slz_size_type str_size = str_buffer.size();
        serializer(str_size);
        serializer(str_buffer.data(), str_buffer.size());

        const hash_node* hnode = it.m_current_hash_node;
        tsl_ht_assert(hnode != nullptr);
        hnode->array_hash().serialize(serializer);

        it.skip_hash_node();
      }
    }
  }

  template <class Serializer, class U = T,
            typename std::enable_if<!has_value<U>::value>::type* = nullptr>
  void serialize_value(Serializer& /*serializer*/,
                       const_iterator /*it*/) const {}

  template <class Serializer, class U = T,
            typename std::enable_if<has_value<U>::value>::type* = nullptr>
  void serialize_value(Serializer& serializer, const_iterator it) const {
    serializer(it.value());
  }

  template <class Deserializer>
  void deserialize_impl(Deserializer& deserializer, bool hash_compatible) {
    tsl_ht_assert(m_nb_elements == 0 &&
                  m_root == nullptr);  // Current trie must be empty

    const slz_size_type version =
        deserialize_value<slz_size_type>(deserializer);
    // For now we only have one version of the serialization protocol.
    // If it doesn't match there is a problem with the file.
    if (version != SERIALIZATION_PROTOCOL_VERSION) {
      throw std::runtime_error(
          "Can't deserialize the htrie_map/set. The protocol version header is "
          "invalid.");
    }

    const slz_size_type nb_elements =
        deserialize_value<slz_size_type>(deserializer);
    const float max_load_factor = deserialize_value<float>(deserializer);
    const slz_size_type burst_threshold =
        deserialize_value<slz_size_type>(deserializer);

    this->burst_threshold(numeric_cast<std::size_t>(
        burst_threshold, "Deserialized burst_threshold is too big."));
    this->max_load_factor(max_load_factor);

    std::vector<CharT> str_buffer;
    while (m_nb_elements < nb_elements) {
      CharT node_type_marker;
      deserializer(&node_type_marker, 1);

      static_assert(
          std::is_same<
              CharT, typename std::underlying_type<slz_node_type>::type>::value,
          "");
      const slz_node_type node_type =
          static_cast<slz_node_type>(node_type_marker);
      if (node_type == slz_node_type::TRIE_NODE) {
        const std::size_t str_size = numeric_cast<std::size_t>(
            deserialize_value<slz_size_type>(deserializer),
            "Deserialized str_size is too big.");

        str_buffer.resize(str_size);
        deserializer(str_buffer.data(), str_size);

        trie_node* current_node =
            insert_prefix_trie_nodes(str_buffer.data(), str_size);
        deserialize_value_node(deserializer, current_node);
        m_nb_elements++;
      } else if (node_type == slz_node_type::HASH_NODE) {
        const std::size_t str_size = numeric_cast<std::size_t>(
            deserialize_value<slz_size_type>(deserializer),
            "Deserialized str_size is too big.");

        if (str_size == 0) {
          tsl_ht_assert(m_nb_elements == 0 && !m_root);

          m_root = make_unique<hash_node>(
              array_hash_type::deserialize(deserializer, hash_compatible));
          m_nb_elements += m_root->as_hash_node().array_hash().size();

          tsl_ht_assert(m_nb_elements == nb_elements);
        } else {
          str_buffer.resize(str_size);
          deserializer(str_buffer.data(), str_size);

          auto hnode = make_unique<hash_node>(
              array_hash_type::deserialize(deserializer, hash_compatible));
          m_nb_elements += hnode->array_hash().size();

          trie_node* current_node =
              insert_prefix_trie_nodes(str_buffer.data(), str_size - 1);
          current_node->set_child(str_buffer[str_size - 1], std::move(hnode));
        }
      } else {
        throw std::runtime_error("Unknown deserialized node type.");
      }
    }

    tsl_ht_assert(m_nb_elements == nb_elements);
  }

  trie_node* insert_prefix_trie_nodes(const CharT* prefix,
                                      std::size_t prefix_size) {
    if (m_root == nullptr) {
      m_root = make_unique<trie_node>();
    }

    trie_node* current_node = &m_root->as_trie_node();
    for (std::size_t iprefix = 0; iprefix < prefix_size; iprefix++) {
      if (current_node->child(prefix[iprefix]) == nullptr) {
        current_node->set_child(prefix[iprefix], make_unique<trie_node>());
      }

      current_node = &current_node->child(prefix[iprefix])->as_trie_node();
    }

    return current_node;
  }

  template <class Deserializer, class U = T,
            typename std::enable_if<!has_value<U>::value>::type* = nullptr>
  void deserialize_value_node(Deserializer& /*deserializer*/,
                              trie_node* current_node) {
    tsl_ht_assert(!current_node->val_node());
    current_node->val_node() = make_unique<value_node>();
  }

  template <class Deserializer, class U = T,
            typename std::enable_if<has_value<U>::value>::type* = nullptr>
  void deserialize_value_node(Deserializer& deserializer,
                              trie_node* current_node) {
    tsl_ht_assert(!current_node->val_node());
    current_node->val_node() =
        make_unique<value_node>(deserialize_value<T>(deserializer));
  }

  template <class U, class Deserializer>
  static U deserialize_value(Deserializer& deserializer) {
    // MSVC < 2017 is not conformant, circumvent the problem by removing the
    // template keyword
#if defined(_MSC_VER) && _MSC_VER < 1910
    return deserializer.Deserializer::operator()<U>();
#else
    return deserializer.Deserializer::template operator()<U>();
#endif
  }

  // Same as std::make_unique for non-array types which is only available in
  // C++14 (we need to support C++11).
  template <typename U, typename... Args>
  static std::unique_ptr<U> make_unique(Args&&... args) {
    return std::unique_ptr<U>(new U(std::forward<Args>(args)...));
  }

 public:
  static constexpr float HASH_NODE_DEFAULT_MAX_LOAD_FACTOR = 8.0f;
  static const size_type DEFAULT_BURST_THRESHOLD = 16384;

 private:
  /**
   * Fixed size type used to represent size_type values on serialization. Need
   * to be big enough to represent a std::size_t on 32 and 64 bits platforms,
   * and must be the same size on both platforms.
   */
  using slz_size_type = std::uint64_t;
  enum class slz_node_type : CharT { TRIE_NODE = 0, HASH_NODE = 1 };

  /**
   * Protocol version currenlty used for serialization.
   */
  static const slz_size_type SERIALIZATION_PROTOCOL_VERSION = 1;

  static const size_type HASH_NODE_DEFAULT_INIT_BUCKETS_COUNT = 32;
  static const size_type MIN_BURST_THRESHOLD = 4;

  std::unique_ptr<anode> m_root;
  size_type m_nb_elements;
  Hash m_hash;
  float m_max_load_factor;
  size_type m_burst_threshold;
};

}  // end namespace detail_htrie_hash
}  // end namespace tsl

#endif
