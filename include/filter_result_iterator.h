#pragma once

#include <string>
#include <map>
#include <utility>
#include <vector>
#include "option.h"
#include "posting_list.h"

class Index;
struct filter_node_t;

struct reference_filter_result_t {
    uint32_t count = 0;
    uint32_t* docs = nullptr;

    reference_filter_result_t& operator=(const reference_filter_result_t& obj) noexcept {
        if (&obj == this)
            return *this;

        count = obj.count;
        docs = new uint32_t[count];
        memcpy(docs, obj.docs, count * sizeof(uint32_t));

        return *this;
    }

    ~reference_filter_result_t() {
        delete[] docs;
    }
};

struct single_filter_result_t {
    uint32_t seq_id = 0;
    // Collection name -> Reference filter result
    std::map<std::string, reference_filter_result_t> reference_filter_results = {};

    single_filter_result_t() = default;

    single_filter_result_t(uint32_t seq_id, std::map<std::string, reference_filter_result_t>&& reference_filter_results) :
                            seq_id(seq_id), reference_filter_results(std::move(reference_filter_results)) {}

    single_filter_result_t(const single_filter_result_t& obj) {
        if (&obj == this)
            return;

        seq_id = obj.seq_id;

        // Copy every collection's reference.
        for (const auto &item: obj.reference_filter_results) {
            auto& ref_coll_name = item.first;
            reference_filter_results[ref_coll_name] = item.second;
        }
    }
};

struct filter_result_t {
    uint32_t count = 0;
    uint32_t* docs = nullptr;
    // Collection name -> Reference filter result
    std::map<std::string, reference_filter_result_t*> reference_filter_results;

    filter_result_t() = default;

    filter_result_t(uint32_t count, uint32_t* docs) : count(count), docs(docs) {}

    filter_result_t(const filter_result_t& obj) {
        if (&obj == this)
            return;

        count = obj.count;
        docs = new uint32_t[count];
        memcpy(docs, obj.docs, count * sizeof(uint32_t));

        // Copy every collection's references.
        for (const auto &item: obj.reference_filter_results) {
            auto& ref_coll_name = item.first;
            reference_filter_results[ref_coll_name] = new reference_filter_result_t[count];
            for (uint32_t i = 0; i < count; i++) {
                reference_filter_results[ref_coll_name][i] = item.second[i];
            }
        }
    }

    filter_result_t& operator=(const filter_result_t& obj) noexcept {
        if (&obj == this)
            return *this;

        count = obj.count;
        docs = new uint32_t[count];
        memcpy(docs, obj.docs, count * sizeof(uint32_t));

        // Copy every collection's references.
        for (const auto &item: obj.reference_filter_results) {
            reference_filter_results[item.first] = new reference_filter_result_t[count];

            for (uint32_t i = 0; i < count; i++) {
                reference_filter_results[item.first][i] = item.second[i];
            }
        }

        return *this;
    }

    filter_result_t& operator=(filter_result_t&& obj) noexcept {
        if (&obj == this)
            return *this;

        count = obj.count;
        docs = obj.docs;
        reference_filter_results = std::map(obj.reference_filter_results);

        obj.docs = nullptr;
        obj.reference_filter_results.clear();

        return *this;
    }

    ~filter_result_t() {
        delete[] docs;
        for (const auto &item: reference_filter_results) {
            delete[] item.second;
        }
    }

    static void and_filter_results(const filter_result_t& a, const filter_result_t& b, filter_result_t& result);

    static void or_filter_results(const filter_result_t& a, const filter_result_t& b, filter_result_t& result);
};

class filter_result_iterator_t {
private:
    std::string collection_name;
    const Index* index = nullptr;
    const filter_node_t* filter_node = nullptr;
    filter_result_iterator_t* left_it = nullptr;
    filter_result_iterator_t* right_it = nullptr;

    /// Used in case of id and reference filter.
    uint32_t result_index = 0;

    /// Stores the result of the filters that cannot be iterated.
    filter_result_t filter_result;
    bool is_filter_result_initialized = false;

    /// Initialized in case of filter on string field.
    /// Sample filter values: ["foo bar", "baz"]. Each filter value is split into tokens. We get posting list iterator
    /// for each token.
    ///
    /// Multiple filter values: Multiple tokens: posting list iterator
    std::vector<std::vector<posting_list_t*>> posting_lists;
    std::vector<std::vector<posting_list_t::iterator_t>> posting_list_iterators;
    std::vector<posting_list_t*> expanded_plists;

    bool delete_filter_node = false;

    /// Initializes the state of iterator node after it's creation.
    void init();

    /// Performs AND on the subtrees of operator.
    void and_filter_iterators();

    /// Performs OR on the subtrees of operator.
    void or_filter_iterators();

    /// Advance all the token iterators that are at seq_id.
    void advance_string_filter_token_iterators();

    /// Finds the first match for a filter on string field.
    void get_string_filter_first_match(const bool& field_is_array);

    /// Finds the next match for a filter on string field.
    void get_string_filter_next_match(const bool& field_is_array);

    explicit filter_result_iterator_t(uint32_t approx_filter_ids_length);

    /// Collects n doc ids while advancing the iterator. The iterator may become invalid during this operation.
    void get_n_ids(const uint32_t& n, filter_result_t& result);

public:
    uint32_t seq_id = 0;
    /// Collection name -> references
    std::map<std::string, reference_filter_result_t> reference;

    /// Set to false when this iterator or it's subtree becomes invalid.
    bool is_valid = true;

    /// Initialization status of the iterator.
    Option<bool> status = Option(true);

    /// Holds the upper-bound of the number of seq ids this iterator would match.
    /// Useful in a scenario where we need to differentiate between filter iterator not matching any document v/s filter
    /// iterator reaching it's end. (is_valid would be false in both these cases)
    uint32_t approx_filter_ids_length = 0;

    explicit filter_result_iterator_t(uint32_t* ids, const uint32_t& ids_count);

    explicit filter_result_iterator_t(const std::string collection_name,
                                      Index const* const index, filter_node_t const* const filter_node);

    ~filter_result_iterator_t();

    filter_result_iterator_t& operator=(filter_result_iterator_t&& obj) noexcept;

    /// Returns the status of the initialization of iterator tree.
    Option<bool> init_status();

    /// Recursively computes the result of each node and stores the final result in the root node.
    void compute_result();

    /// Returns a tri-state:
    ///     0: id is not valid
    ///     1: id is valid
    ///    -1: end of iterator
    ///
    ///  Handles moving the individual iterators internally.
    [[nodiscard]] int valid(uint32_t id);

    /// Advances the iterator to get the next value of doc and reference. The iterator may become invalid during this
    /// operation.
    void next();

    /// Collects n doc ids while advancing the iterator. The ids present in excluded_result_ids are ignored. The
    /// iterator may become invalid during this operation.
    void get_n_ids(const uint32_t& n,
                   uint32_t& excluded_result_index,
                   uint32_t const* const excluded_result_ids, const size_t& excluded_result_ids_size,
                   filter_result_t& result);

    /// Advances the iterator until the doc value reaches or just overshoots id. The iterator may become invalid during
    /// this operation.
    void skip_to(uint32_t id);

    /// Returns true if at least one id from the posting list object matches the filter.
    bool contains_atleast_one(const void* obj);

    /// Returns to the initial state of the iterator.
    void reset();

    /// Iterates and collects all the filter ids into filter_array.
    /// \return size of the filter array
    uint32_t to_filter_id_array(uint32_t*& filter_array);

    /// Performs AND with the contents of A and allocates a new array of results.
    /// \return size of the results array
    uint32_t and_scalar(const uint32_t* A, const uint32_t& lenA, uint32_t*& results);

    void and_scalar(const uint32_t* A, const uint32_t& lenA, filter_result_t& result);

    static void add_phrase_ids(filter_result_iterator_t*& filter_result_iterator,
                               uint32_t* phrase_result_ids, const uint32_t& phrase_result_count);
};
