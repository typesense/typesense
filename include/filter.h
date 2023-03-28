#pragma once

#include <string>
#include <map>
#include "posting_list.h"
#include "index.h"

class filter_result_iterator_t {
private:
    std::string collection_name;
    const Index* index;
    filter_node_t* filter_node;
    filter_result_iterator_t* left_it = nullptr;
    filter_result_iterator_t* right_it = nullptr;

    /// Used in case of id and reference filter.
    uint32_t result_index = 0;

    /// Stores the result of the filters that cannot be iterated.
    filter_result_t filter_result;

    /// Initialized in case of filter on string field.
    /// Sample filter values: ["foo bar", "baz"]. Each filter value is split into tokens. We get posting list iterator
    /// for each token.
    ///
    /// Multiple filter values: Multiple tokens: posting list iterator
    std::vector<std::vector<posting_list_t::iterator_t>> posting_list_iterators;
    std::vector<posting_list_t*> expanded_plists;

    /// Set to false when this iterator or it's subtree becomes invalid.
    bool is_valid = true;

    /// Initializes the state of iterator node after it's creation.
    void init();

    /// Performs AND on the subtrees of operator.
    void and_filter_iterators();

    /// Performs OR on the subtrees of operator.
    void or_filter_iterators();

    /// Finds the next match for a filter on string field.
    void doc_matching_string_filter();

public:
    uint32_t seq_id = 0;
    // Collection name -> references
    std::map<std::string, reference_filter_result_t> reference;
    Option<bool> status = Option(true);

    explicit filter_result_iterator_t(const std::string& collection_name,
                                      const Index* index, filter_node_t* filter_node) :
                                      collection_name(collection_name),
                                      index(index),
                                      filter_node(filter_node) {
        // Generate the iterator tree and then initialize each node.
        if (filter_node->isOperator) {
            left_it = new filter_result_iterator_t(collection_name, index, filter_node->left);
            right_it = new filter_result_iterator_t(collection_name, index, filter_node->right);
        }

        init();
    }

    ~filter_result_iterator_t() {
        // In case the filter was on string field.
        for(auto expanded_plist: expanded_plists) {
            delete expanded_plist;
        }

        delete left_it;
        delete right_it;
    }

    /// Returns the status of the initialization of iterator tree.
    Option<bool> init_status();

    /// Returns true when doc and reference hold valid values. Used in conjunction with next() and skip_to(id).
    [[nodiscard]] bool valid();

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

    /// Advances the iterator until the doc value reaches or just overshoots id. The iterator may become invalid during
    /// this operation.
    void skip_to(uint32_t id);

    bool contains_atleast_one(const void* obj);
};