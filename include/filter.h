#pragma once

#include <string>
#include <map>
#include "tsl/htrie_map.h"
#include "json.hpp"
#include "store.h"

#ifdef TEST_BUILD
    constexpr uint32_t COMPUTE_FILTER_ITERATOR_THRESHOLD = 3;
#else
    constexpr uint32_t COMPUTE_FILTER_ITERATOR_THRESHOLD = 25'000;
#endif

constexpr size_t DEFAULT_FILTER_BY_CANDIDATES = 4;

/// For searching places within a given radius of a given latlong (mi for miles and km for kilometers)
static constexpr const char* GEO_FILTER_RADIUS_KEY = "radius";

/// Radius threshold beyond which exact filtering on geo_result_ids will not be done.
static constexpr const char* EXACT_GEO_FILTER_RADIUS_KEY = "exact_filter_radius";
static constexpr double DEFAULT_EXACT_GEO_FILTER_RADIUS_VALUE = 10000; // meters

static const std::string RANGE_OPERATOR = "..";

enum NUM_COMPARATOR {
    LESS_THAN,
    LESS_THAN_EQUALS,
    EQUALS,
    NOT_EQUALS,
    CONTAINS,
    GREATER_THAN,
    GREATER_THAN_EQUALS,
    RANGE_INCLUSIVE
};

enum FILTER_OPERATOR {
    AND,
    OR
};

struct filter_node_t;
struct field;

struct filter {
    std::string field_name{};
    std::vector<std::string> values{};
    std::vector<NUM_COMPARATOR> comparators{};
    // Would be set when `field: != ...` is encountered with id/string field or `field: != [ ... ]` is encountered in the
    // case of int and float fields. During filtering, all the results of matching the field against the values are
    // aggregated and then this flag is checked if negation on the aggregated result is required.
    bool apply_not_equals = false;

    // Would store `Foo` in case of a filter expression like `$Foo(bar := baz)`
    std::string referenced_collection_name{};
    bool is_negate_join = false;

    std::vector<nlohmann::json> params{};

    bool is_ignored_filter = false;
};

struct filter_node_t {
    filter filter_exp;
    FILTER_OPERATOR filter_operator = AND;
    bool isOperator = false;
    filter_node_t* left = nullptr;
    filter_node_t* right = nullptr;
    std::string filter_query;
    bool is_object_filter_root = false;
    std::string object_field_name;

    filter_node_t() = default;

    explicit filter_node_t(filter filter_exp)
            : filter_exp(std::move(filter_exp)),
              isOperator(false),
              left(nullptr),
              right(nullptr) {}

    filter_node_t(FILTER_OPERATOR filter_operator,
                  filter_node_t* left,
                  filter_node_t* right)
            : filter_operator(filter_operator),
              isOperator(true),
              left(left),
              right(right) {}

    ~filter_node_t() {
        delete left;
        delete right;
    }

    filter_node_t& operator=(filter_node_t&& obj) noexcept {
        if (&obj == this) {
            return *this;
        }

        if (obj.isOperator) {
            isOperator = true;
            filter_operator = obj.filter_operator;
            left = obj.left;
            right = obj.right;

            obj.left = nullptr;
            obj.right = nullptr;
        } else {
            isOperator = false;
            filter_exp = obj.filter_exp;
        }

        return *this;
    }

    bool is_match_all_ids_filter() const {
        return !isOperator && filter_exp.field_name == "id" && !filter_exp.values.empty() &&  filter_exp.values[0] == "*";
    }
};

struct reference_filter_result_t {
    uint32_t count = 0;
    uint32_t* docs = nullptr;
    bool is_reference_array_field = true;

    // In case of nested join, references can further have references.
    std::map<std::string, reference_filter_result_t>* coll_to_references = nullptr;

    explicit reference_filter_result_t(uint32_t count = 0, uint32_t* docs = nullptr,
                                       bool is_reference_array_field = true) : count(count), docs(docs),
                                                                               is_reference_array_field(is_reference_array_field) {}

    reference_filter_result_t(const reference_filter_result_t& obj) {
        if (&obj == this) {
            return;
        }

        count = obj.count;
        docs = new uint32_t[count];
        memcpy(docs, obj.docs, count * sizeof(uint32_t));
        is_reference_array_field = obj.is_reference_array_field;

        copy_references(obj, *this);
    }

    reference_filter_result_t& operator=(const reference_filter_result_t& obj) noexcept {
        if (&obj == this) {
            return *this;
        }

        count = obj.count;
        docs = new uint32_t[count];
        memcpy(docs, obj.docs, count * sizeof(uint32_t));
        is_reference_array_field = obj.is_reference_array_field;

        copy_references(obj, *this);
        return *this;
    }

    reference_filter_result_t& operator=(reference_filter_result_t&& obj) noexcept {
        if (&obj == this) {
            return *this;
        }

        count = obj.count;
        docs = obj.docs;
        coll_to_references = obj.coll_to_references;
        is_reference_array_field = obj.is_reference_array_field;

        // Set default values in obj.
        obj.count = 0;
        obj.docs = nullptr;
        obj.coll_to_references = nullptr;
        obj.is_reference_array_field = true;

        return *this;
    }

    ~reference_filter_result_t() {
        delete[] docs;
        delete[] coll_to_references;
    }

    static void copy_references(const reference_filter_result_t& from, reference_filter_result_t& to);

    /// Returns whether at least one common reference doc_id was found or not.
    static bool and_references(const std::map<std::string, reference_filter_result_t>& a_references,
                               const std::map<std::string, reference_filter_result_t>& b_references,
                               std::map<std::string, reference_filter_result_t>& result_references);

    static void or_references(const std::map<std::string, reference_filter_result_t>& a_references,
                              const std::map<std::string, reference_filter_result_t>& b_references,
                              std::map<std::string, reference_filter_result_t>& result_references);
};

struct single_filter_result_t {
    uint32_t seq_id = 0;
    // Collection name -> Reference filter result
    std::map<std::string, reference_filter_result_t> reference_filter_results = {};
    bool is_reference_array_field = true;

    single_filter_result_t() = default;

    single_filter_result_t(uint32_t seq_id, std::map<std::string, reference_filter_result_t>&& reference_filter_results,
                           bool is_reference_array_field = true) :
            seq_id(seq_id), reference_filter_results(std::move(reference_filter_results)),
            is_reference_array_field(is_reference_array_field) {}

    single_filter_result_t(const single_filter_result_t& obj) {
        if (&obj == this) {
            return;
        }

        seq_id = obj.seq_id;
        is_reference_array_field = obj.is_reference_array_field;

        // Copy every collection's reference.
        for (const auto &item: obj.reference_filter_results) {
            auto& ref_coll_name = item.first;
            reference_filter_results[ref_coll_name] = item.second;
        }
    }

    single_filter_result_t(single_filter_result_t&& obj) {
        if (&obj == this) {
            return;
        }

        seq_id = obj.seq_id;
        is_reference_array_field = obj.is_reference_array_field;
        reference_filter_results = std::move(obj.reference_filter_results);
    }

    single_filter_result_t& operator=(const single_filter_result_t& obj) noexcept {
        if (&obj == this) {
            return *this;
        }

        seq_id = obj.seq_id;
        is_reference_array_field = obj.is_reference_array_field;

        // Copy every collection's reference.
        for (const auto &item: obj.reference_filter_results) {
            auto& ref_coll_name = item.first;
            reference_filter_results[ref_coll_name] = item.second;
        }

        return *this;
    }

    single_filter_result_t& operator=(single_filter_result_t&& obj) noexcept {
        if (&obj == this) {
            return *this;
        }

        seq_id = obj.seq_id;
        is_reference_array_field = obj.is_reference_array_field;
        reference_filter_results = std::move(obj.reference_filter_results);

        return *this;
    }
};

struct filter_result_t {
    uint32_t count = 0;
    uint32_t* docs = nullptr;
    // Collection name -> Reference filter result
    std::map<std::string, reference_filter_result_t>* coll_to_references = nullptr;

    filter_result_t() = default;

    filter_result_t(uint32_t count, uint32_t* docs,
                    std::map<std::string, reference_filter_result_t>* coll_to_references = nullptr) :
            count(count), docs(docs), coll_to_references(coll_to_references) {}

    filter_result_t(const filter_result_t& obj) {
        if (&obj == this) {
            return;
        }

        count = obj.count;
        docs = new uint32_t[count];
        memcpy(docs, obj.docs, count * sizeof(uint32_t));

        copy_references(obj, *this);
    }

    filter_result_t& operator=(const filter_result_t& obj) noexcept {
        if (&obj == this) {
            return *this;
        }

        delete[] docs;
        delete[] coll_to_references;

        count = obj.count;
        docs = new uint32_t[count];
        memcpy(docs, obj.docs, count * sizeof(uint32_t));

        copy_references(obj, *this);

        return *this;
    }

    filter_result_t& operator=(filter_result_t&& obj) noexcept {
        if (&obj == this) {
            return *this;
        }

        delete[] docs;
        delete[] coll_to_references;

        count = obj.count;
        docs = obj.docs;
        coll_to_references = obj.coll_to_references;

        // Set default values in obj.
        obj.count = 0;
        obj.docs = nullptr;
        obj.coll_to_references = nullptr;

        return *this;
    }

    ~filter_result_t() {
        delete[] docs;
        delete[] coll_to_references;
    }

    static void and_filter_results(const filter_result_t& a, const filter_result_t& b, filter_result_t& result);

    static void or_filter_results(const filter_result_t& a, const filter_result_t& b, filter_result_t& result);

    static void copy_references(const filter_result_t& from, filter_result_t& to);
};

Option<bool> parse_filter_query(const std::string& filter_query,
                                const tsl::htrie_map<char, field>& search_schema,
                                const Store* store,
                                const std::string& doc_id_prefix,
                                filter_node_t*& root,
                                const bool& validate_field_names = true,
                                const std::string& object_field_prefix = "");

Option<bool> tokenize_filter_query(const std::string& filter_query, std::queue<std::string>& tokens);

Option<bool> parse_filter_string(const std::string& filter_query, std::string& token, size_t& index);
