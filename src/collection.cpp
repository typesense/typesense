#include "collection.h"

#include <numeric>
#include <chrono>
#include <array_utils.h>
#include <match_score.h>
#include <string_utils.h>
#include <art.h>

Collection::Collection(const std::string name, const uint32_t collection_id, const uint32_t next_seq_id, Store *store,
                       const std::vector<field> &search_fields, const std::vector<field> & facet_fields,
                       const std::vector<field> & sort_fields, const std::string token_ordering_field):
                       name(name), collection_id(collection_id), next_seq_id(next_seq_id), store(store),
                       sort_fields(sort_fields), token_ordering_field(token_ordering_field) {

    for(const field& field: search_fields) {
        art_tree *t = new art_tree;
        art_tree_init(t);
        search_index.emplace(field.name, t);
        search_schema.emplace(field.name, field);
    }

    for(const field& field: facet_fields) {
        art_tree *t = new art_tree;
        art_tree_init(t);
        facet_index.emplace(field.name, t);
        facet_schema.emplace(field.name, field);
    }

    for(const field & sort_field: sort_fields) {
        spp::sparse_hash_map<uint32_t, int64_t> * doc_to_score = new spp::sparse_hash_map<uint32_t, int64_t>();
        sort_index.emplace(sort_field.name, doc_to_score);
    }
}

Collection::~Collection() {
    for(std::pair<std::string, field> name_field: search_schema) {
        art_tree *t = search_index.at(name_field.first);
        art_tree_destroy(t);
        t = nullptr;
    }

    for(std::pair<std::string, field> name_field: facet_schema) {
        art_tree *t = facet_index.at(name_field.first);
        art_tree_destroy(t);
        t = nullptr;
    }

    for(std::pair<std::string, spp::sparse_hash_map<uint32_t, int64_t>*> name_map: sort_index) {
        delete name_map.second;
    }
}

uint32_t Collection::get_next_seq_id() {
    store->increment(get_next_seq_id_key(name), 1);
    return next_seq_id++;
}

Option<std::string> Collection::add(std::string json_str) {
    nlohmann::json document = nlohmann::json::parse(json_str);

    uint32_t seq_id = get_next_seq_id();
    std::string seq_id_str = std::to_string(seq_id);

    if(document.count("id") == 0) {
        document["id"] = seq_id_str;
    }

    const Option<uint32_t> & index_memory_op = index_in_memory(document, seq_id);

    if(!index_memory_op.ok()) {
        return Option<std::string>(index_memory_op.code(), index_memory_op.error());
    }

    store->insert(get_seq_id_key(seq_id), document.dump());
    store->insert(get_doc_id_key(document["id"]), seq_id_str);

    std::string doc_id = document["id"];
    return Option<std::string>(doc_id);
}

Option<uint32_t> Collection::index_in_memory(const nlohmann::json &document, uint32_t seq_id) {
    if(!token_ordering_field.empty() && document.count(token_ordering_field) == 0) {
        return Option<>(400, "Field `" + token_ordering_field  + "` has been declared as a token ordering field, "
                        "but is not found in the document.");
    }

    if(!token_ordering_field.empty() && !document[token_ordering_field].is_number()) {
        return Option<>(400, "Token ordering field `" + token_ordering_field  + "` must be an INT32.");
    }

    if(!token_ordering_field.empty() && document[token_ordering_field].get<int64_t>() > INT32_MAX) {
        return Option<>(400, "Token ordering field `" + token_ordering_field  + "` exceeds maximum value of INT32.");
    }

    uint32_t points = 0;
    if(!token_ordering_field.empty()) {
        points = document[token_ordering_field];
    }

    for(const std::pair<std::string, field> & field_pair: search_schema) {
        const std::string & field_name = field_pair.first;

        if(document.count(field_name) == 0) {
            return Option<>(400, "Field `" + field_name  + "` has been declared as a search field in the schema, "
                            "but is not found in the document.");
        }

        art_tree *t = search_index.at(field_name);

        if(field_pair.second.type == field_types::STRING) {
            if(!document[field_name].is_string()) {
                return Option<>(400, "Search field `" + field_name  + "` must be a STRING.");
            }
            const std::string & text = document[field_name];
            index_string_field(text, points, t, seq_id, false);
        } else if(field_pair.second.type == field_types::INT32) {
            if(!document[field_name].is_number()) {
                return Option<>(400, "Search field `" + field_name  + "` must be an INT32.");
            }

            if(document[field_name].get<int64_t>() > INT32_MAX) {
                return Option<>(400, "Search field `" + field_name  + "` exceeds maximum value of INT32.");
            }

            uint32_t value = document[field_name];
            index_int32_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::INT64) {
            if(!document[field_name].is_number()) {
                return Option<>(400, "Search field `" + field_name  + "` must be an INT64.");
            }

            uint64_t value = document[field_name];
            index_int64_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::STRING_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Search field `" + field_name  + "` must be a STRING_ARRAY.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_string()) {
                return Option<>(400, "Search field `" + field_name  + "` must be a STRING_ARRAY.");
            }

            std::vector<std::string> strings = document[field_name];
            index_string_array_field(strings, points, t, seq_id, false);
        } else if(field_pair.second.type == field_types::INT32_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Search field `" + field_name  + "` must be an INT32_ARRAY.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_number()) {
                return Option<>(400, "Search field `" + field_name  + "` must be an INT32_ARRAY.");
            }

            std::vector<int32_t> values = document[field_name];
            index_int32_array_field(values, points, t, seq_id);
        } else if(field_pair.second.type == field_types::INT64_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Search field `" + field_name  + "` must be an INT64_ARRAY.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_number()) {
                return Option<>(400, "Search field `" + field_name  + "` must be an INT64_ARRAY.");
            }

            std::vector<int64_t> values = document[field_name];
            index_int64_array_field(values, points, t, seq_id);
        }
    }

    for(const std::pair<std::string, field> & field_pair: facet_schema) {
        const std::string & field_name = field_pair.first;

        if(document.count(field_name) == 0) {
            return Option<>(400, "Field `" + field_name  + "` has been declared as a facet field in the schema, "
                            "but is not found in the document.");
        }

        art_tree *t = facet_index.at(field_name);
        if(field_pair.second.type == field_types::STRING) {
            if(!document[field_name].is_string()) {
                return Option<>(400, "Facet field `" + field_name  + "` must be a STRING.");
            }
            const std::string & text = document[field_name];
            index_string_field(text, points, t, seq_id, true);
        } else if(field_pair.second.type == field_types::STRING_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Facet field `" + field_name  + "` must be a STRING_ARRAY.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_string()) {
                return Option<>(400, "Facet field `" + field_name  + "` must be a STRING_ARRAY.");
            }

            std::vector<std::string> strings = document[field_name];
            index_string_array_field(strings, points, t, seq_id, true);
        }
    }

    for(const field & sort_field: sort_fields) {
        if(document.count(sort_field.name) == 0) {
            return Option<>(400, "Field `" + sort_field.name  + "` has been declared as a sort field in the schema, "
                    "but is not found in the document.");
        }

        if(!document[sort_field.name].is_number()) {
            return Option<>(400, "Sort field `" + sort_field.name  + "` must be a number.");
        }

        spp::sparse_hash_map<uint32_t, int64_t> *doc_to_score = sort_index.at(sort_field.name);
        doc_to_score->emplace(seq_id, document[sort_field.name].get<int64_t>());
    }

    return Option<>(200);
}

void Collection::index_int32_field(const int32_t value, uint32_t score, art_tree *t, uint32_t seq_id) const {
    const int KEY_LEN = 8;
    unsigned char key[KEY_LEN];

    encode_int32(value, key);

    uint32_t num_hits = 0;
    art_leaf* leaf = (art_leaf *) art_search(t, key, KEY_LEN);
    if(leaf != NULL) {
        num_hits = leaf->values->ids.getLength();
    }

    num_hits += 1;

    art_document art_doc;
    art_doc.id = seq_id;
    art_doc.score = score;
    art_doc.offsets_len = 0;
    art_doc.offsets = nullptr;

    art_insert(t, key, KEY_LEN, &art_doc, num_hits);
}

void Collection::index_int64_field(const int64_t value, uint32_t score, art_tree *t, uint32_t seq_id) const {
    const int KEY_LEN = 8;
    unsigned char key[KEY_LEN];

    encode_int64(value, key);

    uint32_t num_hits = 0;
    art_leaf* leaf = (art_leaf *) art_search(t, key, KEY_LEN);
    if(leaf != NULL) {
        num_hits = leaf->values->ids.getLength();
    }

    num_hits += 1;

    art_document art_doc;
    art_doc.id = seq_id;
    art_doc.score = score;
    art_doc.offsets_len = 0;
    art_doc.offsets = nullptr;

    art_insert(t, key, KEY_LEN, &art_doc, num_hits);
}

void Collection::index_string_field(const std::string & text, const uint32_t score, art_tree *t,
                                    uint32_t seq_id, const bool verbatim) const {
    std::vector<std::string> tokens;
    std::unordered_map<std::string, std::vector<uint32_t>> token_to_offsets;

    if(verbatim) {
        tokens.push_back(text);
        token_to_offsets[text].push_back(0);
    } else {
        StringUtils::tokenize(text, tokens, " ", true);
        for(uint32_t i=0; i<tokens.size(); i++) {
            auto token = tokens[i];
            transform(token.begin(), token.end(), token.begin(), tolower);
            token_to_offsets[token].push_back(i);
        }
    }

    for(auto & kv: token_to_offsets) {
        art_document art_doc;
        art_doc.id = seq_id;
        art_doc.score = score;
        art_doc.offsets_len = (uint32_t) kv.second.size();
        art_doc.offsets = new uint32_t[kv.second.size()];

        uint32_t num_hits = 0;

        const unsigned char *key = (const unsigned char *) kv.first.c_str();
        int key_len = (int) kv.first.length() + 1;  // for the terminating \0 char

        art_leaf* leaf = (art_leaf *) art_search(t, key, key_len);
        if(leaf != NULL) {
            num_hits = leaf->values->ids.getLength();
        }

        num_hits += 1;

        for(auto i=0; i<kv.second.size(); i++) {
            art_doc.offsets[i] = kv.second[i];
        }

        art_insert(t, key, key_len, &art_doc, num_hits);
        delete art_doc.offsets;
    }
}

void Collection::index_string_array_field(const std::vector<std::string> & strings, const uint32_t score, art_tree *t,
                                          uint32_t seq_id, const bool verbatim) const {
    for(const std::string & str: strings) {
        index_string_field(str, score, t, seq_id, verbatim);
    }
}

void Collection::index_int32_array_field(const std::vector<int32_t> & values, const uint32_t score, art_tree *t,
                                         uint32_t seq_id) const {
    for(const int32_t value: values) {
        index_int32_field(value, score, t, seq_id);
    }
}

void Collection::index_int64_array_field(const std::vector<int64_t> & values, const uint32_t score, art_tree *t,
                                         uint32_t seq_id) const {
    for(const int64_t value: values) {
        index_int64_field(value, score, t, seq_id);
    }
}

void Collection::do_facets(std::vector<facet> & facets, uint32_t* result_ids, size_t results_size) {
    for(auto & a_facet: facets) {
        // assumed that facet fields have already been validated upstream
        const field & facet_field = facet_schema.at(a_facet.field_name);

        // loop through the field, get all keys and intersect those ids with result ids
        if(facet_index.count(facet_field.name) != 0) {
            art_tree *t = facet_index.at(facet_field.name);
            std::vector<art_leaf *> leaves;

            art_topk_iter(t->root, MAX_SCORE, 10, leaves);

            for(const art_leaf* leaf: leaves) {
                const uint32_t* facet_ids = leaf->values->ids.uncompress();
                size_t facet_ids_size = leaf->values->ids.getLength();

                uint32_t* facet_results = new uint32_t[std::min(facet_ids_size, results_size)];
                const size_t facet_results_size = ArrayUtils::and_scalar(result_ids, results_size,
                                                                           facet_ids, facet_ids_size,
                                                                           facet_results);

                const std::string facet_value((const char *)leaf->key, leaf->key_len-1); // drop trailing null
                a_facet.result_map.insert(std::pair<std::string, size_t>(facet_value, facet_results_size));

                delete [] facet_ids;
                delete [] facet_results;
            }
        }
    }
}

void Collection::search_candidates(uint32_t* filter_ids, size_t filter_ids_length, std::vector<facet> & facets,
                                   const std::vector<sort_field> & sort_fields, int & token_rank,
                                   std::vector<std::vector<art_leaf*>> & token_leaves, Topster<100> & topster,
                                   size_t & total_results, uint32_t** all_result_ids, size_t & all_result_ids_len,
                                   const size_t & max_results) {
    const size_t combination_limit = 10;
    auto product = []( long long a, std::vector<art_leaf*>& b ) { return a*b.size(); };
    long long int N = std::accumulate(token_leaves.begin(), token_leaves.end(), 1LL, product);

    for(long long n=0; n<N && n<combination_limit; ++n) {
        // every element in `query_suggestion` contains a token and its associated hits
        std::vector<art_leaf *> query_suggestion = next_suggestion(token_leaves, n);
        token_rank++;

        // initialize results with the starting element (for further intersection)
        uint32_t* result_ids = query_suggestion[0]->values->ids.uncompress();
        size_t result_size = query_suggestion[0]->values->ids.getLength();

        if(result_size == 0) continue;

        // intersect the document ids for each token to find docs that contain all the tokens (stored in `result_ids`)
        for(auto i=1; i < query_suggestion.size(); i++) {
            uint32_t* out = nullptr;
            result_size = query_suggestion[i]->values->ids.intersect(result_ids, result_size, &out);
            delete[] result_ids;
            result_ids = out;
        }

        if(filter_ids != nullptr) {
            // intersect once again with filter ids
            uint32_t* filtered_result_ids = new uint32_t[std::min(filter_ids_length, result_size)];
            size_t filtered_results_size = ArrayUtils::and_scalar(filter_ids, filter_ids_length, result_ids,
                                                                  result_size, filtered_result_ids);

            uint32_t* new_all_result_ids;
            all_result_ids_len = ArrayUtils::or_scalar(*all_result_ids, all_result_ids_len, filtered_result_ids,
                                  filtered_results_size, &new_all_result_ids);
            delete [] *all_result_ids;
            *all_result_ids = new_all_result_ids;

            do_facets(facets, filtered_result_ids, filtered_results_size);

            // go through each matching document id and calculate match score
            score_results(sort_fields, token_rank, topster, query_suggestion, filtered_result_ids, filtered_results_size);

            delete[] filtered_result_ids;
            delete[] result_ids;
        } else {
            do_facets(facets, result_ids, result_size);

            uint32_t* new_all_result_ids;
            all_result_ids_len = ArrayUtils::or_scalar(*all_result_ids, all_result_ids_len, result_ids,
                                  result_size, &new_all_result_ids);
            delete [] *all_result_ids;
            *all_result_ids = new_all_result_ids;

            score_results(sort_fields, token_rank, topster, query_suggestion, result_ids, result_size);
            delete[] result_ids;
        }

        total_results += topster.size;

        if(total_results >= max_results) {
            break;
        }
    }
}

size_t Collection::union_of_leaf_ids(std::vector<const art_leaf *> &leaves, uint32_t **results_out) {
    uint32_t *results = nullptr;
    size_t results_length = 0;

    uint32_t *prev_results = nullptr;
    size_t prev_results_length = 0;

    for(const art_leaf* leaf: leaves) {
        results_length = leaf->values->ids.do_union(prev_results, prev_results_length, &results);

        delete [] prev_results;
        prev_results = results;
        prev_results_length = results_length;
    }

    *results_out = results;
    return results_length;
}

Option<uint32_t> Collection::do_filtering(uint32_t** filter_ids_out, const std::string & simple_filter_str) {
    // parse the filter string
    std::vector<std::string> filter_blocks;
    StringUtils::split(simple_filter_str, filter_blocks, "&&");

    std::vector<filter> filters;

    for(const std::string & filter_block: filter_blocks) {
        // split into [field_name, value]
        std::vector<std::string> expression_parts;
        StringUtils::split(filter_block, expression_parts, ":");
        if(expression_parts.size() != 2) {
            return Option<>(400, "Could not parse the filter query.");
        }

        const std::string & field_name = expression_parts[0];
        if(search_schema.count(field_name) == 0) {
            return Option<>(400, "Could not find a filter field named `" + field_name + "` in the schema.");
        }

        field _field = search_schema.at(field_name);
        const std::string & raw_value = expression_parts[1];
        filter f;

        if(_field.integer()) {
            // could be a single value or a list
            if(raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
                std::vector<std::string> filter_values;
                StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, ",");

                for(const std::string & filter_value: filter_values) {
                    if(!StringUtils::is_integer(filter_value)) {
                        return Option<>(400, "Error with field `" + _field.name + "`: Not an integer.");
                    }
                }

                f = {field_name, filter_values, EQUALS};
            } else {
                Option<NUM_COMPARATOR> op_comparator = filter::extract_num_comparator(raw_value);
                if(!op_comparator.ok()) {
                    return Option<>(400, "Error with field `" + _field.name + "`: " + op_comparator.error());
                }

                // extract numerical value
                std::string filter_value;
                if(op_comparator.get() == LESS_THAN || op_comparator.get() == GREATER_THAN) {
                    filter_value = raw_value.substr(1);
                } else if(op_comparator.get() == LESS_THAN_EQUALS || op_comparator.get() == GREATER_THAN_EQUALS) {
                    filter_value = raw_value.substr(2);
                } else {
                    // EQUALS
                    filter_value = raw_value;
                }

                filter_value = StringUtils::trim(filter_value);

                if(!StringUtils::is_integer(filter_value)) {
                    return Option<>(400, "Error with field `" + _field.name + "`: Not an integer.");
                }

                f = {field_name, {filter_value}, op_comparator.get()};
            }
        } else {
            if(raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
                std::vector<std::string> filter_values;
                StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, ",");
                f = {field_name, filter_values, EQUALS};
            } else {
                f = {field_name, {raw_value}, EQUALS};
            }
        }

        filters.push_back(f);
    }

    uint32_t* filter_ids = nullptr;
    uint32_t filter_ids_length = 0;

    // process the filters first
    for(const filter & a_filter: filters) {
        if(search_index.count(a_filter.field_name) != 0) {
            art_tree* t = search_index.at(a_filter.field_name);
            field f = search_schema.at(a_filter.field_name);
            std::vector<const art_leaf*> leaves;

            if(f.integer()) {
                for(const std::string & filter_value: a_filter.values) {
                    if(f.type == field_types::INT32 || f.type == field_types::INT32_ARRAY) {
                        int32_t value = (int32_t) std::stoi(filter_value);
                        art_int32_search(t, value, a_filter.compare_operator, leaves);
                    } else {
                        int64_t value = (int64_t) std::stoi(filter_value);
                        art_int64_search(t, value, a_filter.compare_operator, leaves);
                    }
                }
            } else if(f.type == field_types::STRING || f.type == field_types::STRING_ARRAY) {
                for(const std::string & filter_value: a_filter.values) {
                    art_leaf* leaf = (art_leaf *) art_search(t, (const unsigned char*) filter_value.c_str(), filter_value.length()+1);
                    if(leaf != nullptr) {
                        leaves.push_back(leaf);
                    }
                }
            }

            uint32_t* result_ids = nullptr;
            size_t result_ids_length = union_of_leaf_ids(leaves, &result_ids);

            if(filter_ids == nullptr) {
                filter_ids = result_ids;
                filter_ids_length = result_ids_length;
            } else {
                uint32_t* filtered_results = new uint32_t[std::min((size_t)filter_ids_length, result_ids_length)];
                filter_ids_length = ArrayUtils::and_scalar(filter_ids, filter_ids_length, result_ids,
                                                             result_ids_length, filtered_results);
                delete [] filter_ids;
                delete [] result_ids;
                filter_ids = filtered_results;
            }
        }
    }

    *filter_ids_out = filter_ids;
    return Option<>(filter_ids_length);
}

nlohmann::json Collection::search(std::string query, const std::vector<std::string> search_fields,
                                  const std::string & simple_filter_query, const std::vector<std::string> & facet_fields,
                                  const std::vector<sort_field> & sort_fields, const int num_typos,
                                  const size_t num_results, const token_ordering token_order, const bool prefix) {
    nlohmann::json result = nlohmann::json::object();
    std::vector<facet> facets;

    // validate search fields
    for(const std::string & field_name: search_fields) {
        if(search_schema.count(field_name) == 0) {
            result["error"] = "Could not find a search field named `" + field_name + "` in the schema.";
            return result;
        }

        field search_field = search_schema.at(field_name);
        if(search_field.type != field_types::STRING && search_field.type != field_types::STRING_ARRAY) {
            result["error"] = "Search field `" + field_name + "` should be a string or a string array.";
            return result;
        }
    }

    // validate facet fields
    for(const std::string & field_name: facet_fields) {
        if(facet_schema.count(field_name) == 0) {
            result["error"] = "Could not find a facet field named `" + field_name + "` in the schema.";
            return result;
        }
        facets.push_back(facet(field_name));
    }

    // validate sort fields
    for(const sort_field & _sort_field: sort_fields) {
        if(sort_index.count(_sort_field.name) == 0) {
            result["error"] = "Could not find a sort field named `" + _sort_field.name + "` in the schema.";
            return result;
        }

        if(_sort_field.order != sort_field_const::asc && _sort_field.order != sort_field_const::desc) {
            result["error"] = "Order for sort field` " + _sort_field.name + "` should be either ASC or DESC.";
            return result;
        }
    }

    // process the filters first
    uint32_t* filter_ids = nullptr;
    Option<uint32_t> op_filter_ids_length = do_filtering(&filter_ids, simple_filter_query);
    if(!op_filter_ids_length.ok()) {
        result["error"] = op_filter_ids_length.error();
        return result;
    }

    const uint32_t filter_ids_length = op_filter_ids_length.get();

    // Order of `fields` are used to sort results
    auto begin = std::chrono::high_resolution_clock::now();
    std::vector<std::pair<int, Topster<100>::KV>> field_order_kvs;
    uint32_t* all_result_ids = nullptr;
    size_t all_result_ids_len = 0;

    for(int i = 0; i < search_fields.size(); i++) {
        Topster<100> topster;
        const std::string & field = search_fields[i];
        // proceed to query search only when no filters are provided or when filtering produces results
        if(simple_filter_query.size() == 0 || filter_ids_length > 0) {
            search_field(query, field, filter_ids, filter_ids_length, facets, sort_fields, num_typos, num_results,
                         topster, &all_result_ids, all_result_ids_len, token_order, prefix);
            topster.sort();
        }

        for(auto t = 0; t < topster.size && t < num_results; t++) {
            field_order_kvs.push_back(std::make_pair(search_fields.size() - i, topster.getKV(t)));
        }
    }

    delete [] filter_ids;

    // All fields are sorted descending
    std::sort(field_order_kvs.begin(), field_order_kvs.end(),
      [](const std::pair<int, Topster<100>::KV> & a, const std::pair<int, Topster<100>::KV> & b) {
        if(a.second.match_score != b.second.match_score) return a.second.match_score > b.second.match_score;
        if(a.second.primary_attr != b.second.primary_attr) return a.second.primary_attr > b.second.primary_attr;
        if(a.second.secondary_attr != b.second.secondary_attr) return a.second.secondary_attr > b.second.secondary_attr;
        if(a.first != b.first) return a.first > b.first;
        return a.second.key > b.second.key;
    });

    result["hits"] = nlohmann::json::array();

    for(auto field_order_kv: field_order_kvs) {
        std::string value;
        const std::string &seq_id_key = get_seq_id_key((uint32_t) field_order_kv.second.key);
        store->get(seq_id_key, value);
        nlohmann::json document = nlohmann::json::parse(value);
        result["hits"].push_back(document);
    }

    result["found"] = all_result_ids_len;

    result["facet_counts"] = nlohmann::json::array();

    // populate facets
    for(const facet & a_facet: facets) {
        nlohmann::json facet_result = nlohmann::json::object();
        facet_result["field_name"] = a_facet.field_name;
        facet_result["counts"] = nlohmann::json::array();

        for(auto kv: a_facet.result_map) {
            nlohmann::json facet_value_count = nlohmann::json::object();
            facet_value_count["value"] = kv.first;
            facet_value_count["count"] = kv.second;
            facet_result["counts"].push_back(facet_value_count);
        }

        result["facet_counts"].push_back(facet_result);
    }

    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    //!std::cout << "Time taken for result calc: " << timeMillis << "us" << std::endl;
    //!store->print_memory_usage();
    return result;
}

/*
   1. Split the query into tokens
   2. Outer loop will generate bounded cartesian product with costs for each token
   3. Inner loop will iterate on each token with associated cost
   4. Cartesian product of the results of the token searches will be used to form search phrases
      (cartesian product adapted from: http://stackoverflow.com/a/31169617/131050)
   4. Intersect the lists to find docs that match each phrase
   5. Sort the docs based on some ranking criteria
*/
void Collection::search_field(std::string & query, const std::string & field, uint32_t *filter_ids, size_t filter_ids_length,
                              std::vector<facet> & facets, const std::vector<sort_field> & sort_fields, const int num_typos,
                              const size_t num_results, Topster<100> &topster, uint32_t** all_result_ids,
                              size_t & all_result_ids_len, const token_ordering token_order, const bool prefix) {
    std::vector<std::string> tokens;
    StringUtils::tokenize(query, tokens, " ", true);

    const int max_cost = (num_typos < 0 || num_typos > 2) ? 2 : num_typos;
    const size_t max_results = std::min(num_results, (size_t) Collection::MAX_RESULTS);

    size_t total_results = topster.size;

    // To prevent us from doing ART search repeatedly as we iterate through possible corrections
    spp::sparse_hash_map<std::string, std::vector<art_leaf*>> token_cost_cache;

    // Used to drop the least occurring token(s) for partial searches
    spp::sparse_hash_map<std::string, uint32_t> token_to_count;

    std::vector<std::vector<int>> token_to_costs;
    std::vector<int> all_costs;

    for(int cost = 0; cost <= max_cost; cost++) {
        all_costs.push_back(cost);
    }

    for(size_t token_index = 0; token_index < tokens.size(); token_index++) {
        token_to_costs.push_back(all_costs);
        std::transform(tokens[token_index].begin(), tokens[token_index].end(), tokens[token_index].begin(), ::tolower);
    }

    std::vector<std::vector<art_leaf*>> token_leaves;

    const size_t combination_limit = 10;
    auto product = []( long long a, std::vector<int>& b ) { return a*b.size(); };
    int token_rank = 0;
    long long n = 0;
    long long int N = std::accumulate(token_to_costs.begin(), token_to_costs.end(), 1LL, product);

    while(n < N && n < combination_limit) {
        // Outerloop generates combinations of [cost to max_cost] for each token
        // For e.g. for a 3-token query: [0, 0, 0], [0, 0, 1], [0, 1, 1] etc.
        std::vector<uint32_t> costs(token_to_costs.size());
        ldiv_t q { n, 0 };
        for(long long i = (token_to_costs.size() - 1); 0 <= i ; --i ) {
            q = ldiv(q.quot, token_to_costs[i].size());
            costs[i] = token_to_costs[i][q.rem];
        }

        token_leaves.clear();
        int token_index = 0;

        while(token_index < tokens.size()) {
            // For each token, look up the generated cost for this iteration and search using that cost
            std::string token = tokens[token_index];
            const std::string token_cost_hash = token + std::to_string(costs[token_index]);

            std::vector<art_leaf*> leaves;
            /*std::cout << "\nSearching for: " << token << " - cost: " << costs[token_index] << ", token_rank: "
                      << token_rank << std::endl;*/

            if(token_cost_cache.count(token_cost_hash) != 0) {
                leaves = token_cost_cache[token_cost_hash];
            } else {
                int token_len = prefix ? (int) token.length() : (int) token.length() + 1;

                int count = search_index.count(field);

                art_fuzzy_search(search_index.at(field), (const unsigned char *) token.c_str(), token_len,
                                 costs[token_index], costs[token_index], 3, token_order, prefix, leaves);

                if(!leaves.empty()) {
                    token_cost_cache.emplace(token_cost_hash, leaves);
                }
            }

            if(!leaves.empty()) {
                //!log_leaves(costs[token_index], token, leaves);
                token_leaves.push_back(leaves);
                token_to_count[token] = std::max(token_to_count[token], leaves.at(0)->values->ids.getLength());
            } else {
                // No result at `cost = costs[token_index]`. Remove costs until `cost` for token and re-do combinations
                auto it = std::find(token_to_costs[token_index].begin(), token_to_costs[token_index].end(), costs[token_index]);
                if(it != token_to_costs[token_index].end()) {
                    token_to_costs[token_index].erase(it);

                    // no more costs left for this token, clean up
                    if(token_to_costs[token_index].empty()) {
                        token_to_costs.erase(token_to_costs.begin()+token_index);
                        tokens.erase(tokens.begin()+token_index);
                        token_index--;
                    }
                }

                // To continue outerloop on new cost combination
                n = -1;
                N = std::accumulate(token_to_costs.begin(), token_to_costs.end(), 1LL, product);
                break;
            }

            token_index++;
        }

        if(token_leaves.size() != 0 && token_leaves.size() == tokens.size()) {
            // If all tokens were found, go ahead and search for candidates with what we have so far
            search_candidates(filter_ids, filter_ids_length, facets, sort_fields, token_rank, token_leaves, topster,
                              total_results, all_result_ids, all_result_ids_len, max_results);

            if (total_results >= max_results) {
                // If we don't find enough results, we continue outerloop (looking at tokens with greater cost)
                break;
            }
        }

        n++;
    }

    // When there are not enough overall results and atleast one token has results
    if(topster.size < max_results && token_to_count.size() > 1) {
        // Drop certain token with least hits and try searching again
        std::string truncated_query;

        std::vector<std::pair<std::string, uint32_t>> token_count_pairs;
        for (auto itr = token_to_count.begin(); itr != token_to_count.end(); ++itr) {
            token_count_pairs.push_back(*itr);
        }

        std::sort(token_count_pairs.begin(), token_count_pairs.end(), [=]
            (const std::pair<std::string, uint32_t>& a, const std::pair<std::string, uint32_t>& b) {
                return a.second > b.second;
            }
        );

        for(uint32_t i = 0; i < token_count_pairs.size()-1; i++) {
            if(token_to_count.count(token_count_pairs[i].first) != 0) {
                truncated_query += " " + token_count_pairs.at(i).first;
            }
        }

        return search_field(truncated_query, field, filter_ids, filter_ids_length, facets, sort_fields, num_typos,
                            num_results, topster, all_result_ids, all_result_ids_len, token_order, prefix);
    }
}

void Collection::log_leaves(const int cost, const std::string &token, const std::vector<art_leaf *> &leaves) const {
    printf("Token: %s, cost: %d, candidates: \n", token.c_str(), cost);
    for(auto i=0; i < leaves.size(); i++) {
        printf("%.*s, ", leaves[i]->key_len, leaves[i]->key);
        printf("frequency: %d, max_score: %d\n", leaves[i]->values->ids.getLength(), leaves[i]->max_score);
        /*for(auto j=0; j<leaves[i]->values->ids.getLength(); j++) {
            printf("id: %d\n", leaves[i]->values->ids.at(j));
        }*/
    }
}

void Collection::score_results(const std::vector<sort_field> & sort_fields, const int & token_rank,
                               Topster<100> & topster, const std::vector<art_leaf *> &query_suggestion,
                               const uint32_t *result_ids, const size_t result_size) const {

    const int max_token_rank = 250;
    spp::sparse_hash_map<art_leaf*, uint32_t*> leaf_to_indices;

    if(query_suggestion.size() != 1) {
        // won't be needing positional ranking when there is only 1 token in the query
        for (art_leaf *token_leaf : query_suggestion) {
            uint32_t *indices = new uint32_t[result_size];
            token_leaf->values->ids.indexOf(result_ids, result_size, indices);
            leaf_to_indices.emplace(token_leaf, indices);
        }
    }

    spp::sparse_hash_map<uint32_t, int64_t> * primary_rank_scores = nullptr;
    spp::sparse_hash_map<uint32_t, int64_t> * secondary_rank_scores = nullptr;

    // Used for asc/desc ordering. NOTE: Topster keeps biggest keys (i.e. it's desc in nature)
    int64_t primary_rank_factor = 1;
    int64_t secondary_rank_factor = 1;

    if(sort_fields.size() > 0) {
        // assumed that rank field exists in the index - checked earlier in the chain
        primary_rank_scores = sort_index.at(sort_fields[0].name);
        if(sort_fields[0].order == sort_field_const::asc) {
            primary_rank_factor = -1;
        }
    }

    if(sort_fields.size() > 1) {
        secondary_rank_scores = sort_index.at(sort_fields[1].name);
        if(sort_fields[1].order == sort_field_const::asc) {
            secondary_rank_factor = -1;
        }
    }

    for(auto i=0; i<result_size; i++) {
        uint32_t seq_id = result_ids[i];
        std::vector<std::vector<uint16_t>> token_positions;

        MatchScore mscore;

        if(query_suggestion.size() == 1) {
            mscore = MatchScore{1, 1};
        } else {
            // for each token in the query, find the positions that it appears in this document
            for (art_leaf *token_leaf : query_suggestion) {
                std::vector<uint16_t> positions;
                int doc_index = leaf_to_indices.at(token_leaf)[i];
                if(doc_index == token_leaf->values->ids.getLength()) {
                    continue;
                }

                uint32_t start_offset = token_leaf->values->offset_index.at(doc_index);
                uint32_t end_offset = (doc_index == token_leaf->values->ids.getLength() - 1) ?
                                      token_leaf->values->offsets.getLength() :
                                      token_leaf->values->offset_index.at(doc_index+1);

                while(start_offset < end_offset) {
                    positions.push_back((uint16_t) token_leaf->values->offsets.at(start_offset));
                    start_offset++;
                }

                token_positions.push_back(positions);
            }

            mscore = MatchScore::match_score(seq_id, token_positions);
        }

        int token_rank_score = max_token_rank - token_rank;

        // Construct a single match_score from individual components (for multi-field sort)
        const uint64_t match_score = (token_rank_score << 16) +
                                     ((uint64_t)(mscore.words_present) << 8) +
                                     (MAX_SEARCH_TOKENS - mscore.distance);

        int64_t primary_rank_score = (primary_rank_scores && primary_rank_scores->count(seq_id) > 0) ?
                                     primary_rank_scores->at(seq_id) : 0;
        int64_t secondary_rank_score = (secondary_rank_scores && secondary_rank_scores->count(seq_id) > 0) ?
                                       secondary_rank_scores->at(seq_id) : 0;
        topster.add(seq_id, match_score,
                    primary_rank_factor * primary_rank_score,
                    secondary_rank_factor * secondary_rank_score);

        /*std::cout << "token_rank_score: " << token_rank_score << ", match_score: "
                  << match_score << ", primary_rank_score: " << primary_rank_score << ", seq_id: " << seq_id << std::endl;*/
    }

    for (auto it = leaf_to_indices.begin(); it != leaf_to_indices.end(); it++) {
        delete [] it->second;
        it->second = nullptr;
    }
}

inline std::vector<art_leaf *> Collection::next_suggestion(const std::vector<std::vector<art_leaf *>> &token_leaves,
                                                           long long int n) {
    std::vector<art_leaf*> query_suggestion(token_leaves.size());

    // generate the next combination from `token_leaves` and store it in `query_suggestion`
    ldiv_t q { n, 0 };
    for(long long i=token_leaves.size()-1 ; 0<=i ; --i ) {
        q = ldiv(q.quot, token_leaves[i].size());
        query_suggestion[i] = token_leaves[i][q.rem];
    }

    // sort ascending based on matched documents for each token for faster intersection
    sort(query_suggestion.begin(), query_suggestion.end(), [](const art_leaf* left, const art_leaf* right) {
        return left->values->ids.getLength() < right->values->ids.getLength();
    });

    return query_suggestion;
}

void Collection::remove_and_shift_offset_index(sorted_array &offset_index, const uint32_t *indices_sorted,
                                               const uint32_t indices_length) {
    uint32_t *curr_array = offset_index.uncompress();
    uint32_t *new_array = new uint32_t[offset_index.getLength()];

    new_array[0] = 0;
    uint32_t new_index = 0;
    uint32_t curr_index = 0;
    uint32_t indices_counter = 0;
    uint32_t shift_value = 0;

    while(curr_index < offset_index.getLength()) {
        if(indices_counter < indices_length && curr_index >= indices_sorted[indices_counter]) {
            // skip copying
            if(curr_index == indices_sorted[indices_counter]) {
                curr_index++;
                const uint32_t diff = curr_index == offset_index.getLength() ?
                                0 : (offset_index.at(curr_index) - offset_index.at(curr_index-1));

                shift_value += diff;
            }
            indices_counter++;
        } else {
            new_array[new_index++] = curr_array[curr_index++] - shift_value;
        }
    }

    offset_index.load(new_array, new_index);

    delete[] curr_array;
    delete[] new_array;
}

Option<std::string> Collection::remove(std::string id) {
    nlohmann::json result = nlohmann::json::object();

    std::string seq_id_str;
    StoreStatus status = store->get(get_doc_id_key(id), seq_id_str);

    if(status == StoreStatus::NOT_FOUND) {
        return Option<std::string>(404, "Could not find a document with id: " + id);
    }

    uint32_t seq_id = (uint32_t) std::stol(seq_id_str);

    std::string parsed_document;
    store->get(get_seq_id_key(seq_id), parsed_document);

    nlohmann::json document = nlohmann::json::parse(parsed_document);

    std::vector<std::string> tokens;
    StringUtils::tokenize(document["title"], tokens, " ", true);

    for(auto token: tokens) {
        std::transform(token.begin(), token.end(), token.begin(), ::tolower);

        const unsigned char *key = (const unsigned char *) token.c_str();
        int key_len = (int) (token.length() + 1);

        art_leaf* leaf = (art_leaf *) art_search(search_index.at("title"), key, key_len);
        if(leaf != NULL) {
            uint32_t seq_id_values[1] = {seq_id};

            uint32_t doc_index = leaf->values->ids.indexOf(seq_id);
            uint32_t start_offset = leaf->values->offset_index.at(doc_index);
            uint32_t end_offset = (doc_index == leaf->values->ids.getLength() - 1) ?
                                  leaf->values->offsets.getLength() :
                                  leaf->values->offset_index.at(doc_index+1);

            uint32_t doc_indices[1] = {doc_index};
            remove_and_shift_offset_index(leaf->values->offset_index, doc_indices, 1);

            leaf->values->offsets.remove_index(start_offset, end_offset);
            leaf->values->ids.remove_values(seq_id_values, 1);

            /*len = leaf->values->offset_index.getLength();
            for(auto i=0; i<len; i++) {
                std::cout << "i: " << i << ", val: " << leaf->values->offset_index.at(i) << std::endl;
            }
            std::cout << "----" << std::endl;*/

            if(leaf->values->ids.getLength() == 0) {
                art_delete(search_index.at("title"), key, key_len);
            }
        }
    }

    store->remove(get_doc_id_key(id));
    store->remove(get_seq_id_key(seq_id));

    return Option<std::string>(id);
}

std::string Collection::get_next_seq_id_key(std::string collection_name) {
    return std::string(COLLECTION_NEXT_SEQ_PREFIX) + "_" + collection_name;
}

std::string Collection::get_seq_id_key(uint32_t seq_id) {
    // We can't simply do std::to_string() because we want to preserve the byte order
    unsigned char bytes[4];
    bytes[0] = (unsigned char) ((seq_id >> 24) & 0xFF);
    bytes[1] = (unsigned char) ((seq_id >> 16) & 0xFF);
    bytes[2] = (unsigned char) ((seq_id >> 8) & 0xFF);
    bytes[3] = (unsigned char) ((seq_id & 0xFF));

    return get_seq_id_collection_prefix() + "_" + std::string(bytes, bytes+4);
}

std::string Collection::get_doc_id_key(std::string doc_id) {
    return std::to_string(collection_id) + "_" + DOC_ID_PREFIX + doc_id;
}

uint32_t Collection::get_collection_id() {
    return collection_id;
}

uint32_t Collection::doc_id_to_seq_id(std::string doc_id) {
    std::string seq_id_str;
    store->get(get_doc_id_key(doc_id), seq_id_str);
    uint32_t seq_id = (uint32_t) std::stoi(seq_id_str);
    return seq_id;
}

std::vector<std::string> Collection::get_facet_fields() {
    std::vector<std::string> facet_fields_copy;
    for(auto it = facet_schema.begin(); it != facet_schema.end(); ++it) {
        facet_fields_copy.push_back(it->first);
    }

    return facet_fields_copy;
}

std::vector<field> Collection::get_sort_fields() {
    return sort_fields;
}

spp::sparse_hash_map<std::string, field> Collection::get_schema() {
    return search_schema;
};

std::string Collection::get_meta_key(std::string collection_name) {
    return COLLECTION_META_PREFIX + collection_name;
}

std::string Collection::get_seq_id_collection_prefix() {
    return std::to_string(collection_id) + "_" + std::string(SEQ_ID_PREFIX);
}

std::string Collection::get_token_ordering_field() {
    return token_ordering_field;
}