#include "collection.h"

#include <numeric>
#include <chrono>
#include <array_utils.h>
#include <match_score.h>
#include <string_utils.h>
#include <art.h>
#include <thread>
#include <chrono>
#include <rocksdb/write_batch.h>
#include "topster.h"
#include "logger.h"

struct match_index_t {
    Match match;
    uint64_t match_score = 0;
    size_t index;

    match_index_t(Match match, uint64_t match_score, size_t index): match(match), match_score(match_score),
                                                                    index(index) {

    }

    bool operator<(const match_index_t& a) const {
        if(match_score != a.match_score) {
            return match_score > a.match_score;
        }
        return index < a.index;
    }
};

Collection::Collection(const std::string name, const uint32_t collection_id, const uint32_t next_seq_id, Store *store,
                       const std::vector<field> &fields, const std::string & default_sorting_field,
                       const size_t num_indices):
                       name(name), collection_id(collection_id), next_seq_id(next_seq_id), store(store),
                       fields(fields), default_sorting_field(default_sorting_field), num_indices(num_indices) {

    for(const field& field: fields) {
        search_schema.emplace(field.name, field);

        if(field.is_facet()) {
            facet_value fvalue;
            facet_schema.emplace(field.name, field);
        }

        if(field.is_single_integer() || field.is_single_float() || field.is_single_bool()) {
            sort_schema.emplace(field.name, field);
        }
    }

    for(size_t i = 0; i < num_indices; i++) {
        Index* index = new Index(name+std::to_string(i), search_schema, facet_schema, sort_schema);
        indices.push_back(index);
        std::thread* thread = new std::thread(&Index::run_search, index);
        index_threads.push_back(thread);
    }

    num_documents = 0;
}

Collection::~Collection() {
    for(size_t i = 0; i < indices.size(); i++) {
        std::thread *t = index_threads[i];
        Index* index = indices[i];
        index->ready = true;
        index->terminate = true;
        index->cv.notify_one();
        t->join();
        delete t;
        delete indices[i];
    }
}

uint32_t Collection::get_next_seq_id() {
    store->increment(get_next_seq_id_key(name), 1);
    return next_seq_id++;
}

void Collection::set_next_seq_id(uint32_t seq_id) {
    next_seq_id = seq_id;
}

void Collection::increment_next_seq_id_field() {
    next_seq_id++;
}

Option<nlohmann::json> Collection::add(const std::string & json_str) {
    nlohmann::json document;
    try {
        document = nlohmann::json::parse(json_str);
    } catch(const std::exception& e) {
        LOG(ERR) << "JSON error: " << e.what();
        return Option<nlohmann::json>(400, "Bad JSON.");
    }

    uint32_t seq_id = get_next_seq_id();
    std::string seq_id_str = std::to_string(seq_id);

    if(document.count("id") == 0) {
        document["id"] = seq_id_str;
    } else if(!document["id"].is_string()) {
        return Option<nlohmann::json>(400, "Document's `id` field should be a string.");
    }

    std::string doc_id = document["id"];
    Option<nlohmann::json> doc_option = get(doc_id);

    // we need to check if document ID already exists before attempting to index
    if(doc_option.ok()) {
        return Option<nlohmann::json>(409, std::string("A document with id ") + doc_id + " already exists.");
    }

    const Option<uint32_t> & index_memory_op = index_in_memory(document, seq_id);

    if(!index_memory_op.ok()) {
        return Option<nlohmann::json>(index_memory_op.code(), index_memory_op.error());
    }

    rocksdb::WriteBatch batch;
    batch.Put(get_doc_id_key(document["id"]), seq_id_str);
    batch.Put(get_seq_id_key(seq_id), document.dump());
    bool write_ok = store->batch_write(batch);

    if(!write_ok) {
        return Option<nlohmann::json>(500, "Could not write to on-disk storage.");
    }

    return Option<nlohmann::json>(document);
}

Option<uint32_t> Collection::validate_index_in_memory(const nlohmann::json &document, uint32_t seq_id) {
    if(document.count(default_sorting_field) == 0) {
        return Option<>(400, "Field `" + default_sorting_field  + "` has been declared as a default sorting field, "
                "but is not found in the document.");
    }

    if(!document[default_sorting_field].is_number_integer() && !document[default_sorting_field].is_number_float()) {
        return Option<>(400, "Default sorting field `" + default_sorting_field  + "` must be of type int32 or float.");
    }

    if(document[default_sorting_field].is_number_integer() &&
       document[default_sorting_field].get<int64_t>() > std::numeric_limits<int32_t>::max()) {
        return Option<>(400, "Default sorting field `" + default_sorting_field  + "` exceeds maximum value of an int32.");
    }

    if(document[default_sorting_field].is_number_float() &&
       document[default_sorting_field].get<float>() > std::numeric_limits<float>::max()) {
        return Option<>(400, "Default sorting field `" + default_sorting_field  + "` exceeds maximum value of a float.");
    }

    for(const std::pair<std::string, field> & field_pair: search_schema) {
        const std::string & field_name = field_pair.first;

        if(document.count(field_name) == 0) {
            return Option<>(400, "Field `" + field_name  + "` has been declared in the schema, "
                    "but is not found in the document.");
        }

        if(field_pair.second.type == field_types::STRING) {
            if(!document[field_name].is_string()) {
                return Option<>(400, "Field `" + field_name  + "` must be a string.");
            }
        } else if(field_pair.second.type == field_types::INT32) {
            if(!document[field_name].is_number_integer()) {
                return Option<>(400, "Field `" + field_name  + "` must be an int32.");
            }

            if(document[field_name].get<int64_t>() > INT32_MAX) {
                return Option<>(400, "Field `" + field_name  + "` exceeds maximum value of int32.");
            }
        } else if(field_pair.second.type == field_types::INT64) {
            if(!document[field_name].is_number_integer()) {
                return Option<>(400, "Field `" + field_name  + "` must be an int64.");
            }
        } else if(field_pair.second.type == field_types::FLOAT) {
            if(!document[field_name].is_number()) { // allows integer to be passed to a float field
                return Option<>(400, "Field `" + field_name  + "` must be a float.");
            }
        } else if(field_pair.second.type == field_types::BOOL) {
            if(!document[field_name].is_boolean()) {
                return Option<>(400, "Field `" + field_name  + "` must be a bool.");
            }
        } else if(field_pair.second.type == field_types::STRING_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Field `" + field_name  + "` must be a string array.");
            }
            if(document[field_name].size() > 0 && !document[field_name][0].is_string()) {
                return Option<>(400, "Field `" + field_name  + "` must be a string array.");
            }
        } else if(field_pair.second.type == field_types::INT32_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Field `" + field_name  + "` must be an int32 array.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_number_integer()) {
                return Option<>(400, "Field `" + field_name  + "` must be an int32 array.");
            }
        } else if(field_pair.second.type == field_types::INT64_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Field `" + field_name  + "` must be an int64 array.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_number_integer()) {
                return Option<>(400, "Field `" + field_name  + "` must be an int64 array.");
            }
        } else if(field_pair.second.type == field_types::FLOAT_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Field `" + field_name  + "` must be a float array.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_number_float()) {
                return Option<>(400, "Field `" + field_name  + "` must be a float array.");
            }
        } else if(field_pair.second.type == field_types::BOOL_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Field `" + field_name  + "` must be a bool array.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_boolean()) {
                return Option<>(400, "Field `" + field_name  + "` must be a bool array.");
            }
        }
    }

    for(const std::pair<std::string, field> & field_pair: facet_schema) {
        const std::string & field_name = field_pair.first;

        if(document.count(field_name) == 0) {
            return Option<>(400, "Field `" + field_name  + "` has been declared as a facet field in the schema, "
                    "but is not found in the document.");
        }

        if(field_pair.second.type == field_types::STRING) {
            if(!document[field_name].is_string()) {
                return Option<>(400, "Facet field `" + field_name  + "` must be a string.");
            }
        } else if(field_pair.second.type == field_types::STRING_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Facet field `" + field_name  + "` must be a string array.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_string()) {
                return Option<>(400, "Facet field `" + field_name  + "` must be a string array.");
            }
        } else {
            return Option<>(400, "Facet field `" + field_name  + "` must be a string or a string[].");
        }
    }

    return Option<>(200);
}

Option<uint32_t> Collection::index_in_memory(const nlohmann::json &document, uint32_t seq_id) {
    Option<uint32_t> validation_op = validate_index_in_memory(document, seq_id);

    if(!validation_op.ok()) {
        return validation_op;
    }

    int32_t points = 0;

    if(!default_sorting_field.empty()) {
        if(document[default_sorting_field].is_number_float()) {
            // serialize float to an integer and reverse the inverted range
            float n = document[default_sorting_field];
            memcpy(&points, &n, sizeof(int32_t));
            points ^= ((points >> (std::numeric_limits<int32_t>::digits - 1)) | INT32_MIN);
            points = -1 * (INT32_MAX - points);
        } else {
            points = document[default_sorting_field];
        }
    }

    Index* index = indices[seq_id % num_indices];
    index->index_in_memory(document, seq_id, points);

    num_documents += 1;
    return Option<>(200);
}

void Collection::prune_document(nlohmann::json &document, const spp::sparse_hash_set<std::string> include_fields,
                                const spp::sparse_hash_set<std::string> exclude_fields) {
    auto it = document.begin();
    for(; it != document.end(); ) {
        if(exclude_fields.count(it.key()) != 0 || (include_fields.size() != 0 && include_fields.count(it.key()) == 0)) {
            it = document.erase(it);
        } else {
            ++it;
        }
    }
}

Option<nlohmann::json> Collection::search(std::string query, const std::vector<std::string> search_fields,
                                  const std::string & simple_filter_query, const std::vector<std::string> & facet_fields,
                                  const std::vector<sort_by> & sort_fields, const int num_typos,
                                  const size_t per_page, const size_t page,
                                  const token_ordering token_order, const bool prefix,
                                  const size_t drop_tokens_threshold,
                                  const spp::sparse_hash_set<std::string> include_fields,
                                  const spp::sparse_hash_set<std::string> exclude_fields) {
    std::vector<facet> facets;

    // validate search fields
    for(const std::string & field_name: search_fields) {
        if(search_schema.count(field_name) == 0) {
            std::string error = "Could not find a field named `" + field_name + "` in the schema.";
            return Option<nlohmann::json>(404, error);
        }

        field search_field = search_schema.at(field_name);
        if(search_field.type != field_types::STRING && search_field.type != field_types::STRING_ARRAY) {
            std::string error = "Field `" + field_name + "` should be a string or a string array.";
            return Option<nlohmann::json>(400, error);
        }

        if(search_field.facet) {
            std::string error = "Field `" + field_name + "` is a faceted field - it cannot be used as a query field.";
            return Option<nlohmann::json>(400, error);
        }
    }

    // validate filter fields
    std::vector<std::string> filter_blocks;
    StringUtils::split(simple_filter_query, filter_blocks, "&&");

    std::vector<filter> filters;
    for(const std::string & filter_block: filter_blocks) {
        // split into [field_name, value]
        std::vector<std::string> expression_parts;
        StringUtils::split(filter_block, expression_parts, ":");
        if(expression_parts.size() != 2) {
            return Option<nlohmann::json>(400, "Could not parse the filter query.");
        }

        const std::string & field_name = expression_parts[0];
        if(search_schema.count(field_name) == 0) {
            return Option<nlohmann::json>(404, "Could not find a filter field named `" + field_name + "` in the schema.");
        }

        field _field = search_schema.at(field_name);
        std::string & raw_value = expression_parts[1];
        filter f;

        if(_field.is_integer() || _field.is_float()) {
            // could be a single value or a list
            if(raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
                std::vector<std::string> filter_values;
                StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, ",");

                for(const std::string & filter_value: filter_values) {
                    if(_field.is_integer() && !StringUtils::is_integer(filter_value)) {
                        return Option<nlohmann::json>(400, "Error with field `" + _field.name + "`: Not an integer.");
                    }

                    if(_field.is_float() && !StringUtils::is_float(filter_value)) {
                        return Option<nlohmann::json>(400, "Error with field `" + _field.name + "`: Not a float.");
                    }
                }

                f = {field_name, filter_values, EQUALS};
            } else {
                Option<NUM_COMPARATOR> op_comparator = filter::extract_num_comparator(raw_value);
                if(!op_comparator.ok()) {
                    return Option<nlohmann::json>(400, "Error with field `" + _field.name + "`: " + op_comparator.error());
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

                if(_field.is_integer() && !StringUtils::is_integer(filter_value)) {
                    return Option<nlohmann::json>(400, "Error with field `" + _field.name + "`: Not an integer.");
                }

                if(_field.is_float() && !StringUtils::is_float(filter_value)) {
                    return Option<nlohmann::json>(400, "Error with field `" + _field.name + "`: Not a float.");
                }

                f = {field_name, {filter_value}, op_comparator.get()};
            }
        } else if(_field.is_bool()) {
            if(raw_value != "true" && raw_value != "false") {
                return Option<nlohmann::json>(400, "Value of field `" + _field.name + "`: must be `true` or `false`.");
            }
            std::string bool_value = (raw_value == "true") ? "1" : "0";
            f = {field_name, {bool_value}, EQUALS};
        } else if(_field.is_string()) {
            if(raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
                std::vector<std::string> filter_values;
                StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, ",");
                f = {field_name, filter_values, EQUALS};
            } else {
                f = {field_name, {raw_value}, EQUALS};
            }
        } else {
            return Option<nlohmann::json>(400, "Error with field `" + _field.name + "`: Unidentified field type.");
        }

        filters.push_back(f);
    }

    // for a wildcard query, if filter is not specified, use default_sorting_field as a catch-all
    if(query == "*" && filters.size() == 0) {
        field f = search_schema.at(default_sorting_field);
        std::string max_value = f.is_float() ? std::to_string(std::numeric_limits<float>::max()) :
                                std::to_string(std::numeric_limits<int32_t>::max());
        filter catch_all_filter = {f.name, {max_value}, LESS_THAN_EQUALS};
        filters.push_back(catch_all_filter);
    }

    // validate facet fields
    for(const std::string & field_name: facet_fields) {
        if(facet_schema.count(field_name) == 0) {
            std::string error = "Could not find a facet field named `" + field_name + "` in the schema.";
            return Option<nlohmann::json>(404, error);
        }
        facets.push_back(facet(field_name));
    }

    // validate sort fields and standardize

    std::vector<sort_by> sort_fields_std;

    for(const sort_by & _sort_field: sort_fields) {
        if(sort_schema.count(_sort_field.name) == 0) {
            std::string error = "Could not find a field named `" + _sort_field.name + "` in the schema for sorting.";
            return Option<nlohmann::json>(404, error);
        }

        std::string sort_order = _sort_field.order;
        StringUtils::toupper(sort_order);

        if(sort_order != sort_field_const::asc && sort_order != sort_field_const::desc) {
            std::string error = "Order for field` " + _sort_field.name + "` should be either ASC or DESC.";
            return Option<nlohmann::json>(400, error);
        }

        sort_fields_std.push_back({_sort_field.name, sort_order});
    }

    if(sort_fields_std.empty()) {
        sort_fields_std.push_back({default_sorting_field, sort_field_const::desc});
    }

    // check for valid pagination
    if(page < 1) {
        std::string message = "Page must be an integer of value greater than 0.";
        return Option<nlohmann::json>(422, message);
    }

    const size_t num_results = (page * per_page);

    if(num_results > MAX_RESULTS) {
        std::string message = "Only the first " + std::to_string(MAX_RESULTS) + " results are available.";
        return Option<nlohmann::json>(422, message);
    }

    //auto begin = std::chrono::high_resolution_clock::now();

    // all search queries that were used for generating the results
    std::vector<std::vector<art_leaf*>> searched_queries;
    std::vector<Topster<512>::KV> field_order_kvs;
    size_t total_found = 0;

    // send data to individual index threads
    for(Index* index: indices) {
        index->search_params = search_args(query, search_fields, filters, facets, sort_fields_std,
                                           num_typos, per_page, page, token_order, prefix, drop_tokens_threshold);
        {
            std::lock_guard<std::mutex> lk(index->m);
            index->ready = true;
            index->processed = false;
        }
        index->cv.notify_one();
        //std::this_thread::sleep_for(std::chrono::milliseconds(400));
    }

    Option<nlohmann::json> index_search_op({});  // stores the last error across all index threads

    for(Index* index: indices) {
        // wait for the worker
        {
            std::unique_lock<std::mutex> lk(index->m);
            index->cv.wait(lk, [index]{return index->processed;});
        }

        if(!index->search_params.outcome.ok()) {
            index_search_op = Option<nlohmann::json>(index->search_params.outcome.code(),
                                                    index->search_params.outcome.error());
        }

        if(!index_search_op.ok()) {
            // we still need to iterate without breaking to release the locks
            continue;
        }

        for(auto & field_order_kv: index->search_params.field_order_kvs) {
            field_order_kv.query_index += searched_queries.size();
            field_order_kvs.push_back(field_order_kv);
        }

        searched_queries.insert(searched_queries.end(), index->search_params.searched_queries.begin(),
                                index->search_params.searched_queries.end());

        for(size_t fi = 0; fi < index->search_params.facets.size(); fi++) {
            auto & this_facet = index->search_params.facets[fi];
            auto & acc_facet = facets[fi];

            for(auto & facet_kv: this_facet.result_map) {
                size_t count = 0;

                if(acc_facet.result_map.count(facet_kv.first) == 0) {
                    // not found, so set it
                    count = facet_kv.second;
                } else {
                    count = acc_facet.result_map[facet_kv.first] + facet_kv.second;
                }

                acc_facet.result_map[facet_kv.first] = count;
            }
        }

        total_found += index->search_params.all_result_ids_len;
    }

    if(!index_search_op.ok()) {
        return index_search_op;
    }

    // All fields are sorted descending
    std::sort(field_order_kvs.begin(), field_order_kvs.end(), Topster<>::is_greater_kv_value);

    nlohmann::json result = nlohmann::json::object();

    result["hits"] = nlohmann::json::array();
    result["found"] = total_found;

    const int start_result_index = (page - 1) * per_page;
    const int kvsize = field_order_kvs.size();

    if(start_result_index > (kvsize - 1)) {
        return Option<nlohmann::json>(result);
    }

    const int end_result_index = std::min(int(num_results), kvsize) - 1;

    // construct results array
    for(int field_order_kv_index = start_result_index; field_order_kv_index <= end_result_index; field_order_kv_index++) {
        const auto & field_order_kv = field_order_kvs[field_order_kv_index];
        const std::string& seq_id_key = get_seq_id_key((uint32_t) field_order_kv.key);

        std::string json_doc_str;
        StoreStatus json_doc_status = store->get(seq_id_key, json_doc_str);

        if(json_doc_status != StoreStatus::FOUND) {
            LOG(ERR) << "Could not locate the JSON document for sequence ID: " << seq_id_key;
            continue;
        }

        nlohmann::json document;

        try {
            document = nlohmann::json::parse(json_doc_str);
        } catch(...) {
            return Option<nlohmann::json>(500, "Error while parsing stored document.");
        }

        nlohmann::json wrapper_doc;
        wrapper_doc["highlights"] = nlohmann::json::array();
        std::vector<highlight_t> highlights;
        StringUtils string_utils;

        for(const std::string & field_name: search_fields) {
            field search_field = search_schema.at(field_name);
            if(query != "*" && (search_field.type == field_types::STRING ||
                                search_field.type == field_types::STRING_ARRAY)) {
                highlight_t highlight;
                highlight_result(search_field, searched_queries, field_order_kv, document, string_utils, highlight);
                if(!highlight.snippets.empty()) {
                    highlights.push_back(highlight);
                }
            }
        }

        std::sort(highlights.begin(), highlights.end());

        for(const auto highlight: highlights) {
            nlohmann::json h_json = nlohmann::json::object();
            h_json["field"] = highlight.field;
            if(!highlight.indices.empty()) {
                h_json["indices"] = highlight.indices;
                h_json["snippets"] = highlight.snippets;
            } else {
                h_json["snippet"] = highlight.snippets[0];
            }

            wrapper_doc["highlights"].push_back(h_json);
        }

        prune_document(document, include_fields, exclude_fields);
        wrapper_doc["document"] = document;
        //wrapper_doc["match_score"] = field_order_kv.match_score;
        //wrapper_doc["seq_id"] = (uint32_t) field_order_kv.key;

        result["hits"].push_back(wrapper_doc);
    }

    result["facet_counts"] = nlohmann::json::array();

    // populate facets
    for(const facet & a_facet: facets) {
        nlohmann::json facet_result = nlohmann::json::object();
        facet_result["field_name"] = a_facet.field_name;
        facet_result["counts"] = nlohmann::json::array();

        // keep only top 10 facets
        std::vector<std::pair<std::string, size_t>> value_to_count;
        for (auto itr = a_facet.result_map.begin(); itr != a_facet.result_map.end(); ++itr) {
            value_to_count.push_back(*itr);
        }

        std::sort(value_to_count.begin(), value_to_count.end(),
                  [=](std::pair<std::string, size_t>& a, std::pair<std::string, size_t>& b) {
                      return a.second > b.second;
                  });

        for(size_t i = 0; i < std::min((size_t)10, value_to_count.size()); i++) {
            auto & kv = value_to_count[i];
            nlohmann::json facet_value_count = nlohmann::json::object();
            facet_value_count["value"] = kv.first;
            facet_value_count["count"] = kv.second;
            facet_result["counts"].push_back(facet_value_count);
        }

        result["facet_counts"].push_back(facet_result);
    }

    //long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    //!LOG(INFO) << "Time taken for result calc: " << timeMillis << "us";
    //!store->print_memory_usage();
    return result;
}

void Collection::highlight_result(const field &search_field,
                                  const std::vector<std::vector<art_leaf *>> &searched_queries,
                                  const Topster<512>::KV & field_order_kv, const nlohmann::json & document,
                                  StringUtils & string_utils, highlight_t & highlight) {
    
    spp::sparse_hash_map<const art_leaf*, uint32_t*> leaf_to_indices;
    std::vector<art_leaf *> query_suggestion;

    for (const art_leaf *token_leaf : searched_queries[field_order_kv.query_index]) {
        // Must search for the token string fresh on that field for the given document since `token_leaf`
        // is from the best matched field and need not be present in other fields of a document.
        Index* index = indices[field_order_kv.key % num_indices];
        art_leaf *actual_leaf = index->get_token_leaf(search_field.name, &token_leaf->key[0], token_leaf->key_len);
        if(actual_leaf != NULL) {
            query_suggestion.push_back(actual_leaf);
            std::vector<uint16_t> positions;
            uint32_t doc_index = actual_leaf->values->ids.indexOf(field_order_kv.key);
            uint32_t *indices = new uint32_t[1];
            indices[0] = doc_index;
            leaf_to_indices.emplace(actual_leaf, indices);
        }
    }

    // positions in the field of each token in the query
    std::vector<std::vector<std::vector<uint16_t>>> array_token_positions;
    Index::populate_token_positions(query_suggestion, leaf_to_indices, 0, array_token_positions);

    if(array_token_positions.size() == 0) {
        // none of the tokens from the query were found on this field
        return ;
    }

    std::vector<match_index_t> match_indices;

    for(size_t array_index = 0; array_index < array_token_positions.size(); array_index++) {
        const std::vector<std::vector<uint16_t>> & token_positions = array_token_positions[array_index];

        if(token_positions.empty()) {
            continue;
        }

        const Match & this_match = Match::match(field_order_kv.key, token_positions);
        uint64_t this_match_score = this_match.get_match_score(1, field_order_kv.field_id);
        match_indices.push_back(match_index_t(this_match, this_match_score, array_index));
    }

    const size_t max_array_matches = std::min((size_t)MAX_ARRAY_MATCHES, match_indices.size());
    std::partial_sort(match_indices.begin(), match_indices.begin()+max_array_matches, match_indices.end());

    for(size_t index = 0; index < max_array_matches; index++) {
        const match_index_t & match_index = match_indices[index];
        const Match & match = match_index.match;

        std::vector<std::string> tokens;
        if(search_field.type == field_types::STRING) {
            StringUtils::split(document[search_field.name], tokens, " ");
        } else {
            StringUtils::split(document[search_field.name][match_index.index], tokens, " ");
        }

        // unpack `match.offset_diffs` into `token_indices`
        std::vector<size_t> token_indices;
        spp::sparse_hash_set<std::string> token_hits;

        size_t num_tokens_found = (size_t) match.offset_diffs[0];
        for(size_t i = 1; i <= num_tokens_found; i++) {
            if(match.offset_diffs[i] != std::numeric_limits<int8_t>::max()) {
                size_t token_index = (size_t)(match.start_offset + match.offset_diffs[i]);
                token_indices.push_back(token_index);
                std::string token = tokens[token_index];
                string_utils.unicode_normalize(token);
                token_hits.insert(token);
            }
        }

        auto minmax = std::minmax_element(token_indices.begin(), token_indices.end());

        // For longer strings, pick surrounding tokens within N tokens of min_index and max_index for the snippet
        const size_t start_index = (tokens.size() <= SNIPPET_STR_ABOVE_LEN) ? 0 :
                                   std::max(0, (int)(*(minmax.first) - 5));

        const size_t end_index = (tokens.size() <= SNIPPET_STR_ABOVE_LEN) ? tokens.size() :
                                 std::min((int)tokens.size(), (int)(*(minmax.second) + 5));

        std::stringstream snippet_stream;
        for(size_t snippet_index = start_index; snippet_index < end_index; snippet_index++) {
            if(snippet_index != start_index) {
                snippet_stream << " ";
            }

            std::string token = tokens[snippet_index];
            string_utils.unicode_normalize(token);

            if(token_hits.count(token) != 0) {
                snippet_stream << "<mark>" + tokens[snippet_index] + "</mark>";
            } else {
                snippet_stream << tokens[snippet_index];
            }
        }

        highlight.snippets.push_back(snippet_stream.str());

        if(search_field.type == field_types::STRING_ARRAY) {
            highlight.indices.push_back(match_index.index);
        }
    }

    highlight.field = search_field.name;
    highlight.match_score = match_indices[0].match_score;

    for (auto it = leaf_to_indices.begin(); it != leaf_to_indices.end(); it++) {
        delete [] it->second;
        it->second = nullptr;
    }
}

Option<nlohmann::json> Collection::get(const std::string & id) {
    std::string seq_id_str;
    StoreStatus seq_id_status = store->get(get_doc_id_key(id), seq_id_str);

    if(seq_id_status == StoreStatus::NOT_FOUND) {
        return Option<nlohmann::json>(404, "Could not find a document with id: " + id);
    }

    if(seq_id_status == StoreStatus::ERROR) {
        return Option<nlohmann::json>(500, "Error while fetching the document.");
    }

    uint32_t seq_id = (uint32_t) std::stol(seq_id_str);

    std::string parsed_document;
    StoreStatus doc_status = store->get(get_seq_id_key(seq_id), parsed_document);

    if(doc_status == StoreStatus::NOT_FOUND) {
        LOG(ERR) << "Sequence ID exists, but document is missing for id: " << id;
        return Option<nlohmann::json>(404, "Could not find a document with id: " + id);
    }

    if(doc_status == StoreStatus::ERROR) {
        return Option<nlohmann::json>(500, "Error while fetching the document.");
    }

    nlohmann::json document;
    try {
        document = nlohmann::json::parse(parsed_document);
    } catch(...) {
        return Option<nlohmann::json>(500, "Error while parsing stored document.");
    }

    return Option<nlohmann::json>(document);
}

Option<std::string> Collection::remove(const std::string & id, const bool remove_from_store) {
    std::string seq_id_str;
    StoreStatus seq_id_status = store->get(get_doc_id_key(id), seq_id_str);

    if(seq_id_status == StoreStatus::NOT_FOUND) {
        return Option<std::string>(404, "Could not find a document with id: " + id);
    }

    if(seq_id_status == StoreStatus::ERROR) {
        return Option<std::string>(500, "Error while fetching the document.");
    }

    uint32_t seq_id = (uint32_t) std::stol(seq_id_str);

    std::string parsed_document;
    StoreStatus doc_status = store->get(get_seq_id_key(seq_id), parsed_document);

    if(doc_status == StoreStatus::NOT_FOUND) {
        LOG(ERR) << "Sequence ID exists, but document is missing for id: " << id;
        return Option<std::string>(404, "Could not find a document with id: " + id);
    }

    if(doc_status == StoreStatus::ERROR) {
        return Option<std::string>(500, "Error while fetching the document.");
    }

    nlohmann::json document;
    try {
        document = nlohmann::json::parse(parsed_document);
    } catch(...) {
        return Option<std::string>(500, "Error while parsing stored document.");
    }

    Index* index = indices[seq_id % num_indices];
    index->remove(seq_id, document);

    if(remove_from_store) {
        store->remove(get_doc_id_key(id));
        store->remove(get_seq_id_key(seq_id));
    }

    num_documents -= 1;

    return Option<std::string>(id);
}

std::string Collection::get_next_seq_id_key(const std::string & collection_name) {
    return std::string(COLLECTION_NEXT_SEQ_PREFIX) + "_" + collection_name;
}

std::string Collection::get_seq_id_key(uint32_t seq_id) {
    // We can't simply do std::to_string() because we want to preserve the byte order.
    // & 0xFF masks all but the lowest eight bits.
    const std::string & serialized_id = StringUtils::serialize_uint32_t(seq_id);
    return get_seq_id_collection_prefix() + "_" + serialized_id;
}

std::string Collection::get_doc_id_key(const std::string & doc_id) {
    return std::to_string(collection_id) + "_" + DOC_ID_PREFIX + "_" + doc_id;
}

std::string Collection::get_name() {
    return name;
}

size_t Collection::get_num_documents() {
    return num_documents;
}

uint32_t Collection::get_collection_id() {
    return collection_id;
}

Option<uint32_t> Collection::doc_id_to_seq_id(std::string doc_id) {
    std::string seq_id_str;
    StoreStatus status = store->get(get_doc_id_key(doc_id), seq_id_str);
    if(status == StoreStatus::FOUND) {
        uint32_t seq_id = (uint32_t) std::stoi(seq_id_str);
        return Option<uint32_t>(seq_id);
    }

    if(status == StoreStatus::NOT_FOUND) {
        return Option<uint32_t>(404, "Not found.");
    }

    return Option<uint32_t>(500, "Error while fetching doc_id from store.");
}

std::vector<std::string> Collection::get_facet_fields() {
    std::vector<std::string> facet_fields_copy;
    for(auto it = facet_schema.begin(); it != facet_schema.end(); ++it) {
        facet_fields_copy.push_back(it->first);
    }

    return facet_fields_copy;
}

std::vector<field> Collection::get_sort_fields() {
    std::vector<field> sort_fields_copy;
    for(auto it = sort_schema.begin(); it != sort_schema.end(); ++it) {
        sort_fields_copy.push_back(it->second);
    }

    return sort_fields_copy;
}

std::vector<field> Collection::get_fields() {
    return fields;
}

std::unordered_map<std::string, field> Collection::get_schema() {
    return search_schema;
};

std::string Collection::get_meta_key(const std::string & collection_name) {
    return std::string(COLLECTION_META_PREFIX) + "_" + collection_name;
}

std::string Collection::get_seq_id_collection_prefix() {
    return std::to_string(collection_id) + "_" + std::string(SEQ_ID_PREFIX);
}

std::string Collection::get_default_sorting_field() {
    return default_sorting_field;
}