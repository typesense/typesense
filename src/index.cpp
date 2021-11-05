#include "index.h"

#include <numeric>
#include <chrono>
#include <set>
#include <unordered_map>
#include <array_utils.h>
#include <match_score.h>
#include <string_utils.h>
#include <art.h>
#include <tokenizer.h>
#include <s2/s2point.h>
#include <s2/s2latlng.h>
#include <s2/s2region_term_indexer.h>
#include <s2/s2cap.h>
#include <s2/s2earth.h>
#include <s2/s2loop.h>
#include <s2/s2builder.h>
#include <posting.h>
#include <thread_local_vars.h>
#include "logger.h"

#define RETURN_CIRCUIT_BREAKER if(std::chrono::duration_cast<std::chrono::milliseconds>(\
                                std::chrono::high_resolution_clock::now() - begin).count() > search_stop_ms) { \
                                    search_cutoff = true;                                                    \
                                    return ;\
                                }

#define BREAK_CIRCUIT_BREAKER if(std::chrono::duration_cast<std::chrono::milliseconds>(\
                                std::chrono::high_resolution_clock::now() - begin).count() > search_stop_ms) { \
                                    search_cutoff = true;                              \
                                    break;\
                                }

spp::sparse_hash_map<uint32_t, int64_t> Index::text_match_sentinel_value;
spp::sparse_hash_map<uint32_t, int64_t> Index::seq_id_sentinel_value;
spp::sparse_hash_map<uint32_t, int64_t> Index::geo_sentinel_value;

Index::Index(const std::string& name, const uint32_t collection_id, const Store* store, ThreadPool* thread_pool,
             const std::unordered_map<std::string, field> & search_schema,
             const std::map<std::string, field>& facet_schema, const std::unordered_map<std::string, field>& sort_schema,
             const std::vector<char>& symbols_to_index, const std::vector<char>& token_separators):
        name(name), collection_id(collection_id), store(store), thread_pool(thread_pool),
        search_schema(search_schema), facet_schema(facet_schema), sort_schema(sort_schema),
        symbols_to_index(symbols_to_index), token_separators(token_separators) {

    for(const auto & fname_field: search_schema) {
        if(fname_field.second.is_string()) {
            if(fname_field.second.index) {
                art_tree *t = new art_tree;
                art_tree_init(t);
                search_index.emplace(fname_field.first, t);
            }
        } else if(fname_field.second.is_geopoint()) {
            auto field_geo_index = new spp::sparse_hash_map<std::string, std::vector<uint32_t>>();
            geopoint_index.emplace(fname_field.first, field_geo_index);

            if(!fname_field.second.is_single_geopoint()) {
                spp::sparse_hash_map<uint32_t, int64_t*> * doc_to_geos = new spp::sparse_hash_map<uint32_t, int64_t*>();
                geo_array_index.emplace(fname_field.first, doc_to_geos);
            }
        } else {
            num_tree_t* num_tree = new num_tree_t;
            numerical_index.emplace(fname_field.first, num_tree);
        }

        // initialize for non-string facet fields
        if(fname_field.second.facet && !fname_field.second.is_string()) {
            art_tree *ft = new art_tree;
            art_tree_init(ft);
            search_index.emplace(fname_field.second.faceted_name(), ft);
        }
    }

    for(const auto & pair: sort_schema) {
        if(pair.second.type != field_types::GEOPOINT_ARRAY) {
            spp::sparse_hash_map<uint32_t, int64_t> * doc_to_score = new spp::sparse_hash_map<uint32_t, int64_t>();
            sort_index.emplace(pair.first, doc_to_score);
        }
    }

    for(const auto& pair: facet_schema) {
        auto doc_to_values = new spp::sparse_hash_map<uint32_t, facet_hash_values_t>();
        facet_index_v3.emplace(pair.first, doc_to_values);
    }

    num_documents = 0;
}

Index::~Index() {
    std::unique_lock lock(mutex);

    for(auto & name_tree: search_index) {
        art_tree_destroy(name_tree.second);
        delete name_tree.second;
        name_tree.second = nullptr;
    }

    search_index.clear();

    for(auto & name_index: geopoint_index) {
        delete name_index.second;
        name_index.second = nullptr;
    }

    geopoint_index.clear();

    for(auto& name_index: geo_array_index) {
        for(auto& kv: *name_index.second) {
            delete [] kv.second;
        }

        delete name_index.second;
        name_index.second = nullptr;
    }

    geo_array_index.clear();

    for(auto & name_tree: numerical_index) {
        delete name_tree.second;
        name_tree.second = nullptr;
    }

    numerical_index.clear();

    for(auto & name_map: sort_index) {
        delete name_map.second;
        name_map.second = nullptr;
    }

    sort_index.clear();

    for(auto& kv: facet_index_v3) {
        delete kv.second;
        kv.second = nullptr;
    }

    facet_index_v3.clear();
}

int64_t Index::get_points_from_doc(const nlohmann::json &document, const std::string & default_sorting_field) {
    int64_t points = 0;

    if(document[default_sorting_field].is_number_float()) {
        // serialize float to an integer and reverse the inverted range
        float n = document[default_sorting_field];
        memcpy(&points, &n, sizeof(int32_t));
        points ^= ((points >> (std::numeric_limits<int32_t>::digits - 1)) | INT32_MIN);
        points = -1 * (INT32_MAX - points);
    } else {
        points = document[default_sorting_field];
    }

    return points;
}

int64_t Index::float_to_in64_t(float f) {
    // https://stackoverflow.com/questions/60530255/convert-float-to-int64-t-while-preserving-ordering
    int32_t i;
    memcpy(&i, &f, sizeof i);
    if (i < 0) {
        i ^= INT32_MAX;
    }
    return i;
}

void Index::compute_token_offsets_facets(index_record& record,
                                          const std::unordered_map<std::string, field>& search_schema,
                                          const std::map<std::string, field>& facet_schema,
                                          const std::vector<char>& local_token_separators,
                                          const std::vector<char>& local_symbols_to_index) {

    const auto& document = record.doc;

    for(const auto& field_pair: search_schema) {
        const std::string& field_name = field_pair.first;
        if(document.count(field_name) == 0 || !field_pair.second.index) {
            continue;
        }

        offsets_facet_hashes_t offset_facet_hashes;

        bool is_facet = (facet_schema.count(field_name) != 0);

        // non-string, non-geo faceted field should be indexed as faceted string field as well
        if(field_pair.second.facet && !field_pair.second.is_string() && !field_pair.second.is_geopoint()) {
            if(field_pair.second.is_array()) {
                std::vector<std::string> strings;

                if(field_pair.second.type == field_types::INT32_ARRAY) {
                    for(int32_t value: document[field_name]){
                        strings.push_back(std::to_string(value));
                    }
                } else if(field_pair.second.type == field_types::INT64_ARRAY) {
                    for(int64_t value: document[field_name]){
                        strings.push_back(std::to_string(value));
                    }
                } else if(field_pair.second.type == field_types::FLOAT_ARRAY) {
                    for(float value: document[field_name]){
                        strings.push_back(StringUtils::float_to_str(value));
                    }
                } else if(field_pair.second.type == field_types::BOOL_ARRAY) {
                    for(bool value: document[field_name]){
                        strings.push_back(std::to_string(value));
                    }
                }

                tokenize_string_array_with_facets(strings, is_facet, field_pair.second,
                                                  local_symbols_to_index, local_token_separators,
                                                  offset_facet_hashes.offsets, offset_facet_hashes.facet_hashes);
            } else {
                std::string text;

                if(field_pair.second.type == field_types::INT32) {
                    text = std::to_string(document[field_name].get<int32_t>());
                } else if(field_pair.second.type == field_types::INT64) {
                    text = std::to_string(document[field_name].get<int64_t>());
                } else if(field_pair.second.type == field_types::FLOAT) {
                    text = StringUtils::float_to_str(document[field_name].get<float>());
                } else if(field_pair.second.type == field_types::BOOL) {
                    text = std::to_string(document[field_name].get<bool>());
                }

                tokenize_string_with_facets(text, is_facet, field_pair.second,
                                            local_symbols_to_index, local_token_separators,
                                            offset_facet_hashes.offsets, offset_facet_hashes.facet_hashes);
            }
        }

        if(field_pair.second.is_string()) {

            if(field_pair.second.type == field_types::STRING) {
                tokenize_string_with_facets(document[field_name], is_facet, field_pair.second,
                                            local_symbols_to_index, local_token_separators,
                                            offset_facet_hashes.offsets, offset_facet_hashes.facet_hashes);
            } else {
                tokenize_string_array_with_facets(document[field_name], is_facet, field_pair.second,
                                                  local_symbols_to_index, local_token_separators,
                                                  offset_facet_hashes.offsets, offset_facet_hashes.facet_hashes);
            }
        }

        if(!offset_facet_hashes.offsets.empty() || !offset_facet_hashes.facet_hashes.empty()) {
            record.field_index.emplace(field_name, std::move(offset_facet_hashes));
        }
    }
}

Option<uint32_t> Index::index_in_memory(const index_record& record, uint32_t seq_id,
                                        const std::string & default_sorting_field,
                                        const bool is_update) {
    std::unique_lock lock(mutex);

    int64_t points = 0;
    auto& document = record.doc;

    if(document.count(default_sorting_field) == 0) {
        auto default_sorting_field_it = sort_index.find(default_sorting_field);
        if(default_sorting_field_it != sort_index.end()) {
            auto seq_id_it = default_sorting_field_it->second->find(seq_id);
            if(seq_id_it != default_sorting_field_it->second->end()) {
                points = seq_id_it->second;
            } else {
                points = INT64_MIN;
            }
        } else {
            points = INT64_MIN;
        }
    } else {
        points = get_points_from_doc(document, default_sorting_field);
    }

    if(!is_update) {
        // for updates, the seq_id will already exist
        seq_ids.append(seq_id);
    }

    // assumes that validation has already been done
    for(const auto& field_pair: search_schema) {
        const std::string & field_name = field_pair.first;

        if(document.count(field_name) == 0 || !field_pair.second.index) {
            continue;
        }

        bool is_facet = (facet_schema.count(field_name) != 0);

        // non-string, non-geo faceted field should be indexed as faceted string field as well
        if(field_pair.second.facet && !field_pair.second.is_string() && !field_pair.second.is_geopoint()) {
            auto field_index_it = record.field_index.find(field_name);
            if(field_index_it == record.field_index.end()) {
                continue;
            }

            art_tree *t = search_index.at(field_pair.second.faceted_name());

            index_strings_field(points, t, seq_id, is_facet, field_pair.second,
                                field_index_it->second.offsets,
                                field_index_it->second.facet_hashes);
        }

        if(field_pair.second.is_string()) {
            auto field_index_it = record.field_index.find(field_name);
            if(field_index_it == record.field_index.end()) {
                continue;
            }

            art_tree *t = search_index.at(field_name);
            index_strings_field(points, t, seq_id, is_facet, field_pair.second,
                                field_index_it->second.offsets,
                                field_index_it->second.facet_hashes);
        } else if(field_pair.second.type == field_types::INT32) {
            auto num_tree = numerical_index.at(field_name);
            int32_t value = document[field_name].get<int32_t>();
            num_tree->insert(value, seq_id);
        } else if(field_pair.second.type == field_types::INT64) {
            auto num_tree = numerical_index.at(field_name);
            int64_t value = document[field_name].get<int64_t>();
            num_tree->insert(value, seq_id);
        } else if(field_pair.second.type == field_types::FLOAT) {
            auto num_tree = numerical_index.at(field_name);
            float fvalue = document[field_name].get<float>();
            int64_t value = float_to_in64_t(fvalue);
            num_tree->insert(value, seq_id);
        } else if(field_pair.second.type == field_types::BOOL) {
            auto num_tree = numerical_index.at(field_name);
            bool value = document[field_name];
            num_tree->insert(value, seq_id);
        } else if(field_pair.second.type == field_types::GEOPOINT) {
            const std::vector<double>& latlong = document[field_name];
            auto geo_index = geopoint_index.at(field_name);

            S2RegionTermIndexer::Options options;
            options.set_index_contains_points_only(true);
            S2RegionTermIndexer indexer(options);
            S2Point point = S2LatLng::FromDegrees(latlong[0], latlong[1]).ToPoint();

            for(const auto& term: indexer.GetIndexTerms(point, "")) {
                (*geo_index)[term].push_back(seq_id);
            }
        } else if(field_pair.second.type == field_types::GEOPOINT_ARRAY) {
            const std::vector<std::vector<double>>& latlongs = document[field_name];
            auto geo_index = geopoint_index.at(field_name);
            S2RegionTermIndexer::Options options;
            options.set_index_contains_points_only(true);
            S2RegionTermIndexer indexer(options);

            int64_t* packed_latlongs = new int64_t[latlongs.size() + 1];
            packed_latlongs[0] = latlongs.size();

            for(size_t li = 0; li < latlongs.size(); li++) {
                auto& latlong = latlongs[li];
                S2Point point = S2LatLng::FromDegrees(latlong[0], latlong[1]).ToPoint();
                std::set<std::string> terms;
                for(const auto& term: indexer.GetIndexTerms(point, "")) {
                    terms.insert(term);
                }

                for(const auto& term: terms) {
                    (*geo_index)[term].push_back(seq_id);
                }

                int64_t packed_latlong = GeoPoint::pack_lat_lng(latlong[0], latlong[1]);
                packed_latlongs[li + 1] = packed_latlong;
            }

            geo_array_index.at(field_name)->emplace(seq_id, packed_latlongs);
        }

        else if(field_pair.second.is_array()) {
            // all other numerical arrays
            auto num_tree = numerical_index.at(field_name);

            for(size_t arr_i = 0; arr_i < document[field_name].size(); arr_i++) {
                const auto& arr_value = document[field_name][arr_i];

                if(field_pair.second.type == field_types::INT32_ARRAY) {
                    const int32_t value = arr_value;
                    num_tree->insert(value, seq_id);
                }

                else if(field_pair.second.type == field_types::INT64_ARRAY) {
                    const int64_t value = arr_value;
                    num_tree->insert(value, seq_id);
                }

                else if(field_pair.second.type == field_types::FLOAT_ARRAY) {
                    const float fvalue = arr_value;
                    int64_t value = float_to_in64_t(fvalue);
                    num_tree->insert(value, seq_id);
                }

                else if(field_pair.second.type == field_types::BOOL_ARRAY) {
                    const bool value = document[field_name][arr_i];
                    num_tree->insert(int64_t(value), seq_id);
                }
            }
        }

        // add numerical values automatically into sort index
        if(field_pair.second.type == field_types::INT32 || field_pair.second.type == field_types::INT64 ||
           field_pair.second.type == field_types::FLOAT || field_pair.second.type == field_types::BOOL ||
           field_pair.second.type == field_types::GEOPOINT) {
            spp::sparse_hash_map<uint32_t, int64_t> *doc_to_score = sort_index.at(field_pair.first);

            if(field_pair.second.is_integer() ) {
                doc_to_score->emplace(seq_id, document[field_pair.first].get<int64_t>());
            } else if(field_pair.second.is_float()) {
                int64_t ifloat = float_to_in64_t(document[field_pair.first].get<float>());
                doc_to_score->emplace(seq_id, ifloat);
            } else if(field_pair.second.is_bool()) {
                doc_to_score->emplace(seq_id, (int64_t) document[field_pair.first].get<bool>());
            } else if(field_pair.second.is_geopoint()) {
                const std::vector<double>& latlong = document[field_pair.first];
                int64_t lat_lng = GeoPoint::pack_lat_lng(latlong[0], latlong[1]);
                doc_to_score->emplace(seq_id, lat_lng);
            }
        }
    }

    num_documents += 1;
    return Option<>(201);
}

Option<uint32_t> Index::validate_index_in_memory(nlohmann::json& document, uint32_t seq_id,
                                                 const std::string & default_sorting_field,
                                                 const std::unordered_map<std::string, field> & search_schema,
                                                 const std::map<std::string, field> & facet_schema,
                                                 const index_operation_t op,
                                                 const std::string& fallback_field_type,
                                                 const DIRTY_VALUES& dirty_values) {

    bool missing_default_sort_field = (!default_sorting_field.empty() && document.count(default_sorting_field) == 0);

    if(op != UPDATE && missing_default_sort_field) {
        return Option<>(400, "Field `" + default_sorting_field  + "` has been declared as a default sorting field, "
                "but is not found in the document.");
    }

    for(const auto& field_pair: search_schema) {
        const std::string& field_name = field_pair.first;
        const field& a_field = field_pair.second;

        if(field_name == "id") {
            continue;
        }

        if((a_field.optional || op == UPDATE) && document.count(field_name) == 0) {
            continue;
        }

        if(document.count(field_name) == 0) {
            return Option<>(400, "Field `" + field_name  + "` has been declared in the schema, "
                                 "but is not found in the document.");
        }

        if(a_field.optional && document[field_name].is_null()) {
            // we will ignore `null` on an option field
            document.erase(field_name);
            continue;
        }

        nlohmann::json::iterator dummy_iter;
        bool array_ele_erased = false;

        if(a_field.type == field_types::STRING && !document[field_name].is_string()) {
            Option<uint32_t> coerce_op = coerce_string(dirty_values, fallback_field_type, a_field, document, field_name, dummy_iter, false, array_ele_erased);
            if(!coerce_op.ok()) {
                return coerce_op;
            }
        } else if(a_field.type == field_types::INT32) {
            if(!document[field_name].is_number_integer()) {
                Option<uint32_t> coerce_op = coerce_int32_t(dirty_values, a_field, document, field_name, dummy_iter, false, array_ele_erased);
                if(!coerce_op.ok()) {
                    return coerce_op;
                }
            }

            if(document[field_name].get<int64_t>() > INT32_MAX) {
                if(a_field.optional && (dirty_values == DIRTY_VALUES::DROP || dirty_values == DIRTY_VALUES::COERCE_OR_REJECT)) {
                    document.erase(field_name);
                    continue;
                } else {
                    return Option<>(400, "Field `" + field_name  + "` exceeds maximum value of int32.");
                }
            }
        } else if(a_field.type == field_types::INT64 && !document[field_name].is_number_integer()) {
            Option<uint32_t> coerce_op = coerce_int64_t(dirty_values, a_field, document, field_name, dummy_iter, false, array_ele_erased);
            if(!coerce_op.ok()) {
                return coerce_op;
            }
        } else if(a_field.type == field_types::FLOAT && !document[field_name].is_number()) {
            // using `is_number` allows integer to be passed to a float field
            Option<uint32_t> coerce_op = coerce_float(dirty_values, a_field, document, field_name, dummy_iter, false, array_ele_erased);
            if(!coerce_op.ok()) {
                return coerce_op;
            }
        } else if(a_field.type == field_types::BOOL && !document[field_name].is_boolean()) {
            Option<uint32_t> coerce_op = coerce_bool(dirty_values, a_field, document, field_name, dummy_iter, false, array_ele_erased);
            if(!coerce_op.ok()) {
                return coerce_op;
            }
        } else if(a_field.type == field_types::GEOPOINT) {
            if(!document[field_name].is_array() || document[field_name].size() != 2) {
                return Option<>(400, "Field `" + field_name  + "` must be a 2 element array: [lat, lng].");
            }

            if(!(document[field_name][0].is_number() && document[field_name][1].is_number())) {
                // one or more elements is not an number, try to coerce
                Option<uint32_t> coerce_op = coerce_geopoint(dirty_values, a_field, document, field_name, dummy_iter, false, array_ele_erased);
                if(!coerce_op.ok()) {
                    return coerce_op;
                }
            }
        } else if(a_field.is_array()) {
            if(!document[field_name].is_array()) {
                if(a_field.optional && (dirty_values == DIRTY_VALUES::DROP ||
                                          dirty_values == DIRTY_VALUES::COERCE_OR_DROP)) {
                    document.erase(field_name);
                    continue;
                } else {
                    return Option<>(400, "Field `" + field_name  + "` must be an array.");
                }
            }

            nlohmann::json::iterator it = document[field_name].begin();
            for(; it != document[field_name].end(); ) {
                const auto& item = it.value();
                array_ele_erased = false;

                if (a_field.type == field_types::STRING_ARRAY && !item.is_string()) {
                    Option<uint32_t> coerce_op = coerce_string(dirty_values, fallback_field_type, a_field, document, field_name, it, true, array_ele_erased);
                    if (!coerce_op.ok()) {
                        return coerce_op;
                    }
                } else if (a_field.type == field_types::INT32_ARRAY && !item.is_number_integer()) {
                    Option<uint32_t> coerce_op = coerce_int32_t(dirty_values, a_field, document, field_name, it, true, array_ele_erased);
                    if (!coerce_op.ok()) {
                        return coerce_op;
                    }
                } else if (a_field.type == field_types::INT64_ARRAY && !item.is_number_integer()) {
                    Option<uint32_t> coerce_op = coerce_int64_t(dirty_values, a_field, document, field_name, it, true, array_ele_erased);
                    if (!coerce_op.ok()) {
                        return coerce_op;
                    }
                } else if (a_field.type == field_types::FLOAT_ARRAY && !item.is_number()) {
                    // we check for `is_number` to allow whole numbers to be passed into float fields
                    Option<uint32_t> coerce_op = coerce_float(dirty_values, a_field, document, field_name, it, true, array_ele_erased);
                    if (!coerce_op.ok()) {
                        return coerce_op;
                    }
                } else if (a_field.type == field_types::BOOL_ARRAY && !item.is_boolean()) {
                    Option<uint32_t> coerce_op = coerce_bool(dirty_values, a_field, document, field_name, it, true, array_ele_erased);
                    if (!coerce_op.ok()) {
                        return coerce_op;
                    }
                } else if (a_field.type == field_types::GEOPOINT_ARRAY) {
                    if(!item.is_array() || item.size() != 2) {
                        return Option<>(400, "Field `" + field_name  + "` must contain 2 element arrays: [ [lat, lng],... ].");
                    }

                    if(!(item[0].is_number() && item[1].is_number())) {
                        // one or more elements is not an number, try to coerce
                        Option<uint32_t> coerce_op = coerce_geopoint(dirty_values, a_field, document, field_name, it, true, array_ele_erased);
                        if(!coerce_op.ok()) {
                            return coerce_op;
                        }
                    }
                }

                if(!array_ele_erased) {
                    // if it is erased, the iterator will be reassigned
                    it++;
                }
            }
        }
    }

    return Option<>(200);
}

void Index::validate_and_preprocess(Index *index, std::vector<index_record>& iter_batch,
                                      const size_t batch_start_index, const size_t batch_size,
                                      const std::string& default_sorting_field,
                                      const std::unordered_map<std::string, field>& search_schema,
                                      const std::map<std::string, field>& facet_schema,
                                      const std::string& fallback_field_type,
                                      const std::vector<char>& token_separators,
                                      const std::vector<char>& symbols_to_index) {

    // runs in a partitioned thread

    for(size_t i = 0; i < batch_size; i++) {
        index_record& index_rec = iter_batch[batch_start_index + i];

        if(!index_rec.indexed.ok()) {
            // some records could have been invalidated upstream
            continue;
        }

        if(index_rec.operation == DELETE) {
            continue;
        }

        Option<uint32_t> validation_op = validate_index_in_memory(index_rec.doc, index_rec.seq_id,
                                                                  default_sorting_field,
                                                                  search_schema, facet_schema,
                                                                  index_rec.operation,
                                                                  fallback_field_type,
                                                                  index_rec.dirty_values);

        if(!validation_op.ok()) {
            index_rec.index_failure(validation_op.code(), validation_op.error());
            continue;
        }

        if(index_rec.is_update) {
            // scrub string fields to reduce delete ops
            get_doc_changes(index_rec.operation, index_rec.doc, index_rec.old_doc, index_rec.new_doc,
                            index_rec.del_doc);
            scrub_reindex_doc(search_schema, index_rec.doc, index_rec.del_doc, index_rec.old_doc);
        }

        compute_token_offsets_facets(index_rec, search_schema, facet_schema, token_separators, symbols_to_index);
    }
}

size_t Index::batch_memory_index(Index *index, std::vector<index_record>& iter_batch,
                                 const std::string & default_sorting_field,
                                 const std::unordered_map<std::string, field> & search_schema,
                                 const std::map<std::string, field> & facet_schema,
                                 const std::string& fallback_field_type,
                                 const std::vector<char>& token_separators,
                                 const std::vector<char>& symbols_to_index) {

    const size_t concurrency = 4;
    const size_t num_threads = std::min(concurrency, iter_batch.size());
    const size_t window_size = (num_threads == 0) ? 0 :
                               (iter_batch.size() + num_threads - 1) / num_threads;  // rounds up

    size_t num_indexed = 0;
    size_t num_processed = 0;
    std::mutex m_process;
    std::condition_variable cv_process;

    size_t num_queued = 0;
    size_t batch_index = 0;

    for(size_t thread_id = 0; thread_id < num_threads && batch_index < iter_batch.size(); thread_id++) {
        size_t batch_len = window_size;

        if(batch_index + window_size > iter_batch.size()) {
            batch_len = iter_batch.size() - batch_index;
        }

        num_queued++;

        index->thread_pool->enqueue([&, batch_index, batch_len]() {
            validate_and_preprocess(index, iter_batch, batch_index, batch_len, default_sorting_field, search_schema,
                                    facet_schema, fallback_field_type,
                                    token_separators, symbols_to_index);

            std::unique_lock<std::mutex> lock(m_process);
            num_processed++;
            cv_process.notify_one();
        });

        batch_index += batch_len;
    }

    std::unique_lock<std::mutex> lock_process(m_process);
    cv_process.wait(lock_process, [&](){ return num_processed == num_queued; });

    Option<uint32_t> index_mem_op(0);

    for(auto& index_rec: iter_batch) {
        if(index_rec.operation == DELETE) {
            continue;
        }

        if(!index_rec.indexed.ok()) {
            // some records could have been invalidated upstream
            continue;
        }

        if(index_rec.is_update) {
            index->remove(index_rec.seq_id, index_rec.del_doc, index_rec.is_update);
        }

        try {
            index_mem_op = index->index_in_memory(index_rec, index_rec.seq_id, default_sorting_field, index_rec.is_update);
        } catch(const std::exception& e) {
            const std::string& error_msg = std::string("Fatal error during indexing: ") + e.what();
            LOG(ERROR) << error_msg << ", document: " << index_rec.doc;
            index_mem_op = Option<uint32_t>(500, error_msg);
        }

        index_rec.index_success();

        if(!index_rec.is_update) {
            num_indexed++;
        }
    }

    return num_indexed;
}

void Index::insert_doc(const int64_t score, art_tree *t, uint32_t seq_id,
                       const std::unordered_map<std::string, std::vector<uint32_t>> &token_to_offsets) const {
    for(auto & kv: token_to_offsets) {
        art_document art_doc;
        art_doc.id = seq_id;
        art_doc.score = score;
        art_doc.offsets = kv.second;

        uint32_t num_hits = 0;

        const unsigned char *key = (const unsigned char *) kv.first.c_str();
        int key_len = (int) kv.first.length() + 1;  // for the terminating \0 char

        //LOG(INFO) << "key: " << key << ", art_doc.id: " << art_doc.id;
        art_insert(t, key, key_len, &art_doc);
    }
}

uint64_t Index::facet_token_hash(const field & a_field, const std::string &token) {
    // for integer/float use their native values
    uint64_t hash = 0;

    if(a_field.is_float()) {
        float f = std::stof(token);
        reinterpret_cast<float&>(hash) = f;  // store as int without loss of precision
    } else if(a_field.is_integer() || a_field.is_bool()) {
        hash = atoll(token.c_str());
    } else {
        // string field
        hash = StringUtils::hash_wy(token.c_str(), token.size());
    }

    return hash;
}

void Index::tokenize_string_with_facets(const std::string& text, bool is_facet, const field& a_field,
                                        const std::vector<char>& symbols_to_index,
                                        const std::vector<char>& token_separators,
                                        std::unordered_map<std::string, std::vector<uint32_t>>& token_to_offsets,
                                        std::vector<uint64_t>& facet_hashes) {

    Tokenizer tokenizer(text, true, !a_field.is_string(), a_field.locale, symbols_to_index, token_separators);
    std::string token;
    std::string last_token;
    size_t token_index = 0;

    while(tokenizer.next(token, token_index)) {
        if(token.empty()) {
            continue;
        }

        token_to_offsets[token].push_back(token_index + 1);
        last_token = token;
    }

    if(!token_to_offsets.empty()) {
        // push 0 for the last occurring token (used for exact match ranking)
        token_to_offsets[last_token].push_back(0);
    }

    if(is_facet) {
        uint64_t hash = Index::facet_token_hash(a_field, text);
        facet_hashes.push_back(hash);
    }
}

void Index::index_strings_field(const int64_t score, art_tree *t,
                               uint32_t seq_id, bool is_facet, const field & a_field,
                               const std::unordered_map<std::string, std::vector<uint32_t>>& token_to_offsets,
                               const std::vector<uint64_t>& facet_hashes) {

    // requires unique lock from caller

    if(token_to_offsets.empty()) {
        return;
    }

    insert_doc(score, t, seq_id, token_to_offsets);

    if(is_facet) {
        facet_hash_values_t fhashvalues;
        fhashvalues.length = facet_hashes.size();
        fhashvalues.hashes = new uint64_t[facet_hashes.size()];

        for(size_t i  = 0; i < facet_hashes.size(); i++) {
            fhashvalues.hashes[i] = facet_hashes[i];
        }

        facet_index_v3[a_field.name]->emplace(seq_id, std::move(fhashvalues));
    }
}

void Index::tokenize_string_array_with_facets(const std::vector<std::string>& strings, bool is_facet,
                                              const field& a_field,
                                              const std::vector<char>& symbols_to_index,
                                              const std::vector<char>& token_separators,
                                              std::unordered_map<std::string, std::vector<uint32_t>>& token_to_offsets,
                                              std::vector<uint64_t>& facet_hashes) {

    for(size_t array_index = 0; array_index < strings.size(); array_index++) {
        const std::string& str = strings[array_index];
        std::set<std::string> token_set;  // required to deal with repeating tokens

        Tokenizer tokenizer(str, true, !a_field.is_string(), a_field.locale, symbols_to_index, token_separators);
        std::string token, last_token;
        size_t token_index = 0;

        // iterate and append offset positions
        while(tokenizer.next(token, token_index)) {
            if(token.empty()) {
                continue;
            }

            token_to_offsets[token].push_back(token_index + 1);
            token_set.insert(token);
            last_token = token;
        }

        //LOG(INFO) << "Str: " << str << ", last_token: " << last_token;

        if(token_set.empty()) {
            continue;
        }

        if(is_facet) {
            uint64_t hash = facet_token_hash(a_field, str);
            //LOG(INFO) << "indexing " << token  << ", hash:" << hash;
            facet_hashes.push_back(hash);
        }

        for(auto& the_token: token_set) {
            // repeat last element to indicate end of offsets for this array index
            token_to_offsets[the_token].push_back(token_to_offsets[the_token].back());

            // iterate and append this array index to all tokens
            token_to_offsets[the_token].push_back(array_index);
        }

        // push 0 for the last occurring token (used for exact match ranking)
        token_to_offsets[last_token].push_back(0);
    }
}

void Index::compute_facet_stats(facet &a_facet, uint64_t raw_value, const std::string & field_type) {
    if(field_type == field_types::INT32 || field_type == field_types::INT32_ARRAY) {
        int32_t val = raw_value;
        if (val < a_facet.stats.fvmin) {
            a_facet.stats.fvmin = val;
        }
        if (val > a_facet.stats.fvmax) {
            a_facet.stats.fvmax = val;
        }
        a_facet.stats.fvsum += val;
        a_facet.stats.fvcount++;
    } else if(field_type == field_types::INT64 || field_type == field_types::INT64_ARRAY) {
        int64_t val = raw_value;
        if(val < a_facet.stats.fvmin) {
            a_facet.stats.fvmin = val;
        }
        if(val > a_facet.stats.fvmax) {
            a_facet.stats.fvmax = val;
        }
        a_facet.stats.fvsum += val;
        a_facet.stats.fvcount++;
    } else if(field_type == field_types::FLOAT || field_type == field_types::FLOAT_ARRAY) {
        float val = reinterpret_cast<float&>(raw_value);
        if(val < a_facet.stats.fvmin) {
            a_facet.stats.fvmin = val;
        }
        if(val > a_facet.stats.fvmax) {
            a_facet.stats.fvmax = val;
        }
        a_facet.stats.fvsum += val;
        a_facet.stats.fvcount++;
    }
}

void Index::do_facets(std::vector<facet> & facets, facet_query_t & facet_query,
                      const std::vector<facet_info_t>& facet_infos,
                      const size_t group_limit, const std::vector<std::string>& group_by_fields,
                      const uint32_t* result_ids, size_t results_size) const {
    
    // assumed that facet fields have already been validated upstream
    for(size_t findex=0; findex < facets.size(); findex++) {
        auto& a_facet = facets[findex];
        const auto& facet_field = facet_infos[findex].facet_field;
        const bool use_facet_query = facet_infos[findex].use_facet_query;
        const auto& fquery_hashes = facet_infos[findex].hashes;
        const bool should_compute_stats = facet_infos[findex].should_compute_stats;

        const auto& field_facet_mapping_it = facet_index_v3.find(a_facet.field_name);
        if(field_facet_mapping_it == facet_index_v3.end()) {
            continue;
        }

        const auto& field_facet_mapping = field_facet_mapping_it->second;

        for(size_t i = 0; i < results_size; i++) {
            uint32_t doc_seq_id = result_ids[i];
            const auto& facet_hashes_it = field_facet_mapping->find(doc_seq_id);

            if(facet_hashes_it == field_facet_mapping->end()) {
                continue;
            }

            const auto& facet_hashes = facet_hashes_it->second;
            const uint64_t distinct_id = group_limit ? get_distinct_id(group_by_fields, doc_seq_id) : 0;

            for(size_t j = 0; j < facet_hashes.size(); j++) {
                auto fhash = facet_hashes.hashes[j];

                if(should_compute_stats) {
                    compute_facet_stats(a_facet, fhash, facet_field.type);
                }

                if(!use_facet_query || fquery_hashes.find(fhash) != fquery_hashes.end()) {
                    if(a_facet.result_map.count(fhash) == 0) {
                        a_facet.result_map.emplace(fhash, facet_count_t{0, spp::sparse_hash_set<uint64_t>(),
                                                                        doc_seq_id, 0, {}});
                    }

                    facet_count_t& facet_count = a_facet.result_map[fhash];

                    //LOG(INFO) << "field: " << a_facet.field_name << ", doc id: " << doc_seq_id << ", hash: " <<  fhash;

                    facet_count.doc_id = doc_seq_id;
                    facet_count.array_pos = j;

                    if(group_limit) {
                        facet_count.groups.emplace(distinct_id);
                    } else {
                        facet_count.count += 1;
                    }

                    if(use_facet_query) {
                        facet_count.tokens = fquery_hashes.at(fhash);
                    }
                }
            }
        }
    }
}

void Index::aggregate_topster(Topster* agg_topster, Topster* index_topster) {
    if(index_topster->distinct) {
        for(auto &group_topster_entry: index_topster->group_kv_map) {
            Topster* group_topster = group_topster_entry.second;
            for(const auto& map_kv: group_topster->kv_map) {
                agg_topster->add(map_kv.second);
            }
        }
    } else {
        for(const auto& map_kv: index_topster->kv_map) {
            agg_topster->add(map_kv.second);
        }
    }
}

void Index::search_candidates(const uint8_t & field_id, bool field_is_array,
                              const uint32_t* filter_ids, size_t filter_ids_length,
                              const uint32_t* exclude_token_ids, size_t exclude_token_ids_size,
                              const std::vector<uint32_t>& curated_ids,
                              const std::vector<sort_by> & sort_fields,
                              std::vector<token_candidates> & token_candidates_vec,
                              std::vector<std::vector<art_leaf*>> & searched_queries,
                              Topster* topster,
                              spp::sparse_hash_set<uint64_t>& groups_processed,
                              uint32_t** all_result_ids, size_t & all_result_ids_len,
                              size_t& field_num_results,
                              const size_t typo_tokens_threshold,
                              const size_t group_limit,
                              const std::vector<std::string>& group_by_fields,
                              const std::vector<token_t>& query_tokens,
                              bool prioritize_exact_match,
                              const bool exhaustive_search,
                              const size_t concurrency,
                              std::set<uint64>& query_hashes) const {

    auto product = []( long long a, token_candidates & b ) { return a*b.candidates.size(); };
    long long int N = std::accumulate(token_candidates_vec.begin(), token_candidates_vec.end(), 1LL, product);

    int sort_order[3]; // 1 or -1 based on DESC or ASC respectively
    std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3> field_values;
    std::vector<size_t> geopoint_indices;

    populate_sort_mapping(sort_order, geopoint_indices, sort_fields, field_values);

    size_t combination_limit = exhaustive_search ? Index::COMBINATION_MAX_LIMIT : Index::COMBINATION_MIN_LIMIT;

    for(long long n=0; n<N && n<combination_limit; ++n) {
        RETURN_CIRCUIT_BREAKER

        // every element in `query_suggestion` contains a token and its associated hits
        std::vector<art_leaf*> query_suggestion(token_candidates_vec.size());

        // actual query suggestion preserves original order of tokens in query
        std::vector<art_leaf*> actual_query_suggestion(token_candidates_vec.size());
        uint64 qhash;

        uint32_t token_bits = (uint32_t(1) << 31);  // top most bit set to guarantee atleast 1 bit set
        uint32_t total_cost = next_suggestion(token_candidates_vec, n, actual_query_suggestion,
                                              query_suggestion, token_bits, qhash);

        if(query_hashes.find(qhash) != query_hashes.end()) {
            // skip this query since it has already been processed before
            continue;
        }

        query_hashes.insert(qhash);

        //LOG(INFO) << "field_num_results: " << field_num_results << ", typo_tokens_threshold: " << typo_tokens_threshold;
        //LOG(INFO) << "n: " << n;

        /*std::stringstream fullq;
        for(const auto& qleaf : actual_query_suggestion) {
            std::string qtok(reinterpret_cast<char*>(qleaf->key),qleaf->key_len - 1);
            fullq << qtok << " ";
        }
        LOG(INFO) << "field: " << size_t(field_id) << ", query: " << fullq.str() << ", total_cost: " << total_cost;*/

        // Prepare excluded document IDs that we can later remove from the result set
        uint32_t* excluded_result_ids = nullptr;
        size_t excluded_result_ids_size = ArrayUtils::or_scalar(exclude_token_ids, exclude_token_ids_size,
                                                            &curated_ids[0], curated_ids.size(), &excluded_result_ids);

        std::vector<void*> posting_lists;

        for(auto& query_leaf : query_suggestion) {
            posting_lists.push_back(query_leaf->values);
        }

        posting_list_t::result_iter_state_t iter_state(
            excluded_result_ids, excluded_result_ids_size, filter_ids, filter_ids_length
        );

        // We fetch offset positions only for multi token query
        bool fetch_offsets = (query_suggestion.size() > 1);
        bool single_exact_query_token = false;

        if(total_cost == 0 && query_suggestion.size() == query_tokens.size() == 1) {
            // does this candidate suggestion token match query token exactly?
            single_exact_query_token = true;
        }

        std::vector<uint32_t> result_id_vecs[concurrency];
        Topster* topsters[concurrency];
        std::vector<spp::sparse_hash_set<uint64_t>> groups_processed_vec(concurrency);

        if(topster == nullptr) {
            posting_t::block_intersector_t(
                posting_lists, iter_state, thread_pool, 100
            )
            .intersect([&](uint32_t seq_id, std::vector<posting_list_t::iterator_t>& its, size_t index) {
                result_id_vecs[index].push_back(seq_id);
            }, concurrency);
        } else {
            for(size_t i = 0; i < concurrency; i++) {
                topsters[i] = new Topster(topster->MAX_SIZE, topster->distinct);
            }

            posting_t::block_intersector_t(
                posting_lists, iter_state, thread_pool, 100
            )
            .intersect([&](uint32_t seq_id, std::vector<posting_list_t::iterator_t>& its, size_t index) {
                score_results(sort_fields, searched_queries.size(), field_id, field_is_array,
                              total_cost, topsters[index], query_suggestion, groups_processed_vec[index],
                              seq_id, sort_order, field_values, geopoint_indices,
                              group_limit, group_by_fields, token_bits,
                              prioritize_exact_match, single_exact_query_token, its);

                result_id_vecs[index].push_back(seq_id);
            }, concurrency);
        }

        delete [] excluded_result_ids;

        size_t num_result_ids = 0;

        for(size_t i = 0; i < concurrency; i++) {
            // empty vec can happen if not all threads produce results
            if (!result_id_vecs[i].empty()) {
                uint32_t* new_all_result_ids = nullptr;
                all_result_ids_len = ArrayUtils::or_scalar(*all_result_ids, all_result_ids_len, &result_id_vecs[i][0],
                                                           result_id_vecs[i].size(), &new_all_result_ids);
                delete[] *all_result_ids;
                *all_result_ids = new_all_result_ids;

                num_result_ids += result_id_vecs[i].size();

                if (topster != nullptr) {
                    // topster is null when used by overrides which requires only IDs but not actual processing
                    aggregate_topster(topster, topsters[i]);
                    groups_processed.insert(groups_processed_vec[i].begin(), groups_processed_vec[i].end());
                }
            }

            if(topster != nullptr) {
                delete topsters[i];
            }
        }

        if(num_result_ids == 0) {
            continue;
        }

        field_num_results += num_result_ids;
        searched_queries.push_back(actual_query_suggestion);
    }
}

void Index::do_filtering(uint32_t*& filter_ids, uint32_t& filter_ids_length,
                         const std::vector<filter>& filters,
                         const bool enable_short_circuit) const {
    //auto begin = std::chrono::high_resolution_clock::now();
    for(size_t i = 0; i < filters.size(); i++) {
        const filter & a_filter = filters[i];

        if(enable_short_circuit) {
            RETURN_CIRCUIT_BREAKER
        }

        if(a_filter.field_name == "id") {
            // we handle `ids` separately
            std::vector<uint32> result_ids;
            for(const auto& id_str: a_filter.values) {
                result_ids.push_back(std::stoul(id_str));
            }

            if(i == 0) {
                filter_ids = new uint32[result_ids.size()];
                std::copy(result_ids.begin(), result_ids.end(), filter_ids);
                filter_ids_length = result_ids.size();
            } else {
                uint32_t* filtered_results = nullptr;
                filter_ids_length = ArrayUtils::and_scalar(filter_ids, filter_ids_length, &result_ids[0],
                                                           result_ids.size(), &filtered_results);
                delete [] filter_ids;
                filter_ids = filtered_results;
            }

            continue;
        }

        bool has_search_index = search_index.count(a_filter.field_name) != 0 ||
                                numerical_index.count(a_filter.field_name) != 0 ||
                                geopoint_index.count(a_filter.field_name) != 0;

        if(!has_search_index) {
            continue;
        }

        field f = search_schema.at(a_filter.field_name);

        uint32_t* result_ids = nullptr;
        size_t result_ids_len = 0;

        if(f.is_integer()) {
            auto num_tree = numerical_index.at(a_filter.field_name);

            for(size_t fi=0; fi < a_filter.values.size(); fi++) {
                const std::string & filter_value = a_filter.values[fi];
                int64_t value = (int64_t) std::stol(filter_value);

                if(a_filter.comparators[fi] == RANGE_INCLUSIVE && fi+1 < a_filter.values.size()) {
                    const std::string& next_filter_value = a_filter.values[fi+1];
                    int64_t range_end_value = (int64_t) std::stol(next_filter_value);
                    num_tree->range_inclusive_search(value, range_end_value, &result_ids, result_ids_len);
                    fi++;
                } else {
                    num_tree->search(a_filter.comparators[fi], value, &result_ids, result_ids_len);
                }
            }

        } else if(f.is_float()) {
            auto num_tree = numerical_index.at(a_filter.field_name);

            for(size_t fi=0; fi < a_filter.values.size(); fi++) {
                const std::string & filter_value = a_filter.values[fi];
                float value = (float) std::atof(filter_value.c_str());
                int64_t float_int64 = float_to_in64_t(value);

                if(a_filter.comparators[fi] == RANGE_INCLUSIVE && fi+1 < a_filter.values.size()) {
                    const std::string& next_filter_value = a_filter.values[fi+1];
                    int64_t range_end_value = float_to_in64_t((float) std::atof(next_filter_value.c_str()));
                    num_tree->range_inclusive_search(float_int64, range_end_value, &result_ids, result_ids_len);
                    fi++;
                } else {
                    num_tree->search(a_filter.comparators[fi], float_int64, &result_ids, result_ids_len);
                }
            }

        } else if(f.is_bool()) {
            auto num_tree = numerical_index.at(a_filter.field_name);

            size_t value_index = 0;
            for(const std::string & filter_value: a_filter.values) {
                int64_t bool_int64 = (filter_value == "1") ? 1 : 0;
                if(a_filter.comparators[value_index] == NOT_EQUALS) {
                    uint32_t* to_exclude_ids = nullptr;
                    size_t to_exclude_ids_len = 0;
                    num_tree->search(EQUALS, bool_int64, &to_exclude_ids, to_exclude_ids_len);

                    auto all_ids = seq_ids.uncompress();
                    auto all_ids_size = seq_ids.getLength();

                    uint32_t* excluded_ids = nullptr;
                    size_t excluded_ids_len = 0;

                    excluded_ids_len = ArrayUtils::exclude_scalar(all_ids, all_ids_size, to_exclude_ids,
                                                                  to_exclude_ids_len, &excluded_ids);

                    delete [] all_ids;
                    delete [] to_exclude_ids;

                    uint32_t *out = nullptr;
                    result_ids_len = ArrayUtils::or_scalar(result_ids, result_ids_len,
                                                           excluded_ids, excluded_ids_len, &out);
                    delete [] result_ids;
                    result_ids = out;
                    delete [] excluded_ids;
                } else {
                    num_tree->search(a_filter.comparators[value_index], bool_int64, &result_ids, result_ids_len);
                }

                value_index++;
            }

        } else if(f.is_geopoint()) {
            std::set<uint32_t> geo_result_ids;

            for(const std::string& filter_value: a_filter.values) {
                std::vector<std::string> filter_value_parts;
                StringUtils::split(filter_value, filter_value_parts, ",");  // x, y, 2 km (or) list of points

                bool is_polygon = StringUtils::is_float(filter_value_parts.back());
                S2Region* query_region;

                if(is_polygon) {
                    const int num_verts = int(filter_value_parts.size()) / 2;
                    std::vector<S2Point> vertices;
                    double sum = 0.0;

                    for(size_t point_index = 0; point_index < size_t(num_verts); point_index++) {
                        double lat = std::stod(filter_value_parts[point_index * 2]);
                        double lon = std::stod(filter_value_parts[point_index * 2 + 1]);
                        S2Point vertex = S2LatLng::FromDegrees(lat, lon).ToPoint();
                        vertices.emplace_back(vertex);
                    }

                    for(size_t vi = 0; vi < vertices.size(); vi++) {
                        auto& v1 = vertices[vi];
                        auto& v2 = vertices[(vi + 1) % vertices.size()];
                        sum += (v2.x() - v1.x()) * (v2.y() + v1.y());
                    }

                    bool is_clockwise = (sum > 0.0);

                    if(is_clockwise) {
                        std::reverse(vertices.begin(), vertices.end());
                    }

                    query_region = new S2Loop(vertices);
                } else {
                    double radius = std::stof(filter_value_parts[2]);
                    const auto& unit = filter_value_parts[3];

                    if(unit == "km") {
                        radius *= 1000;
                    } else {
                        // assume "mi" (validated upstream)
                        radius *= 1609.34;
                    }

                    S1Angle query_radius = S1Angle::Radians(S2Earth::MetersToRadians(radius));
                    double query_lat = std::stod(filter_value_parts[0]);
                    double query_lng = std::stod(filter_value_parts[1]);
                    S2Point center = S2LatLng::FromDegrees(query_lat, query_lng).ToPoint();
                    query_region = new S2Cap(center, query_radius);
                }

                S2RegionTermIndexer::Options options;
                options.set_index_contains_points_only(true);
                S2RegionTermIndexer indexer(options);

                for (const auto& term : indexer.GetQueryTerms(*query_region, "")) {
                    auto geo_index = geopoint_index.at(a_filter.field_name);
                    const auto& ids_it = geo_index->find(term);
                    if(ids_it != geo_index->end()) {
                        geo_result_ids.insert(ids_it->second.begin(), ids_it->second.end());
                    }
                }

                // `geo_result_ids` will contain all IDs that are within approximately within query radius
                // we still need to do another round of exact filtering on them

                std::vector<uint32_t> exact_geo_result_ids;

                if(f.is_single_geopoint()) {
                    for(auto result_id: geo_result_ids) {
                        // no need to check for existence of `result_id` because of indexer based pre-filtering above
                        int64_t lat_lng = sort_index.at(f.name)->at(result_id);
                        S2LatLng s2_lat_lng;
                        GeoPoint::unpack_lat_lng(lat_lng, s2_lat_lng);
                        if (query_region->Contains(s2_lat_lng.ToPoint())) {
                            exact_geo_result_ids.push_back(result_id);
                        }
                    }
                } else {
                    for(auto result_id: geo_result_ids) {
                        int64_t* lat_lngs = geo_array_index.at(f.name)->at(result_id);

                        bool point_found = false;

                        // any one point should exist
                        for(size_t li = 0; li < lat_lngs[0]; li++) {
                            int64_t lat_lng = lat_lngs[li + 1];
                            S2LatLng s2_lat_lng;
                            GeoPoint::unpack_lat_lng(lat_lng, s2_lat_lng);
                            if (query_region->Contains(s2_lat_lng.ToPoint())) {
                                point_found = true;
                                break;
                            }
                        }

                        if(point_found) {
                            exact_geo_result_ids.push_back(result_id);
                        }
                    }
                }

                std::sort(exact_geo_result_ids.begin(), exact_geo_result_ids.end());

                uint32_t *out = nullptr;
                result_ids_len = ArrayUtils::or_scalar(&exact_geo_result_ids[0], exact_geo_result_ids.size(),
                                                       result_ids, result_ids_len, &out);

                delete [] result_ids;
                result_ids = out;

                delete query_region;
            }

        } else if(f.is_string()) {
            art_tree* t = search_index.at(a_filter.field_name);

            uint32_t* ids = nullptr;
            size_t ids_size = 0;

            for(const std::string & filter_value: a_filter.values) {
                uint32_t* strt_ids = nullptr;
                size_t strt_ids_size = 0;

                std::vector<void*> posting_lists;

                // there could be multiple tokens in a filter value, which we have to treat as ANDs
                // e.g. country: South Africa

                Tokenizer tokenizer(filter_value, true, false, f.locale, symbols_to_index, token_separators);

                std::string str_token;
                size_t token_index = 0;
                std::vector<std::string> str_tokens;

                while(tokenizer.next(str_token, token_index)) {
                    str_tokens.push_back(str_token);

                    art_leaf* leaf = (art_leaf *) art_search(t, (const unsigned char*) str_token.c_str(),
                                                             str_token.length()+1);
                    if(leaf == nullptr) {
                        continue;
                    }

                    posting_lists.push_back(leaf->values);
                }

                // For NOT_EQUALS alone, it is okay for none of the results to match prior to negation
                // e.g. field:!= [RANDOM_NON_EXISTING_STRING]
                if(a_filter.comparators[0] != NOT_EQUALS && posting_lists.size() != str_tokens.size()) {
                    continue;
                }

                std::vector<uint32_t> result_id_vec;
                posting_t::intersect(posting_lists, result_id_vec);
                if(!result_id_vec.empty()) {
                    strt_ids = new uint32_t [result_id_vec.size()];
                    std::copy(result_id_vec.begin(), result_id_vec.end(), strt_ids);
                    strt_ids_size = result_id_vec.size();
                }

                if((a_filter.comparators[0] == EQUALS || a_filter.comparators[0] == NOT_EQUALS) && f.is_facet()) {
                    // need to do exact match (unlike CONTAINS) by using the facet index
                    // field being a facet is already enforced upstream
                    uint32_t* exact_strt_ids = new uint32_t[strt_ids_size];
                    size_t exact_strt_size = 0;

                    posting_t::get_exact_matches(posting_lists, f.is_array(), strt_ids, strt_ids_size,
                                                 exact_strt_ids, exact_strt_size);

                    delete[] strt_ids;
                    strt_ids = exact_strt_ids;
                    strt_ids_size = exact_strt_size;
                }

                if(a_filter.comparators[0] == NOT_EQUALS && f.is_facet()) {
                    // exclude records from existing IDs (from previous filters or ALL records)
                    // upstream will guarantee that NOT_EQUALS is placed right at the end of filters list
                    if(ids == nullptr) {
                        if(filter_ids == nullptr) {
                            ids = seq_ids.uncompress();
                            ids_size = seq_ids.getLength();
                        } else {
                            ids = filter_ids;
                            ids_size = filter_ids_length;
                        }
                    }

                    uint32_t* excluded_strt_ids = nullptr;
                    size_t excluded_strt_size = 0;
                    excluded_strt_size = ArrayUtils::exclude_scalar(ids, ids_size, strt_ids,
                                                                    strt_ids_size, &excluded_strt_ids);

                    if(filter_ids == nullptr) {
                        // means we had to uncompress `seq_ids` so need to free that
                        delete [] ids;
                    }

                    ids = excluded_strt_ids;
                    ids_size = excluded_strt_size;
                    delete[] strt_ids;
                } else {
                    // Otherwise, we just ensure that given record contains tokens in the filter query
                    uint32_t* out = nullptr;
                    ids_size = ArrayUtils::or_scalar(ids, ids_size, strt_ids, strt_ids_size, &out);
                    delete[] strt_ids;
                    delete[] ids;
                    ids = out;
                }
            }

            result_ids = ids;
            result_ids_len = ids_size;
        }

        if(i == 0) {
            filter_ids = result_ids;
            filter_ids_length = result_ids_len;
        } else {
            uint32_t* filtered_results = nullptr;
            filter_ids_length = ArrayUtils::and_scalar(filter_ids, filter_ids_length, result_ids,
                                                       result_ids_len, &filtered_results);
            delete [] result_ids;
            delete [] filter_ids;
            filter_ids = filtered_results;
        }
    }

    /*long long int timeMillis =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();

    LOG(INFO) << "Time taken for filtering: " << timeMillis << "ms";*/
}


void Index::do_filtering_with_lock(uint32_t*& filter_ids, uint32_t& filter_ids_length,
                                   const std::vector<filter>& filters) const {
    std::shared_lock lock(mutex);
    do_filtering(filter_ids, filter_ids_length, filters, false);
}

void Index::run_search(search_args* search_params) {
    search(search_params->field_query_tokens,
           search_params->search_fields,
           search_params->filters, search_params->facets, search_params->facet_query,
           search_params->included_ids, search_params->excluded_ids,
           search_params->sort_fields_std, search_params->num_typos,
           search_params->topster, search_params->curated_topster,
           search_params->per_page, search_params->page, search_params->token_order,
           search_params->prefixes, search_params->drop_tokens_threshold,
           search_params->all_result_ids_len, search_params->groups_processed,
           search_params->searched_queries,
           search_params->raw_result_kvs, search_params->override_result_kvs,
           search_params->typo_tokens_threshold,
           search_params->group_limit, search_params->group_by_fields,
           search_params->filter_overrides,
           search_params->default_sorting_field,
           search_params->prioritize_exact_match,
           search_params->exhaustive_search,
           search_params->concurrency,
           search_params->search_cutoff_ms,
           search_params->min_len_1typo,
           search_params->min_len_2typo);
}

void Index::collate_included_ids(const std::vector<std::string>& q_included_tokens,
                                 const std::string & field, const uint8_t field_id,
                                 const std::map<size_t, std::map<size_t, uint32_t>> & included_ids_map,
                                 Topster* curated_topster,
                                 std::vector<std::vector<art_leaf*>> & searched_queries) const {

    if(included_ids_map.empty()) {
        return;
    }

    // calculate match_score and add to topster independently

    std::vector<art_leaf *> override_query;

    for(const std::string& token: q_included_tokens) {
        if(token == "*") {
            continue;
        }

        const size_t token_len = token.size() + 1;

        std::vector<art_leaf*> leaves;
        art_fuzzy_search(search_index.at(field), (const unsigned char *) token.c_str(), token_len,
                         0, 0, 1, token_ordering::MAX_SCORE, false, nullptr, 0, leaves);

        if(!leaves.empty()) {
            override_query.push_back(leaves[0]);
        }
    }

    for(const auto& pos_ids: included_ids_map) {
        const size_t outer_pos = pos_ids.first;

        for(const auto& index_seq_id: pos_ids.second) {
            uint32_t inner_pos = index_seq_id.first;
            uint32_t seq_id = index_seq_id.second;

            uint64_t distinct_id = outer_pos;                           // outer pos is the group distinct key
            uint64_t match_score = (64000 - outer_pos - inner_pos);    // both outer pos and inner pos inside group

            // LOG(INFO) << "seq_id: " << seq_id << " - " << match_score;

            int64_t scores[3];
            scores[0] = match_score;
            scores[1] = int64_t(1);
            scores[2] = int64_t(1);

            uint32_t token_bits = (uint32_t(1) << 31);
            KV kv(field_id, searched_queries.size(), token_bits, seq_id, distinct_id, 0, scores);
            curated_topster->add(&kv);
        }
    }

    searched_queries.push_back(override_query);
}

void Index::concat_topster_ids(Topster* topster, spp::sparse_hash_map<uint64_t, std::vector<KV*>>& topster_ids) {
    if(topster->distinct) {
        for(auto &group_topster_entry: topster->group_kv_map) {
            Topster* group_topster = group_topster_entry.second;
            for(const auto& map_kv: group_topster->kv_map) {
                topster_ids[map_kv.first].push_back(map_kv.second);
            }
        }
    } else {
        for(const auto& map_kv: topster->kv_map) {
            //LOG(INFO) << "map_kv.second.key: " << map_kv.second->key;
            //LOG(INFO) << "map_kv.first: " << map_kv.first;
            topster_ids[map_kv.first].push_back(map_kv.second);
        }
    }
}

bool Index::static_filter_query_eval(const override_t* override,
                                     std::vector<std::string>& tokens,
                                     std::vector<filter>& filters) const {

    std::string query = StringUtils::join(tokens, " ");

    if( (override->rule.match == override_t::MATCH_EXACT && override->rule.query == query) ||
        (override->rule.match == override_t::MATCH_CONTAINS && query.find(override->rule.query) != std::string::npos) )  {

        Option<bool> filter_op = filter::parse_filter_query(override->filter_by, search_schema,
                                                            store, "", filters);
        return filter_op.ok();
    }

    return false;
}

bool Index::resolve_override(const std::vector<std::string>& rule_tokens, const bool exact_rule_match,
                             const std::vector<std::string>& query_tokens,
                             token_ordering token_order, std::set<std::string>& absorbed_tokens,
                             std::string& filter_by_clause) const {

    bool resolved_override = false;
    size_t i = 0, j = 0;

    std::unordered_map<std::string, std::vector<std::string>> field_placeholder_tokens;

    while(i < rule_tokens.size()) {
        if(rule_tokens[i].front() == '{' && rule_tokens[i].back() == '}') {
            // found a field placeholder
            std::vector<std::string> field_names;
            std::string rule_part = rule_tokens[i];
            field_names.emplace_back(rule_part.erase(0, 1).erase(rule_part.size() - 1));

            // skip until we find a non-placeholder token
            i++;

            while(i < rule_tokens.size() && (rule_tokens[i].front() == '{' && rule_tokens[i].back() == '}')) {
                rule_part = rule_tokens[i];
                field_names.emplace_back(rule_part.erase(0, 1).erase(rule_part.size() - 1));
                i++;
            }

            std::vector<std::string> matched_tokens;

            // `i` now points to either end of array or at a non-placeholder rule token
            // end of array: add remaining query tokens as matched tokens
            // non-placeholder: skip query tokens until it matches a rule token

            while(j < query_tokens.size() && (i == rule_tokens.size() || rule_tokens[i] != query_tokens[j])) {
                matched_tokens.emplace_back(query_tokens[j]);
                j++;
            }

            resolved_override = true;

            // we try to map `field_names` against `matched_tokens` now
            for(size_t findex = 0; findex < field_names.size(); findex++) {
                const auto& field_name = field_names[findex];
                bool slide_window = (findex == 0);  // fields following another field should match exactly
                std::vector<std::string> field_absorbed_tokens;
                resolved_override &= check_for_overrides(token_order, field_name, slide_window,
                                                         exact_rule_match, matched_tokens, absorbed_tokens,
                                                         field_absorbed_tokens);

                if(!resolved_override) {
                    goto RETURN_EARLY;
                }

                field_placeholder_tokens[field_name] = field_absorbed_tokens;
            }
        } else {
            // rule token is not a placeholder, so we have to skip the query tokens until it matches rule token
            while(j < query_tokens.size() && query_tokens[j] != rule_tokens[i]) {
                if(exact_rule_match) {
                    // a single mismatch is enough to fail exact match
                    return false;
                }
                j++;
            }

            // either we have exhausted all query tokens
            if(j == query_tokens.size()) {
                return false;
            }

            //  or query token matches rule token, so we can proceed

            i++;
            j++;
        }
    }

    RETURN_EARLY:

    if(!resolved_override || (exact_rule_match && query_tokens.size() != absorbed_tokens.size())) {
        return false;
    }

    // replace placeholder with field_absorbed_tokens in rule_tokens
    for(const auto& kv: field_placeholder_tokens) {
        std::string pattern = "{" + kv.first + "}";
        std::string replacement = StringUtils::join(kv.second, " ");
        StringUtils::replace_all(filter_by_clause, pattern, replacement);
    }

    return true;
}

void Index::process_filter_overrides(const std::vector<const override_t*>& filter_overrides,
                                     std::vector<query_tokens_t>& field_query_tokens,
                                     token_ordering token_order,
                                     std::vector<filter>& filters) const {

    size_t orig_filters_size = filters.size();

    for(auto& override: filter_overrides) {
        if(!override->rule.dynamic_query) {
            // Simple static filtering: add to filter_by and rewrite query if needed.
            // Check the original query and then the synonym variants until a rule matches.
            bool resolved_override = static_filter_query_eval(override, field_query_tokens[0].q_include_tokens, filters);

            if(!resolved_override) {
                // we have not been able to resolve an override, so look at synonyms
                for(auto& syn_tokens: field_query_tokens[0].q_synonyms) {
                    static_filter_query_eval(override, syn_tokens, filters);
                    if(orig_filters_size != filters.size()) {
                        // we have been able to resolve an override via synonym, so can stop looking
                        resolved_override = true;
                        break;
                    }
                }
            }

            if(resolved_override && override->remove_matched_tokens) {
                std::vector<std::string>& tokens = field_query_tokens[0].q_include_tokens;

                std::vector<std::string> rule_tokens;
                Tokenizer(override->rule.query, true).tokenize(rule_tokens);
                std::set<std::string> rule_token_set(rule_tokens.begin(), rule_tokens.end());

                remove_matched_tokens(tokens, rule_token_set);

                for(auto& syn_tokens: field_query_tokens[0].q_synonyms) {
                    remove_matched_tokens(syn_tokens, rule_token_set);
                }

                // copy over for other fields
                for(size_t i = 1; i < field_query_tokens.size(); i++) {
                    field_query_tokens[i] = field_query_tokens[0];
                }
            }

            if(resolved_override) {
                return ;
            }
        } else {
            // need to extract placeholder field names from the search query, filter on them and rewrite query
            // we will cover both original query and synonyms

            std::vector<std::string> rule_parts;
            StringUtils::split(override->rule.query, rule_parts, " ");

            std::vector<std::string>& query_tokens = field_query_tokens[0].q_include_tokens;

            uint32_t* field_override_ids = nullptr;
            size_t field_override_ids_len = 0;

            bool exact_rule_match = override->rule.match == override_t::MATCH_EXACT;

            std::string filter_by_clause = override->filter_by;

            std::set<std::string> absorbed_tokens;
            bool resolved_override = resolve_override(rule_parts, exact_rule_match, query_tokens, token_order,
                                                      absorbed_tokens, filter_by_clause);

            if(!resolved_override) {
                // try resolving synonym

                for(size_t i = 0; i < field_query_tokens[0].q_synonyms.size(); i++) {
                    absorbed_tokens.clear();
                    std::vector<std::string>& syn_tokens = field_query_tokens[0].q_synonyms[i];
                    resolved_override = resolve_override(rule_parts, exact_rule_match, syn_tokens, token_order,
                                                         absorbed_tokens, filter_by_clause);

                    if(resolved_override) {
                        break;
                    }
                }
            }

            if(resolved_override) {
                Option<bool> filter_parse_op = filter::parse_filter_query(filter_by_clause, search_schema, store, "",
                                                                          filters);
                if(filter_parse_op.ok()) {
                    if(override->remove_matched_tokens) {
                        std::vector<std::string>& tokens = field_query_tokens[0].q_include_tokens;
                        remove_matched_tokens(tokens, absorbed_tokens);

                        for(auto& syn_tokens: field_query_tokens[0].q_synonyms) {
                            remove_matched_tokens(syn_tokens, absorbed_tokens);
                        }

                        // copy over for other fields
                        for(size_t i = 1; i < field_query_tokens.size(); i++) {
                            field_query_tokens[i] = field_query_tokens[0];
                        }
                    }
                }

                return ;
            }
        }
    }
}

void Index::remove_matched_tokens(std::vector<std::string>& tokens, const std::set<std::string>& rule_token_set) {
    std::vector<std::string> new_tokens;

    for(std::string& token: tokens) {
        if(rule_token_set.count(token) == 0) {
            new_tokens.push_back(token);
        }
    }

    if(new_tokens.empty()) {
        tokens = {"*"};
    } else {
        tokens = new_tokens;
    }
}

bool Index::check_for_overrides(const token_ordering& token_order, const string& field_name, const bool slide_window,
                                bool exact_rule_match, std::vector<std::string>& tokens,
                                std::set<std::string>& absorbed_tokens,
                                std::vector<std::string>& field_absorbed_tokens) const {

    for(size_t window_len = tokens.size(); window_len > 0; window_len--) {
        for(size_t start_index = 0; start_index+window_len-1 < tokens.size(); start_index++) {
            std::vector<token_t> window_tokens;
            std::set<std::string> window_tokens_set;
            for (size_t i = start_index; i < start_index + window_len; i++) {
                window_tokens.push_back({i, tokens[i]});
                window_tokens_set.emplace(tokens[i]);
            }

            std::vector<token_t> search_tokens = window_tokens;

            std::vector<facet> facets;
            std::vector<std::vector<art_leaf*>> searched_queries;
            Topster* topster = nullptr;
            spp::sparse_hash_set<uint64_t> groups_processed;
            uint32_t* result_ids = nullptr;
            size_t result_ids_len = 0;
            size_t field_num_results = 0;
            std::vector<std::string> group_by_fields;
            std::set<uint64> query_hashes;

            size_t num_toks_dropped = 0;

            auto field_it = search_schema.find(field_name);
            if(field_it == search_schema.end()) {
                continue;
            }

            search_field(0, window_tokens, search_tokens, nullptr, 0, num_toks_dropped, field_it->second, field_name,
                         nullptr, 0, {}, {}, 2, searched_queries, topster, groups_processed,
                         &result_ids, result_ids_len, field_num_results, 0, group_by_fields,
                         false, 4, query_hashes, token_order, false, 0, 1, false, 3, 7);

            delete [] result_ids;

            if(result_ids_len != 0) {
                // remove window_tokens from `tokens`
                std::vector<std::string> new_tokens;
                for(size_t new_i = start_index; new_i < tokens.size(); new_i++) {
                    const auto& token = tokens[new_i];
                    if(window_tokens_set.count(token) == 0) {
                        new_tokens.emplace_back(token);
                    } else {
                        absorbed_tokens.insert(token);
                        field_absorbed_tokens.emplace_back(token);
                    }
                }

                tokens = new_tokens;
                return true;
            }

            if(!slide_window) {
                break;
            }
        }
    }

    return false;
}

void Index::search(std::vector<query_tokens_t>& field_query_tokens,
                   const std::vector<search_field_t>& search_fields,
                   std::vector<filter>& filters,
                   std::vector<facet>& facets, facet_query_t& facet_query,
                   const std::map<size_t, std::map<size_t, uint32_t>> & included_ids_map,
                   const std::vector<uint32_t> & excluded_ids,
                   const std::vector<sort_by> & sort_fields_std, const std::vector<uint32_t>& num_typos,
                   Topster* topster,
                   Topster* curated_topster,
                   const size_t per_page, const size_t page, const token_ordering token_order,
                   const std::vector<bool>& prefixes, const size_t drop_tokens_threshold,
                   size_t & all_result_ids_len,
                   spp::sparse_hash_set<uint64_t>& groups_processed,
                   std::vector<std::vector<art_leaf*>>& searched_queries,
                   std::vector<std::vector<KV*>> & raw_result_kvs,
                   std::vector<std::vector<KV*>> & override_result_kvs,
                   const size_t typo_tokens_threshold,
                   const size_t group_limit,
                   const std::vector<std::string>& group_by_fields,
                   const std::vector<const override_t*>& filter_overrides,
                   const std::string& default_sorting_field,
                   bool prioritize_exact_match,
                   const bool exhaustive_search,
                   const size_t concurrency,
                   const size_t search_cutoff_ms,
                   size_t min_len_1typo,
                   size_t min_len_2typo) const {

    begin = std::chrono::high_resolution_clock::now();
    search_stop_ms = search_cutoff_ms;

    // process the filters

    uint32_t* filter_ids = nullptr;
    uint32_t filter_ids_length = 0;

    std::shared_lock lock(mutex);

    // we will be removing all curated IDs from organic result ids before running topster
    std::set<uint32_t> curated_ids;
    std::vector<uint32_t> included_ids;

    for(const auto& outer_pos_ids: included_ids_map) {
        for(const auto& inner_pos_seq_id: outer_pos_ids.second) {
            curated_ids.insert(inner_pos_seq_id.second);
            included_ids.push_back(inner_pos_seq_id.second);
        }
    }

    curated_ids.insert(excluded_ids.begin(), excluded_ids.end());

    std::vector<uint32_t> curated_ids_sorted(curated_ids.begin(), curated_ids.end());
    std::sort(curated_ids_sorted.begin(), curated_ids_sorted.end());

    process_filter_overrides(filter_overrides, field_query_tokens, token_order, filters);

    do_filtering(filter_ids, filter_ids_length, filters, true);

    // Order of `fields` are used to sort results
    //auto begin = std::chrono::high_resolution_clock::now();
    uint32_t* all_result_ids = nullptr;

    const size_t num_search_fields = std::min(search_fields.size(), (size_t) FIELD_LIMIT_NUM);
    uint32_t *exclude_token_ids = nullptr;
    size_t exclude_token_ids_size = 0;

    // find documents that contain the excluded tokens to exclude them from results later
    for(size_t i = 0; i < num_search_fields; i++) {
        const std::string & field_name = search_fields[i].name;

        std::vector<void*> posting_lists;

        for(const std::string& exclude_token: field_query_tokens[i].q_exclude_tokens) {
            art_leaf* leaf = (art_leaf *) art_search(search_index.at(field_name),
                                                     (const unsigned char *) exclude_token.c_str(),
                                                     exclude_token.size() + 1);

            if(leaf) {
                posting_lists.push_back(leaf->values);
            }
        }

        std::vector<uint32_t> exclude_token_id_vec;
        if(!posting_lists.empty()) {
            posting_t::merge(posting_lists, exclude_token_id_vec);
        }

        uint32_t *exclude_token_ids_merged = nullptr;
        exclude_token_ids_size = ArrayUtils::or_scalar(exclude_token_ids, exclude_token_ids_size,
                                                       &exclude_token_id_vec[0], exclude_token_id_vec.size(),
                                                       &exclude_token_ids_merged);
        delete[] exclude_token_ids;
        exclude_token_ids = exclude_token_ids_merged;
    }

    std::vector<Topster*> ftopsters;

    if (!field_query_tokens.empty() && !field_query_tokens[0].q_include_tokens.empty() &&
        field_query_tokens[0].q_include_tokens[0] == "*") {
        const uint8_t field_id = (uint8_t)(FIELD_LIMIT_NUM - 0);
        const std::string& field = search_fields[0].name;

        curate_filtered_ids(filters, curated_ids, exclude_token_ids,
                            exclude_token_ids_size, filter_ids, filter_ids_length, curated_ids_sorted);

        search_wildcard(field_query_tokens[0].q_include_tokens, filters, included_ids_map, sort_fields_std, topster,
                        curated_topster, groups_processed, searched_queries, group_limit, group_by_fields,
                        curated_ids, curated_ids_sorted,
                        exclude_token_ids, exclude_token_ids_size, field_id, field,
                        all_result_ids, all_result_ids_len, filter_ids, filter_ids_length, concurrency);
    } else {
        // In multi-field searches, a record can be matched across different fields, so we use this for aggregation
        spp::sparse_hash_map<uint64_t, std::vector<KV*>> topster_ids;

        //begin = std::chrono::high_resolution_clock::now();

        // non-wildcard
        for(size_t i = 0; i < num_search_fields; i++) {
            std::vector<token_t> q_include_pos_tokens;
            for(size_t j=0; j < field_query_tokens[i].q_include_tokens.size(); j++) {
                q_include_pos_tokens.push_back({j, field_query_tokens[i].q_include_tokens[j]});
            }

            std::vector<std::vector<token_t>> q_pos_synonyms;
            for(const auto& q_syn_vec: field_query_tokens[i].q_synonyms) {
                std::vector<token_t> q_pos_syn;
                for(size_t j=0; j < q_syn_vec.size(); j++) {
                    q_pos_syn.push_back({j, q_syn_vec[j]});
                }
                q_pos_synonyms.emplace_back(q_pos_syn);
            }

            // num_typos is already validated upstream, but still playing safe
            int field_num_typos = (i < num_typos.size()) ? num_typos[i] : num_typos[0];

            bool field_prefix = (i < prefixes.size()) ? prefixes[i] : prefixes[0];

            // proceed to query search only when no filters are provided or when filtering produces results
            if(filters.empty() || filter_ids_length > 0) {
                const uint8_t field_id = (uint8_t)(FIELD_LIMIT_NUM - i);
                const std::string& field_name = search_fields[i].name;

                auto field_it = search_schema.find(field_name);
                if(field_it == search_schema.end()) {
                    continue;
                }

                std::vector<token_t> query_tokens = q_include_pos_tokens;
                std::vector<token_t> search_tokens = q_include_pos_tokens;
                size_t num_tokens_dropped = 0;

                //LOG(INFO) << "searching field_name! " << field_name;
                Topster* ftopster = new Topster(topster->MAX_SIZE, topster->distinct);
                ftopsters.push_back(ftopster);

                // Don't waste additional cycles for single field_name searches
                Topster* actual_topster = (num_search_fields == 1) ? topster : ftopster;

                // tracks the number of results found for the current field_name
                size_t field_num_results = 0;
                std::set<uint64> query_hashes;

                search_field(field_id, query_tokens, search_tokens, exclude_token_ids, exclude_token_ids_size,
                             num_tokens_dropped, field_it->second, field_name,
                             filter_ids, filter_ids_length, curated_ids_sorted, sort_fields_std,
                             field_num_typos, searched_queries, actual_topster, groups_processed, &all_result_ids, all_result_ids_len,
                             field_num_results, group_limit, group_by_fields, prioritize_exact_match, concurrency,
                             query_hashes, token_order, field_prefix,
                             drop_tokens_threshold, typo_tokens_threshold, exhaustive_search,
                             min_len_1typo, min_len_2typo);

                bool syn_wildcard_filter_init_done = false;

                // do synonym based searches
                for(const auto& syn_tokens: q_pos_synonyms) {
                    num_tokens_dropped = 0;
                    field_num_results = 0;
                    query_tokens = search_tokens = syn_tokens;
                    query_hashes.clear();

                    if(query_tokens.size() == 1 && query_tokens[0].value == "*") {
                        // synonym can be a wildcard if there was an override filter used
                        // we will only do filtering in that case

                        if(!syn_wildcard_filter_init_done) {
                            curate_filtered_ids(filters, curated_ids, exclude_token_ids, exclude_token_ids_size,
                                                filter_ids, filter_ids_length, curated_ids_sorted);
                            syn_wildcard_filter_init_done = true;
                        }

                        search_wildcard({"*"}, filters, included_ids_map, sort_fields_std, actual_topster,
                                        curated_topster,
                                        groups_processed, searched_queries, group_limit, group_by_fields,
                                        curated_ids, curated_ids_sorted,
                                        exclude_token_ids, exclude_token_ids_size, field_id, field_name,
                                        all_result_ids, all_result_ids_len, filter_ids, filter_ids_length, concurrency);
                    } else {
                        search_field(field_id, query_tokens, search_tokens, exclude_token_ids, exclude_token_ids_size, num_tokens_dropped,
                                     field_it->second, field_name, filter_ids, filter_ids_length, curated_ids_sorted, sort_fields_std,
                                     field_num_typos, searched_queries, actual_topster, groups_processed, &all_result_ids, all_result_ids_len,
                                     field_num_results, group_limit, group_by_fields, prioritize_exact_match, concurrency,
                                     query_hashes, token_order, field_prefix,
                                     drop_tokens_threshold, typo_tokens_threshold, exhaustive_search,
                                     min_len_1typo, min_len_2typo);
                    }
                }

                // concat is done only for multi-field searches as `ftopster` will be empty for single-field search
                concat_topster_ids(ftopster, topster_ids);
                collate_included_ids(field_query_tokens[i].q_include_tokens, field_name, field_id, included_ids_map, curated_topster, searched_queries);
                //LOG(INFO) << "topster_ids.size: " << topster_ids.size();
            }
        }

        for(auto& seq_id_kvs: topster_ids) {
            const uint64_t seq_id = seq_id_kvs.first;
            auto& kvs = seq_id_kvs.second; // each `kv` can be from a different field

            std::sort(kvs.begin(), kvs.end(), Topster::is_greater);
            kvs[0]->query_indices = new uint64_t[kvs.size() + 1];
            kvs[0]->query_indices[0] = kvs.size();

            //LOG(INFO) << "DOC ID: " << seq_id << ", score: " << kvs[0]->scores[kvs[0]->match_score_index];

            // to calculate existing aggregate scores across best matching fields
            spp::sparse_hash_map<uint8_t, KV*> existing_field_kvs;
            for(size_t kv_i = 0; kv_i < kvs.size(); kv_i++) {
                existing_field_kvs.emplace(kvs[kv_i]->field_id, kvs[kv_i]);
                kvs[0]->query_indices[kv_i+1] = kvs[kv_i]->query_index;
                /*LOG(INFO) << "kv_i: " << kv_i << ", kvs[kv_i]->query_index: " << kvs[kv_i]->query_index << ", "
                          << "searched_query: " << searched_queries[kvs[kv_i]->query_index][0];*/
            }

            uint32_t token_bits = (uint32_t(1) << 31);      // top most bit set to guarantee atleast 1 bit set
            uint64_t total_typos = 0, total_distances = 0, min_typos = 1000;

            uint64_t verbatim_match_fields = 0;        // query matching field verbatim
            uint64_t exact_match_fields = 0;           // number of fields that contains all of query tokens
            uint64_t max_weighted_tokens_match = 0;    // weighted max number of tokens matched in a field
            uint64_t total_token_matches = 0;          // total matches across fields (including fuzzy ones)

            //LOG(INFO) << "Init pop count: " << __builtin_popcount(token_bits);

            for(size_t i = 0; i < num_search_fields; i++) {
                const auto field_id = (uint8_t)(FIELD_LIMIT_NUM - i);
                const size_t priority = search_fields[i].priority;
                const size_t weight = search_fields[i].weight;

                //LOG(INFO) << "--- field index: " << i << ", priority: " << priority;
                // using `5` here because typo + prefix combo score range is: 0 - 5
                // 0    1    2
                // 0,1  2,3  4,5
                int64_t MAX_SUM_TYPOS = 5 * field_query_tokens[i].q_include_tokens.size();

                if(existing_field_kvs.count(field_id) != 0) {
                    // for existing field, we will simply sum field-wise weighted scores
                    token_bits |= existing_field_kvs[field_id]->token_bits;
                    //LOG(INFO) << "existing_field_kvs.count pop count: " << __builtin_popcount(token_bits);

                    int64_t match_score = existing_field_kvs[field_id]->scores[existing_field_kvs[field_id]->match_score_index];

                    uint64_t tokens_found = ((match_score >> 24) & 0xFF);
                    uint64_t field_typos = 255 - ((match_score >> 16) & 0xFF);
                    total_typos += (field_typos + 1) * priority;
                    total_distances += ((100 - ((match_score >> 8) & 0xFF)) + 1) * priority;

                    int64_t exact_match_score = match_score & 0xFF;
                    verbatim_match_fields += (weight * exact_match_score);

                    uint64_t unique_tokens_found =
                            int64_t(__builtin_popcount(existing_field_kvs[field_id]->token_bits)) - 1;

                    if(field_typos == 0 && unique_tokens_found == field_query_tokens[i].q_include_tokens.size()) {
                        exact_match_fields += weight;
                    }

                    auto weighted_tokens_match = (tokens_found * weight) + (MAX_SUM_TYPOS - field_typos + 1);
                    if(weighted_tokens_match > max_weighted_tokens_match) {
                        max_weighted_tokens_match = weighted_tokens_match;
                    }

                    if(field_typos < min_typos) {
                        min_typos = field_typos;
                    }

                    total_token_matches += (weight * tokens_found);

                    /*LOG(INFO) << "seq_id: " << seq_id << ", total_typos: " << (255 - ((match_score >> 8) & 0xFF))
                                  << ", weighted typos: " << std::max<uint64_t>((255 - ((match_score >> 8) & 0xFF)), 1) * priority
                                  << ", total dist: " << (((match_score & 0xFF)))
                                  << ", weighted dist: " << std::max<uint64_t>((100 - (match_score & 0xFF)), 1) * priority;*/
                    continue;
                }

                const std::string& field = search_fields[i].name;
                const bool field_prefix = (i < prefixes.size()) ? prefixes[i] : prefixes[0];

                // compute approximate match score for this field from actual query

                size_t words_present = 0;

                for(size_t token_index=0; token_index < field_query_tokens[i].q_include_tokens.size(); token_index++) {
                    const auto& token = field_query_tokens[i].q_include_tokens[token_index];

                    std::vector<art_leaf*> leaves;
                    const bool prefix_search = field_prefix && (token_index == field_query_tokens[i].q_include_tokens.size()-1);
                    const size_t token_len = prefix_search ? (int) token.length() : (int) token.length() + 1;
                    art_fuzzy_search(search_index.at(field), (const unsigned char *) token.c_str(), token_len,
                                     0, 0, 1, token_order, prefix_search, nullptr, 0, leaves);

                    if(leaves.empty()) {
                        continue;
                    }

                    if(!posting_t::contains(leaves[0]->values, seq_id)) {
                        continue;
                    }

                    token_bits |= 1UL << token_index; // sets nth bit
                    //LOG(INFO) << "token_index: " << token_index << ", pop count: " << __builtin_popcount(token_bits);

                    words_present += 1;

                    /*if(!leaves.empty()) {
                        LOG(INFO) << "tok: " << leaves[0]->key;
                    }*/
                }

                if(words_present != 0) {
                    uint64_t match_score = Match::get_match_score(words_present, 0, 0);

                    uint64_t tokens_found = ((match_score >> 24) & 0xFF);
                    uint64_t field_typos = 255 - ((match_score >> 16) & 0xFF);
                    total_distances += ((100 - ((match_score >> 8) & 0xFF)) + 1) * priority;
                    total_typos += (field_typos + 1) * priority;

                    if(field_typos == 0 && tokens_found == field_query_tokens[i].q_include_tokens.size()) {
                        exact_match_fields += weight;
                        verbatim_match_fields += weight;  // this is only an approximate
                    }

                    auto weighted_tokens_match = (tokens_found * weight) + (MAX_SUM_TYPOS - field_typos + 1);

                    if(weighted_tokens_match > max_weighted_tokens_match) {
                        max_weighted_tokens_match = weighted_tokens_match;
                    }

                    if(field_typos < min_typos) {
                        min_typos = field_typos;
                    }

                    total_token_matches += (weight * tokens_found);
                    //LOG(INFO) << "seq_id: " << seq_id << ", total_typos: " << ((match_score >> 8) & 0xFF);
                }
            }

            // num tokens present across fields including those containing typos
            int64_t uniq_tokens_found = int64_t(__builtin_popcount(token_bits)) - 1;

            // verbtaim match should not consider dropped-token cases
            if(uniq_tokens_found != field_query_tokens[0].q_include_tokens.size()) {
                verbatim_match_fields = 0;
            }

            verbatim_match_fields = std::min<uint64_t>(255, verbatim_match_fields);
            exact_match_fields = std::min<uint64_t>(255, exact_match_fields);
            max_weighted_tokens_match = std::min<uint64_t>(255, max_weighted_tokens_match);
            total_typos = std::min<uint64_t>(255, total_typos);
            total_distances = std::min<uint64_t>(100, total_distances);

            uint64_t aggregated_score = (
                (verbatim_match_fields << 56)  |      // field value *exactly* same as query tokens
                (exact_match_fields << 48)  |         // number of fields that contain *all tokens* in the query
                (max_weighted_tokens_match << 40) |   // weighted max number of tokens matched in a field
                (uniq_tokens_found << 32)   |         // number of unique tokens found across fields including typos
                ((255 - min_typos) << 24)   |         // minimum typo cost across all fields
                (total_token_matches << 16) |         // total matches across fields including typos
                ((255 - total_typos) << 8) |          // total typos across fields (weighted)
                ((100 - total_distances) << 0)        // total distances across fields (weighted)
            );

            //LOG(INFO) << "seq id: " << seq_id << ", aggregated_score: " << aggregated_score;

            /*LOG(INFO) << "seq id: " << seq_id
                      << ", verbatim_match_fields: " << verbatim_match_fields
                      << ", exact_match_fields: " << exact_match_fields
                      << ", max_weighted_tokens_match: " << max_weighted_tokens_match
                      << ", uniq_tokens_found: " << uniq_tokens_found
                      << ", min typo score: " << (255 - min_typos)
                      << ", total_token_matches: " << total_token_matches
                      << ", typo score: " << (255 - total_typos)
                      << ", distance score: " << (100 - total_distances)
                      << ", aggregated_score: " << aggregated_score << ", token_bits: " << token_bits;*/

            kvs[0]->scores[kvs[0]->match_score_index] = aggregated_score;
            topster->add(kvs[0]);
        }
    }

    //LOG(INFO) << "topster size: " << topster->size;

    delete [] exclude_token_ids;

    if(!facets.empty()) {
        const size_t num_threads = std::min(concurrency, all_result_ids_len);
        const size_t window_size = (num_threads == 0) ? 0 :
                                   (all_result_ids_len + num_threads - 1) / num_threads;  // rounds up
        size_t num_processed = 0;
        std::mutex m_process;
        std::condition_variable cv_process;

        std::vector<facet_info_t> facet_infos(facets.size());
        compute_facet_infos(facets, facet_query, all_result_ids, all_result_ids_len,
                                     group_by_fields, facet_infos);

        std::vector<std::vector<facet>> facet_batches(num_threads);
        for(size_t i = 0; i < num_threads; i++) {
            for(const auto& this_facet: facets) {
                facet_batches[i].emplace_back(facet(this_facet.field_name));
            }
        }

        size_t num_queued = 0;
        size_t result_index = 0;

        //auto beginF = std::chrono::high_resolution_clock::now();

        for(size_t thread_id = 0; thread_id < num_threads && result_index < all_result_ids_len; thread_id++) {
            size_t batch_res_len = window_size;

            if(result_index + window_size > all_result_ids_len) {
                batch_res_len = all_result_ids_len - result_index;
            }

            uint32_t* batch_result_ids = all_result_ids + result_index;
            num_queued++;

            thread_pool->enqueue([this, thread_id, &facet_batches, &facet_query, group_limit, group_by_fields,
                                         batch_result_ids, batch_res_len, &facet_infos,
                                         &num_processed, &m_process, &cv_process]() {
                auto fq = facet_query;
                do_facets(facet_batches[thread_id], fq, facet_infos, group_limit, group_by_fields,
                          batch_result_ids, batch_res_len);
                std::unique_lock<std::mutex> lock(m_process);
                num_processed++;
                cv_process.notify_one();
            });

            result_index += batch_res_len;
        }

        std::unique_lock<std::mutex> lock_process(m_process);
        cv_process.wait(lock_process, [&](){ return num_processed == num_queued; });

        for(const auto& facet_batch: facet_batches) {
            for(size_t fi = 0; fi < facet_batch.size(); fi++) {
                auto& this_facet = facet_batch[fi];
                auto& acc_facet = facets[fi];

                for(auto & facet_kv: this_facet.result_map) {
                    if(group_limit) {
                        // we have to add all group sets
                        acc_facet.result_map[facet_kv.first].groups.insert(
                                facet_kv.second.groups.begin(), facet_kv.second.groups.end()
                        );
                    } else {
                        size_t count = 0;
                        if(acc_facet.result_map.count(facet_kv.first) == 0) {
                            // not found, so set it
                            count = facet_kv.second.count;
                        } else {
                            count = acc_facet.result_map[facet_kv.first].count + facet_kv.second.count;
                        }
                        acc_facet.result_map[facet_kv.first].count = count;
                    }

                    acc_facet.result_map[facet_kv.first].doc_id = facet_kv.second.doc_id;
                    acc_facet.result_map[facet_kv.first].array_pos = facet_kv.second.array_pos;
                    acc_facet.result_map[facet_kv.first].tokens = facet_kv.second.tokens;
                }

                if(this_facet.stats.fvcount != 0) {
                    acc_facet.stats.fvcount += this_facet.stats.fvcount;
                    acc_facet.stats.fvsum += this_facet.stats.fvsum;
                    acc_facet.stats.fvmax = std::max(acc_facet.stats.fvmax, this_facet.stats.fvmax);
                    acc_facet.stats.fvmin = std::min(acc_facet.stats.fvmin, this_facet.stats.fvmin);
                }
            }
        }

        /*long long int timeMillisF = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now() - beginF).count();
        LOG(INFO) << "Time for faceting: " << timeMillisF;*/
    }

    std::vector<facet_info_t> facet_infos(facets.size());
    compute_facet_infos(facets, facet_query, &included_ids[0], included_ids.size(), group_by_fields, facet_infos);
    do_facets(facets, facet_query, facet_infos, group_limit, group_by_fields, &included_ids[0], included_ids.size());

    all_result_ids_len += curated_topster->size;

    delete [] filter_ids;
    delete [] all_result_ids;

    for(Topster* ftopster: ftopsters) {
        delete ftopster;
    }

    //LOG(INFO) << "all_result_ids_len " << all_result_ids_len << " for index " << name;
    //long long int timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    //LOG(INFO) << "Time taken for result calc: " << timeMillis << "ms";
}

void Index::compute_facet_infos(const std::vector<facet>& facets, facet_query_t& facet_query,
                                const uint32_t* all_result_ids, const size_t& all_result_ids_len,
                                const std::vector<std::string>& group_by_fields,
                                std::vector<facet_info_t>& facet_infos) const {

    if(all_result_ids_len == 0) {
        return;
    }
    
    for(size_t findex=0; findex < facets.size(); findex++) {
        const auto& a_facet = facets[findex];

        const auto field_facet_mapping_it = facet_index_v3.find(a_facet.field_name);
        if(field_facet_mapping_it == facet_index_v3.end()) {
            continue;
        }

        facet_infos[findex].use_facet_query = false;

        const field &facet_field = facet_schema.at(a_facet.field_name);
        facet_infos[findex].facet_field = facet_field;

        facet_infos[findex].should_compute_stats = (facet_field.type != field_types::STRING &&
                                                    facet_field.type != field_types::BOOL &&
                                                    facet_field.type != field_types::STRING_ARRAY &&
                                                    facet_field.type != field_types::BOOL_ARRAY);

        if(a_facet.field_name == facet_query.field_name && !facet_query.query.empty()) {
            facet_infos[findex].use_facet_query = true;

            if (facet_field.is_bool()) {
                if (facet_query.query == "true") {
                    facet_query.query = "1";
                } else if (facet_query.query == "false") {
                    facet_query.query = "0";
                }
            }

            //LOG(INFO) << "facet_query.query: " << facet_query.query;

            std::vector<std::string> query_tokens;
            Tokenizer(facet_query.query, true, !facet_field.is_string()).tokenize(query_tokens);

            std::vector<token_t> search_tokens, qtokens;

            for (size_t qtoken_index = 0; qtoken_index < query_tokens.size(); qtoken_index++) {
                search_tokens.emplace_back(token_t{qtoken_index, query_tokens[qtoken_index]});
                qtokens.emplace_back(token_t{qtoken_index, query_tokens[qtoken_index]});
            }

            std::vector<std::vector<art_leaf*>> searched_queries;
            Topster* topster = nullptr;
            spp::sparse_hash_set<uint64_t> groups_processed;
            uint32_t* field_result_ids = nullptr;
            size_t field_result_ids_len = 0;
            size_t field_num_results = 0;
            std::set<uint64> query_hashes;
            size_t num_toks_dropped = 0;

            search_field(0, qtokens, search_tokens, nullptr, 0, num_toks_dropped,
                         facet_field, facet_field.faceted_name(),
                         all_result_ids, all_result_ids_len, {}, {}, 2, searched_queries, topster, groups_processed,
                         &field_result_ids, field_result_ids_len, field_num_results, 0, group_by_fields,
                         false, 4, query_hashes, MAX_SCORE, true, 0, 1, false, 3, 1000);

            //LOG(INFO) << "searched_queries.size: " << searched_queries.size();

            // NOTE: `field_result_ids` will consists of IDs across ALL queries in searched_queries

            for(size_t si = 0; si < searched_queries.size(); si++) {
                const auto& searched_query = searched_queries[si];
                std::vector<std::string> searched_tokens;

                std::vector<void*> posting_lists;
                for(auto leaf: searched_query) {
                    posting_lists.push_back(leaf->values);
                    std::string tok(reinterpret_cast<char*>(leaf->key), leaf->key_len - 1);
                    searched_tokens.push_back(tok);
                    //LOG(INFO) << "tok: " << tok;
                }

                //LOG(INFO) << "si: " << si << ", field_result_ids_len: " << field_result_ids_len;

                for(size_t i = 0; i < std::min<size_t>(1000, field_result_ids_len); i++) {
                    uint32_t seq_id = field_result_ids[i];

                    const auto doc_fvalues_it = field_facet_mapping_it->second->find(seq_id);
                    if(doc_fvalues_it == field_facet_mapping_it->second->end()) {
                        continue;
                    }

                    bool id_matched = true;

                    for(auto pl: posting_lists) {
                        if(!posting_t::contains(pl, seq_id)) {
                            // need to ensure that document ID actually contains both searched_query tokens
                            id_matched = false;
                            break;
                        }
                    }

                    if(!id_matched) {
                        continue;
                    }

                    if(facet_field.is_array()) {
                        std::vector<size_t> array_indices;
                        posting_t::get_matching_array_indices(posting_lists, seq_id, array_indices);

                        for(size_t array_index: array_indices) {
                            if(array_index < doc_fvalues_it->second.length) {
                                uint64_t hash = doc_fvalues_it->second.hashes[array_index];

                                /*LOG(INFO) << "seq_id: " << seq_id << ", hash: " << hash << ", array index: "
                                          << array_index;*/

                                if(facet_infos[findex].hashes.count(hash) == 0) {
                                    facet_infos[findex].hashes.emplace(hash, searched_tokens);
                                }
                            }
                        }
                    } else {
                        uint64_t hash = doc_fvalues_it->second.hashes[0];
                        if(facet_infos[findex].hashes.count(hash) == 0) {
                            facet_infos[findex].hashes.emplace(hash, searched_tokens);
                        }
                    }
                }
            }

            delete [] field_result_ids;
        }
    }
}

void Index::curate_filtered_ids(const std::vector<filter>& filters, const std::set<uint32_t>& curated_ids,
                                const uint32_t* exclude_token_ids, size_t exclude_token_ids_size,
                                uint32_t*& filter_ids, uint32_t& filter_ids_length,
                                const std::vector<uint32_t>& curated_ids_sorted) const {
    // if filtered results are not available, use the seq_ids index to generate the list of all document ids

    if(filters.empty() && filter_ids_length == 0) {
        filter_ids_length = seq_ids.getLength();
        filter_ids = seq_ids.uncompress();
    }

    if(!curated_ids.empty()) {
        uint32_t *excluded_result_ids = nullptr;
        filter_ids_length = ArrayUtils::exclude_scalar(filter_ids, filter_ids_length, &curated_ids_sorted[0],
                                                       curated_ids_sorted.size(), &excluded_result_ids);
        delete [] filter_ids;
        filter_ids = excluded_result_ids;
    }

    // Exclude document IDs associated with excluded tokens from the result set
    if(exclude_token_ids_size != 0) {
        uint32_t *excluded_result_ids = nullptr;
        filter_ids_length = ArrayUtils::exclude_scalar(filter_ids, filter_ids_length, exclude_token_ids,
                                                       exclude_token_ids_size, &excluded_result_ids);
        delete[] filter_ids;
        filter_ids = excluded_result_ids;
    }
}

void Index::search_wildcard(const std::vector<std::string>& qtokens, const std::vector<filter>& filters,
                            const std::map<size_t, std::map<size_t, uint32_t>>& included_ids_map,
                            const std::vector<sort_by>& sort_fields_std, Topster* topster, Topster* curated_topster,
                            spp::sparse_hash_set<uint64_t>& groups_processed,
                            std::vector<std::vector<art_leaf*>>& searched_queries, const size_t group_limit,
                            const std::vector<std::string>& group_by_fields, const std::set<uint32_t>& curated_ids,
                            const std::vector<uint32_t>& curated_ids_sorted, const uint32_t* exclude_token_ids,
                            size_t exclude_token_ids_size, const uint8_t field_id, const string& field,
                            uint32_t*& all_result_ids, size_t& all_result_ids_len, const uint32_t* filter_ids,
                            uint32_t filter_ids_length, const size_t concurrency) const {

    int sort_order[3]; // 1 or -1 based on DESC or ASC respectively
    std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3> field_values;
    std::vector<size_t> geopoint_indices;
    populate_sort_mapping(sort_order, geopoint_indices, sort_fields_std, field_values);

    uint32_t token_bits = 255;
    const bool check_for_circuit_break = (filter_ids_length > 1000000);

    //auto beginF = std::chrono::high_resolution_clock::now();

    const size_t num_threads = std::min<size_t>(concurrency, filter_ids_length);
    const size_t window_size = (num_threads == 0) ? 0 :
                               (filter_ids_length + num_threads - 1) / num_threads;  // rounds up

    spp::sparse_hash_set<uint64_t> tgroups_processed[num_threads];
    Topster* topsters[num_threads];
    std::vector<posting_list_t::iterator_t> plists;

    size_t num_processed = 0;
    std::mutex m_process;
    std::condition_variable cv_process;

    size_t num_queued = 0;
    size_t filter_index = 0;

    for(size_t thread_id = 0; thread_id < num_threads && filter_index < filter_ids_length; thread_id++) {
        size_t batch_res_len = window_size;

        if(filter_index + window_size > filter_ids_length) {
            batch_res_len = filter_ids_length - filter_index;
        }

        const uint32_t* batch_result_ids = filter_ids + filter_index;
        num_queued++;

        topsters[thread_id] = new Topster(topster->MAX_SIZE, topster->distinct);

        thread_pool->enqueue([this, thread_id, &sort_fields_std, &searched_queries, &field_id,
                                     &group_limit, &group_by_fields, &topsters, &tgroups_processed,
                                     &sort_order, field_values, &geopoint_indices, &token_bits, &plists,
                                     check_for_circuit_break,
                                     batch_result_ids, batch_res_len,
                                     &num_processed, &m_process, &cv_process]() {

            for(size_t i = 0; i < batch_res_len; i++) {
                const uint32_t seq_id = batch_result_ids[i];
                score_results(sort_fields_std, (uint16_t) searched_queries.size(), field_id, false, 0,
                              topsters[thread_id], {}, tgroups_processed[thread_id], seq_id, sort_order, field_values,
                              geopoint_indices, group_limit, group_by_fields, token_bits,
                              false, false, plists);

                if(check_for_circuit_break && i % (1 << 17) == 0) {
                    // check only once every 2^17 docs to reduce overhead
                    BREAK_CIRCUIT_BREAKER
                }
            }

            std::unique_lock<std::mutex> lock(m_process);
            num_processed++;
            cv_process.notify_one();
        });

        filter_index += batch_res_len;
    }

    std::unique_lock<std::mutex> lock_process(m_process);
    cv_process.wait(lock_process, [&](){ return num_processed == num_queued; });

    for(size_t thread_id = 0; thread_id < num_processed; thread_id++) {
        groups_processed.insert(tgroups_processed[thread_id].begin(), tgroups_processed[thread_id].end());
        aggregate_topster(topster, topsters[thread_id]);
        delete topsters[thread_id];
    }

    /*long long int timeMillisF = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - beginF).count();
    LOG(INFO) << "Time for raw scoring: " << timeMillisF;*/

    collate_included_ids(qtokens, field, field_id, included_ids_map, curated_topster, searched_queries);

    uint32_t* new_all_result_ids = nullptr;
    all_result_ids_len = ArrayUtils::or_scalar(all_result_ids, all_result_ids_len, filter_ids,
                                               filter_ids_length, &new_all_result_ids);
    delete [] all_result_ids;
    all_result_ids = new_all_result_ids;
}

void Index::populate_sort_mapping(int* sort_order, std::vector<size_t>& geopoint_indices,
                                  const std::vector<sort_by>& sort_fields_std,
                                  std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3>& field_values) const {
    for (size_t i = 0; i < sort_fields_std.size(); i++) {
        sort_order[i] = 1;
        if (sort_fields_std[i].order == sort_field_const::asc) {
            sort_order[i] = -1;
        }

        if (sort_fields_std[i].name == sort_field_const::text_match) {
            field_values[i] = &text_match_sentinel_value;
        } else if (sort_fields_std[i].name == sort_field_const::seq_id) {
            field_values[i] = &seq_id_sentinel_value;
        } else if (sort_schema.count(sort_fields_std[i].name) != 0) {
            if (sort_schema.at(sort_fields_std[i].name).type == field_types::GEOPOINT_ARRAY) {
                geopoint_indices.push_back(i);
                field_values[i] = nullptr; // GEOPOINT_ARRAY uses a multi-valued index
            } else {
                field_values[i] = sort_index.at(sort_fields_std[i].name);

                if (sort_schema.at(sort_fields_std[i].name).is_geopoint()) {
                    geopoint_indices.push_back(i);
                }
            }
        }
    }
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
void Index::search_field(const uint8_t & field_id,
                         std::vector<token_t>& query_tokens,
                         std::vector<token_t>& search_tokens,
                         const uint32_t* exclude_token_ids,
                         size_t exclude_token_ids_size,
                         size_t& num_tokens_dropped,
                         const field& the_field, const std::string& field_name, // to handle faceted index
                         const uint32_t *filter_ids, size_t filter_ids_length,
                         const std::vector<uint32_t>& curated_ids,
                         const std::vector<sort_by> & sort_fields, const int num_typos,
                         std::vector<std::vector<art_leaf*>> & searched_queries,
                         Topster* topster, spp::sparse_hash_set<uint64_t>& groups_processed,
                         uint32_t** all_result_ids, size_t & all_result_ids_len, size_t& field_num_results,
                         const size_t group_limit, const std::vector<std::string>& group_by_fields,
                         bool prioritize_exact_match,
                         const size_t concurrency,
                         std::set<uint64>& query_hashes,
                         const token_ordering token_order, const bool prefix,
                         const size_t drop_tokens_threshold,
                         const size_t typo_tokens_threshold,
                         const bool exhaustive_search,
                         size_t min_len_1typo,
                         size_t min_len_2typo) const {

    // NOTE: `query_tokens` preserve original tokens, while `search_tokens` could be a result of dropped tokens

    size_t max_cost = (num_typos < 0 || num_typos > 2) ? 2 : num_typos;

    if(the_field.locale != "" && the_field.locale != "en") {
        // disable fuzzy trie traversal for non-english locales
        max_cost = 0;
    }

    // To prevent us from doing ART search repeatedly as we iterate through possible corrections
    spp::sparse_hash_map<std::string, std::vector<art_leaf*>> token_cost_cache;

    std::vector<std::vector<int>> token_to_costs;

    for(size_t stoken_index=0; stoken_index < search_tokens.size(); stoken_index++) {
        const std::string& token = search_tokens[stoken_index].value;

        std::vector<int> all_costs;
        // This ensures that we don't end up doing a cost of 1 for a single char etc.
        int bounded_cost = get_bounded_typo_cost(max_cost, token.length(), min_len_1typo, min_len_2typo);

        for(int cost = 0; cost <= bounded_cost; cost++) {
            all_costs.push_back(cost);
        }

        token_to_costs.push_back(all_costs);
    }

    // stores candidates for each token, i.e. i-th index would have all possible tokens with a cost of "c"
    std::vector<token_candidates> token_candidates_vec;
    std::set<art_leaf*> unique_tokens;

    auto product = []( long long a, std::vector<int>& b ) { return a*b.size(); };
    long long n = 0;
    long long int N = std::accumulate(token_to_costs.begin(), token_to_costs.end(), 1LL, product);

    const size_t combination_limit = exhaustive_search ? Index::COMBINATION_MAX_LIMIT : Index::COMBINATION_MIN_LIMIT;
    const size_t num_fuzzy_candidates = exhaustive_search ? 10000 : 4;

    while(n < N && n < combination_limit) {
        RETURN_CIRCUIT_BREAKER

        // Outerloop generates combinations of [cost to max_cost] for each token
        // For e.g. for a 3-token query: [0, 0, 0], [0, 0, 1], [0, 1, 1] etc.
        std::vector<uint32_t> costs(token_to_costs.size());
        ldiv_t q { n, 0 };
        for(long long i = (token_to_costs.size() - 1); 0 <= i ; --i ) {
            q = ldiv(q.quot, token_to_costs[i].size());
            costs[i] = token_to_costs[i][q.rem];
        }

        unique_tokens.clear();
        token_candidates_vec.clear();
        size_t token_index = 0;

        while(token_index < search_tokens.size()) {
            // For each token, look up the generated cost for this iteration and search using that cost
            const std::string& token = search_tokens[token_index].value;
            const std::string token_cost_hash = token + std::to_string(costs[token_index]);

            std::vector<art_leaf*> leaves;
            //LOG(INFO) << "Searching for field: " << field << ", token:" << token << " - cost: " << costs[token_index];

            const bool prefix_search = prefix && (token_index == search_tokens.size()-1);

            if(token_cost_cache.count(token_cost_hash) != 0) {
                leaves = token_cost_cache[token_cost_hash];
            } else {
                // prefix should apply only for last token
                const size_t token_len = prefix_search ? (int) token.length() : (int) token.length() + 1;

                // need less candidates for filtered searches since we already only pick tokens with results
                art_fuzzy_search(search_index.at(field_name), (const unsigned char *) token.c_str(), token_len,
                                 costs[token_index], costs[token_index], num_fuzzy_candidates, token_order, prefix_search,
                                 filter_ids, filter_ids_length, leaves, unique_tokens);

                if(!leaves.empty()) {
                    token_cost_cache.emplace(token_cost_hash, leaves);
                    for(auto leaf: leaves) {
                        unique_tokens.emplace(leaf);
                    }
                }
            }

            if(!leaves.empty()) {
                //log_leaves(costs[token_index], token, leaves);
                token_candidates_vec.push_back(
                        token_candidates{search_tokens[token_index], costs[token_index], prefix_search, leaves});
            } else {
                // No result at `cost = costs[token_index]`. Remove `cost` for token and re-do combinations
                auto it = std::find(token_to_costs[token_index].begin(), token_to_costs[token_index].end(), costs[token_index]);
                if(it != token_to_costs[token_index].end()) {
                    token_to_costs[token_index].erase(it);

                    // when no more costs are left for this token
                    if(token_to_costs[token_index].empty()) {
                        // we can try to drop the token and search with remaining tokens

                        if(!exhaustive_search && field_num_results >= drop_tokens_threshold) {
                            // but if drop_tokens_threshold is breached, we are done
                            return ;
                        }

                        token_to_costs.erase(token_to_costs.begin()+token_index);
                        search_tokens.erase(search_tokens.begin()+token_index);
                        query_tokens.erase(query_tokens.begin()+token_index);
                        costs.erase(costs.begin()+token_index);
                    }
                }

                // Continue outerloop on new cost combination
                n = -1;
                N = std::accumulate(token_to_costs.begin(), token_to_costs.end(), 1LL, product);
                goto resume_typo_loop;
            }

            token_index++;
        }

        if(!token_candidates_vec.empty()) {
            // If atleast one token is found, go ahead and search for candidates
            search_candidates(field_id, the_field.is_array(), filter_ids, filter_ids_length,
                              exclude_token_ids, exclude_token_ids_size,
                              curated_ids, sort_fields, token_candidates_vec, searched_queries, topster,
                              groups_processed, all_result_ids, all_result_ids_len, field_num_results,
                              typo_tokens_threshold, group_limit, group_by_fields, query_tokens,
                              prioritize_exact_match, combination_limit, concurrency, query_hashes);
        }

        resume_typo_loop:

        if(!exhaustive_search && field_num_results >= typo_tokens_threshold) {
            // if typo threshold is breached, we are done
            return ;
        }

        n++;
    }

    // When atleast one token from the query is available
    if(!query_tokens.empty() && num_tokens_dropped < query_tokens.size()) {
        // Drop tokens from right until (len/2 + 1), and then from left until (len/2 + 1)

        if(!exhaustive_search && field_num_results >= drop_tokens_threshold) {
            // if drop_tokens_threshold is breached, we are done
            return ;
        }

        std::vector<token_t> truncated_tokens;
        num_tokens_dropped++;

        size_t mid_index = (query_tokens.size() / 2);
        if(num_tokens_dropped <= mid_index) {
            // drop from right
            size_t end_index = (query_tokens.size() - 1) - num_tokens_dropped;
            for(size_t i=0; i <= end_index; i++) {
                truncated_tokens.push_back({query_tokens[i].position, query_tokens[i].value});
            }
        } else {
            // drop from left
            size_t start_index = (num_tokens_dropped - mid_index);
            for(size_t i=start_index; i<query_tokens.size(); i++) {
                truncated_tokens.push_back({query_tokens[i].position, query_tokens[i].value});
            }
        }

        return search_field(field_id, query_tokens, truncated_tokens, exclude_token_ids, exclude_token_ids_size,
                            num_tokens_dropped, the_field, field_name, filter_ids, filter_ids_length, curated_ids,
                            sort_fields, num_typos,searched_queries, topster, groups_processed, all_result_ids,
                            all_result_ids_len, field_num_results, group_limit, group_by_fields,
                            prioritize_exact_match, concurrency, query_hashes,
                            token_order, prefix, drop_tokens_threshold, typo_tokens_threshold,
                            exhaustive_search, min_len_1typo, min_len_2typo);
    }
}

int Index::get_bounded_typo_cost(const size_t max_cost, const size_t token_len,
                                 const size_t min_len_1typo, const size_t min_len_2typo) {
    if(token_len < min_len_1typo) {
        // typo correction is disabled for small tokens
        return 0;
    }

    if(token_len < min_len_2typo) {
        // 2-typos are enabled only at token length of 7 chars
        return std::min<int>(max_cost, 1);
    }

    return std::min<int>(max_cost, 2);
}

void Index::log_leaves(const int cost, const std::string &token, const std::vector<art_leaf *> &leaves) const {
    LOG(INFO) << "Index: " << name << ", token: " << token << ", cost: " << cost;

    for(size_t i=0; i < leaves.size(); i++) {
        std::string key((char*)leaves[i]->key, leaves[i]->key_len);
        LOG(INFO) << key << " - " << posting_t::num_ids(leaves[i]->values);
        LOG(INFO) << "frequency: " << posting_t::num_ids(leaves[i]->values) << ", max_score: " << leaves[i]->max_score;
        /*for(auto j=0; j<leaves[i]->values->ids.getLength(); j++) {
            LOG(INFO) << "id: " << leaves[i]->values->ids.at(j);
        }*/
    }
}

void Index::score_results(const std::vector<sort_by> & sort_fields, const uint16_t & query_index,
                          const uint8_t & field_id, const bool field_is_array, const uint32_t total_cost,
                          Topster* topster /**/,
                          const std::vector<art_leaf *> &query_suggestion,
                          spp::sparse_hash_set<uint64_t>& groups_processed /**/,
                          const uint32_t seq_id, const int sort_order[3],
                          std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3> field_values /**/,
                          const std::vector<size_t>& geopoint_indices,
                          const size_t group_limit, const std::vector<std::string>& group_by_fields,
                          const uint32_t token_bits,
                          const bool prioritize_exact_match,
                          const bool single_exact_query_token,
                          const std::vector<posting_list_t::iterator_t>& posting_lists) const {

    int64_t geopoint_distances[3];

    for(auto& i: geopoint_indices) {
        spp::sparse_hash_map<uint32_t, int64_t>* geopoints = field_values[i];
        int64_t dist = INT32_MAX;

        S2LatLng reference_lat_lng;
        GeoPoint::unpack_lat_lng(sort_fields[i].geopoint, reference_lat_lng);

        if(geopoints != nullptr) {
            auto it = geopoints->find(seq_id);

            if(it != geopoints->end()) {
                int64_t packed_latlng = it->second;
                S2LatLng s2_lat_lng;
                GeoPoint::unpack_lat_lng(packed_latlng, s2_lat_lng);
                dist = GeoPoint::distance(s2_lat_lng, reference_lat_lng);
            }
        } else {
            // indicates geo point array
            auto field_it = geo_array_index.at(sort_fields[i].name);
            auto it = field_it->find(seq_id);

            if(it != field_it->end()) {
                int64_t* latlngs = it->second;
                for(size_t li = 0; li < latlngs[0]; li++) {
                    S2LatLng s2_lat_lng;
                    int64_t packed_latlng = latlngs[li + 1];
                    GeoPoint::unpack_lat_lng(packed_latlng, s2_lat_lng);
                    int64_t this_dist = GeoPoint::distance(s2_lat_lng, reference_lat_lng);
                    if(this_dist < dist) {
                        dist = this_dist;
                    }
                }
            }
        }

        if(dist < sort_fields[i].exclude_radius) {
            dist = 0;
        }

        if(sort_fields[i].geo_precision > 0) {
            dist = dist + sort_fields[i].geo_precision - 1 -
                   (dist + sort_fields[i].geo_precision - 1) % sort_fields[i].geo_precision;
        }

        geopoint_distances[i] = dist;

        // Swap (id -> latlong) index to (id -> distance) index
        field_values[i] = &geo_sentinel_value;
    }

    //auto begin = std::chrono::high_resolution_clock::now();
    //const std::string first_token((const char*)query_suggestion[0]->key, query_suggestion[0]->key_len-1);

    uint64_t match_score = 0;

    if (posting_lists.size() <= 1) {
        const uint8_t is_verbatim_match = uint8_t(
            prioritize_exact_match && single_exact_query_token &&
            posting_list_t::is_single_token_verbatim_match(posting_lists[0], field_is_array)
        );
        Match single_token_match = Match(1, 0, is_verbatim_match);
        match_score = single_token_match.get_match_score(total_cost);
    } else {
        uint64_t total_tokens_found = 0, total_num_typos = 0, total_distance = 0, total_verbatim = 0;

        std::unordered_map<size_t, std::vector<token_positions_t>> array_token_positions;
        posting_list_t::get_offsets(posting_lists, array_token_positions);

        for (const auto& kv: array_token_positions) {
            const std::vector<token_positions_t>& token_positions = kv.second;
            if (token_positions.empty()) {
                continue;
            }
            const Match &match = Match(seq_id, token_positions, false, prioritize_exact_match);
            uint64_t this_match_score = match.get_match_score(total_cost);

            total_tokens_found += ((this_match_score >> 24) & 0xFF);
            total_num_typos += 255 - ((this_match_score >> 16) & 0xFF);
            total_distance += 100 - ((this_match_score >> 8) & 0xFF);
            total_verbatim += (this_match_score & 0xFF);

            /*std::ostringstream os;
            os << name << ", total_cost: " << (255 - total_cost)
               << ", words_present: " << match.words_present
               << ", match_score: " << match_score
               << ", match.distance: " << match.distance
               << ", seq_id: " << seq_id << std::endl;
            LOG(INFO) << os.str();*/
        }

        match_score = (
            (uint64_t(total_tokens_found) << 24) |
            (uint64_t(255 - total_num_typos) << 16) |
            (uint64_t(100 - total_distance) << 8) |
            (uint64_t(total_verbatim) << 0)
        );

        /*LOG(INFO) << "Match score: " << match_score << ", for seq_id: " << seq_id
                  << " - total_tokens_found: " << total_tokens_found
                  << " - total_num_typos: " << total_num_typos
                  << " - total_distance: " << total_distance
                  << " - total_verbatim: " << total_verbatim
                  << " - total_cost: " << total_cost;*/
    }

    const int64_t default_score = INT64_MIN;  // to handle field that doesn't exist in document (e.g. optional)
    int64_t scores[3] = {0};
    size_t match_score_index = 0;

    // avoiding loop
    if (sort_fields.size() > 0) {
        if (field_values[0] == &text_match_sentinel_value) {
            scores[0] = int64_t(match_score);
            match_score_index = 0;
        } else if (field_values[0] == &seq_id_sentinel_value) {
            scores[0] = seq_id;
        } else if(field_values[0] == &geo_sentinel_value) {
            scores[0] = geopoint_distances[0];
        } else {
            auto it = field_values[0]->find(seq_id);
            scores[0] = (it == field_values[0]->end()) ? default_score : it->second;
        }

        if (sort_order[0] == -1) {
            scores[0] = -scores[0];
        }
    }

    if(sort_fields.size() > 1) {
        if (field_values[1] == &text_match_sentinel_value) {
            scores[1] = int64_t(match_score);
            match_score_index = 1;
        } else if (field_values[1] == &seq_id_sentinel_value) {
            scores[1] = seq_id;
        } else if(field_values[1] == &geo_sentinel_value) {
            scores[1] = geopoint_distances[1];
        } else {
            auto it = field_values[1]->find(seq_id);
            scores[1] = (it == field_values[1]->end()) ? default_score : it->second;
        }

        if (sort_order[1] == -1) {
            scores[1] = -scores[1];
        }
    }

    if(sort_fields.size() > 2) {
        if (field_values[2] == &text_match_sentinel_value) {
            scores[2] = int64_t(match_score);
            match_score_index = 2;
        } else if (field_values[2] == &seq_id_sentinel_value) {
            scores[2] = seq_id;
        } else if(field_values[2] == &geo_sentinel_value) {
            scores[2] = geopoint_distances[2];
        } else {
            auto it = field_values[2]->find(seq_id);
            scores[2] = (it == field_values[2]->end()) ? default_score : it->second;
        }

        if (sort_order[2] == -1) {
            scores[2] = -scores[2];
        }
    }

    uint64_t distinct_id = seq_id;

    if(group_limit != 0) {
        distinct_id = get_distinct_id(group_by_fields, seq_id);
        groups_processed.emplace(distinct_id);
    }

    //LOG(INFO) << "Seq id: " << seq_id << ", match_score: " << match_score;
    KV kv(field_id, query_index, token_bits, seq_id, distinct_id, match_score_index, scores);
    topster->add(&kv);

    //long long int timeNanos = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    //LOG(INFO) << "Time taken for results iteration: " << timeNanos << "ms";
}

// pre-filter group_by_fields such that we can avoid the find() check
uint64_t Index::get_distinct_id(const std::vector<std::string>& group_by_fields,
                                const uint32_t seq_id) const {
    uint64_t distinct_id = 1; // some constant initial value

    // calculate hash from group_by_fields
    for(const auto& field: group_by_fields) {
        const auto& field_facet_mapping_it = facet_index_v3.find(field);
        if(field_facet_mapping_it == facet_index_v3.end()) {
            continue;
        }

        const auto& field_facet_mapping = field_facet_mapping_it->second;
        const auto& facet_hashes_it = field_facet_mapping->find(seq_id);

        if(facet_hashes_it == field_facet_mapping->end()) {
            continue;
        }

        const auto& facet_hashes = facet_hashes_it->second;

        for(size_t i = 0; i < facet_hashes.size(); i++) {
            distinct_id = hash_combine(distinct_id, facet_hashes.hashes[i]);
        }
    }

    return distinct_id;
}

inline uint32_t Index::next_suggestion(const std::vector<token_candidates> &token_candidates_vec,
                                   long long int n,
                                   std::vector<art_leaf *>& actual_query_suggestion,
                                   std::vector<art_leaf *>& query_suggestion,
                                   uint32_t& token_bits,
                                   uint64& qhash) {
    uint32_t total_cost = 0;
    qhash = 1;

    // generate the next combination from `token_leaves` and store it in `query_suggestion`
    ldiv_t q { n, 0 };
    for(long long i = 0 ; i < (long long) token_candidates_vec.size(); i++) {
        size_t token_size = token_candidates_vec[i].token.value.size();
        q = ldiv(q.quot, token_candidates_vec[i].candidates.size());
        actual_query_suggestion[i] = token_candidates_vec[i].candidates[q.rem];
        query_suggestion[i] = token_candidates_vec[i].candidates[q.rem];

        bool exact_match = token_candidates_vec[i].cost == 0 && token_size == actual_query_suggestion[i]->key_len-1;
        bool incr_for_prefix_search = token_candidates_vec[i].prefix_search && !exact_match;

        size_t actual_cost = (2 * token_candidates_vec[i].cost) + uint32_t(incr_for_prefix_search);

        total_cost += actual_cost;

        token_bits |= 1UL << token_candidates_vec[i].token.position; // sets n-th bit

        uintptr_t addr_val = (uintptr_t) query_suggestion[i];
        qhash = Index::hash_combine(qhash, addr_val);

        /*LOG(INFO) << "suggestion key: " << actual_query_suggestion[i]->key << ", token: "
                  << token_candidates_vec[i].token.value << ", actual_cost: " << actual_cost;
        LOG(INFO) << ".";*/
    }

    // Sort ascending based on matched documents for each token for faster intersection.
    // However, this causes the token order to deviate from original query's order.
    sort(query_suggestion.begin(), query_suggestion.end(), [](const art_leaf* left, const art_leaf* right) {
        return posting_t::num_ids(left->values) < posting_t::num_ids(right->values);
    });

    return total_cost;
}

Option<uint32_t> Index::remove(const uint32_t seq_id, const nlohmann::json & document, const bool is_update) {
    std::unique_lock lock(mutex);

    for(auto it = document.begin(); it != document.end(); ++it) {
        const std::string& field_name = it.key();
        const auto& search_field_it = search_schema.find(field_name);
        if(search_field_it == search_schema.end()) {
            continue;
        }

        const auto& search_field = search_field_it->second;

        if(!search_field.index) {
            continue;
        }

        // Go through all the field names and find the keys+values so that they can be removed from in-memory index
        if(search_field.type == field_types::STRING_ARRAY || search_field.type == field_types::STRING) {
            std::vector<std::string> tokens;
            tokenize_string_field(document, search_field, tokens, search_field.locale);

            for(size_t i = 0; i < tokens.size(); i++) {
                const auto& token = tokens[i];
                const unsigned char *key = (const unsigned char *) token.c_str();
                int key_len = (int) (token.length() + 1);

                art_leaf* leaf = (art_leaf *) art_search(search_index.at(field_name), key, key_len);
                if(leaf != nullptr) {
                    posting_t::erase(leaf->values, seq_id);
                    if (posting_t::num_ids(leaf->values) == 0) {
                        void* values = art_delete(search_index.at(field_name), key, key_len);
                        posting_t::destroy_list(values);
                    }
                }
            }
        } else if(search_field.is_int32()) {
            const std::vector<int32_t>& values = search_field.is_single_integer() ?
                    std::vector<int32_t>{document[field_name].get<int32_t>()} :
                    document[field_name].get<std::vector<int32_t>>();
            for(int32_t value: values) {
                num_tree_t* num_tree = numerical_index.at(field_name);
                num_tree->remove(value, seq_id);
            }
        } else if(search_field.is_int64()) {
            const std::vector<int64_t>& values = search_field.is_single_integer() ?
                                                 std::vector<int64_t>{document[field_name].get<int64_t>()} :
                                                 document[field_name].get<std::vector<int64_t>>();
            for(int64_t value: values) {
                num_tree_t* num_tree = numerical_index.at(field_name);
                num_tree->remove(value, seq_id);
            }
        } else if(search_field.is_float()) {
            const std::vector<float>& values = search_field.is_single_float() ?
                                                 std::vector<float>{document[field_name].get<float>()} :
                                                 document[field_name].get<std::vector<float>>();
            for(float value: values) {
                num_tree_t* num_tree = numerical_index.at(field_name);
                int64_t fintval = float_to_in64_t(value);
                num_tree->remove(fintval, seq_id);
            }
        } else if(search_field.is_bool()) {

            const std::vector<bool>& values = search_field.is_single_bool() ?
                                               std::vector<bool>{document[field_name].get<bool>()} :
                                               document[field_name].get<std::vector<bool>>();
            for(bool value: values) {
                num_tree_t* num_tree = numerical_index.at(field_name);
                int64_t bool_int64 = value ? 1 : 0;
                num_tree->remove(bool_int64, seq_id);
            }
        } else if(search_field.is_geopoint()) {
            auto geo_index = geopoint_index[field_name];
            S2RegionTermIndexer::Options options;
            options.set_index_contains_points_only(true);
            S2RegionTermIndexer indexer(options);

            const std::vector<std::vector<double>>& latlongs = search_field.is_single_geopoint() ?
                                  std::vector<std::vector<double>>{document[field_name].get<std::vector<double>>()} :
                                  document[field_name].get<std::vector<std::vector<double>>>();

            for(const std::vector<double>& latlong: latlongs) {
                S2Point point = S2LatLng::FromDegrees(latlong[0], latlong[1]).ToPoint();
                for(const auto& term: indexer.GetIndexTerms(point, "")) {
                    auto term_it = geo_index->find(term);
                    if(term_it == geo_index->end()) {
                        continue;
                    }
                    std::vector<uint32_t>& ids = term_it->second;
                    ids.erase(std::remove(ids.begin(), ids.end(), seq_id), ids.end());
                    if(ids.empty()) {
                        geo_index->erase(term);
                    }
                }
            }

            if(!search_field.is_single_geopoint()) {
                spp::sparse_hash_map<uint32_t, int64_t*>*& field_geo_array_map = geo_array_index.at(field_name);
                auto geo_array_it = field_geo_array_map->find(seq_id);
                if(geo_array_it != field_geo_array_map->end()) {
                    delete [] geo_array_it->second;
                    field_geo_array_map->erase(seq_id);
                }
            }
        }

        // remove facets
        const auto& field_facets_it = facet_index_v3.find(field_name);

        if(field_facets_it != facet_index_v3.end()) {
            const auto& fvalues_it = field_facets_it->second->find(seq_id);
            if(fvalues_it != field_facets_it->second->end()) {
                field_facets_it->second->erase(fvalues_it);
            }
        }

        // remove sort field
        if(sort_index.count(field_name) != 0) {
            sort_index[field_name]->erase(seq_id);
        }
    }

    if(!is_update) {
        seq_ids.remove_value(seq_id);
    }

    return Option<uint32_t>(seq_id);
}

void Index::tokenize_string_field(const nlohmann::json& document, const field& search_field,
                                  std::vector<std::string>& tokens, const std::string& locale) {

    const std::string& field_name = search_field.name;

    if(search_field.type == field_types::STRING) {
        Tokenizer(document[field_name], true, false, locale).tokenize(tokens);
    } else if(search_field.type == field_types::STRING_ARRAY) {
        const std::vector<std::string>& values = document[field_name].get<std::vector<std::string>>();
        for(const std::string & value: values) {
            Tokenizer(value, true, false, locale).tokenize(tokens);
        }
    }
}

art_leaf* Index::get_token_leaf(const std::string & field_name, const unsigned char* token, uint32_t token_len) {
    std::shared_lock lock(mutex);
    const art_tree *t = search_index.at(field_name);
    return (art_leaf*) art_search(t, token, (int) token_len);
}

const spp::sparse_hash_map<std::string, art_tree *> &Index::_get_search_index() const {
    return search_index;
}

const spp::sparse_hash_map<std::string, num_tree_t*>& Index::_get_numerical_index() const {
    return numerical_index;
}

void Index::refresh_schemas(const std::vector<field>& new_fields) {
    std::unique_lock lock(mutex);

    for(const auto & new_field: new_fields) {
        search_schema.emplace(new_field.name, new_field);
        sort_schema.emplace(new_field.name, new_field);

        if(search_index.count(new_field.name) == 0) {
            if(new_field.is_string() || field_types::is_string_or_array(new_field.type)) {
                art_tree *t = new art_tree;
                art_tree_init(t);
                search_index.emplace(new_field.name, t);
            } else if(new_field.is_geopoint()) {
                auto field_geo_index = new spp::sparse_hash_map<std::string, std::vector<uint32_t>>();
                geopoint_index.emplace(new_field.name, field_geo_index);
                if(!new_field.is_single_geopoint()) {
                    auto geo_array_map = new spp::sparse_hash_map<uint32_t, int64_t*>();
                    geo_array_index.emplace(new_field.name, geo_array_map);
                }
            } else {
                num_tree_t* num_tree = new num_tree_t;
                numerical_index.emplace(new_field.name, num_tree);
            }
        }

        if(new_field.is_facet()) {
            facet_schema.emplace(new_field.name, new_field);

            auto doc_to_values = new spp::sparse_hash_map<uint32_t, facet_hash_values_t>();
            facet_index_v3.emplace(new_field.name, doc_to_values);

            // initialize for non-string facet fields
            if(!new_field.is_string()) {
                art_tree *ft = new art_tree;
                art_tree_init(ft);
                search_index.emplace(new_field.faceted_name(), ft);
            }
        }

        if(sort_index.count(new_field.name) == 0) {
            spp::sparse_hash_map<uint32_t, int64_t> * doc_to_score = new spp::sparse_hash_map<uint32_t, int64_t>();
            sort_index.emplace(new_field.name, doc_to_score);
        }
    }
}

Option<uint32_t> Index::coerce_string(const DIRTY_VALUES& dirty_values, const std::string& fallback_field_type,
                                      const field& a_field, nlohmann::json &document,
                                      const std::string &field_name, nlohmann::json::iterator& array_iter,
                                      bool is_array, bool& array_ele_erased) {
    std::string suffix = is_array ? "an array of" : "a";
    auto& item = is_array ? array_iter.value() : document[field_name];

    if(dirty_values == DIRTY_VALUES::REJECT) {
        return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " string.");
    }

    if(dirty_values == DIRTY_VALUES::DROP) {
        if(!a_field.optional) {
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " string.");
        }

        if(!is_array) {
            document.erase(field_name);
        } else {
            array_iter = document[field_name].erase(array_iter);
            array_ele_erased = true;
        }
        return Option<uint32_t>(200);
    }

    // we will try to coerce the value to a string

    if (item.is_number_integer()) {
        item = std::to_string((int64_t)item);
    }

    else if(item.is_number_float()) {
        item = StringUtils::float_to_str((float)item);
    }

    else if(item.is_boolean()) {
        item = item == true ? "true" : "false";
    }

    else {
        if(dirty_values == DIRTY_VALUES::COERCE_OR_DROP) {
            if(!a_field.optional) {
                return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " string.");
            }

            if(!is_array) {
                document.erase(field_name);
            } else {
                array_iter = document[field_name].erase(array_iter);
                array_ele_erased = true;
            }
        } else {
            // COERCE_OR_REJECT / non-optional + DROP
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " string.");
        }
    }

    return Option<>(200);
}

Option<uint32_t> Index::coerce_int32_t(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                       const std::string &field_name,
                                       nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased) {
    std::string suffix = is_array ? "an array of" : "an";
    auto& item = is_array ? array_iter.value() : document[field_name];

    if(dirty_values == DIRTY_VALUES::REJECT) {
        return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int32.");
    }

    if(dirty_values == DIRTY_VALUES::DROP) {
        if(!a_field.optional) {
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int32.");
        }

        if(!is_array) {
            document.erase(field_name);
        } else {
            array_iter = document[field_name].erase(array_iter);
            array_ele_erased = true;
        }
        return Option<uint32_t>(200);
    }

    // try to value coerce into an integer

    if(item.is_number_float()) {
        item = static_cast<int32_t>(item.get<float>());
    }

    else if(item.is_boolean()) {
        item = item == true ? 1 : 0;
    }

    else if(item.is_string() && StringUtils::is_int32_t(item)) {
        item = std::atol(item.get<std::string>().c_str());
    }

    else {
        if(dirty_values == DIRTY_VALUES::COERCE_OR_DROP) {
            if(!a_field.optional) {
                return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int32.");
            }

            if(!is_array) {
                document.erase(field_name);
            } else {
                array_iter = document[field_name].erase(array_iter);
                array_ele_erased = true;
            }
        } else {
            // COERCE_OR_REJECT / non-optional + DROP
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int32.");
        }
    }

    return Option<uint32_t>(200);
}

Option<uint32_t> Index::coerce_int64_t(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                       const std::string &field_name,
                                       nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased) {
    std::string suffix = is_array ? "an array of" : "an";
    auto& item = is_array ? array_iter.value() : document[field_name];

    if(dirty_values == DIRTY_VALUES::REJECT) {
        return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int64.");
    }

    if(dirty_values == DIRTY_VALUES::DROP) {
        if(!a_field.optional) {
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int64.");
        }

        if(!is_array) {
            document.erase(field_name);
        } else {
            array_iter = document[field_name].erase(array_iter);
            array_ele_erased = true;
        }
        return Option<uint32_t>(200);
    }

    // try to value coerce into an integer

    if(item.is_number_float()) {
        item = static_cast<int64_t>(item.get<float>());
    }

    else if(item.is_boolean()) {
        item = item == true ? 1 : 0;
    }

    else if(item.is_string() && StringUtils::is_int64_t(item)) {
        item = std::atoll(item.get<std::string>().c_str());
    }

    else {
        if(dirty_values == DIRTY_VALUES::COERCE_OR_DROP) {
            if(!a_field.optional) {
                return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int64.");
            }

            if(!is_array) {
                document.erase(field_name);
            } else {
                array_iter = document[field_name].erase(array_iter);
                array_ele_erased = true;
            }
        } else {
            // COERCE_OR_REJECT / non-optional + DROP
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int64.");
        }
    }

    return Option<uint32_t>(200);
}

Option<uint32_t> Index::coerce_bool(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                    const std::string &field_name,
                                    nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased) {
    std::string suffix = is_array ? "a array of" : "a";
    auto& item = is_array ? array_iter.value() : document[field_name];

    if(dirty_values == DIRTY_VALUES::REJECT) {
        return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " bool.");
    }

    if(dirty_values == DIRTY_VALUES::DROP) {
        if(!a_field.optional) {
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " bool.");
        }

        if(!is_array) {
            document.erase(field_name);
        } else {
            array_iter = document[field_name].erase(array_iter);
            array_ele_erased = true;
        }
        return Option<uint32_t>(200);
    }

    // try to value coerce into a bool
    if (item.is_number_integer() &&
        (item.get<int64_t>() == 1 || item.get<int64_t>() == 0)) {
        item = item.get<int64_t>() == 1;
    }

    else if(item.is_string()) {
        std::string str_val = item.get<std::string>();
        StringUtils::tolowercase(str_val);
        if(str_val == "true") {
            item = true;
            return Option<uint32_t>(200);
        } else if(str_val == "false") {
            item = false;
            return Option<uint32_t>(200);
        } else {
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " bool.");
        }
    }

    else {
        if(dirty_values == DIRTY_VALUES::COERCE_OR_DROP) {
            if(!a_field.optional) {
                return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " bool.");
            }

            if(!is_array) {
                document.erase(field_name);
            } else {
                array_iter = document[field_name].erase(array_iter);
                array_ele_erased = true;
            }
        } else {
            // COERCE_OR_REJECT / non-optional + DROP
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " bool.");
        }
    }

    return Option<uint32_t>(200);
}

Option<uint32_t> Index::coerce_geopoint(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                        const std::string &field_name,
                                        nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased) {
    std::string suffix = is_array ? "an array of" : "a";
    auto& item = is_array ? array_iter.value() : document[field_name];

    if(dirty_values == DIRTY_VALUES::REJECT) {
        return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " geopoint.");
    }

    if(dirty_values == DIRTY_VALUES::DROP) {
        if(!a_field.optional) {
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " geopoint.");
        }

        if(!is_array) {
            document.erase(field_name);
        } else {
            array_iter = document[field_name].erase(array_iter);
            array_ele_erased = true;
        }
        return Option<uint32_t>(200);
    }

    // try to value coerce into a geopoint

    if(!item[0].is_number() && item[0].is_string()) {
        if(StringUtils::is_float(item[0])) {
            item[0] = std::stof(item[0].get<std::string>());
        }
    }

    if(!item[1].is_number() && item[1].is_string()) {
        if(StringUtils::is_float(item[1])) {
            item[1] = std::stof(item[1].get<std::string>());
        }
    }

    if(!item[0].is_number() || !item[1].is_number()) {
        if(dirty_values == DIRTY_VALUES::COERCE_OR_DROP) {
            if(!a_field.optional) {
                return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " geopoint.");
            }

            if(!is_array) {
                document.erase(field_name);
            } else {
                array_iter = document[field_name].erase(array_iter);
                array_ele_erased = true;
            }
        } else {
            // COERCE_OR_REJECT / non-optional + DROP
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " geopoint.");
        }
    }

    return Option<uint32_t>(200);
}

Option<uint32_t> Index::coerce_float(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                     const std::string &field_name,
                                     nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased) {
    std::string suffix = is_array ? "a array of" : "a";
    auto& item = is_array ? array_iter.value() : document[field_name];

    if(dirty_values == DIRTY_VALUES::REJECT) {
        return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " float.");
    }

    if(dirty_values == DIRTY_VALUES::DROP) {
        if(!a_field.optional) {
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " float.");
        }

        if(!is_array) {
            document.erase(field_name);
        } else {
            array_iter = document[field_name].erase(array_iter);
            array_ele_erased = true;
        }
        return Option<uint32_t>(200);
    }

    // try to value coerce into a float

    if(item.is_string() && StringUtils::is_float(item)) {
        item = std::atof(item.get<std::string>().c_str());
    }

    else if(item.is_boolean()) {
        item = item == true ? 1.0 : 0.0;
    }

    else {
        if(dirty_values == DIRTY_VALUES::COERCE_OR_DROP) {
            if(!a_field.optional) {
                return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " float.");
            }

            if(!is_array) {
                document.erase(field_name);
            } else {
                array_iter = document[field_name].erase(array_iter);
                array_ele_erased = true;
            }
        } else {
            // COERCE_OR_REJECT / non-optional + DROP
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " float.");
        }
    }

    return Option<uint32_t>(200);
}

void Index::get_doc_changes(const index_operation_t op, const nlohmann::json& update_doc,
                            const nlohmann::json& old_doc, nlohmann::json& new_doc, nlohmann::json& del_doc) {

    for(auto it = old_doc.begin(); it != old_doc.end(); ++it) {
        if(op == UPSERT && !update_doc.contains(it.key())) {
            del_doc[it.key()] = it.value();
        } else {
            new_doc[it.key()] = it.value();
        }
    }

    for(auto it = update_doc.begin(); it != update_doc.end(); ++it) {
        // adds new key or overrides existing key from `old_doc`
        new_doc[it.key()] = it.value();

        // if the update update_doc contains a field that exists in old, we record that (for delete + reindex)
        bool field_exists_in_old_doc = (old_doc.count(it.key()) != 0);
        if(field_exists_in_old_doc) {
            // key exists in the stored doc, so it must be reindexed
            // we need to check for this because a field can be optional
            del_doc[it.key()] = old_doc[it.key()];
        }
    }
}

void Index::scrub_reindex_doc(const std::unordered_map<std::string, field>& search_schema,
                              nlohmann::json& update_doc,
                              nlohmann::json& del_doc,
                              const nlohmann::json& old_doc) {
    std::vector<std::string> del_keys;

    for(auto it = del_doc.cbegin(); it != del_doc.cend(); it++) {
        const std::string& field_name = it.key();

        const auto& search_field_it = search_schema.find(field_name);
        if(search_field_it == search_schema.end()) {
            continue;
        }

        const auto search_field = search_field_it->second;  // copy, don't use reference!

        // compare values between old and update docs:
        // if they match, we will remove them from both del and update docs

        if(update_doc.contains(search_field.name) && update_doc[search_field.name] == old_doc[search_field.name]) {
            del_keys.push_back(field_name);
        }
    }

    for(const auto& del_key: del_keys) {
        del_doc.erase(del_key);
        update_doc.erase(del_key);
    }
}

/*
// https://stackoverflow.com/questions/924171/geo-fencing-point-inside-outside-polygon
// NOTE: polygon and point should have been transformed with `transform_for_180th_meridian`
bool Index::is_point_in_polygon(const Geofence& poly, const GeoCoord &point) {
    int i, j;
    bool c = false;

    for (i = 0, j = poly.numVerts - 1; i < poly.numVerts; j = i++) {
        if ((((poly.verts[i].lat <= point.lat) && (point.lat < poly.verts[j].lat))
             || ((poly.verts[j].lat <= point.lat) && (point.lat < poly.verts[i].lat)))
            && (point.lon < (poly.verts[j].lon - poly.verts[i].lon) * (point.lat - poly.verts[i].lat)
                            / (poly.verts[j].lat - poly.verts[i].lat) + poly.verts[i].lon)) {

            c = !c;
        }
    }

    return c;
}

double Index::transform_for_180th_meridian(Geofence &poly) {
    double offset = 0.0;
    double maxLon = -1000, minLon = 1000;

    for(int v=0; v < poly.numVerts; v++) {
        if(poly.verts[v].lon < minLon) {
            minLon = poly.verts[v].lon;
        }

        if(poly.verts[v].lon > maxLon) {
            maxLon = poly.verts[v].lon;
        }

        if(std::abs(minLon - maxLon) > 180) {
            offset = 360.0;
        }
    }

    int i, j;
    for (i = 0, j = poly.numVerts - 1; i < poly.numVerts; j = i++) {
        if (poly.verts[i].lon < 0.0) {
            poly.verts[i].lon += offset;
        }

        if (poly.verts[j].lon < 0.0) {
            poly.verts[j].lon += offset;
        }
    }

    return offset;
}

void Index::transform_for_180th_meridian(GeoCoord &point, double offset) {
    point.lon = point.lon < 0.0 ? point.lon + offset : point.lon;
}
*/
