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
#include <h3api.h>
#include "logger.h"

Index::Index(const std::string name, const std::unordered_map<std::string, field> & search_schema,
             std::map<std::string, field> facet_schema, std::unordered_map<std::string, field> sort_schema):
        name(name), search_schema(search_schema), facet_schema(facet_schema), sort_schema(sort_schema) {

    for(const auto & fname_field: search_schema) {
        if(fname_field.second.is_string()) {
            art_tree *t = new art_tree;
            art_tree_init(t);
            search_index.emplace(fname_field.first, t);
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
        spp::sparse_hash_map<uint32_t, int64_t> * doc_to_score = new spp::sparse_hash_map<uint32_t, int64_t>();
        sort_index.emplace(pair.first, doc_to_score);
    }

    for(const auto& pair: facet_schema) {
        spp::sparse_hash_map<uint32_t, facet_hash_values_t> *doc_to_values = new spp::sparse_hash_map<uint32_t, facet_hash_values_t>();
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

    for(auto & name_tree: numerical_index) {
        delete name_tree.second;
        name_tree.second = nullptr;
    }

    search_index.clear();

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

Option<uint32_t> Index::index_in_memory(const nlohmann::json &document, uint32_t seq_id,
                                        const std::string & default_sorting_field) {

    std::unique_lock lock(mutex);

    int64_t points = 0;

    if(document.count(default_sorting_field) == 0) {
        if(sort_index.count(default_sorting_field) != 0 && sort_index[default_sorting_field]->count(seq_id)) {
            points = sort_index[default_sorting_field]->at(seq_id);
        } else {
            points = INT64_MIN;
        }
    } else {
        points = get_points_from_doc(document, default_sorting_field);
    }

    seq_ids.append(seq_id);

    // assumes that validation has already been done
    for(const auto& field_pair: search_schema) {
        const std::string & field_name = field_pair.first;

        if(document.count(field_name) == 0 || !field_pair.second.index) {
            continue;
        }

        bool is_facet = (facet_schema.count(field_name) != 0);

        // non-string, non-geo faceted field should be indexed as faceted string field as well
        if(field_pair.second.facet && !field_pair.second.is_string() && field_pair.second.type != field_types::GEOPOINT) {
            art_tree *t = search_index.at(field_pair.second.faceted_name());

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
                        strings.push_back(std::to_string(value));
                    }
                } else if(field_pair.second.type == field_types::BOOL_ARRAY) {
                    for(bool value: document[field_name]){
                        strings.push_back(std::to_string(value));
                    }
                }
                index_string_array_field(strings, points, t, seq_id, is_facet, field_pair.second);
            } else {
                std::string text;

                if(field_pair.second.type == field_types::INT32) {
                    text = std::to_string(document[field_name].get<int32_t>());
                } else if(field_pair.second.type == field_types::INT64) {
                    text = std::to_string(document[field_name].get<int64_t>());
                } else if(field_pair.second.type == field_types::FLOAT) {
                    text = std::to_string(document[field_name].get<float>());
                } else if(field_pair.second.type == field_types::BOOL) {
                    text = std::to_string(document[field_name].get<bool>());
                }

                index_string_field(text, points, t, seq_id, is_facet, field_pair.second);
            }
        }

        if(field_pair.second.type == field_types::STRING) {
            art_tree *t = search_index.at(field_name);
            const std::string & text = document[field_name];
            index_string_field(text, points, t, seq_id, is_facet, field_pair.second);
        }

        else if(field_pair.second.type == field_types::INT32) {
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
            auto num_tree = numerical_index.at(field_name);
            const std::vector<double>& latlong = document[field_name];
            GeoCoord x {degsToRads(latlong[0]), degsToRads(latlong[1])};
            H3Index geoHash = geoToH3(&x, field_pair.second.geo_resolution);
            //LOG(INFO) << "Indexing h3 index " << geoHash << " for seq_id " << seq_id << " at res: " << size_t(field_pair.second.geo_resolution);
            num_tree->insert(geoHash, seq_id);
        } else if(field_pair.second.type == field_types::STRING_ARRAY) {
            art_tree *t = search_index.at(field_name);
            index_string_array_field(document[field_name], points, t, seq_id, is_facet, field_pair.second);
        }

        else if(field_pair.second.is_array()) {
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
                GeoCoord x {degsToRads(latlong[0]), degsToRads(latlong[1])};
                H3Index geoHash = geoToH3(&x, FINEST_GEO_RESOLUTION);
                doc_to_score->emplace(seq_id, (int64_t)(geoHash));
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
                                                 bool is_update,
                                                 const std::string& fallback_field_type,
                                                 const DIRTY_VALUES& dirty_values) {

    bool missing_default_sort_field = (!default_sorting_field.empty() && document.count(default_sorting_field) == 0);

    if(!is_update && missing_default_sort_field) {
        return Option<>(400, "Field `" + default_sorting_field  + "` has been declared as a default sorting field, "
                "but is not found in the document.");
    }

    for(const auto& field_pair: search_schema) {
        const std::string& field_name = field_pair.first;
        const field& a_field = field_pair.second;

        if((a_field.optional || is_update) && document.count(field_name) == 0) {
            continue;
        }

        if(document.count(field_name) == 0) {
            return Option<>(400, "Field `" + field_name  + "` has been declared in the schema, "
                                 "but is not found in the document.");
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

void Index::scrub_reindex_doc(nlohmann::json& update_doc, nlohmann::json& del_doc, nlohmann::json& old_doc) {
    std::vector<std::string> del_keys;

    for(auto it = del_doc.cbegin(); it != del_doc.cend(); it++) {
        const std::string& field_name = it.key();

        std::shared_lock lock(mutex);

        const auto& search_field_it = search_schema.find(field_name);
        if(search_field_it == search_schema.end()) {
            continue;
        }

        const auto search_field = search_field_it->second;  // copy, don't use reference!

        lock.unlock();

        // compare values between old and update docs:
        // if they match, we will remove them from both del and update docs

        if(update_doc[search_field.name] == old_doc[search_field.name]) {
            del_keys.push_back(field_name);
        }
    }

    for(const auto& del_key: del_keys) {
        del_doc.erase(del_key);
        update_doc.erase(del_key);
    }
}

size_t Index::batch_memory_index(Index *index, std::vector<index_record> & iter_batch,
                                 const std::string & default_sorting_field,
                                 const std::unordered_map<std::string, field> & search_schema,
                                 const std::map<std::string, field> & facet_schema,
                                 const std::string& fallback_field_type) {

    size_t num_indexed = 0;

    for(auto & index_rec: iter_batch) {
        if(!index_rec.indexed.ok()) {
            // some records could have been invalidated upstream
            continue;
        }

        if(index_rec.operation != DELETE) {
            Option<uint32_t> validation_op = validate_index_in_memory(index_rec.doc, index_rec.seq_id,
                                                                      default_sorting_field,
                                                                      search_schema, facet_schema,
                                                                      index_rec.is_update,
                                                                      fallback_field_type,
                                                                      index_rec.dirty_values);

            if(!validation_op.ok()) {
                index_rec.index_failure(validation_op.code(), validation_op.error());
                continue;
            }

            if(index_rec.is_update) {
                // scrub string fields to reduce delete ops
                get_doc_changes(index_rec.doc, index_rec.old_doc, index_rec.new_doc, index_rec.del_doc);
                index->scrub_reindex_doc(index_rec.doc, index_rec.del_doc, index_rec.old_doc);
                index->remove(index_rec.seq_id, index_rec.del_doc);
            }

            Option<uint32_t> index_mem_op(0);

            try {
                index_mem_op = index->index_in_memory(index_rec.doc, index_rec.seq_id, default_sorting_field);
            } catch(const std::exception& e) {
                const std::string& error_msg = std::string("Fatal error during indexing: ") + e.what();
                LOG(ERROR) << error_msg << ", document: " << index_rec.doc;
                index_mem_op = Option<uint32_t>(500, error_msg);
            }

            if(!index_mem_op.ok()) {
                index->index_in_memory(index_rec.del_doc, index_rec.seq_id, default_sorting_field);
                index_rec.index_failure(index_mem_op.code(), index_mem_op.error());
                continue;
            }

            index_rec.index_success();

            if(!index_rec.is_update) {
                num_indexed++;
            }
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

        for(size_t i=0; i<kv.second.size(); i++) {
            art_doc.offsets[i] = kv.second[i];
        }

        //LOG(INFO) << "key: " << key << ", art_doc.id: " << art_doc.id;
        art_insert(t, key, key_len, &art_doc, num_hits);
        delete [] art_doc.offsets;
        art_doc.offsets = nullptr;
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

void Index::index_string_field(const std::string & text, const int64_t score, art_tree *t,
                                    uint32_t seq_id, bool is_facet, const field & a_field) {
    std::unordered_map<std::string, std::vector<uint32_t>> token_to_offsets;

    Tokenizer tokenizer(text, false, true, !a_field.is_string(), a_field.locale);
    std::string token;
    size_t token_index = 0;

    std::vector<uint64_t> facet_hashes;

    while(tokenizer.next(token, token_index)) {
        if(token.empty()) {
            continue;
        }

        if(is_facet) {
            uint64_t hash = facet_token_hash(a_field, token);
            facet_hashes.push_back(hash);
        }

        token_to_offsets[token].push_back(token_index);
    }

    /*if(seq_id == 0) {
        LOG(INFO) << "field name: " << a_field.name;
    }*/

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

void Index::index_string_array_field(const std::vector<std::string> & strings, const int64_t score, art_tree *t,
                                          uint32_t seq_id, bool is_facet, const field & a_field) {
    std::unordered_map<std::string, std::vector<uint32_t>> token_positions;
    std::vector<uint64_t> facet_hashes;

    for(size_t array_index = 0; array_index < strings.size(); array_index++) {
        const std::string& str = strings[array_index];
        std::set<std::string> token_set;  // required to deal with repeating tokens

        Tokenizer tokenizer(str, false, true, !a_field.is_string(), a_field.locale);
        std::string token;
        size_t token_index = 0;

        // iterate and append offset positions
        while(tokenizer.next(token, token_index)) {
            if(token.empty()) {
                continue;
            }

            if(is_facet) {
                uint64_t hash = facet_token_hash(a_field, token);
                facet_hashes.push_back(hash);
                //LOG(INFO) << "indexing " << token  << ", hash:" << hash;
            }

            token_positions[token].push_back(token_index);
            token_set.insert(token);
        }

        if(is_facet) {
            facet_hashes.push_back(FACET_ARRAY_DELIMETER); // as a delimiter
        }

        // repeat last element to indicate end of offsets for this array index
        for(auto & the_token: token_set) {
            token_positions[the_token].push_back(token_positions[the_token].back());
        }

        // iterate and append this array index to all tokens
        for(auto & the_token: token_set) {
            token_positions[the_token].push_back(array_index);
        }
    }

    if(is_facet) {
        facet_hash_values_t fhashvalues;
        fhashvalues.length = facet_hashes.size();
        fhashvalues.hashes = new uint64_t[facet_hashes.size()];

        for(size_t i  = 0; i < facet_hashes.size(); i++) {
            fhashvalues.hashes[i] = facet_hashes[i];
        }

        facet_index_v3[a_field.name]->emplace(seq_id, std::move(fhashvalues));
    }

    insert_doc(score, t, seq_id, token_positions);
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
                      size_t group_limit, const std::vector<std::string>& group_by_fields,
                      const uint32_t* result_ids, size_t results_size) const {

    struct facet_info_t {
        // facet hash => token position in the query
        std::unordered_map<uint64_t, token_pos_cost_t> fhash_qtoken_pos;

        bool use_facet_query = false;
        bool should_compute_stats = false;
        field facet_field{"", "", false};
    };

    std::vector<facet_info_t> facet_infos(facets.size());

    for(size_t findex=0; findex < facets.size(); findex++) {
        const auto& a_facet = facets[findex];

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

            // for non-string fields, `faceted_name` returns their aliased stringified field name
            art_tree *t = search_index.at(facet_field.faceted_name());

            std::vector<std::string> query_tokens;
            Tokenizer(facet_query.query, false, true, !facet_field.is_string()).tokenize(query_tokens);

            for (size_t qtoken_index = 0; qtoken_index < query_tokens.size(); qtoken_index++) {
                auto &q = query_tokens[qtoken_index];

                int bounded_cost = (q.size() < 3) ? 0 : 1;
                bool prefix_search = (qtoken_index ==
                                      (query_tokens.size() - 1)); // only last token must be used as prefix

                std::vector<art_leaf *> leaves;

                art_fuzzy_search(t, (const unsigned char *) q.c_str(),
                                 q.size(), 0, bounded_cost, 10000,
                                 token_ordering::MAX_SCORE, prefix_search, nullptr, 0, leaves);

                for (size_t leaf_index = 0; leaf_index < leaves.size(); leaf_index++) {
                    const auto &leaf = leaves[leaf_index];
                    // calculate hash without terminating null char
                    std::string key_str((const char *) leaf->key, leaf->key_len - 1);
                    uint64_t hash = facet_token_hash(facet_field, key_str);

                    token_pos_cost_t token_pos_cost = {qtoken_index, 0};
                    facet_infos[findex].fhash_qtoken_pos.emplace(hash, token_pos_cost);
                    //printf("%.*s - %llu\n", leaf->key_len, leaf->key, hash);
                }
            }
        }
    }

    // assumed that facet fields have already been validated upstream
    for(size_t findex=0; findex < facets.size(); findex++) {
        auto& a_facet = facets[findex];
        const auto& facet_field = facet_infos[findex].facet_field;
        const bool use_facet_query = facet_infos[findex].use_facet_query;
        const auto& fhash_qtoken_pos = facet_infos[findex].fhash_qtoken_pos;
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

            // FORMAT OF VALUES
            // String: h1 h2 h3
            // String array: h1 h2 h3 0 h1 0 h1 h2 0
            const auto& facet_hashes = facet_hashes_it->second;

            const uint64_t distinct_id = group_limit ? get_distinct_id(group_by_fields, doc_seq_id) : 0;

            int array_pos = 0;
            bool fvalue_found = false;
            uint64_t combined_hash = 1;  // for hashing the entire facet value (multiple tokens)

            std::unordered_map<uint32_t, token_pos_cost_t> query_token_positions;
            size_t field_token_index = -1;
            auto fhashes = facet_hashes.hashes;

            for(size_t j = 0; j < facet_hashes.size(); j++) {
                if(fhashes[j] != FACET_ARRAY_DELIMETER) {
                    uint64_t ftoken_hash = fhashes[j];
                    field_token_index++;

                    // reference: https://stackoverflow.com/a/4182771/131050
                    // we also include token index to maintain orderliness
                    combined_hash *= (1779033703 + 2*ftoken_hash*(field_token_index+1));

                    // ftoken_hash is the raw value for numeric fields
                    if(should_compute_stats) {
                        compute_facet_stats(a_facet, ftoken_hash, facet_field.type);
                    }

                    const auto fhash_qtoken_pos_it = fhash_qtoken_pos.find(ftoken_hash);

                    // not using facet query or this particular facet value is found in facet filter
                    if(!use_facet_query || fhash_qtoken_pos_it != fhash_qtoken_pos.end()) {
                        fvalue_found = true;

                        if(use_facet_query) {
                            // map token index to query index (used for highlighting later on)
                            const token_pos_cost_t& qtoken_pos = fhash_qtoken_pos_it->second;

                            // if the query token has already matched another token in the string
                            // we will replace the position only if the cost is lower
                            if(query_token_positions.find(qtoken_pos.pos) == query_token_positions.end() ||
                               query_token_positions[qtoken_pos.pos].cost >= qtoken_pos.cost ) {
                                token_pos_cost_t ftoken_pos_cost = {field_token_index, qtoken_pos.cost};
                                query_token_positions[qtoken_pos.pos] = ftoken_pos_cost;
                            }
                        }
                    }
                }

                // 0 indicates separator, while the second condition checks for non-array string
                if(fhashes[j] == FACET_ARRAY_DELIMETER || (facet_hashes.back() != FACET_ARRAY_DELIMETER && j == facet_hashes.size() - 1)) {
                    if(!use_facet_query || fvalue_found) {
                        uint64_t fhash = combined_hash;

                        if(a_facet.result_map.count(fhash) == 0) {
                            a_facet.result_map.emplace(fhash, facet_count_t{0, spp::sparse_hash_set<uint64_t>(),
                                                                            doc_seq_id, 0,
                                                                            std::unordered_map<uint32_t, token_pos_cost_t>()});
                        }

                        facet_count_t& facet_count = a_facet.result_map[fhash];

                        /*LOG(INFO) << "field: " << a_facet.field_name << ", doc id: " << doc_seq_id
                                  << ", hash: " <<  fhash;*/

                        facet_count.doc_id = doc_seq_id;
                        facet_count.array_pos = array_pos;

                        if(group_limit) {
                            facet_count.groups.emplace(distinct_id);
                        } else {
                            facet_count.count += 1;
                        }

                        if(use_facet_query) {
                            facet_count.query_token_pos = query_token_positions;
                        }
                    }

                    array_pos++;
                    fvalue_found = false;
                    combined_hash = 1;
                    std::unordered_map<uint32_t, token_pos_cost_t>().swap(query_token_positions);
                    field_token_index = -1;
                }
            }
        }
    }
}

void Index::search_candidates(const uint8_t & field_id,
                              uint32_t* filter_ids, size_t filter_ids_length,
                              const uint32_t* exclude_token_ids, size_t exclude_token_ids_size,
                              const std::vector<uint32_t>& curated_ids,
                              const std::vector<sort_by> & sort_fields,
                              std::vector<token_candidates> & token_candidates_vec,
                              std::vector<std::vector<art_leaf*>> & searched_queries, Topster* topster,
                              spp::sparse_hash_set<uint64_t>& groups_processed,
                              uint32_t** all_result_ids, size_t & all_result_ids_len,
                              size_t& field_num_results,
                              const size_t typo_tokens_threshold,
                              const size_t group_limit, const std::vector<std::string>& group_by_fields) const {
    const long long combination_limit = 10;

    auto product = []( long long a, token_candidates & b ) { return a*b.candidates.size(); };
    long long int N = std::accumulate(token_candidates_vec.begin(), token_candidates_vec.end(), 1LL, product);

    for(long long n=0; n<N && n<combination_limit; ++n) {
        // every element in `query_suggestion` contains a token and its associated hits
        std::vector<art_leaf*> query_suggestion(token_candidates_vec.size());

        // actual query suggestion preserves original order of tokens in query
        std::vector<art_leaf*> actual_query_suggestion(token_candidates_vec.size());

        uint32_t token_bits = (uint32_t(1) << 31);  // top most bit set to guarantee atleast 1 bit set
        uint32_t total_cost = next_suggestion(token_candidates_vec, n, actual_query_suggestion,
                                              query_suggestion, token_bits);

        /*LOG(INFO) << "n: " << n;
        for(size_t i=0; i < query_suggestion.size(); i++) {
            LOG(INFO) << "i: " << i << " - " << query_suggestion[i]->key << ", ids: "
                      << query_suggestion[i]->values->ids.getLength() << ", total_cost: " << total_cost;
        }*/

        // initialize results with the starting element (for further intersection)
        size_t result_size = query_suggestion[0]->values->ids.getLength();
        if(result_size == 0) {
            continue;
        }

        uint32_t* result_ids = query_suggestion[0]->values->ids.uncompress();

        // intersect the document ids for each token to find docs that contain all the tokens (stored in `result_ids`)
        for(size_t i=1; i < query_suggestion.size(); i++) {
            uint32_t* out = nullptr;
            uint32_t* ids = query_suggestion[i]->values->ids.uncompress();
            result_size = ArrayUtils::and_scalar(ids, query_suggestion[i]->values->ids.getLength(), result_ids, result_size, &out);
            delete[] ids;
            delete[] result_ids;
            result_ids = out;
        }

        if(result_size == 0) {
            delete[] result_ids;
            continue;
        }

        // Exclude document IDs associated with excluded tokens from the result set
        if(exclude_token_ids_size != 0) {
            uint32_t *excluded_result_ids = nullptr;
            result_size = ArrayUtils::exclude_scalar(result_ids, result_size, exclude_token_ids, exclude_token_ids_size,
                                                     &excluded_result_ids);
            delete[] result_ids;
            result_ids = excluded_result_ids;
        }

        if(!curated_ids.empty()) {
            uint32_t *excluded_result_ids = nullptr;
            result_size = ArrayUtils::exclude_scalar(result_ids, result_size, &curated_ids[0],
                                                     curated_ids.size(), &excluded_result_ids);

            delete [] result_ids;
            result_ids = excluded_result_ids;
        }

        //LOG(INFO) << "n: " << n;
        /*std::stringstream log_query;
        for(size_t i=0; i < query_suggestion.size(); i++) {
            log_query << query_suggestion[i]->key << " ";
        }*/

        if(filter_ids != nullptr) {
            // intersect once again with filter ids
            uint32_t* filtered_result_ids = nullptr;
            size_t filtered_results_size = ArrayUtils::and_scalar(filter_ids, filter_ids_length, result_ids,
                                                                  result_size, &filtered_result_ids);

            uint32_t* new_all_result_ids = nullptr;
            all_result_ids_len = ArrayUtils::or_scalar(*all_result_ids, all_result_ids_len, filtered_result_ids,
                                  filtered_results_size, &new_all_result_ids);
            delete [] *all_result_ids;
            *all_result_ids = new_all_result_ids;

            // go through each matching document id and calculate match score
            score_results(sort_fields, (uint16_t) searched_queries.size(), field_id, total_cost, topster, query_suggestion,
                          groups_processed, filtered_result_ids, filtered_results_size,
                          group_limit, group_by_fields, token_bits);

            field_num_results += filtered_results_size;

            delete[] filtered_result_ids;
            delete[] result_ids;
        } else {
            uint32_t* new_all_result_ids = nullptr;
            all_result_ids_len = ArrayUtils::or_scalar(*all_result_ids, all_result_ids_len, result_ids,
                                  result_size, &new_all_result_ids);
            delete [] *all_result_ids;
            *all_result_ids = new_all_result_ids;

            /*if(result_size != 0) {
                LOG(INFO) << size_t(field_id) << " - " << log_query.str() << ", result_size: " << result_size;
            }*/

            score_results(sort_fields, (uint16_t) searched_queries.size(), field_id, total_cost, topster, query_suggestion,
                          groups_processed, result_ids, result_size, group_limit, group_by_fields, token_bits);

            field_num_results += result_size;

            delete[] result_ids;
        }

        searched_queries.push_back(actual_query_suggestion);

        //LOG(INFO) << "field_num_results: " << field_num_results << ", typo_tokens_threshold: " << typo_tokens_threshold;
        if(field_num_results >= typo_tokens_threshold) {
            break;
        }
    }
}

uint32_t Index::do_filtering(uint32_t** filter_ids_out, const std::vector<filter> & filters) const {
    //auto begin = std::chrono::high_resolution_clock::now();

    uint32_t* filter_ids = nullptr;
    uint32_t filter_ids_length = 0;

    for(size_t i = 0; i < filters.size(); i++) {
        const filter & a_filter = filters[i];
        bool has_search_index = search_index.count(a_filter.field_name) != 0 ||
                                numerical_index.count(a_filter.field_name) != 0;

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
                num_tree->search(a_filter.comparators[value_index], bool_int64, &result_ids, result_ids_len);
                value_index++;
            }

        } else if(f.is_geopoint()) {
            auto num_tree = numerical_index.at(a_filter.field_name);
            auto record_to_geo = sort_index.at(a_filter.field_name);

            double indexed_edge_len = edgeLengthM(f.geo_resolution);

            for(const std::string& filter_value: a_filter.values) {
                std::vector<std::string> filter_value_parts;
                StringUtils::split(filter_value, filter_value_parts, ",");  // x, y, 2 km (or) list of points

                std::vector<uint32_t> geo_result_ids;

                bool is_polygon = StringUtils::is_float(filter_value_parts.back());

                if(is_polygon) {
                    const int num_verts = int(filter_value_parts.size()) / 2;
                    GeoCoord* verts = new GeoCoord[num_verts];

                    for(size_t point_index = 0; point_index < size_t(num_verts); point_index++) {
                        double lat = degsToRads(std::stod(filter_value_parts[point_index * 2]));
                        double lon = degsToRads(std::stod(filter_value_parts[point_index * 2 + 1]));
                        verts[point_index] = {lat, lon};
                    }

                    Geofence geo_fence = {num_verts, verts};
                    GeoPolygon geo_polygon = {geo_fence, 0, nullptr};
                    double lon_offset = transform_for_180th_meridian(geo_fence);

                    size_t num_hexagons = maxPolyfillSize(&geo_polygon, f.geo_resolution);

                    H3Index* hexagons = static_cast<H3Index *>(calloc(num_hexagons, sizeof(H3Index)));

                    polyfill(&geo_polygon, f.geo_resolution, hexagons);

                    // we will have to expand by kring=1 to ensure that hexagons completely cover the polygon
                    // see: https://github.com/uber/h3/issues/332
                    std::set<uint64_t> expanded_hexagons;

                    for (size_t hex_index = 0; hex_index < num_hexagons; hex_index++) {
                        // Some indexes may be 0 to indicate fewer than the maximum number of indexes.
                        if (hexagons[hex_index] != 0) {
                            expanded_hexagons.emplace(hexagons[hex_index]);

                            size_t k_rings = 1;
                            size_t max_neighboring = maxKringSize(k_rings);
                            H3Index* neighboring_indices = static_cast<H3Index *>(calloc(max_neighboring, sizeof(H3Index)));
                            kRing(hexagons[hex_index], k_rings, neighboring_indices);

                            for (size_t neighbour_index = 0; neighbour_index < max_neighboring; neighbour_index++) {
                                if (neighboring_indices[neighbour_index] != 0) {
                                    expanded_hexagons.emplace(neighboring_indices[neighbour_index]);
                                }
                            }

                            free(neighboring_indices);
                        }
                    }

                    for(auto hex_id: expanded_hexagons) {
                         num_tree->get(hex_id, geo_result_ids);
                    }

                    // we will do an exact filtering again with point-in-poly checks
                    std::vector<uint32_t> exact_geo_result_ids;
                    for(auto result_id: geo_result_ids) {
                        GeoCoord point;
                        h3ToGeo(record_to_geo->at(result_id), &point);
                        point.lon = point.lon < 0.0 ? point.lon + lon_offset : point.lon;

                        if(is_point_in_polygon(geo_fence, point)) {
                            exact_geo_result_ids.push_back(result_id);
                        }
                    }
            
                    std::sort(exact_geo_result_ids.begin(), exact_geo_result_ids.end());

                    uint32_t *out = nullptr;
                    result_ids_len = ArrayUtils::or_scalar(&exact_geo_result_ids[0], exact_geo_result_ids.size(),
                                                         result_ids, result_ids_len, &out);

                    delete [] result_ids;
                    result_ids = out;

                    free(hexagons);
                    delete [] verts;
                } else {
                    double radius = std::stof(filter_value_parts[2]);
                    const auto& unit = filter_value_parts[3];

                    if(unit == "km") {
                        radius *= 1000;
                    } else {
                        // assume "mi" (validated upstream)
                        radius *= 1609.34;
                    }

                    GeoCoord location;
                    location.lat = degsToRads(std::stod(filter_value_parts[0]));
                    location.lon = degsToRads(std::stod(filter_value_parts[1]));
                    H3Index query_index = geoToH3(&location, f.geo_resolution);

                    //LOG(INFO) << "query latlon: " << std::stod(filter_value_parts[0]) << ", " << std::stod(filter_value_parts[1]);
                    //LOG(INFO) << "query h3 index: " << query_index << " at res: " << size_t(f.geo_resolution);

                    size_t k_rings = size_t(std::ceil(radius / indexed_edge_len));
                    size_t max_neighboring = maxKringSize(k_rings);
                    H3Index* neighboring_indices = static_cast<H3Index *>(calloc(max_neighboring, sizeof(H3Index)));
                    kRing(query_index, k_rings, neighboring_indices);

                    for (size_t hex_index = 0; hex_index < max_neighboring; hex_index++) {
                        // Some indexes may be 0 to indicate fewer than the maximum number of indexes.
                        if (neighboring_indices[hex_index] != 0) {
                            //LOG(INFO) << "Neighbour index: " << neighboring_indices[hex_index];
                            num_tree->get(neighboring_indices[hex_index], geo_result_ids);
                        }
                    }

                    free(neighboring_indices);

                    // `geo_result_ids` will contain all IDs that are within K-ring hexagons
                    // we still need to do another round of exact filtering on them

                    std::vector<uint32_t> exact_geo_result_ids;

                    H3Index query_point_index = geoToH3(&location, FINEST_GEO_RESOLUTION);
                    for(auto result_id: geo_result_ids) {
                        size_t actual_dist_meters = h3Distance(query_point_index, record_to_geo->at(result_id));
                        if(actual_dist_meters <= radius) {
                            exact_geo_result_ids.push_back(result_id);
                        }
                    }

                    std::sort(exact_geo_result_ids.begin(), exact_geo_result_ids.end());

                    uint32_t *out = nullptr;
                    result_ids_len = ArrayUtils::or_scalar(&exact_geo_result_ids[0], exact_geo_result_ids.size(),
                                                         result_ids, result_ids_len, &out);

                    delete [] result_ids;
                    result_ids = out;
                }
            }

        } else if(f.is_string()) {
            art_tree* t = search_index.at(a_filter.field_name);

            uint32_t* ids = nullptr;
            size_t ids_size = 0;

            for(const std::string & filter_value: a_filter.values) {
                uint32_t* strt_ids = nullptr;
                size_t strt_ids_size = 0;

                std::vector<art_leaf *> query_suggestion;

                // there could be multiple tokens in a filter value, which we have to treat as ANDs
                // e.g. country: South Africa

                Tokenizer tokenizer(filter_value, false, true, false, f.locale);

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

                    query_suggestion.push_back(leaf);
                }

                if(query_suggestion.size() != str_tokens.size()) {
                    continue;
                }

                for(const auto& leaf: query_suggestion) {
                    if(strt_ids == nullptr) {
                        strt_ids = leaf->values->ids.uncompress();
                        strt_ids_size = leaf->values->ids.getLength();
                    } else {
                        // do AND for an exact match
                        uint32_t* out = nullptr;
                        uint32_t* leaf_ids = leaf->values->ids.uncompress();
                        strt_ids_size = ArrayUtils::and_scalar(strt_ids, strt_ids_size, leaf_ids,
                                                               leaf->values->ids.getLength(), &out);
                        delete[] leaf_ids;
                        delete[] strt_ids;
                        strt_ids = out;
                    }
                }

                if(a_filter.comparators[0] == EQUALS && f.is_facet()) {
                    // need to do exact match (unlike CONTAINS) by using the facet index
                    // field being a facet is already enforced upstream
                    uint32_t* exact_strt_ids = new uint32_t[strt_ids_size];
                    size_t exact_strt_size = 0;
                    
                    for(size_t strt_ids_index = 0; strt_ids_index < strt_ids_size; strt_ids_index++) {
                        uint32_t seq_id = strt_ids[strt_ids_index];
                        const auto& fvalues = facet_index_v3.at(f.name)->at(seq_id);
                        bool found_filter = false;

                        if(!f.is_array()) {
                            found_filter = (query_suggestion.size() == fvalues.length);
                        } else {
                            uint64_t filter_hash = 1;

                            for(size_t sindex=0; sindex < str_tokens.size(); sindex++) {
                                auto& this_str_token = str_tokens[sindex];
                                uint64_t thash = facet_token_hash(f, this_str_token);
                                filter_hash *= (1779033703 + 2*thash*(sindex+1));
                            }

                            uint64_t all_fvalue_hash = 1;
                            size_t ftindex = 0;

                            for(size_t findex=0; findex < fvalues.size(); findex++) {
                                auto fhash = fvalues.hashes[findex];
                                if(fhash == FACET_ARRAY_DELIMETER) {
                                    // end of array, check hash
                                    if(all_fvalue_hash == filter_hash) {
                                        found_filter = true;
                                        break;
                                    }
                                    all_fvalue_hash = 1;
                                    ftindex = 0;
                                } else {
                                    all_fvalue_hash *= (1779033703 + 2*fhash*(ftindex + 1));
                                    ftindex++;
                                }
                            }
                        }

                        if(found_filter) {
                            exact_strt_ids[exact_strt_size] = seq_id;
                            exact_strt_size++;
                        }
                    }

                    delete[] strt_ids;
                    strt_ids = exact_strt_ids;
                    strt_ids_size = exact_strt_size;
                }

                // Otherwise, we just ensure that given record contains tokens in the filter query
                // (NOT implemented) if the query is wrapped by double quotes, ensure phrase match
                // bool exact_match = (filter_value.front() == '"' && filter_value.back() == '"');
                uint32_t* out = nullptr;
                ids_size = ArrayUtils::or_scalar(ids, ids_size, strt_ids, strt_ids_size, &out);
                delete[] strt_ids;
                delete[] ids;
                ids = out;
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

    *filter_ids_out = filter_ids;
    return filter_ids_length;
}

void Index::eq_str_filter_plain(const uint32_t *strt_ids, size_t strt_ids_size,
                                const std::vector<art_leaf *>& query_suggestion, uint32_t *exact_strt_ids,
                                size_t& exact_strt_size) const {
    std::vector<uint32_t*> leaf_to_indices;
    for (art_leaf *token_leaf: query_suggestion) {
        if(token_leaf == nullptr) {
            leaf_to_indices.push_back(nullptr);
            continue;
        }

        uint32_t *indices = new uint32_t[strt_ids_size];
        token_leaf->values->ids.indexOf(strt_ids, strt_ids_size, indices);
        leaf_to_indices.push_back(indices);
    }

    // e.g. First In First Out => hash([0, 1, 0, 2])
    spp::sparse_hash_map<size_t, uint32_t> leaf_to_id;
    size_t next_id = 1;
    size_t filter_hash = 1;

    for(size_t leaf_index=0; leaf_index<query_suggestion.size(); leaf_index++) {
        if(leaf_to_id.count(leaf_index) == 0) {
            leaf_to_id.emplace(leaf_index, next_id++);
        }

        uint32_t leaf_id = leaf_to_id[leaf_index];
        filter_hash *= (1779033703 + 2*leaf_id*(leaf_index+1));
    }

    for(size_t strt_ids_index = 0; strt_ids_index < strt_ids_size; strt_ids_index++) {
        std::unordered_map<size_t, std::vector<std::vector<uint16_t>>> array_token_positions;
        populate_token_positions(query_suggestion, leaf_to_indices, strt_ids_index, array_token_positions);
        // iterate array_token_positions and compute hash

        for(const auto& kv: array_token_positions) {
            const std::vector<std::vector<uint16_t>>& token_positions = kv.second;
            size_t this_hash = 1;

            for(size_t token_index = 0; token_index < token_positions.size(); token_index++) {
                auto& positions = token_positions[token_index];
                for(auto pos: positions) {
                    this_hash *= (1779033703 + 2*(token_index+1)*(pos+1));
                }
            }

            if(this_hash == filter_hash) {
                exact_strt_ids[exact_strt_size++] = strt_ids[strt_ids_index];
                break;
            }
        }
    }
}

void Index::run_search(search_args* search_params) {
    search(search_params->field_query_tokens,
           search_params->search_fields,
           search_params->filters, search_params->facets, search_params->facet_query,
           search_params->included_ids, search_params->excluded_ids,
           search_params->sort_fields_std, search_params->num_typos,
           search_params->topster, search_params->curated_topster,
           search_params->per_page, search_params->page, search_params->token_order,
           search_params->prefix, search_params->drop_tokens_threshold,
           search_params->all_result_ids_len, search_params->groups_processed,
           search_params->searched_queries,
           search_params->raw_result_kvs, search_params->override_result_kvs,
           search_params->typo_tokens_threshold,
           search_params->group_limit, search_params->group_by_fields,
           search_params->default_sorting_field);
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
        const size_t token_len = token.length();

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

void Index::search(const std::vector<query_tokens_t>& field_query_tokens,
                   const std::vector<search_field_t>& search_fields,
                   const std::vector<filter>& filters,
                   std::vector<facet>& facets, facet_query_t& facet_query,
                   const std::map<size_t, std::map<size_t, uint32_t>> & included_ids_map,
                   const std::vector<uint32_t> & excluded_ids,
                   const std::vector<sort_by> & sort_fields_std, const int num_typos,
                   Topster* topster,
                   Topster* curated_topster,
                   const size_t per_page, const size_t page, const token_ordering token_order,
                   const bool prefix, const size_t drop_tokens_threshold,
                   size_t & all_result_ids_len,
                   spp::sparse_hash_set<uint64_t>& groups_processed,
                   std::vector<std::vector<art_leaf*>>& searched_queries,
                   std::vector<std::vector<KV*>> & raw_result_kvs,
                   std::vector<std::vector<KV*>> & override_result_kvs,
                   const size_t typo_tokens_threshold,
                   const size_t group_limit,
                   const std::vector<std::string>& group_by_fields,
                   const std::string& default_sorting_field) const {

    std::shared_lock lock(mutex);

    //auto begin = std::chrono::high_resolution_clock::now();

    // process the filters

    uint32_t* filter_ids = nullptr;
    uint32_t filter_ids_length = do_filtering(&filter_ids, filters);

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

    // Order of `fields` are used to sort results
    //auto begin = std::chrono::high_resolution_clock::now();
    uint32_t* all_result_ids = nullptr;

    const size_t num_search_fields = std::min(search_fields.size(), (size_t) FIELD_LIMIT_NUM);
    uint32_t *exclude_token_ids = nullptr;
    size_t exclude_token_ids_size = 0;

    // find documents that contain the excluded tokens to exclude them from results later
    for(size_t i = 0; i < num_search_fields; i++) {
        const std::string & field_name = search_fields[i].name;
        for(const std::string& exclude_token: field_query_tokens[i].q_exclude_tokens) {
            art_leaf* leaf = (art_leaf *) art_search(search_index.at(field_name),
                                                     (const unsigned char *) exclude_token.c_str(),
                                                     exclude_token.size() + 1);

            if(leaf) {
                uint32_t *ids = leaf->values->ids.uncompress();
                uint32_t *exclude_token_ids_merged = nullptr;
                exclude_token_ids_size = ArrayUtils::or_scalar(exclude_token_ids, exclude_token_ids_size, ids,
                                                               leaf->values->ids.getLength(),
                                                               &exclude_token_ids_merged);
                delete[] ids;
                delete[] exclude_token_ids;
                exclude_token_ids = exclude_token_ids_merged;
            }
        }
    }

    std::vector<Topster*> ftopsters;

    if (!field_query_tokens.empty() && !field_query_tokens[0].q_include_tokens.empty() &&
        field_query_tokens[0].q_include_tokens[0] == "*") {
        const uint8_t field_id = (uint8_t)(FIELD_LIMIT_NUM - 0);
        const std::string& field = search_fields[0].name;

        // if a filter is not specified, use the sorting index to generate the list of all document ids
        if(filters.empty()) {
            if(default_sorting_field.empty()) {
                filter_ids_length = seq_ids.getLength();
                filter_ids = seq_ids.uncompress();
            } else {
                const spp::sparse_hash_map<uint32_t, int64_t> *kvs = sort_index.at(default_sorting_field);
                filter_ids_length = kvs->size();
                filter_ids = new uint32_t[filter_ids_length];

                size_t i = 0;
                for(const auto& kv: *kvs) {
                    filter_ids[i++] = kv.first;
                }

                // ids populated from hash map will not be sorted, but sorting is required for intersection & other ops
                std::sort(filter_ids, filter_ids+filter_ids_length);
            }
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

        uint32_t token_bits = 255;
        score_results(sort_fields_std, (uint16_t) searched_queries.size(), field_id, 0, topster, {},
                      groups_processed, filter_ids, filter_ids_length, group_limit, group_by_fields, token_bits);
        collate_included_ids(field_query_tokens[0].q_include_tokens, field, field_id, included_ids_map, curated_topster, searched_queries);

        all_result_ids_len = filter_ids_length;
        all_result_ids = filter_ids;
        filter_ids = nullptr;
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

            // proceed to query search only when no filters are provided or when filtering produces results
            if(filters.empty() || filter_ids_length > 0) {
                const uint8_t field_id = (uint8_t)(FIELD_LIMIT_NUM - i);
                const std::string& field_name = search_fields[i].name;

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

                search_field(field_id, query_tokens, search_tokens, exclude_token_ids, exclude_token_ids_size, num_tokens_dropped,
                             field_name, filter_ids, filter_ids_length, curated_ids_sorted, facets, sort_fields_std,
                             num_typos, searched_queries, actual_topster, groups_processed, &all_result_ids, all_result_ids_len,
                             field_num_results, group_limit, group_by_fields, token_order, prefix,
                             drop_tokens_threshold, typo_tokens_threshold);

                // do synonym based searches
                for(const auto& syn_tokens: q_pos_synonyms) {
                    num_tokens_dropped = 0;
                    field_num_results = 0;
                    query_tokens = search_tokens = syn_tokens;

                    search_field(field_id, query_tokens, search_tokens, exclude_token_ids, exclude_token_ids_size, num_tokens_dropped,
                                 field_name, filter_ids, filter_ids_length, curated_ids_sorted, facets, sort_fields_std,
                                 num_typos, searched_queries, actual_topster, groups_processed, &all_result_ids, all_result_ids_len,
                                 field_num_results, group_limit, group_by_fields, token_order, prefix,
                                 drop_tokens_threshold, typo_tokens_threshold);
                }

                concat_topster_ids(ftopster, topster_ids);
                collate_included_ids(field_query_tokens[i].q_include_tokens, field_name, field_id, included_ids_map, curated_topster, searched_queries);
                //LOG(INFO) << "topster_ids.size: " << topster_ids.size();
            }
        }

        for(auto& seq_id_kvs: topster_ids) {
            const uint64_t seq_id = seq_id_kvs.first;
            auto& kvs = seq_id_kvs.second; // each `kv` can be from a different field

            std::sort(kvs.begin(), kvs.end(), Topster::is_greater);

            // LOG(INFO) << "DOC ID: " << seq_id << ", score: " << kvs[0]->scores[kvs[0]->match_score_index];

            // to calculate existing aggregate scores across best matching fields
            spp::sparse_hash_map<uint8_t, KV*> existing_field_kvs;
            for(const auto kv: kvs) {
                existing_field_kvs.emplace(kv->field_id, kv);
            }

            uint32_t token_bits = (uint32_t(1) << 31);  // top most bit set to guarantee atleast 1 bit set
            uint64_t total_typos = 0, total_distances = 0;
            uint64_t num_exact_matches = 0;

            //LOG(INFO) << "Init pop count: " << __builtin_popcount(token_bits);

            for(size_t i = 0; i < num_search_fields; i++) {
                const uint8_t field_id = (uint8_t)(FIELD_LIMIT_NUM - i);
                size_t weight = search_fields[i].weight;

                //LOG(INFO) << "--- field index: " << i << ", weight: " << weight;

                if(existing_field_kvs.count(field_id) != 0) {
                    // for existing field, we will simply sum field-wise weighted scores
                    token_bits |= existing_field_kvs[field_id]->token_bits;
                    //LOG(INFO) << "existing_field_kvs.count pop count: " << __builtin_popcount(token_bits);

                    int64_t match_score = existing_field_kvs[field_id]->scores[existing_field_kvs[field_id]->match_score_index];
                    total_distances += ((100 - (match_score & 0xFF)) + 1) * weight;

                    uint64_t tokens_found = ((match_score >> 16) & 0xFF);
                    int64_t field_typos = 255 - ((match_score >> 8) & 0xFF);
                    total_typos += (field_typos + 1) * weight;

                    if(field_typos == 0 && tokens_found == field_query_tokens[i].q_include_tokens.size()) {
                        num_exact_matches++;
                    }

                    /*LOG(INFO) << "seq_id: " << seq_id << ", total_typos: " << (255 - ((match_score >> 8) & 0xFF))
                                  << ", weighted typos: " << std::max<uint64_t>((255 - ((match_score >> 8) & 0xFF)), 1) * weight
                                  << ", total dist: " << (((match_score & 0xFF)))
                                  << ", weighted dist: " << std::max<uint64_t>((100 - (match_score & 0xFF)), 1) * weight;*/
                    continue;
                }

                const std::string& field = search_fields[i].name;

                // compute approximate match score for this field from actual query

                size_t words_present = 0;

                for(size_t token_index=0; token_index < field_query_tokens[i].q_include_tokens.size(); token_index++) {
                    const auto& token = field_query_tokens[i].q_include_tokens[token_index];

                    std::vector<art_leaf*> leaves;
                    const bool prefix_search = prefix && (token_index == field_query_tokens[i].q_include_tokens.size()-1);
                    const size_t token_len = prefix_search ? (int) token.length() : (int) token.length() + 1;
                    art_fuzzy_search(search_index.at(field), (const unsigned char *) token.c_str(), token_len,
                                     0, 0, 1, token_order, prefix_search, nullptr, 0, leaves);

                    if(leaves.empty()) {
                        continue;
                    }

                    uint32_t doc_index = leaves[0]->values->ids.indexOf(seq_id);
                    if (doc_index == leaves[0]->values->ids.getLength()) {
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
                    total_distances += ((100 - (match_score & 0xFF)) + 1) * weight;

                    uint64_t tokens_found = ((match_score >> 16) & 0xFF);
                    uint64_t field_typos = 255 - ((match_score >> 8) & 0xFF);
                    total_typos += (field_typos + 1) * weight;

                    if(field_typos == 0 && tokens_found == field_query_tokens[i].q_include_tokens.size()) {
                        num_exact_matches++;
                    }
                    //LOG(INFO) << "seq_id: " << seq_id << ", total_typos: " << ((match_score >> 8) & 0xFF);
                }
            }

            int64_t tokens_present = int64_t(__builtin_popcount(token_bits)) - 1;
            total_typos = std::min<uint64_t>(255, total_typos);
            total_distances = std::min<uint64_t>(100, total_distances);

            uint64_t aggregated_score = (
                (num_exact_matches << 24) |
                (tokens_present << 16) |
                ((255 - total_typos) << 8) |
                (100 - total_distances)
            );

            /*LOG(INFO) << "seq id: " << seq_id << ", tokens_present: " << tokens_present
                      << ", total_distances: " << total_distances << ", total_typos: " << total_typos
                      << ", aggregated_score: " << aggregated_score << ", token_bits: " << token_bits;*/

            kvs[0]->scores[kvs[0]->match_score_index] = aggregated_score;
            topster->add(kvs[0]);
        }
    }

    //LOG(INFO) << "topster size: " << topster->size;

    delete [] exclude_token_ids;

    do_facets(facets, facet_query, group_limit, group_by_fields, all_result_ids, all_result_ids_len);
    do_facets(facets, facet_query, group_limit, group_by_fields, &included_ids[0], included_ids.size());

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
                         const std::string & field,
                         uint32_t *filter_ids, size_t filter_ids_length,
                         const std::vector<uint32_t>& curated_ids,
                         std::vector<facet> & facets, const std::vector<sort_by> & sort_fields, const int num_typos,
                         std::vector<std::vector<art_leaf*>> & searched_queries,
                         Topster* topster, spp::sparse_hash_set<uint64_t>& groups_processed,
                         uint32_t** all_result_ids, size_t & all_result_ids_len, size_t& field_num_results,
                         const size_t group_limit, const std::vector<std::string>& group_by_fields,
                         const token_ordering token_order, const bool prefix, 
                         const size_t drop_tokens_threshold, const size_t typo_tokens_threshold) const {

    size_t max_cost = (num_typos < 0 || num_typos > 2) ? 2 : num_typos;
    if(search_schema.at(field).locale != "" && search_schema.at(field).locale != "en") {
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
        int bounded_cost = get_bounded_typo_cost(max_cost, token.length());

        for(int cost = 0; cost <= bounded_cost; cost++) {
            all_costs.push_back(cost);
        }

        token_to_costs.push_back(all_costs);
    }

    // stores candidates for each token, i.e. i-th index would have all possible tokens with a cost of "c"
    std::vector<token_candidates> token_candidates_vec;

    const long long combination_limit = 10;
    auto product = []( long long a, std::vector<int>& b ) { return a*b.size(); };
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

        token_candidates_vec.clear();
        size_t token_index = 0;

        while(token_index < search_tokens.size()) {
            // For each token, look up the generated cost for this iteration and search using that cost
            const std::string& token = search_tokens[token_index].value;
            const std::string token_cost_hash = token + std::to_string(costs[token_index]);

            std::vector<art_leaf*> leaves;
            //LOG(INFO) << "\nSearching for field: " << field << ", token:" << token << " - cost: " << costs[token_index];

            if(token_cost_cache.count(token_cost_hash) != 0) {
                leaves = token_cost_cache[token_cost_hash];
            } else {
                // prefix should apply only for last token
                const bool prefix_search = prefix && (token_index == search_tokens.size()-1);
                const size_t token_len = prefix_search ? (int) token.length() : (int) token.length() + 1;

                // need less candidates for filtered searches since we already only pick tokens with results
                const int max_candidates = (filter_ids_length == 0) ? 10 : 3;
                art_fuzzy_search(search_index.at(field), (const unsigned char *) token.c_str(), token_len,
                                 costs[token_index], costs[token_index], max_candidates, token_order, prefix_search,
                                 filter_ids, filter_ids_length, leaves);

                if(!leaves.empty()) {
                    token_cost_cache.emplace(token_cost_hash, leaves);
                }
            }

            if(!leaves.empty()) {
                //log_leaves(costs[token_index], token, leaves);
                token_candidates_vec.push_back(token_candidates{search_tokens[token_index], costs[token_index], leaves});
            } else {
                // No result at `cost = costs[token_index]`. Remove `cost` for token and re-do combinations
                auto it = std::find(token_to_costs[token_index].begin(), token_to_costs[token_index].end(), costs[token_index]);
                if(it != token_to_costs[token_index].end()) {
                    token_to_costs[token_index].erase(it);

                    // when no more costs are left for this token
                    if(token_to_costs[token_index].empty()) {
                        // we can try to drop the token and search with remaining tokens
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
            search_candidates(field_id, filter_ids, filter_ids_length, exclude_token_ids, exclude_token_ids_size,
                              curated_ids, sort_fields, token_candidates_vec, searched_queries, topster,
                              groups_processed, all_result_ids, all_result_ids_len, field_num_results,
                              typo_tokens_threshold, group_limit, group_by_fields);
        }

        resume_typo_loop:

        if(field_num_results >= drop_tokens_threshold || field_num_results >= typo_tokens_threshold) {
            // if either threshold is breached, we are done
            return ;
        }

        n++;
    }

    // When atleast one token from the query is available
    if(!query_tokens.empty() && num_tokens_dropped < query_tokens.size()) {
        // Drop tokens from right until (len/2 + 1), and then from left until (len/2 + 1)

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
                            num_tokens_dropped, field, filter_ids, filter_ids_length, curated_ids,facets,
                            sort_fields, num_typos,searched_queries, topster, groups_processed, all_result_ids,
                            all_result_ids_len, field_num_results, group_limit, group_by_fields,
                            token_order, prefix);
    }
}

int Index::get_bounded_typo_cost(const size_t max_cost, const size_t token_len) {
    int bounded_cost = max_cost;
    if(token_len > 0 && max_cost >= token_len && (token_len == 1 || token_len == 2)) {
        bounded_cost = token_len - 1;
    }
    return bounded_cost;
}

void Index::log_leaves(const int cost, const std::string &token, const std::vector<art_leaf *> &leaves) const {
    LOG(INFO) << "Index: " << name << ", token: " << token << ", cost: " << cost;

    for(size_t i=0; i < leaves.size(); i++) {
        std::string key((char*)leaves[i]->key, leaves[i]->key_len);
        LOG(INFO) << key << " - " << leaves[i]->values->ids.getLength();
        LOG(INFO) << "frequency: " << leaves[i]->values->ids.getLength() << ", max_score: " << leaves[i]->max_score;
        /*for(auto j=0; j<leaves[i]->values->ids.getLength(); j++) {
            LOG(INFO) << "id: " << leaves[i]->values->ids.at(j);
        }*/
    }
}

void Index::score_results(const std::vector<sort_by> & sort_fields, const uint16_t & query_index,
                          const uint8_t & field_id, const uint32_t total_cost, Topster* topster,
                          const std::vector<art_leaf *> &query_suggestion,
                          spp::sparse_hash_set<uint64_t>& groups_processed,
                          const uint32_t *result_ids, const size_t result_size,
                          const size_t group_limit, const std::vector<std::string>& group_by_fields,
                          uint32_t token_bits) const {

    std::vector<uint32_t *> leaf_to_indices;
    for (art_leaf *token_leaf: query_suggestion) {
        uint32_t *indices = new uint32_t[result_size];
        token_leaf->values->ids.indexOf(result_ids, result_size, indices);
        leaf_to_indices.push_back(indices);
    }

    Match single_token_match = Match(1, 0);
    const uint64_t single_token_match_score = single_token_match.get_match_score(total_cost);

    int sort_order[3]; // 1 or -1 based on DESC or ASC respectively
    spp::sparse_hash_map<uint32_t, int64_t>* field_values[3];

    spp::sparse_hash_map<uint32_t, int64_t> geopoint_distances[3];

    spp::sparse_hash_map<uint32_t, int64_t> text_match_sentinel_value, seq_id_sentinel_value;
    spp::sparse_hash_map<uint32_t, int64_t> *TEXT_MATCH_SENTINEL = &text_match_sentinel_value;
    spp::sparse_hash_map<uint32_t, int64_t> *SEQ_ID_SENTINEL = &seq_id_sentinel_value;

    for (size_t i = 0; i < sort_fields.size(); i++) {
        sort_order[i] = 1;
        if (sort_fields[i].order == sort_field_const::asc) {
            sort_order[i] = -1;
        }

        if (sort_fields[i].name == sort_field_const::text_match) {
            field_values[i] = TEXT_MATCH_SENTINEL;
        } else if (sort_fields[i].name == sort_field_const::seq_id) {
            field_values[i] = SEQ_ID_SENTINEL;
        } else if (sort_schema.at(sort_fields[i].name).is_geopoint()) {
            // we have to populate distances that will be used for match scoring
            spp::sparse_hash_map<uint32_t, int64_t> *geopoints = sort_index.at(sort_fields[i].name);

            for (size_t rindex = 0; rindex < result_size; rindex++) {
                const uint32_t seq_id = result_ids[rindex];
                auto it = geopoints->find(seq_id);
                int64_t dist = (it == geopoints->end()) ? INT32_MAX : h3Distance(sort_fields[i].geopoint, it->second);
                geopoint_distances[i].emplace(seq_id, dist);
            }

            field_values[i] = &geopoint_distances[i];
        } else {
            field_values[i] = sort_index.at(sort_fields[i].name);
        }
    }

    //auto begin = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < result_size; i++) {
        const uint32_t seq_id = result_ids[i];

        uint64_t match_score = 0;

        if (query_suggestion.size() <= 1) {
            match_score = single_token_match_score;
        } else {
            std::unordered_map<size_t, std::vector<std::vector<uint16_t>>> array_token_positions;
            populate_token_positions(query_suggestion, leaf_to_indices, i, array_token_positions);

            for (const auto& kv: array_token_positions) {
                const std::vector<std::vector<uint16_t>> &token_positions = kv.second;
                if (token_positions.empty()) {
                    continue;
                }
                const Match &match = Match(seq_id, token_positions, false);
                uint64_t this_match_score = match.get_match_score(total_cost);

                match_score += this_match_score;

                /*std::ostringstream os;
                os << name << ", total_cost: " << (255 - total_cost)
                   << ", words_present: " << match.words_present
                   << ", match_score: " << match_score
                   << ", match.distance: " << match.distance
                   << ", seq_id: " << seq_id << std::endl;
                LOG(INFO) << os.str();*/
            }
        }

        const int64_t default_score = INT64_MIN;  // to handle field that doesn't exist in document (e.g. optional)
        int64_t scores[3] = {0};
        size_t match_score_index = 0;

        // avoiding loop
        if (sort_fields.size() > 0) {
            if (field_values[0] == TEXT_MATCH_SENTINEL) {
                scores[0] = int64_t(match_score);
                match_score_index = 0;
            } else if (field_values[0] == SEQ_ID_SENTINEL) {
                scores[0] = seq_id;
            } else {
                auto it = field_values[0]->find(seq_id);
                scores[0] = (it == field_values[0]->end()) ? default_score : it->second;
            }
            if (sort_order[0] == -1) {
                scores[0] = -scores[0];
            }
        }


        if(sort_fields.size() > 1) {
            if (field_values[1] == TEXT_MATCH_SENTINEL) {
                scores[1] = int64_t(match_score);
                match_score_index = 1;
            } else if (field_values[1] == SEQ_ID_SENTINEL) {
                scores[1] = seq_id;
            } else {
                auto it = field_values[1]->find(seq_id);
                scores[1] = (it == field_values[1]->end()) ? default_score : it->second;
            }

            if (sort_order[1] == -1) {
                scores[1] = -scores[1];
            }
        }

        if(sort_fields.size() > 2) {
            if(field_values[2] != TEXT_MATCH_SENTINEL) {
                scores[2] = int64_t(match_score);
                match_score_index = 2;
            } else if (field_values[2] == SEQ_ID_SENTINEL) {
                scores[2] = seq_id;
            } else {
                auto it = field_values[2]->find(seq_id);
                scores[2] = (it == field_values[2]->end()) ? default_score : it->second;
            }

            if(sort_order[2] == -1) {
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
    }

    //long long int timeNanos = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    //LOG(INFO) << "Time taken for results iteration: " << timeNanos << "ms";

    for(uint32_t* leaf_indices: leaf_to_indices) {
        delete [] leaf_indices;
    }
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

void Index::populate_token_positions(const std::vector<art_leaf *>& query_suggestion,
                                     const std::vector<uint32_t*>& leaf_to_indices,
                                     const size_t result_index,
                                     std::unordered_map<size_t, std::vector<std::vector<uint16_t>>>& array_token_positions) {
    if(query_suggestion.empty()) {
        return ;
    }

    // array_token_positions:
    // for every element in a potential array, for every token in query suggestion, get the positions

    for(size_t i = 0; i < query_suggestion.size(); i++) {
        const art_leaf* token_leaf = query_suggestion[i];
        uint32_t doc_index = leaf_to_indices[i][result_index];
        /*LOG(INFO) << "doc_id: " << token_leaf->values->ids.at(doc_index) << ", token_leaf->values->ids.getLength(): "
                  << token_leaf->values->ids.getLength();*/

        // it's possible for a query token to not appear in a resulting document
        if(doc_index == token_leaf->values->ids.getLength()) {
            continue;
        }

        // Array offset storage format:
        // a) last element is array_index b) second and third last elements will be largest offset
        // (last element is repeated to indicate end of offsets for a given array index)

        /*uint32_t* offsets = token_leaf->values->offsets.uncompress();
        for(size_t ii=0; ii < token_leaf->values->offsets.getLength(); ii++) {
            LOG(INFO) << "offset: " << offsets[ii];
        }

        uint32_t* offset_indices = token_leaf->values->offset_index.uncompress();
        for(size_t ii=0; ii < token_leaf->values->offset_index.getLength(); ii++) {
            LOG(INFO) << "offset index: " << offset_indices[ii];
        }

        LOG(INFO) << "token_leaf->values->offsets.getLength(): " << token_leaf->values->offsets.getLength();*/

        uint32_t start_offset = token_leaf->values->offset_index.at(doc_index);
        uint32_t end_offset = (doc_index == token_leaf->values->ids.getLength() - 1) ?
                              token_leaf->values->offsets.getLength() :
                              token_leaf->values->offset_index.at(doc_index+1);

        std::vector<uint16_t> positions;
        int prev_pos = -1;

        while(start_offset < end_offset) {
            int pos = token_leaf->values->offsets.at(start_offset);
            start_offset++;

            if(pos == prev_pos) {  // indicates end of array index
                if(!positions.empty()) {
                    size_t array_index = (size_t) token_leaf->values->offsets.at(start_offset);
                    array_token_positions[array_index].push_back(positions);
                    positions.clear();
                }

                start_offset++;  // skip current value which is the array index
                prev_pos = -1;
                continue;
            }

            prev_pos = pos;
            positions.push_back((uint16_t)pos);
        }

        if(!positions.empty()) {
            // for plain string fields
            array_token_positions[0].push_back(positions);
        }
    }
}

inline uint32_t Index::next_suggestion(const std::vector<token_candidates> &token_candidates_vec,
                                   long long int n,
                                   std::vector<art_leaf *>& actual_query_suggestion,
                                   std::vector<art_leaf *>& query_suggestion,
                                   uint32_t& token_bits) {
    uint32_t total_cost = 0;

    // generate the next combination from `token_leaves` and store it in `query_suggestion`
    ldiv_t q { n, 0 };
    for(long long i = 0 ; i < (long long) token_candidates_vec.size(); i++) {
        size_t token_size = token_candidates_vec[i].token.value.size();
        q = ldiv(q.quot, token_candidates_vec[i].candidates.size());
        actual_query_suggestion[i] = token_candidates_vec[i].candidates[q.rem];
        query_suggestion[i] = token_candidates_vec[i].candidates[q.rem];
        total_cost += token_candidates_vec[i].cost;

        token_bits |= 1UL << token_candidates_vec[i].token.position; // sets n-th bit

        if(actual_query_suggestion[i]->key_len != token_size+1) {
            total_cost++;
        }
    }

    // Sort ascending based on matched documents for each token for faster intersection.
    // However, this causes the token order to deviate from original query's order.
    sort(query_suggestion.begin(), query_suggestion.end(), [](const art_leaf* left, const art_leaf* right) {
        return left->values->ids.getLength() < right->values->ids.getLength();
    });

    return total_cost;
}

void Index::remove_and_shift_offset_index(sorted_array& offset_index, const uint32_t* indices_sorted,
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

Option<uint32_t> Index::remove(const uint32_t seq_id, const nlohmann::json & document) {
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

            for(auto & token: tokens) {
                const unsigned char *key = (const unsigned char *) token.c_str();
                int key_len = (int) (token.length() + 1);

                art_leaf* leaf = (art_leaf *) art_search(search_index.at(field_name), key, key_len);
                if(leaf != nullptr) {
                    uint32_t doc_index = leaf->values->ids.indexOf(seq_id);

                    if (doc_index == leaf->values->ids.getLength()) {
                        // not found - happens when 2 tokens repeat in a field, e.g "is it or is is not?"
                        continue;
                    }

                    uint32_t start_offset = leaf->values->offset_index.at(doc_index);
                    uint32_t end_offset = (doc_index == leaf->values->ids.getLength() - 1) ?
                                          leaf->values->offsets.getLength() :
                                          leaf->values->offset_index.at(doc_index + 1);

                    uint32_t doc_indices[1] = {doc_index};
                    remove_and_shift_offset_index(leaf->values->offset_index, doc_indices, 1);

                    leaf->values->offsets.remove_index(start_offset, end_offset);
                    leaf->values->ids.remove_value(seq_id);

                    /*len = leaf->values->offset_index.getLength();
                    for(auto i=0; i<len; i++) {
                        LOG(INFO) << "i: " << i << ", val: " << leaf->values->offset_index.at(i);
                    }
                    LOG(INFO) << "----";*/

                    if (leaf->values->ids.getLength() == 0) {
                        art_values *values = (art_values *) art_delete(search_index.at(field_name), key, key_len);
                        delete values;
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

    if(seq_ids.contains(seq_id)) {
        seq_ids.remove_value(seq_id);
    }

    return Option<uint32_t>(seq_id);
}

void Index::tokenize_string_field(const nlohmann::json& document, const field& search_field,
                                  std::vector<std::string>& tokens, const std::string& locale) {

    const std::string& field_name = search_field.name;

    if(search_field.type == field_types::STRING) {
        Tokenizer(document[field_name], false, true, false, locale).tokenize(tokens);
    } else if(search_field.type == field_types::STRING_ARRAY) {
        const std::vector<std::string>& values = document[field_name].get<std::vector<std::string>>();
        for(const std::string & value: values) {
            Tokenizer(value, false, true, false, locale).tokenize(tokens);
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
            } else {
                num_tree_t* num_tree = new num_tree_t;
                numerical_index.emplace(new_field.name, num_tree);
            }
        }

        if(new_field.is_facet()) {
            facet_schema.emplace(new_field.name, new_field);

            spp::sparse_hash_map<uint32_t, facet_hash_values_t> *doc_to_values = new spp::sparse_hash_map<uint32_t, facet_hash_values_t>();
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
        item = std::to_string((float)item);
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

void Index::get_doc_changes(const nlohmann::json &document, nlohmann::json &old_doc, nlohmann::json &new_doc,
                            nlohmann::json &del_doc) {
    for(auto it = old_doc.begin(); it != old_doc.end(); ++it) {
        new_doc[it.key()] = it.value();
    }

    for(auto it = document.begin(); it != document.end(); ++it) {
        // adds new key or overrides existing key from `old_doc`
        new_doc[it.key()] = it.value();

        // if the update document contains a field that exists in old, we record that (for delete + reindex)
        bool field_exists_in_old_doc = (old_doc.count(it.key()) != 0);
        if(field_exists_in_old_doc) {
            // key exists in the stored doc, so it must be reindexed
            // we need to check for this because a field can be optional
            del_doc[it.key()] = old_doc[it.key()];
        }
    }
}

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
