#include "index.h"

#include <numeric>
#include <chrono>
#include <set>
#include <unordered_map>
#include <array_utils.h>
#include <match_score.h>
#include <string_utils.h>
#include <art.h>
#include "logger.h"

Index::Index(const std::string name, const std::unordered_map<std::string, field> & search_schema,
             std::map<std::string, field> facet_schema, std::unordered_map<std::string, field> sort_schema):
        name(name), search_schema(search_schema), facet_schema(facet_schema), sort_schema(sort_schema) {

    for(const auto & pair: search_schema) {
        art_tree *t = new art_tree;
        art_tree_init(t);
        search_index.emplace(pair.first, t);

        // initialize for non-string facet fields
        if(pair.second.facet && !pair.second.is_string()) {
            art_tree *ft = new art_tree;
            art_tree_init(ft);
            search_index.emplace(pair.second.faceted_name(), ft);
        }
    }

    for(const auto & pair: sort_schema) {
        spp::sparse_hash_map<uint32_t, int64_t> * doc_to_score = new spp::sparse_hash_map<uint32_t, int64_t>();
        sort_index.emplace(pair.first, doc_to_score);
    }

    num_documents = 0;

    ready = false;
    processed = false;
    terminate = false;
}

Index::~Index() {
    for(auto & name_tree: search_index) {
        art_tree_destroy(name_tree.second);
        delete name_tree.second;
        name_tree.second = nullptr;
    }

    search_index.clear();

    for(auto & name_map: sort_index) {
        delete name_map.second;
        name_map.second = nullptr;
    }

    sort_index.clear();
}

int64_t Index::get_points_from_doc(const nlohmann::json &document, const std::string & default_sorting_field) {
    int64_t points = 0;

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
                                        const std::string & default_sorting_field, bool is_update) {

    int64_t points = 0;

    if(is_update && document.count(default_sorting_field) == 0) {
        points = sort_index[default_sorting_field]->at(seq_id);
    } else {
        points = get_points_from_doc(document, default_sorting_field);
    }

    std::unordered_map<std::string, size_t> facet_to_id;
    size_t i_facet = 0;
    for(const auto & facet: facet_schema) {
        facet_to_id[facet.first] = i_facet;
        i_facet++;
    }

    // initialize facet index since it will be updated as well during search indexing
    // even if a field is optional, a facet position will be available in the vector for that field
    std::vector<std::vector<uint64_t>> values(facet_schema.size());
    facet_index_v2.emplace(seq_id, values);

    // assumes that validation has already been done
    for(const std::pair<std::string, field> & field_pair: search_schema) {
        const std::string & field_name = field_pair.first;

        if((field_pair.second.optional || is_update) && document.count(field_name) == 0) {
            continue;
        }

        int facet_id = -1;
        if(facet_schema.count(field_name) != 0) {
            facet_id = facet_to_id[field_name];
        }

        // non-string faceted field should be indexed as faceted string field as well
        if(field_pair.second.facet && !field_pair.second.is_string()) {
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
                index_string_array_field(strings, points, t, seq_id, facet_id, field_pair.second);
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

                index_string_field(text, points, t, seq_id, facet_id, field_pair.second);
            }
        }

        art_tree *t = search_index.at(field_name);

        if(field_pair.second.type == field_types::STRING) {
            const std::string & text = document[field_name];
            index_string_field(text, points, t, seq_id, facet_id, field_pair.second);
        } else if(field_pair.second.type == field_types::INT32) {
            uint32_t value = document[field_name];
            index_int32_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::INT64) {
            uint64_t value = document[field_name];
            index_int64_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::FLOAT) {
            float value = document[field_name];
            index_float_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::BOOL) {
            bool value = document[field_name];
            index_bool_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::STRING_ARRAY) {
            std::vector<std::string> strings = document[field_name];
            index_string_array_field(strings, points, t, seq_id, facet_id, field_pair.second);
        } else if(field_pair.second.type == field_types::INT32_ARRAY) {
            std::vector<int32_t> values = document[field_name];
            index_int32_array_field(values, points, t, seq_id);
        } else if(field_pair.second.type == field_types::INT64_ARRAY) {
            std::vector<int64_t> values = document[field_name];
            index_int64_array_field(values, points, t, seq_id);
        } else if(field_pair.second.type == field_types::FLOAT_ARRAY) {
            std::vector<float> values = document[field_name];
            index_float_array_field(values, points, t, seq_id);
        } else if(field_pair.second.type == field_types::BOOL_ARRAY) {
            std::vector<bool> values = document[field_name];
            index_bool_array_field(values, points, t, seq_id);
        }

        // add numerical values automatically into sort index
        if(field_pair.second.type == field_types::INT32 || field_pair.second.type == field_types::INT64 ||
                field_pair.second.type == field_types::FLOAT || field_pair.second.type == field_types::BOOL) {
            spp::sparse_hash_map<uint32_t, int64_t> *doc_to_score = sort_index.at(field_pair.first);

            if(field_pair.second.is_integer() ) {
                doc_to_score->emplace(seq_id, document[field_pair.first].get<int64_t>());
            } else if(field_pair.second.is_float()) {
                int64_t ifloat = float_to_in64_t(document[field_pair.first].get<float>());
                doc_to_score->emplace(seq_id, ifloat);
            } else if(field_pair.second.is_bool()) {
                doc_to_score->emplace(seq_id, (int64_t) document[field_pair.first].get<bool>());
            }
        }
    }

    num_documents += 1;
    return Option<>(201);
}

Option<uint32_t> Index::validate_index_in_memory(const nlohmann::json &document, uint32_t seq_id,
                                                 const std::string & default_sorting_field,
                                                 const std::unordered_map<std::string, field> & search_schema,
                                                 const std::map<std::string, field> & facet_schema,
                                                 bool is_update) {

    bool has_default_sort_field = (document.count(default_sorting_field) != 0);

    if(!has_default_sort_field && !is_update) {
        return Option<>(400, "Field `" + default_sorting_field  + "` has been declared as a default sorting field, "
                "but is not found in the document.");
    }

    if(has_default_sort_field &&
        !document[default_sorting_field].is_number_integer() && !document[default_sorting_field].is_number_float()) {
        return Option<>(400, "Default sorting field `" + default_sorting_field  + "` must be a single valued numerical field.");
    }

    if(has_default_sort_field && search_schema.at(default_sorting_field).is_single_float() &&
       document[default_sorting_field].get<float>() > std::numeric_limits<float>::max()) {
        return Option<>(400, "Default sorting field `" + default_sorting_field  + "` exceeds maximum value of a float.");
    }

    for(const std::pair<std::string, field> & field_pair: search_schema) {
        const std::string & field_name = field_pair.first;

        if((field_pair.second.optional || is_update) && document.count(field_name) == 0) {
            continue;
        }

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

            if(document[field_name].size() > 0 && !document[field_name][0].is_number()) {
                // allows integer to be passed to a float array field
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

    return Option<>(200);
}

void Index::scrub_reindex_doc(nlohmann::json& update_doc, nlohmann::json& del_doc, nlohmann::json& old_doc) {
    auto it = del_doc.cbegin();
    while(it != del_doc.cend()) {
        const std::string& field_name = it.key();
        const auto& search_field_it = search_schema.find(field_name);
        if(search_field_it == search_schema.end()) {
            ++it;
            continue;
        }

        const auto& search_field = search_field_it->second;

        // Go through all the field names and find the keys+values so that they can be removed from in-memory index
        std::vector<std::string> reindex_tokens;
        std::vector<std::string> old_tokens;
        tokenize_doc_field(update_doc, field_name, search_field, reindex_tokens);
        tokenize_doc_field(old_doc, field_name, search_field, old_tokens);

        if(old_tokens.size() != reindex_tokens.size()) {
            ++it;
            continue;
        }

        bool exact_match = true;

        for(size_t i=0; i<reindex_tokens.size(); i++) {
            const std::string& reindex_val = reindex_tokens[i];
            const std::string& old_val = old_tokens[i];
            if(reindex_val != old_val) {
                exact_match = false;
                break;
            }
        }

        if(exact_match) {
            it = del_doc.erase(it);
        } else {
            ++it;
        }
    }
}

size_t Index::batch_memory_index(Index *index, std::vector<index_record> & iter_batch,
                                 const std::string & default_sorting_field,
                                 const std::unordered_map<std::string, field> & search_schema,
                                 const std::map<std::string, field> & facet_schema) {

    size_t num_indexed = 0;

    for(auto & index_rec: iter_batch) {
        if(!index_rec.indexed.ok()) {
            // some records could have been invalidated upstream
            continue;
        }

        bool is_update = (index_rec.operation == UPDATE);

        if(index_rec.operation == CREATE || index_rec.operation == UPDATE) {
            Option<uint32_t> validation_op = validate_index_in_memory(index_rec.doc, index_rec.seq_id,
                                                                      default_sorting_field,
                                                                      search_schema, facet_schema, is_update);

            if(!validation_op.ok()) {
                index_rec.index_failure(validation_op.code(), validation_op.error());
                continue;
            }

            if(is_update) {
                // scrub string fields to reduce delete ops
                index->scrub_reindex_doc(index_rec.doc, index_rec.del_doc, index_rec.old_doc);
                index->remove(index_rec.seq_id, index_rec.del_doc);
            }

            Option<uint32_t> index_mem_op = index->index_in_memory(index_rec.doc, index_rec.seq_id,
                                                                   default_sorting_field, is_update);
            if(!index_mem_op.ok()) {
                index->index_in_memory(index_rec.del_doc, index_rec.seq_id, default_sorting_field, true);
                index_rec.index_failure(index_mem_op.code(), index_mem_op.error());
                continue;
            }

            index_rec.index_success();

            if(!is_update) {
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

void Index::index_int32_field(const int32_t value, int64_t score, art_tree *t, uint32_t seq_id) const {
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

void Index::index_int64_field(const int64_t value, int64_t score, art_tree *t, uint32_t seq_id) const {
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

void Index::index_bool_field(const bool value, const int64_t score, art_tree *t, uint32_t seq_id) const {
    const int KEY_LEN = 1;
    unsigned char key[KEY_LEN];
    key[0] = value ? '1' : '0';

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

void Index::index_float_field(const float value, int64_t score, art_tree *t, uint32_t seq_id) const {
    const int KEY_LEN = 8;
    unsigned char key[KEY_LEN];

    encode_float(value, key);

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
                                    uint32_t seq_id, int facet_id, const field & a_field) {
    std::vector<std::string> tokens;
    StringUtils::split(text, tokens, " ");

    std::unordered_map<std::string, std::vector<uint32_t>> token_to_offsets;

    for(uint32_t i=0; i<tokens.size(); i++) {
        auto & token = tokens[i];

        if(a_field.is_string()) {
            string_utils.unicode_normalize(token);
        }

        if(facet_id >= 0) {
            uint64_t hash = facet_token_hash(a_field, token);
            facet_index_v2[seq_id][facet_id].push_back(hash);
        }

        token_to_offsets[token].push_back(i);
    }

    /*if(seq_id == 0) {
        LOG(INFO) << "field name: " << a_field.name;
    }*/

    insert_doc(score, t, seq_id, token_to_offsets);

    if(facet_id >= 0) {
        facet_index_v2[seq_id][facet_id].shrink_to_fit();
    }
}

void Index::index_string_array_field(const std::vector<std::string> & strings, const int64_t score, art_tree *t,
                                          uint32_t seq_id, int facet_id, const field & a_field) {
    std::unordered_map<std::string, std::vector<uint32_t>> token_positions;

    for(size_t array_index = 0; array_index < strings.size(); array_index++) {
        const std::string & str = strings[array_index];
        std::vector<std::string> tokens;
        std::string delim = " ";
        StringUtils::split(str, tokens, delim);

        std::set<std::string> token_set;  // required to deal with repeating tokens

        // iterate and append offset positions
        for(size_t i=0; i<tokens.size(); i++) {
            auto & token = tokens[i];

            if(a_field.is_string()) {
                string_utils.unicode_normalize(token);
            }

            if(facet_id >= 0) {
                uint64_t hash = facet_token_hash(a_field, token);
                facet_index_v2[seq_id][facet_id].push_back(hash);
                //printf("indexing %.*s - %llu\n", token.size(), token.c_str(), hash);
            }

            token_positions[token].push_back(i);
            token_set.insert(token);
        }

        if(facet_id >= 0) {
            facet_index_v2[seq_id][facet_id].push_back(FACET_ARRAY_DELIMETER); // as a delimiter
        }

        // repeat last element to indicate end of offsets for this array index
        for(auto & token: token_set) {
            token_positions[token].push_back(token_positions[token].back());
        }

        // iterate and append this array index to all tokens
        for(auto & token: token_set) {
            token_positions[token].push_back(array_index);
        }
    }

    if(facet_id >= 0) {
        facet_index_v2[seq_id][facet_id].shrink_to_fit();
    }

    insert_doc(score, t, seq_id, token_positions);
}

void Index::index_int32_array_field(const std::vector<int32_t> & values, const int64_t score, art_tree *t,
                                         uint32_t seq_id) const {
    for(const int32_t value: values) {
        index_int32_field(value, score, t, seq_id);
    }
}

void Index::index_int64_array_field(const std::vector<int64_t> & values, const int64_t score, art_tree *t,
                                         uint32_t seq_id) const {
    for(const int64_t value: values) {
        index_int64_field(value, score, t, seq_id);
    }
}

void Index::index_bool_array_field(const std::vector<bool> & values, const int64_t score, art_tree *t,
                                   uint32_t seq_id) const {
    for(const bool value: values) {
        index_bool_field(value, score, t, seq_id);
    }
}

void Index::index_float_array_field(const std::vector<float> & values, const int64_t score, art_tree *t,
                             uint32_t seq_id) const {
    for(const float value: values) {
        index_float_field(value, score, t, seq_id);
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
                      const uint32_t* result_ids, size_t results_size) {

    std::unordered_map<std::string, size_t> facet_to_index;
    get_facet_to_index(facet_to_index);

    // assumed that facet fields have already been validated upstream
    for(auto & a_facet: facets) {
        spp::sparse_hash_map<uint64_t, token_pos_cost_t> fhash_qtoken_pos;  // facet hash => token position in the query
        bool use_facet_query = false;
        const field & facet_field = facet_schema.at(a_facet.field_name);

        if(a_facet.field_name == facet_query.field_name && !facet_query.query.empty()) {
            use_facet_query = true;

            if(facet_field.is_bool()) {
                if(facet_query.query == "true") {
                    facet_query.query = "1";
                } else if(facet_query.query == "false") {
                    facet_query.query = "0";
                }
            }

            std::vector<std::string> query_tokens;
            StringUtils::split(facet_query.query, query_tokens, " ");

            // for non-string fields, `faceted_name` returns their aliased stringified field name
            art_tree *t = search_index.at(facet_field.faceted_name());

            for(size_t qtoken_index = 0; qtoken_index < query_tokens.size(); qtoken_index++) {
                auto & q = query_tokens[qtoken_index];
                if(facet_field.is_string()) {
                    string_utils.unicode_normalize(q);
                }

                int bounded_cost = (q.size() < 3) ? 0 : 1;
                bool prefix_search = (qtoken_index == (query_tokens.size()-1)); // only last token must be used as prefix

                std::vector<art_leaf*> leaves;

                art_fuzzy_search(t, (const unsigned char *) q.c_str(),
                                 q.size(), 0, bounded_cost, 10000,
                                 token_ordering::MAX_SCORE, prefix_search, leaves);

                for(size_t i = 0; i < leaves.size(); i++) {
                    const auto & leaf = leaves[i];
                    // calculate hash without terminating null char
                    std::string key_str((const char*)leaf->key, leaf->key_len-1);
                    uint64_t hash = facet_token_hash(facet_field, key_str);

                    token_pos_cost_t token_pos_cost = {qtoken_index, 0};
                    fhash_qtoken_pos.emplace(hash, token_pos_cost);
                    //printf("%.*s - %llu\n", leaf->key_len, leaf->key, hash);
                }
            }
        }

        size_t facet_id = facet_to_index[a_facet.field_name];

        bool should_compute_stats = (facet_field.type != field_types::STRING &&
                                     facet_field.type != field_types::BOOL &&
                                     facet_field.type != field_types::STRING_ARRAY &&
                                     facet_field.type != field_types::BOOL_ARRAY);

        for(size_t i = 0; i < results_size; i++) {
            uint32_t doc_seq_id = result_ids[i];

            if(facet_index_v2.count(doc_seq_id) != 0) {
                // FORMAT OF VALUES
                // String: h1 h2 h3
                // String array: h1 h2 h3 0 h1 0 h1 h2 0
                const std::vector<uint64_t> & fhashes = facet_index_v2[doc_seq_id][facet_id];

                int array_pos = 0;
                bool fvalue_found = false;
                uint64_t combined_hash = 1;  // for hashing the entire facet value (multiple tokens)

                spp::sparse_hash_map<uint32_t, token_pos_cost_t> query_token_positions;
                size_t field_token_index = -1;

                for(size_t j = 0; j < fhashes.size(); j++) {
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

                        // not using facet query or this particular facet value is found in facet filter
                        if(!use_facet_query || fhash_qtoken_pos.find(ftoken_hash) != fhash_qtoken_pos.end()) {
                            fvalue_found = true;

                            if(use_facet_query) {
                                // map token index to query index (used for highlighting later on)
                                token_pos_cost_t qtoken_pos = fhash_qtoken_pos[ftoken_hash];

                                // if the query token has already matched another token in the string
                                // we will replace the position only if the cost is lower
                                if(query_token_positions.find(qtoken_pos.pos) == query_token_positions.end() ||
                                   query_token_positions[qtoken_pos.pos].cost >= qtoken_pos.cost ) {
                                    token_pos_cost_t ftoken_pos_cost = {field_token_index, qtoken_pos.cost};
                                    query_token_positions.emplace(qtoken_pos.pos, ftoken_pos_cost);
                                }
                            }
                        }
                    }

                    // 0 indicates separator, while the second condition checks for non-array string
                    if(fhashes[j] == FACET_ARRAY_DELIMETER || (fhashes.back() != FACET_ARRAY_DELIMETER && j == fhashes.size() - 1)) {
                        if(!use_facet_query || fvalue_found) {
                            uint64_t fhash = combined_hash;

                            if(a_facet.result_map.count(fhash) == 0) {
                                a_facet.result_map[fhash] = facet_count_t{0, spp::sparse_hash_set<uint64_t>(),
                                                                          doc_seq_id, 0,
                                                                          spp::sparse_hash_map<uint32_t, token_pos_cost_t>()};
                            }

                            a_facet.result_map[fhash].doc_id = doc_seq_id;
                            a_facet.result_map[fhash].array_pos = array_pos;

                            if(search_params->group_limit) {
                                uint64_t distinct_id = get_distinct_id(facet_to_index, doc_seq_id);
                                a_facet.result_map[fhash].groups.emplace(distinct_id);
                            } else {
                                a_facet.result_map[fhash].count += 1;
                            }

                            if(use_facet_query) {
                                a_facet.result_map[fhash].query_token_pos = query_token_positions;
                            }
                        }

                        array_pos++;
                        fvalue_found = false;
                        combined_hash = 1;
                        spp::sparse_hash_map<uint32_t, token_pos_cost_t>().swap(query_token_positions);
                        field_token_index = -1;
                    }
                }
            }
        }
    }
}

void Index::get_facet_to_index(std::unordered_map<std::string, size_t>& facet_to_index) {
    size_t i_facet = 0;
    for(const auto & facet_kv: facet_schema) {
        facet_to_index.emplace(facet_kv.first, i_facet);
        i_facet++;
    }
}

void Index::search_candidates(const uint8_t & field_id, uint32_t* filter_ids, size_t filter_ids_length,
                              const std::vector<uint32_t>& curated_ids,
                              const std::vector<sort_by> & sort_fields,
                              std::vector<token_candidates> & token_candidates_vec,
                              std::vector<std::vector<art_leaf*>> & searched_queries, Topster* topster,
                              spp::sparse_hash_set<uint64_t>& groups_processed,
                              uint32_t** all_result_ids, size_t & all_result_ids_len,
                              const size_t typo_tokens_threshold) {
    const long long combination_limit = 10;

    auto product = []( long long a, token_candidates & b ) { return a*b.candidates.size(); };
    long long int N = std::accumulate(token_candidates_vec.begin(), token_candidates_vec.end(), 1LL, product);

    for(long long n=0; n<N && n<combination_limit; ++n) {
        // every element in `query_suggestion` contains a token and its associated hits
        std::vector<art_leaf *> query_suggestion = next_suggestion(token_candidates_vec, n);

        /*LOG(INFO) << "n: " << n;
        for(size_t i=0; i < query_suggestion.size(); i++) {
            LOG(INFO) << "i: " << i << " - " << query_suggestion[i]->key << ", ids: "
                      << query_suggestion[i]->values->ids.getLength();
        }*/

        // initialize results with the starting element (for further intersection)
        size_t result_size = query_suggestion[0]->values->ids.getLength();
        if(result_size == 0) {
            continue;
        }

        uint32_t total_cost = 0;
        uint32_t* result_ids = query_suggestion[0]->values->ids.uncompress();

        for(const auto& tc: token_candidates_vec) {
            total_cost += tc.cost;
        }

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

        if(!curated_ids.empty()) {
            uint32_t *excluded_result_ids = nullptr;
            result_size = ArrayUtils::exclude_scalar(result_ids, result_size, &curated_ids[0],
                                                     curated_ids.size(), &excluded_result_ids);

            delete [] result_ids;
            result_ids = excluded_result_ids;
        }

        if(filter_ids != nullptr) {
            // intersect once again with filter ids
            uint32_t* filtered_result_ids = nullptr;
            size_t filtered_results_size = ArrayUtils::and_scalar(filter_ids, filter_ids_length, result_ids,
                                                                  result_size, &filtered_result_ids);

            uint32_t* new_all_result_ids;
            all_result_ids_len = ArrayUtils::or_scalar(*all_result_ids, all_result_ids_len, filtered_result_ids,
                                  filtered_results_size, &new_all_result_ids);
            delete [] *all_result_ids;
            *all_result_ids = new_all_result_ids;

            // go through each matching document id and calculate match score
            score_results(sort_fields, (uint16_t) searched_queries.size(), field_id, total_cost, topster, query_suggestion,
                          groups_processed, filtered_result_ids, filtered_results_size);

            delete[] filtered_result_ids;
            delete[] result_ids;
        } else {
            uint32_t* new_all_result_ids;
            all_result_ids_len = ArrayUtils::or_scalar(*all_result_ids, all_result_ids_len, result_ids,
                                  result_size, &new_all_result_ids);
            delete [] *all_result_ids;
            *all_result_ids = new_all_result_ids;

            score_results(sort_fields, (uint16_t) searched_queries.size(), field_id, total_cost, topster, query_suggestion,
                          groups_processed, result_ids, result_size);
            delete[] result_ids;
        }

        searched_queries.push_back(query_suggestion);

        //LOG(INFO) << "all_result_ids_len: " << all_result_ids_len << ", typo_tokens_threshold: " << typo_tokens_threshold;
        if(all_result_ids_len >= typo_tokens_threshold) {
            break;
        }
    }
}

Option<uint32_t> Index::do_filtering(uint32_t** filter_ids_out, const std::vector<filter> & filters) {
    uint32_t* filter_ids = nullptr;
    uint32_t filter_ids_length = 0;

    std::unordered_map<std::string, size_t> facet_to_index;
    get_facet_to_index(facet_to_index);

    for(size_t i = 0; i < filters.size(); i++) {
        const filter & a_filter = filters[i];
        if(search_index.count(a_filter.field_name) == 0) {
            continue;
        }

        art_tree* t = search_index.at(a_filter.field_name);
        field f = search_schema.at(a_filter.field_name);

        uint32_t* result_ids = nullptr;
        size_t result_ids_len = 0;

        if(f.is_integer()) {
            std::vector<const art_leaf*> leaves;
            std::vector<uint32_t> ids;

            for(const std::string & filter_value: a_filter.values) {
                if(f.type == field_types::INT32 || f.type == field_types::INT32_ARRAY) {
                    int32_t value = (int32_t) std::stoi(filter_value);
                    art_int32_search(t, value, a_filter.compare_operator, leaves);
                } else { // int64
                    int64_t value = (int64_t) std::stol(filter_value);
                    art_int64_search(t, value, a_filter.compare_operator, leaves);
                }
            }

            result_ids = collate_leaf_ids(leaves, result_ids_len);

        } else if(f.is_float()) {
            std::vector<const art_leaf*> leaves;
            std::vector<uint32_t> ids;

            for(const std::string & filter_value: a_filter.values) {
                float value = (float) std::atof(filter_value.c_str());
                art_float_search(t, value, a_filter.compare_operator, leaves);
            }

            result_ids = collate_leaf_ids(leaves, result_ids_len);

        } else if(f.is_bool()) {
            std::vector<const art_leaf*> leaves;

            for(const std::string & filter_value: a_filter.values) {
                art_leaf* leaf = (art_leaf *) art_search(t, (const unsigned char*) filter_value.c_str(),
                                                         filter_value.length());
                if(leaf) {
                    leaves.push_back(leaf);
                }
            }
            result_ids = collate_leaf_ids(leaves, result_ids_len);
        } else if(f.is_string()) {
            uint32_t* ids = nullptr;
            size_t ids_size = 0;

            for(const std::string & filter_value: a_filter.values) {
                std::vector<std::string> str_tokens;
                StringUtils::split(filter_value, str_tokens, " ");

                uint32_t* strt_ids = nullptr;
                size_t strt_ids_size = 0;

                std::vector<art_leaf *> query_suggestion;

                // there could be multiple tokens in a filter value, which we have to treat as ANDs
                // e.g. country: South Africa
                for(auto& str_token: str_tokens) {
                    string_utils.unicode_normalize(str_token);

                    art_leaf* leaf = (art_leaf *) art_search(t, (const unsigned char*) str_token.c_str(),
                                                             str_token.length()+1);
                    if(leaf == nullptr) {
                        continue;
                    }

                    query_suggestion.push_back(leaf);

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

                if(a_filter.compare_operator == EQUALS && f.is_facet()) {
                    // need to do exact match (unlike CONTAINS) by using the facet index
                    // field being a facet is already enforced upstream
                    uint32_t* exact_strt_ids = new uint32_t[strt_ids_size];
                    size_t exact_strt_size = 0;
                    
                    size_t facet_id = facet_to_index[f.name];
                    for(size_t strt_ids_index = 0; strt_ids_index < strt_ids_size; strt_ids_index++) {
                        uint32_t seq_id = strt_ids[strt_ids_index];
                        const std::vector<uint64_t> &fvalues = facet_index_v2[seq_id][facet_id];
                        bool found_filter = false;

                        if(!f.is_array()) {
                            found_filter = (query_suggestion.size() == fvalues.size());
                        } else {
                            uint64_t filter_hash = 1;

                            for(size_t sindex=0; sindex < str_tokens.size(); sindex++) {
                                auto& str_token = str_tokens[sindex];
                                uint64_t thash = facet_token_hash(f, str_token);
                                filter_hash *= (1779033703 + 2*thash*(sindex+1));
                            }

                            uint64_t all_fvalue_hash = 1;
                            size_t ftindex = 0;

                            for(size_t findex=0; findex < fvalues.size(); findex++) {
                                auto fhash = fvalues[findex];
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

    *filter_ids_out = filter_ids;
    return Option<>(filter_ids_length);
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

uint32_t* Index::collate_leaf_ids(const std::vector<const art_leaf *> &leaves, size_t& result_ids_len) const {
    std::vector<uint32_t> ids;

    for(const art_leaf* leaf: leaves) {
        uint32_t num_ids = leaf->values->ids.getLength();
        uint32_t* leaf_ids = leaf->values->ids.uncompress();
        std::copy(leaf_ids, leaf_ids + num_ids, std::back_inserter(ids));
        delete [] leaf_ids;
    }

    uint32_t* result_ids = new uint32_t[ids.size()];
    std::sort(ids.begin(), ids.end());
    std::copy(ids.begin(), ids.end(), result_ids);
    result_ids_len = ids.size();
    return result_ids;
}

void Index::run_search() {
    while(true) {
        // wait until main thread sends data
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [this]{return ready;});

        if(terminate) {
            break;
        }

        // after the wait, we own the lock.
        search(search_params->outcome, search_params->query, search_params->search_fields,
               search_params->filters, search_params->facets, search_params->facet_query,
               search_params->included_ids, search_params->excluded_ids,
               search_params->sort_fields_std, search_params->num_typos,
               search_params->topster, search_params->curated_topster,
               search_params->per_page, search_params->page, search_params->token_order,
               search_params->prefix, search_params->drop_tokens_threshold,
               search_params->all_result_ids_len, search_params->groups_processed,
               search_params->searched_queries,
               search_params->raw_result_kvs, search_params->override_result_kvs,
               search_params->typo_tokens_threshold);

        // hand control back to main thread
        processed = true;
        ready = false;

        // manual unlocking is done before notifying, to avoid waking up the waiting thread only to block again
        lk.unlock();
        cv.notify_one();
    }
}

void Index::collate_included_ids(const std::string & query, const std::string & field, const uint8_t field_id,
                                 const std::map<size_t, std::map<size_t, uint32_t>> & included_ids_map,
                                 Topster* curated_topster,
                                 std::vector<std::vector<art_leaf*>> & searched_queries) {

    if(included_ids_map.empty()) {
        return;
    }

    // calculate match_score and add to topster independently
    std::vector<std::string> tokens;
    StringUtils::split(query, tokens, " ");

    std::vector<art_leaf *> override_query;

    for(size_t token_index = 0; token_index < tokens.size(); token_index++) {
        const auto token = tokens[token_index];
        const size_t token_len = token.length();
        string_utils.unicode_normalize(tokens[token_index]);

        std::vector<art_leaf*> leaves;
        art_fuzzy_search(search_index.at(field), (const unsigned char *) token.c_str(), token_len,
                         0, 0, 1, token_ordering::MAX_SCORE, false, leaves);

        if(!leaves.empty()) {
            override_query.push_back(leaves[0]);
        }
    }

    for(const auto& pos_ids: included_ids_map) {
        const size_t outer_pos = pos_ids.first;

        for(const auto& index_seq_id: pos_ids.second) {
            uint32_t inner_pos = index_seq_id.first;
            uint32_t seq_id = index_seq_id.second;

            uint64_t distinct_id = outer_pos;              // outer pos is the group distinct key
            uint64_t match_score = (64000 - inner_pos);    // inner pos within a group is the match score

            // LOG(INFO) << "seq_id: " << seq_id << " - " << match_score;

            int64_t scores[3];
            scores[0] = match_score;
            scores[1] = int64_t(1);
            scores[2] = int64_t(1);

            KV kv(field_id, searched_queries.size(), seq_id, distinct_id, match_score, scores);
            curated_topster->add(&kv);
        }
    }

    searched_queries.push_back(override_query);
}

void Index::search(Option<uint32_t> & outcome,
                   const std::string & query,
                   const std::vector<std::string> & search_fields,
                   const std::vector<filter> & filters,
                   std::vector<facet> & facets, facet_query_t & facet_query,
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
                   const size_t typo_tokens_threshold) {

    // process the filters

    uint32_t* filter_ids = nullptr;
    Option<uint32_t> op_filter_ids_length = do_filtering(&filter_ids, filters);
    if(!op_filter_ids_length.ok()) {
        outcome = Option<uint32_t>(op_filter_ids_length);
        return ;
    }

    uint32_t filter_ids_length = op_filter_ids_length.get();

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

    if(query == "*") {
        const uint8_t field_id = (uint8_t)(FIELD_LIMIT_NUM - 0);
        const std::string & field = search_fields[0];

        // if a filter is not specified, use the sorting index to generate the list of all document ids
        if(filters.empty()) {
            std::string all_records_field;

            // get the first non-optional field
            for(const auto& kv: sort_schema) {
                if(!kv.second.optional && kv.first != sort_field_const::text_match) {
                    all_records_field = kv.first;
                    break;
                }
            }

            const spp::sparse_hash_map<uint32_t, int64_t> *kvs = sort_index[all_records_field];
            filter_ids_length = kvs->size();
            filter_ids = new uint32_t[filter_ids_length];

            size_t i = 0;
            for(const auto& kv: *kvs) {
                filter_ids[i++] = kv.first;
            }
        }

        if(!curated_ids.empty()) {
            uint32_t *excluded_result_ids = nullptr;
            filter_ids_length = ArrayUtils::exclude_scalar(filter_ids, filter_ids_length, &curated_ids_sorted[0],
                                                     curated_ids.size(), &excluded_result_ids);
            delete [] filter_ids;
            filter_ids = excluded_result_ids;
        }

        score_results(sort_fields_std, (uint16_t) searched_queries.size(), field_id, 0, topster, {},
                      groups_processed, filter_ids, filter_ids_length);
        collate_included_ids(query, field, field_id, included_ids_map, curated_topster, searched_queries);

        all_result_ids_len = filter_ids_length;
        all_result_ids = filter_ids;
        filter_ids = nullptr;
    } else {
        const size_t num_search_fields = std::min(search_fields.size(), (size_t) FIELD_LIMIT_NUM);
        for(size_t i = 0; i < num_search_fields; i++) {
            // proceed to query search only when no filters are provided or when filtering produces results
            if(filters.empty() || filter_ids_length > 0) {
                const uint8_t field_id = (uint8_t)(FIELD_LIMIT_NUM - i); // Order of `fields` are used to sort results
                const std::string & field = search_fields[i];

                search_field(field_id, query, field, filter_ids, filter_ids_length, curated_ids_sorted, facets, sort_fields_std,
                             num_typos, searched_queries, topster, groups_processed, &all_result_ids, all_result_ids_len,
                             token_order, prefix, drop_tokens_threshold, typo_tokens_threshold);
                collate_included_ids(query, field, field_id, included_ids_map, curated_topster, searched_queries);
            }
        }
    }

    do_facets(facets, facet_query, all_result_ids, all_result_ids_len);
    do_facets(facets, facet_query, &included_ids[0], included_ids.size());

    // must be sorted before iterated upon to remove "empty" array entries
    topster->sort();
    curated_topster->sort();

    all_result_ids_len += curated_topster->size;

    delete [] filter_ids;
    delete [] all_result_ids;

    //long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    //!LOG(INFO) << "Time taken for result calc: " << timeMillis << "us";

    outcome = Option<uint32_t>(1);
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
void Index::search_field(const uint8_t & field_id, const std::string & query, const std::string & field,
                         uint32_t *filter_ids, size_t filter_ids_length,
                         const std::vector<uint32_t>& curated_ids,
                         std::vector<facet> & facets, const std::vector<sort_by> & sort_fields, const int num_typos,
                         std::vector<std::vector<art_leaf*>> & searched_queries,
                         Topster* topster, spp::sparse_hash_set<uint64_t>& groups_processed,
                         uint32_t** all_result_ids, size_t & all_result_ids_len,
                         const token_ordering token_order, const bool prefix, 
                         const size_t drop_tokens_threshold, const size_t typo_tokens_threshold) {
    std::vector<std::string> tokens;
    StringUtils::split(query, tokens, " ");

    const size_t max_cost = (num_typos < 0 || num_typos > 2) ? 2 : num_typos;

    // To prevent us from doing ART search repeatedly as we iterate through possible corrections
    spp::sparse_hash_map<std::string, std::vector<art_leaf*>> token_cost_cache;

    // Used to drop the least occurring token(s) for partial searches
    std::unordered_map<std::string, uint32_t> token_to_count;

    std::vector<std::vector<int>> token_to_costs;

    for(size_t token_index = 0; token_index < tokens.size(); token_index++) {
        std::vector<int> all_costs;
        const size_t token_len = tokens[token_index].length();

        // This ensures that we don't end up doing a cost of 1 for a single char etc.
        int bounded_cost = get_bounded_typo_cost(max_cost, token_len);

        for(int cost = 0; cost <= bounded_cost; cost++) {
            all_costs.push_back(cost);
        }

        token_to_costs.push_back(all_costs);
        string_utils.unicode_normalize(tokens[token_index]);
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

        while(token_index < tokens.size()) {
            // For each token, look up the generated cost for this iteration and search using that cost
            std::string token = tokens[token_index];
            const std::string token_cost_hash = token + std::to_string(costs[token_index]);

            std::vector<art_leaf*> leaves;
            //LOG(INFO) << "\nSearching for field: " << field << ", token:" << token << " - cost: " << costs[token_index];

            if(token_cost_cache.count(token_cost_hash) != 0) {
                leaves = token_cost_cache[token_cost_hash];
            } else {
                // prefix should apply only for last token
                const bool prefix_search = prefix && (token_index == tokens.size()-1);
                const size_t token_len = prefix_search ? (int) token.length() : (int) token.length() + 1;

                // If this is a prefix search, look for more candidates and do a union of those document IDs
                const int max_candidates = prefix_search ? 10 : 3;
                art_fuzzy_search(search_index.at(field), (const unsigned char *) token.c_str(), token_len,
                                 costs[token_index], costs[token_index], max_candidates, token_order, prefix_search, leaves);

                if(!leaves.empty()) {
                    token_cost_cache.emplace(token_cost_hash, leaves);
                }
            }

            if(!leaves.empty()) {
                //log_leaves(costs[token_index], token, leaves);
                token_candidates_vec.push_back(token_candidates{token, costs[token_index], leaves});
                token_to_count[token] = std::max(token_to_count[token], leaves.at(0)->values->ids.getLength());
            } else {
                // No result at `cost = costs[token_index]`. Remove costs until `cost` for token and re-do combinations
                auto it = std::find(token_to_costs[token_index].begin(), token_to_costs[token_index].end(), costs[token_index]);
                if(it != token_to_costs[token_index].end()) {
                    token_to_costs[token_index].erase(it);

                    // when no more costs are left for this token and `drop_tokens_threshold` is breached
                    if(token_to_costs[token_index].empty() && all_result_ids_len >= drop_tokens_threshold) {
                        n = combination_limit; // to break outer loop
                        break;
                    }

                    // otherwise, we try to drop the token and search with remaining tokens
                    if(token_to_costs[token_index].empty()) {
                        token_to_costs.erase(token_to_costs.begin()+token_index);
                        tokens.erase(tokens.begin()+token_index);
                        costs.erase(costs.begin()+token_index);
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

        if(!token_candidates_vec.empty() && token_candidates_vec.size() == tokens.size()) {
            // If all tokens were found, go ahead and search for candidates with what we have so far
            search_candidates(field_id, filter_ids, filter_ids_length, curated_ids, sort_fields, token_candidates_vec,
                              searched_queries, topster, groups_processed, all_result_ids, all_result_ids_len,
                              typo_tokens_threshold);
        }

        if (all_result_ids_len >= typo_tokens_threshold) {
            // If we don't find enough results, we continue outerloop (looking at tokens with greater typo cost)
            break;
        }

        n++;
    }

    // When there are not enough overall results and atleast one token has results
    if(all_result_ids_len < drop_tokens_threshold && token_to_count.size() > 1) {
        // Drop token with least hits and try searching again
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
            // iterate till last but one
            truncated_query += " " + token_count_pairs.at(i).first;
        }

        return search_field(field_id, truncated_query, field, filter_ids, filter_ids_length, curated_ids,
                            facets, sort_fields, num_typos,
                            searched_queries, topster, groups_processed, all_result_ids, all_result_ids_len,
                            token_order, prefix);
    }
}

int Index::get_bounded_typo_cost(const size_t max_cost, const size_t token_len) const {
    int bounded_cost = max_cost;
    if(token_len > 0 && max_cost >= token_len && (token_len == 1 || token_len == 2)) {
        bounded_cost = token_len - 1;
    }
    return bounded_cost;
}

void Index::log_leaves(const int cost, const std::string &token, const std::vector<art_leaf *> &leaves) const {
    LOG(INFO) << "Token: " << token << ", cost: " << cost;

    for(size_t i=0; i < leaves.size(); i++) {
        printf("%.*s - %d, ", leaves[i]->key_len, leaves[i]->key, leaves[i]->values->ids.getLength());
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
                          const uint32_t *result_ids, const size_t result_size) {

    std::vector<uint32_t*> leaf_to_indices;
    for (art_leaf *token_leaf: query_suggestion) {
        uint32_t *indices = new uint32_t[result_size];
        token_leaf->values->ids.indexOf(result_ids, result_size, indices);
        leaf_to_indices.push_back(indices);
    }

    std::unordered_map<std::string, size_t> facet_to_index;
    get_facet_to_index(facet_to_index);

    Match single_token_match = Match(1, 0);
    const uint64_t single_token_match_score = single_token_match.get_match_score(total_cost, field_id);

    int sort_order[3]; // 1 or -1 based on DESC or ASC respectively
    spp::sparse_hash_map<uint32_t, int64_t>* field_values[3];

    for(size_t i = 0; i < sort_fields.size(); i++) {
        sort_order[i] = 1;
        if(sort_fields[i].order == sort_field_const::asc) {
            sort_order[i] = -1;
        }

        field_values[i] = (sort_fields[i].name != sort_field_const::text_match) ?
                          sort_index.at(sort_fields[i].name) :
                          nullptr;
    }

    //auto begin = std::chrono::high_resolution_clock::now();

    for(size_t i=0; i<result_size; i++) {
        const uint32_t seq_id = result_ids[i];

        uint64_t match_score = 0;

        if(query_suggestion.size() <= 1) {
            match_score = single_token_match_score;
        } else {
            std::unordered_map<size_t, std::vector<std::vector<uint16_t>>> array_token_positions;
            populate_token_positions(query_suggestion, leaf_to_indices, i, array_token_positions);

            for(const auto& kv: array_token_positions) {
                const std::vector<std::vector<uint16_t>>& token_positions = kv.second;
                if(token_positions.empty()) {
                    continue;
                }
                const Match & match = Match(seq_id, token_positions, false);
                uint64_t this_match_score = match.get_match_score(total_cost, field_id);

                if(this_match_score > match_score) {
                    match_score = this_match_score;
                }

                /*std::ostringstream os;
                os << name << ", total_cost: " << (255 - total_cost)
                   << ", words_present: " << match.words_present
                   << ", match_score: " << match_score
                   << ", match.distance: " << match.distance
                   << ", seq_id: " << seq_id << std::endl;
                LOG(INFO) << os.str();*/
            }
        }

        const int64_t default_score = 0;
        int64_t scores[3] = {0};

        // avoiding loop
        if(sort_fields.size() > 0) {
            if (field_values[0] != nullptr) {
                auto it = field_values[0]->find(seq_id);
                scores[0] = (it == field_values[0]->end()) ? default_score : it->second;
            } else {
                scores[0] = int64_t(match_score);
            }
            if (sort_order[0] == -1) {
                scores[0] = -scores[0];
            }
            scores[1] = 0;
        }

        if(sort_fields.size() > 1) {
            if (field_values[1] != nullptr) {
                auto it = field_values[1]->find(seq_id);
                scores[1] = (it == field_values[1]->end()) ? default_score : it->second;
            } else {
                scores[1] = int64_t(match_score);
            }
            if (sort_order[1] == -1) {
                scores[1] = -scores[1];
            }
            scores[2] = 0;
        }

        if(sort_fields.size() > 2) {
            if(field_values[2] != nullptr) {
                auto it = field_values[2]->find(seq_id);
                scores[2] = (it == field_values[2]->end()) ? default_score : it->second;
            } else {
                scores[2] = int64_t(match_score);
            }
            if(sort_order[2] == -1) {
                scores[2] = -scores[2];
            }
        }

        uint64_t distinct_id = seq_id;

        if(search_params->group_limit != 0) {
            distinct_id = get_distinct_id(facet_to_index, seq_id);
            groups_processed.emplace(distinct_id);
        }

        KV kv(field_id, query_index, seq_id, distinct_id, match_score, scores);
        topster->add(&kv);
    }

    //long long int timeNanos = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    //LOG(INFO) << "Time taken for results iteration: " << timeNanos << "ms";

    for(uint32_t* leaf_indices: leaf_to_indices) {
        delete [] leaf_indices;
    }
}

uint64_t Index::get_distinct_id(const std::unordered_map<std::string, size_t> &facet_to_id,
                                const uint32_t seq_id) const {
    uint64_t distinct_id = 1; // some constant initial value

    // calculate hash from group_by_fields
    for(const auto& field: search_params->group_by_fields) {
        if(facet_to_id.count(field) == 0 || facet_index_v2.count(seq_id) == 0) {
            continue;
        }

        size_t facet_id = facet_to_id.at(field);
        const std::vector<uint64_t>& fhashes = facet_index_v2.at(seq_id)[facet_id];

        for(const auto& hash: fhashes) {
            distinct_id = hash_combine(distinct_id, hash);
        }
    }
    return distinct_id;
}

void Index::populate_token_positions(const std::vector<art_leaf *>& query_suggestion,
                                     std::vector<uint32_t*>& leaf_to_indices,
                                     size_t result_index,
                                     std::unordered_map<size_t, std::vector<std::vector<uint16_t>>>& array_token_positions) {
    if(query_suggestion.empty()) {
        return ;
    }

    // array_token_positions:
    // for every element in a potential array, for every token in query suggestion, get the positions

    for(size_t i = 0; i < query_suggestion.size(); i++) {
        const art_leaf* token_leaf = query_suggestion[i];
        uint32_t doc_index = leaf_to_indices[i][result_index];

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
        }*/

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

inline std::vector<art_leaf *> Index::next_suggestion(const std::vector<token_candidates> &token_candidates_vec,
                                                      long long int n) {
    std::vector<art_leaf*> query_suggestion(token_candidates_vec.size());

    // generate the next combination from `token_leaves` and store it in `query_suggestion`
    ldiv_t q { n, 0 };
    for(long long i = 0 ; i < (long long) token_candidates_vec.size(); i++) {
        q = ldiv(q.quot, token_candidates_vec[i].candidates.size());
        query_suggestion[i] = token_candidates_vec[i].candidates[q.rem];
    }

    // Sort ascending based on matched documents for each token for faster intersection.
    // However, this causes the token order to deviate from original query's order.
    sort(query_suggestion.begin(), query_suggestion.end(), [](const art_leaf* left, const art_leaf* right) {
        return left->values->ids.getLength() < right->values->ids.getLength();
    });

    return query_suggestion;
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
    std::unordered_map<std::string, size_t> facet_to_index;
    get_facet_to_index(facet_to_index);

    for(auto it = document.begin(); it != document.end(); ++it) {
        const std::string& field_name = it.key();
        const auto& search_field_it = search_schema.find(field_name);
        if(search_field_it == search_schema.end()) {
            continue;
        }

        const auto& search_field = search_field_it->second;

        // Go through all the field names and find the keys+values so that they can be removed from in-memory index
        std::vector<std::string> tokens;
        tokenize_doc_field(document, field_name, search_field, tokens);

        for(auto & token: tokens) {
            const unsigned char *key;
            int key_len;

            if(search_field.type == field_types::STRING_ARRAY || search_field.type == field_types::STRING) {
                string_utils.unicode_normalize(token);
                key = (const unsigned char *) token.c_str();
                key_len = (int) (token.length() + 1);
            } else {
                key = (const unsigned char *) token.c_str();
                key_len = (int) (token.length());
            }

            art_leaf* leaf = (art_leaf *) art_search(search_index.at(field_name), key, key_len);
            if(leaf != nullptr) {
                uint32_t seq_id_values[1] = {seq_id};
                uint32_t doc_index = leaf->values->ids.indexOf(seq_id);

                if(doc_index == leaf->values->ids.getLength()) {
                    // not found - happens when 2 tokens repeat in a field, e.g "is it or is is not?"
                    continue;
                }

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
                    LOG(INFO) << "i: " << i << ", val: " << leaf->values->offset_index.at(i);
                }
                LOG(INFO) << "----";*/

                if(leaf->values->ids.getLength() == 0) {
                    art_values* values = (art_values*) art_delete(search_index.at(field_name), key, key_len);
                    delete values;
                }
            }
        }

        // remove facets
        if(facet_to_index.count(field_name) != 0 && facet_index_v2.count(seq_id) != 0) {
            size_t facet_index = facet_to_index[field_name];
            std::vector<std::vector<uint64_t>>& facet_values = facet_index_v2[seq_id];
            facet_values[facet_index].clear();
        }

        // remove sort field
        if(sort_index.count(field_name) != 0) {
            sort_index[field_name]->erase(seq_id);
        }
    }

    return Option<uint32_t>(seq_id);
}

void Index::tokenize_doc_field(const nlohmann::json& document, const std::string& field_name, const field& search_field,
                               std::vector<std::string>& tokens) {
    if(search_field.type == field_types::STRING) {
        StringUtils::split(document[field_name], tokens, " ");
    } else if(search_field.type == field_types::STRING_ARRAY) {
        const std::vector<std::string>& values = document[field_name].get<std::vector<std::string>>();
        for(const std::string & value: values) {
            StringUtils::split(value, tokens, " ");
        }
    } else if(search_field.type == field_types::INT32) {
        const int KEY_LEN = 8;
        unsigned char key[KEY_LEN];
        const int32_t& value = document[field_name].get<int32_t>();
        encode_int32(value, key);
        tokens.emplace_back((char*)key, KEY_LEN);
    } else if(search_field.type == field_types::INT32_ARRAY) {
        const std::vector<int32_t>& values = document[field_name].get<std::vector<int32_t>>();
        for(const int32_t value: values) {
            const int KEY_LEN = 8;
            unsigned char key[KEY_LEN];
            encode_int32(value, key);
            tokens.emplace_back((char*)key, KEY_LEN);
        }
    } else if(search_field.type == field_types::INT64) {
        const int KEY_LEN = 8;
        unsigned char key[KEY_LEN];
        const int64_t& value = document[field_name].get<int64_t>();
        encode_int64(value, key);
        tokens.emplace_back((char*)key, KEY_LEN);
    } else if(search_field.type == field_types::INT64_ARRAY) {
        const std::vector<int64_t>& values = document[field_name].get<std::vector<int64_t>>();
        for(const int64_t value: values) {
            const int KEY_LEN = 8;
            unsigned char key[KEY_LEN];
            encode_int64(value, key);
            tokens.emplace_back((char*)key, KEY_LEN);
        }
    } else if(search_field.type == field_types::FLOAT) {
        const int KEY_LEN = 8;
        unsigned char key[KEY_LEN];
        const int64_t& value = document[field_name].get<int64_t>();
        encode_float(value, key);
        tokens.emplace_back((char*)key, KEY_LEN);
    } else if(search_field.type == field_types::FLOAT_ARRAY) {
        const std::vector<float>& values = document[field_name].get<std::vector<float>>();
        for(const float value: values) {
            const int KEY_LEN = 8;
            unsigned char key[KEY_LEN];
            encode_float(value, key);
            tokens.emplace_back((char*)key, KEY_LEN);
        }
    } else if(search_field.type == field_types::BOOL) {
        const int KEY_LEN = 1;
        unsigned char key[KEY_LEN];
        const bool& value = document[field_name].get<bool>();
        key[0] = value ? '1' : '0';
        tokens.emplace_back((char*)key, KEY_LEN);
    } else if(search_field.type == field_types::BOOL_ARRAY) {
        const std::vector<bool>& values = document[field_name].get<std::vector<bool>>();
        for(const bool value: values) {
            const int KEY_LEN = 1;
            unsigned char key[KEY_LEN];
            key[0] = value ? '1' : '0';
            tokens.emplace_back((char*)key, KEY_LEN);
        }
    }
}

art_leaf* Index::get_token_leaf(const std::string & field_name, const unsigned char* token, uint32_t token_len) {
    const art_tree *t = search_index.at(field_name);
    return (art_leaf*) art_search(t, token, (int) token_len);
}

const spp::sparse_hash_map<std::string, art_tree *> &Index::_get_search_index() const {
    return search_index;
}
