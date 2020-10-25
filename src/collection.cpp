#include "collection.h"

#include <numeric>
#include <chrono>
#include <array_utils.h>
#include <match_score.h>
#include <string_utils.h>
#include <art.h>
#include <thread>
#include <future>
#include <rocksdb/write_batch.h>
#include <system_metrics.h>
#include "topster.h"
#include "logger.h"

const std::string override_t::MATCH_EXACT = "exact";
const std::string override_t::MATCH_CONTAINS = "contains";

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

Collection::Collection(const std::string& name, const uint32_t collection_id, const uint64_t created_at,
                       const uint32_t next_seq_id, Store *store, const std::vector<field> &fields,
                       const std::string& default_sorting_field, const size_t num_memory_shards,
                       const float max_memory_ratio):
        name(name), collection_id(collection_id), next_seq_id(next_seq_id), store(store),
        fields(fields), default_sorting_field(default_sorting_field),
        num_memory_shards(num_memory_shards),
        max_memory_ratio(max_memory_ratio) {

    for(const field& field: fields) {
        search_schema.emplace(field.name, field);

        if(field.is_facet()) {
            facet_schema.emplace(field.name, field);
        }

        if(field.is_single_integer() || field.is_single_float() || field.is_single_bool()) {
            sort_schema.emplace(field.name, field);
        }
    }

    for(size_t i = 0; i < num_memory_shards; i++) {
        Index* index = new Index(name+std::to_string(i), search_schema, facet_schema, sort_schema);
        indices.push_back(index);
        std::thread* thread = new std::thread(&Index::run_search, index);
        index_threads.push_back(thread);
    }

    this->created_at = created_at;
    this->num_documents = 0;
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
        t = nullptr;
        indices[i] = nullptr;
    }

    indices.clear();
    index_threads.clear();
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

Option<doc_seq_id_t> Collection::to_doc(const std::string & json_str, nlohmann::json& document,
                                        const index_operation_t& operation, const std::string& id) {
    try {
        document = nlohmann::json::parse(json_str);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        return Option<doc_seq_id_t>(400, std::string("Bad JSON: ") + e.what());
    }

    if(!document.is_object()) {
        return Option<doc_seq_id_t>(400, "Bad JSON: not a properly formed document.");
    }

    if(document.count("id") != 0 && id != "" && document["id"] != id) {
        return Option<doc_seq_id_t>(400, "The `id` of the resource does not match the `id` in the JSON body.");
    }

    if(document.count("id") == 0 && !id.empty()) {
        // use the explicit ID (usually from a PUT request) if document body does not have it
        document["id"] = id;
    }

    if(document.count("id") != 0 && document["id"] == "") {
        return Option<doc_seq_id_t>(400, "The `id` should not be empty.");
    }

    if(document.count("id") == 0) {
        if(operation == UPDATE) {
            return Option<doc_seq_id_t>(400, "For update, the `id` key must be provided.");
        }
        // for UPSERT or CREATE, if a document does not have an ID, we will treat it as a new doc
        uint32_t seq_id = get_next_seq_id();
        document["id"] = std::to_string(seq_id);
        return Option<doc_seq_id_t>(doc_seq_id_t{seq_id, true});
    } else {
        if(!document["id"].is_string()) {
            return Option<doc_seq_id_t>(400, "Document's `id` field should be a string.");
        }

        const std::string& doc_id = document["id"];

        // try to get the corresponding sequence id from disk if present
        std::string seq_id_str;
        StoreStatus seq_id_status = store->get(get_doc_id_key(doc_id), seq_id_str);

        if(seq_id_status == StoreStatus::ERROR) {
            return Option<doc_seq_id_t>(500, "Error fetching the sequence key for document with id: " + doc_id);
        }

        if(seq_id_status == StoreStatus::FOUND) {
            if(operation == CREATE) {
                return Option<doc_seq_id_t>(409, std::string("A document with id ") + doc_id + " already exists.");
            }

            // UPSERT or UPDATE
            uint32_t seq_id = (uint32_t) std::stoul(seq_id_str);
            return Option<doc_seq_id_t>(doc_seq_id_t{seq_id, false});

        } else {
            if(operation == UPDATE) {
                // for UPDATE, a document with given ID must be found
                return Option<doc_seq_id_t>(404, "Could not find a document with id: " + doc_id);
            } else {
                // for UPSERT or CREATE, if a document with given ID is not found, we will treat it as a new doc
                uint32_t seq_id = get_next_seq_id();
                return Option<doc_seq_id_t>(doc_seq_id_t{seq_id, true});
            }
        }
    }
}

nlohmann::json Collection::get_summary_json() {
    nlohmann::json json_response;

    json_response["name"] = name;
    json_response["num_memory_shards"] = num_memory_shards;
    json_response["num_documents"] = num_documents;
    json_response["created_at"] = created_at;

    nlohmann::json fields_arr;

    for(const field & coll_field: fields) {
        nlohmann::json field_json;
        field_json[fields::name] = coll_field.name;
        field_json[fields::type] = coll_field.type;
        field_json[fields::facet] = coll_field.facet;
        field_json[fields::optional] = coll_field.optional;
        fields_arr.push_back(field_json);
    }

    json_response["fields"] = fields_arr;
    json_response["default_sorting_field"] = default_sorting_field;
    return json_response;
}

Option<nlohmann::json> Collection::add(const std::string & json_str,
                                       const index_operation_t& operation, const std::string& id) {
    nlohmann::json document;
    std::vector<std::string> json_lines = {json_str};
    const nlohmann::json& res = add_many(json_lines, document, operation, id);

    if(!res["success"].get<bool>()) {
        nlohmann::json res_doc;

        try {
            res_doc = nlohmann::json::parse(json_lines[0]);
        } catch(const std::exception& e) {
            LOG(ERROR) << "JSON error: " << e.what();
            return Option<nlohmann::json>(400, std::string("Bad JSON: ") + e.what());
        }

        return Option<nlohmann::json>(res_doc["code"].get<size_t>(), res_doc["error"].get<std::string>());
    }

    return Option<nlohmann::json>(document);
}

void Collection::get_doc_changes(const nlohmann::json &document, nlohmann::json &old_doc,
                                 nlohmann::json &new_doc, nlohmann::json &del_doc) {

    for(auto it = old_doc.begin(); it != old_doc.end(); ++it) {
        new_doc[it.key()] = it.value();
    }

    for(auto it = document.begin(); it != document.end(); ++it) {
        new_doc[it.key()] = it.value();
        if(old_doc.count(it.key()) != 0) {
            // key exists in the stored doc, so it must be reindexed
            // we need to check for this because a field can be optional
            del_doc[it.key()] = old_doc[it.key()];
        }
    }
}

nlohmann::json Collection::add_many(std::vector<std::string>& json_lines, nlohmann::json& document,
                                    const index_operation_t& operation, const std::string& id) {
    //LOG(INFO) << "Memory ratio. Max = " << max_memory_ratio << ", Used = " << SystemMetrics::used_memory_ratio();
    std::vector<std::vector<index_record>> iter_batch;

    for(size_t i = 0; i < num_memory_shards; i++) {
        iter_batch.emplace_back(std::vector<index_record>());
    }

    const size_t index_batch_size = 1000;
    size_t num_indexed = 0;
    bool exceeds_memory_limit = false;

    for(size_t i=0; i < json_lines.size(); i++) {
        const std::string & json_line = json_lines[i];
        Option<doc_seq_id_t> doc_seq_id_op = to_doc(json_line, document, operation, id);

        const uint32_t seq_id = doc_seq_id_op.ok() ? doc_seq_id_op.get().seq_id : 0;
        index_record record(i, seq_id, document, operation);

        // NOTE: we overwrite the input json_lines with result to avoid memory pressure

        record.is_update = false;

        if(!doc_seq_id_op.ok()) {
            record.index_failure(doc_seq_id_op.code(), doc_seq_id_op.error());
        } else {
            record.is_update = !doc_seq_id_op.get().is_new;
            if(record.is_update) {
                get_document_from_store(get_seq_id_key(seq_id), record.old_doc);
                get_doc_changes(document, record.old_doc, record.new_doc, record.del_doc);
            }
        }

        /*
        // check for memory threshold before allowing subsequent batches
        if(is_exceeding_memory_threshold()) {
            exceeds_memory_limit = true;
        }

        if(exceeds_memory_limit) {
            nlohmann::json index_res;
            index_res["error"] = "Max memory ratio exceeded.";
            index_res["success"] = false;
            index_res["document"] = json_line;
            json_lines[i] = index_res.dump();
            record.index_failure(500, "Max memory ratio exceeded.");
        }
        */

        iter_batch[seq_id % this->get_num_memory_shards()].emplace_back(record);

        if((i+1) % index_batch_size == 0 || i == json_lines.size()-1) {
            batch_index(iter_batch, json_lines, num_indexed);
            for(size_t i_index = 0; i_index < get_num_memory_shards(); i_index++) {
                iter_batch[i_index].clear();
            }
        }
    }

    nlohmann::json resp_summary;
    resp_summary["num_imported"] = num_indexed;
    resp_summary["success"] = (num_indexed == json_lines.size());

    return resp_summary;
}

bool Collection::is_exceeding_memory_threshold() const {
    return SystemMetrics::used_memory_ratio() > max_memory_ratio;
}

void Collection::batch_index(std::vector<std::vector<index_record>> &index_batches, std::vector<std::string>& json_out,
                             size_t &num_indexed) {
    std::vector<size_t> indexed_counts;
    indexed_counts.reserve(index_batches.size());
    par_index_in_memory(index_batches, indexed_counts);

    // store only documents that were indexed in-memory successfully
    for(auto& index_batch: index_batches) {
        for(auto& index_record: index_batch) {
            nlohmann::json res;

            if(index_record.indexed.ok()) {
                if(index_record.is_update) {
                    const std::string& serialized_json = index_record.new_doc.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore);
                    bool write_ok = store->insert(get_seq_id_key(index_record.seq_id), serialized_json);

                    if(!write_ok) {
                        // we will attempt to reindex the old doc on a best-effort basis
                        remove_document(index_record.new_doc, index_record.seq_id, false);
                        index_in_memory(index_record.old_doc, index_record.seq_id, false);
                        index_record.index_failure(500, "Could not write to on-disk storage.");
                    } else {
                        num_indexed++;
                        index_record.index_success();
                    }

                } else {
                    const std::string& seq_id_str = std::to_string(index_record.seq_id);
                    const std::string& serialized_json = index_record.doc.dump(-1, ' ', false,
                                                                               nlohmann::detail::error_handler_t::ignore);

                    rocksdb::WriteBatch batch;
                    batch.Put(get_doc_id_key(index_record.doc["id"]), seq_id_str);
                    batch.Put(get_seq_id_key(index_record.seq_id), serialized_json);
                    bool write_ok = store->batch_write(batch);

                    if(!write_ok) {
                        // remove from in-memory store to keep the state synced
                        remove_document(index_record.doc, index_record.seq_id, false);
                        index_record.index_failure(500, "Could not write to on-disk storage.");
                    } else {
                        num_indexed++;
                        index_record.index_success();
                    }
                }

                res["success"] = index_record.indexed.ok();
                if(!index_record.indexed.ok()) {
                    res["document"] = json_out[index_record.position];
                    res["error"] = index_record.indexed.error();
                    res["code"] = index_record.indexed.code();
                }
            } else {
                res["success"] = false;
                res["document"] = json_out[index_record.position];
                res["error"] = index_record.indexed.error();
                res["code"] = index_record.indexed.code();
            }

            json_out[index_record.position] = res.dump();
        }
    }
}

Option<uint32_t> Collection::index_in_memory(const nlohmann::json &document, uint32_t seq_id, bool is_update) {
    if(!is_update) {
        // for update, validation should be done prior
        Option<uint32_t> validation_op = Index::validate_index_in_memory(document, seq_id, default_sorting_field,
                                                                         search_schema, facet_schema, is_update);

        if(!validation_op.ok()) {
            return validation_op;
        }
    }

    Index* index = indices[seq_id % num_memory_shards];
    index->index_in_memory(document, seq_id, default_sorting_field, is_update);

    num_documents += 1;
    return Option<>(200);
}

size_t Collection::par_index_in_memory(std::vector<std::vector<index_record>> & iter_batch,
                                       std::vector<size_t>& indexed_counts) {
    std::vector<std::future<size_t>> futures;
    size_t num_indexed = 0;

    for(size_t i=0; i < num_memory_shards; i++) {
        futures.push_back(
            std::async(&Index::batch_memory_index, indices[i], std::ref(iter_batch[i]), default_sorting_field,
                       search_schema, facet_schema)
        );
    }

    for(size_t i=0; i < futures.size(); i++) {
        size_t num_indexed_future = futures[i].get();
        num_documents += num_indexed_future;
        num_indexed += num_indexed_future;
        indexed_counts[i] = num_indexed_future;
    }

    return num_indexed;
}

void Collection::prune_document(nlohmann::json &document, const spp::sparse_hash_set<std::string>& include_fields,
                                const spp::sparse_hash_set<std::string>& exclude_fields) {
    auto it = document.begin();
    for(; it != document.end(); ) {
        if(exclude_fields.count(it.key()) != 0 || (include_fields.size() != 0 && include_fields.count(it.key()) == 0)) {
            it = document.erase(it);
        } else {
            ++it;
        }
    }
}

void Collection::populate_overrides(std::string query,
                                    const std::map<size_t, std::vector<std::string>>& pinned_hits,
                                    const std::vector<std::string>& hidden_hits,
                                    std::map<size_t, std::vector<uint32_t>>& include_ids,
                                    std::vector<uint32_t> & excluded_ids) {
    StringUtils::tolowercase(query);
    std::set<uint32_t> excluded_set;

    // If pinned or hidden hits are provided, they take precedence over overrides

    // have to ensure that hidden hits take precedence over included hits
    if(!hidden_hits.empty()) {
        for(const auto & hit: hidden_hits) {
            Option<uint32_t> seq_id_op = doc_id_to_seq_id(hit);
            if(seq_id_op.ok()) {
                excluded_ids.push_back(seq_id_op.get());
                excluded_set.insert(seq_id_op.get());
            }
        }
    }

    for(const auto & override_kv: overrides) {
        const auto & override = override_kv.second;

        if( (override.rule.match == override_t::MATCH_EXACT && override.rule.query == query) ||
            (override.rule.match == override_t::MATCH_CONTAINS && query.find(override.rule.query) != std::string::npos) )  {

            // have to ensure that dropped hits take precedence over added hits
            for(const auto & hit: override.drop_hits) {
                Option<uint32_t> seq_id_op = doc_id_to_seq_id(hit.doc_id);
                if(seq_id_op.ok()) {
                    excluded_ids.push_back(seq_id_op.get());
                    excluded_set.insert(seq_id_op.get());
                }
            }

            for(const auto & hit: override.add_hits) {
                Option<uint32_t> seq_id_op = doc_id_to_seq_id(hit.doc_id);
                if(!seq_id_op.ok()) {
                    continue;
                }
                uint32_t seq_id = seq_id_op.get();
                bool excluded = (excluded_set.count(seq_id) != 0);
                if(!excluded) {
                    include_ids[hit.position].push_back(seq_id);
                }
            }
        }
    }

    if(!pinned_hits.empty()) {
        for(const auto& pos_ids: pinned_hits) {
            size_t pos = pos_ids.first;
            for(const std::string& id: pos_ids.second) {
                Option<uint32_t> seq_id_op = doc_id_to_seq_id(id);
                if(!seq_id_op.ok()) {
                    continue;
                }
                uint32_t seq_id = seq_id_op.get();
                bool excluded = (excluded_set.count(seq_id) != 0);
                if(!excluded) {
                    include_ids[pos].push_back(seq_id);
                }
            }
        }
    }
}

Option<nlohmann::json> Collection::search(const std::string & query, const std::vector<std::string> & search_fields,
                                  const std::string & simple_filter_query, const std::vector<std::string> & facet_fields,
                                  const std::vector<sort_by> & sort_fields, const int num_typos,
                                  const size_t per_page, const size_t page,
                                  const token_ordering token_order, const bool prefix,
                                  const size_t drop_tokens_threshold,
                                  const spp::sparse_hash_set<std::string> & include_fields,
                                  const spp::sparse_hash_set<std::string> & exclude_fields,
                                  const size_t max_facet_values,
                                  const std::string & simple_facet_query,
                                  const size_t snippet_threshold,
                                  const size_t highlight_affix_num_tokens,
                                  const std::string & highlight_full_fields,
                                  size_t typo_tokens_threshold,
                                  const std::map<size_t, std::vector<std::string>>& pinned_hits,
                                  const std::vector<std::string>& hidden_hits,
                                  const std::vector<std::string>& group_by_fields,
                                  const size_t group_limit) {

    if(query != "*" && search_fields.empty()) {
        return Option<nlohmann::json>(400, "No search fields specified for the query.");
    }

    if(!group_by_fields.empty() && (group_limit == 0 || group_limit > GROUP_LIMIT_MAX)) {
        return Option<nlohmann::json>(400, "Value of `group_limit` must be between 1 and " +
                                      std::to_string(GROUP_LIMIT_MAX) + ".");
    }

    std::vector<uint32_t> excluded_ids;
    std::map<size_t, std::vector<uint32_t>> include_ids; // position => list of IDs
    populate_overrides(query, pinned_hits, hidden_hits, include_ids, excluded_ids);

    /*for(auto kv: include_ids) {
        LOG(INFO) << "key: " << kv.first;
        for(auto val: kv.second) {
            LOG(INFO) << val;
        }
    }

    LOG(INFO) << "Excludes:";

    for(auto id: excluded_ids) {
        LOG(INFO) << id;
    }

    LOG(INFO) << "include_ids size: " << include_ids.size();
    for(auto& group: include_ids) {
        for(uint32_t& seq_id: group.second) {
            LOG(INFO) << "seq_id: " << seq_id;
        }

        LOG(INFO) << "----";
    }
    */

    std::map<uint32_t, std::map<size_t, std::map<size_t, uint32_t>>> index_to_included_ids;
    std::map<uint32_t, std::vector<uint32_t>> index_to_excluded_ids;

    for(const auto& pos_ids: include_ids) {
        size_t outer_pos = pos_ids.first;
        size_t ids_per_pos = std::max(size_t(1), group_limit);

        for(size_t inner_pos = 0; inner_pos < std::min(ids_per_pos, pos_ids.second.size()); inner_pos++) {
            auto seq_id = pos_ids.second[inner_pos];
            auto index_id = (seq_id % num_memory_shards);
            index_to_included_ids[index_id][outer_pos][inner_pos] = seq_id;
            //LOG(INFO) << "Adding seq_id " << seq_id << " to index_id " << index_id;
        }
    }

    for(auto seq_id: excluded_ids) {
        auto index_id = (seq_id % num_memory_shards);
        index_to_excluded_ids[index_id].push_back(seq_id);
    }

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
    }

    // validate group by fields
    for(const std::string & field_name: group_by_fields) {
        if(search_schema.count(field_name) == 0) {
            std::string error = "Could not find a field named `" + field_name + "` in the schema.";
            return Option<nlohmann::json>(404, error);
        }

        field search_field = search_schema.at(field_name);

        // must be a facet field
        if(!search_field.is_facet()) {
            std::string error = "Group by field `" + field_name + "` should be a facet field.";
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

        // skip past optional `:=` operator, which has no meaning for non-string fields
        if(!_field.is_string() && raw_value[0] == '=') {
            size_t filter_value_index = 0;
            while(raw_value[++filter_value_index] == ' ');
            raw_value = raw_value.substr(filter_value_index);
        }

        if(_field.is_integer() || _field.is_float()) {
            // could be a single value or a list
            if(raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
                std::vector<std::string> filter_values;
                StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, ",");

                for(const std::string & filter_value: filter_values) {
                    if(_field.is_int32()) {
                        if(!StringUtils::is_int32_t(filter_value)) {
                            return Option<nlohmann::json>(400, "Error with filter field `" + _field.name + "`: Not an int32.");
                        }
                    }

                    else if(_field.is_int64()) {
                        if(!StringUtils::is_int64_t(filter_value)) {
                            return Option<nlohmann::json>(400, "Error with filter field `" + _field.name + "`: Not an int64.");
                        }
                    }

                    else if(_field.is_float()) {
                        if(!StringUtils::is_float(filter_value)) {
                            return Option<nlohmann::json>(400, "Error with filter field `" + _field.name + "`: Not a float.");
                        }
                    }
                }

                f = {field_name, filter_values, EQUALS};
            } else {
                Option<NUM_COMPARATOR> op_comparator = filter::extract_num_comparator(raw_value);
                if(!op_comparator.ok()) {
                    return Option<nlohmann::json>(400, "Error with filter field `" + _field.name + "`: " + op_comparator.error());
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

                if(_field.is_int32()) {
                    if(!StringUtils::is_int32_t(filter_value)) {
                        return Option<nlohmann::json>(400, "Error with filter field `" + _field.name + "`: Not an int32.");
                    }
                }

                else if(_field.is_int64()) {
                    if(!StringUtils::is_int64_t(filter_value)) {
                        return Option<nlohmann::json>(400, "Error with filter field `" + _field.name + "`: Not an int64.");
                    }
                }

                else if(_field.is_float()) {
                    if(!StringUtils::is_float(filter_value)) {
                        return Option<nlohmann::json>(400, "Error with filter field `" + _field.name + "`: Not a float.");
                    }
                }

                f = {field_name, {filter_value}, op_comparator.get()};
            }
        } else if(_field.is_bool()) {
            if(raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
                std::vector<std::string> filter_values;
                StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, ",");

                for(std::string & filter_value: filter_values) {
                    if(filter_value != "true" && filter_value != "false") {
                        return Option<nlohmann::json>(400, "Values of filter field `" + _field.name +
                                                      "`: must be `true` or `false`.");
                    }

                    filter_value = (filter_value == "true") ? "1" : "0";
                }

                f = {field_name, filter_values, EQUALS};
            } else {
                if(raw_value != "true" && raw_value != "false") {
                    return Option<nlohmann::json>(400, "Value of filter field `" + _field.name + "` must be `true` or `false`.");
                }
                std::string bool_value = (raw_value == "true") ? "1" : "0";
                f = {field_name, {bool_value}, EQUALS};
            }

        } else if(_field.is_string()) {
            size_t filter_value_index = 0;
            NUM_COMPARATOR str_comparator = CONTAINS;

            if(raw_value[0] == '=') {
                if(!_field.facet) {
                    // EQUALS filtering on string is possible only on facet fields
                    return Option<nlohmann::json>(400, "To perform exact filtering, filter field `" +
                           _field.name + "` must be a facet field.");
                }

                // string filter should be evaluated in strict "equals" mode
                str_comparator = EQUALS;
                while(raw_value[++filter_value_index] == ' ');
            }

            if(raw_value[filter_value_index] == '[' && raw_value[raw_value.size() - 1] == ']') {
                std::vector<std::string> filter_values;
                StringUtils::split(raw_value.substr(filter_value_index+1, raw_value.size() - filter_value_index - 2), filter_values, ",");
                f = {field_name, filter_values, str_comparator};
            } else {
                f = {field_name, {raw_value.substr(filter_value_index)}, str_comparator};
            }
        } else {
            return Option<nlohmann::json>(400, "Error with filter field `" + _field.name + "`: Unidentified field type.");
        }

        filters.push_back(f);
    }

    // validate facet fields
    for(const std::string & field_name: facet_fields) {
        if(facet_schema.count(field_name) == 0) {
            std::string error = "Could not find a facet field named `" + field_name + "` in the schema.";
            return Option<nlohmann::json>(404, error);
        }
        facets.emplace_back(field_name);
    }

    // parse facet query
    std::vector<std::string> facet_query_vec;
    facet_query_t facet_query = {"", ""};

    if(!simple_facet_query.empty() && simple_facet_query.find(':') == std::string::npos) {
        std::string error = "Facet query must be in the `facet_field: value` format.";
        return Option<nlohmann::json>(400, error);
    }

    StringUtils::split(simple_facet_query, facet_query_vec, ":");
    if(!facet_query_vec.empty()) {
        if(facet_fields.empty()) {
            std::string error = "The `facet_query` parameter is supplied without a `facet_by` parameter.";
            return Option<nlohmann::json>(400, error);
        }

        // facet query field must be part of facet fields requested
        facet_query = { StringUtils::trim(facet_query_vec[0]), StringUtils::trim(facet_query_vec[1]) };
        if(std::find(facet_fields.begin(), facet_fields.end(), facet_query.field_name) == facet_fields.end()) {
            std::string error = "Facet query refers to a facet field `" + facet_query.field_name + "` " +
                                "that is not part of `facet_by` parameter.";
            return Option<nlohmann::json>(400, error);
        }

        if(facet_schema.count(facet_query.field_name) == 0) {
            std::string error = "Could not find a facet field named `" + facet_query.field_name + "` in the schema.";
            return Option<nlohmann::json>(404, error);
        }
    }

    // validate sort fields and standardize

    std::vector<sort_by> sort_fields_std;

    for(const sort_by & _sort_field: sort_fields) {
        if(_sort_field.name != sort_field_const::text_match && sort_schema.count(_sort_field.name) == 0) {
            std::string error = "Could not find a field named `" + _sort_field.name + "` in the schema for sorting.";
            return Option<nlohmann::json>(404, error);
        }

        if(sort_schema.count(_sort_field.name) != 0 && sort_schema.at(_sort_field.name).optional) {
            std::string error = "Cannot sort by `" + _sort_field.name + "` as it is defined as an optional field.";
            return Option<nlohmann::json>(400, error);
        }

        std::string sort_order = _sort_field.order;
        StringUtils::toupper(sort_order);

        if(sort_order != sort_field_const::asc && sort_order != sort_field_const::desc) {
            std::string error = "Order for field` " + _sort_field.name + "` should be either ASC or DESC.";
            return Option<nlohmann::json>(400, error);
        }

        sort_fields_std.emplace_back(_sort_field.name, sort_order);
    }

    /*
      1. Empty: [match_score, dsf] upstream
      2. ONE  : [usf, match_score]
      3. TWO  : [usf1, usf2, match_score]
      4. THREE: do nothing
    */
    if(sort_fields_std.empty()) {
        sort_fields_std.emplace_back(sort_field_const::text_match, sort_field_const::desc);
        sort_fields_std.emplace_back(default_sorting_field, sort_field_const::desc);
    }

    bool found_match_score = false;
    for(const auto & sort_field : sort_fields) {
        if(sort_field.name == sort_field_const::text_match) {
            found_match_score = true;
            break;
        }
    }

    if(!found_match_score && sort_fields.size() < 3) {
        sort_fields_std.emplace_back(sort_field_const::text_match, sort_field_const::desc);
    }

    if(sort_fields_std.size() > 3) {
        std::string message = "Only upto 3 sort_by fields can be specified.";
        return Option<nlohmann::json>(422, message);
    }

    // check for valid pagination
    if(page < 1) {
        std::string message = "Page must be an integer of value greater than 0.";
        return Option<nlohmann::json>(422, message);
    }

    if(per_page > PER_PAGE_MAX) {
        std::string message = "Only upto " + std::to_string(PER_PAGE_MAX) + " hits can be fetched per page.";
        return Option<nlohmann::json>(422, message);
    }

    // ensure that (page * per_page) never exceeds number of documents in collection
    const size_t max_hits = std::min((page * per_page), get_num_documents());

    std::vector<std::vector<art_leaf*>> searched_queries;  // search queries used for generating the results
    std::vector<std::vector<KV*>> raw_result_kvs;
    std::vector<std::vector<KV*>> override_result_kvs;

    size_t total_found = 0;
    spp::sparse_hash_set<uint64_t> groups_processed;  // used to calculate total_found for grouped query

    //LOG(INFO) << "Num indices used for querying: " << indices.size();

    // send data to individual index threads
    size_t index_id = 0;
    for(Index* index: indices) {
        index->search_params = new search_args(query, search_fields, filters, facets,
                                               index_to_included_ids[index_id], index_to_excluded_ids[index_id],
                                               sort_fields_std, facet_query, num_typos, max_facet_values, max_hits,
                                               per_page, page, token_order, prefix,
                                               drop_tokens_threshold, typo_tokens_threshold,
                                               group_by_fields, group_limit);
        {
            std::lock_guard<std::mutex> lk(index->m);
            index->ready = true;
            index->processed = false;
        }
        index->cv.notify_one();
        index_id++;
    }

    Option<nlohmann::json> index_search_op({});  // stores the last error across all index threads

    // for grouping we have re-aggregate

    const size_t topster_size = std::max((size_t)1, max_hits);
    Topster topster(topster_size, group_limit);
    Topster curated_topster(topster_size, group_limit);

    for(Index* index: indices) {
        // wait for the worker
        {
            std::unique_lock<std::mutex> lk(index->m);
            index->cv.wait(lk, [index]{return index->processed;});
        }

        if(!index->search_params->outcome.ok()) {
            index_search_op = Option<nlohmann::json>(index->search_params->outcome.code(),
                                                    index->search_params->outcome.error());
        }

        if(!index_search_op.ok()) {
            // we still need to iterate without breaking to release the locks
            continue;
        }

        aggregate_topster(searched_queries.size(), topster, index->search_params->topster);
        aggregate_topster(searched_queries.size(), curated_topster, index->search_params->curated_topster);

        searched_queries.insert(searched_queries.end(), index->search_params->searched_queries.begin(),
                                index->search_params->searched_queries.end());

        for(size_t fi = 0; fi < index->search_params->facets.size(); fi++) {
            auto & this_facet = index->search_params->facets[fi];
            auto & acc_facet = facets[fi];

            for(auto & facet_kv: this_facet.result_map) {
                if(index->search_params->group_limit) {
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
                acc_facet.result_map[facet_kv.first].query_token_pos = facet_kv.second.query_token_pos;
            }

            if(this_facet.stats.fvcount != 0) {
                acc_facet.stats.fvcount += this_facet.stats.fvcount;
                acc_facet.stats.fvsum += this_facet.stats.fvsum;
                acc_facet.stats.fvmax = std::max(acc_facet.stats.fvmax, this_facet.stats.fvmax);
                acc_facet.stats.fvmin = std::min(acc_facet.stats.fvmin, this_facet.stats.fvmin);
            }
        }

        if(group_limit) {
            groups_processed.insert(
                index->search_params->groups_processed.begin(),
                index->search_params->groups_processed.end()
            );
        } else {
            total_found += index->search_params->all_result_ids_len;
        }
    }

    if(!index_search_op.ok()) {
        return index_search_op;
    }

    topster.sort();
    curated_topster.sort();

    populate_result_kvs(&topster, raw_result_kvs);
    populate_result_kvs(&curated_topster, override_result_kvs);

    // for grouping we have to aggregate group set sizes to a count value
    if(group_limit) {
        for(auto& acc_facet: facets) {
            for(auto& facet_kv: acc_facet.result_map) {
                facet_kv.second.count = facet_kv.second.groups.size();
            }
        }

        total_found = groups_processed.size() + override_result_kvs.size();
    }

    // All fields are sorted descending
    std::sort(raw_result_kvs.begin(), raw_result_kvs.end(), Topster::is_greater_kv_group);

    // Sort based on position in overridden list
    std::sort(
      override_result_kvs.begin(), override_result_kvs.end(),
      [](const std::vector<KV*>& a, std::vector<KV*>& b) -> bool {
          return a[0]->distinct_key < b[0]->distinct_key;
      }
    );

    std::vector<std::vector<KV*>> result_group_kvs;
    size_t override_kv_index = 0;
    size_t raw_results_index = 0;

    // merge raw results and override results
    while(override_kv_index < override_result_kvs.size() && raw_results_index < raw_result_kvs.size()) {
        size_t result_position = result_group_kvs.size() + 1;
        uint64_t override_position = override_result_kvs[override_kv_index][0]->distinct_key;

        if(result_position == override_position) {
            override_result_kvs[override_kv_index][0]->match_score = 0;  // to identify curated result
            result_group_kvs.push_back(override_result_kvs[override_kv_index]);
            override_kv_index++;
        } else {
            result_group_kvs.push_back(raw_result_kvs[raw_results_index]);
            raw_results_index++;
        }
    }

    while(override_kv_index < override_result_kvs.size()) {
        override_result_kvs[override_kv_index][0]->match_score = 0;  // to identify curated result
        result_group_kvs.push_back({override_result_kvs[override_kv_index]});
        override_kv_index++;
    }

    while(raw_results_index < raw_result_kvs.size()) {
        result_group_kvs.push_back(raw_result_kvs[raw_results_index]);
        raw_results_index++;
    }

    const long start_result_index = (page - 1) * per_page;
    const long end_result_index = std::min(max_hits, result_group_kvs.size()) - 1;  // could be -1 when max_hits is 0

    nlohmann::json result = nlohmann::json::object();

    result["found"] = total_found;

    std::string hits_key = group_limit ? "grouped_hits" : "hits";
    result[hits_key] = nlohmann::json::array();

    // construct results array
    for(long result_kvs_index = start_result_index; result_kvs_index <= end_result_index; result_kvs_index++) {
        const std::vector<KV*> & kv_group = result_group_kvs[result_kvs_index];

        nlohmann::json group_hits;
        if(group_limit) {
            group_hits["hits"] = nlohmann::json::array();
        }

        nlohmann::json& hits_array = group_limit ? group_hits["hits"] : result["hits"];

        for(const KV* field_order_kv: kv_group) {
            const std::string& seq_id_key = get_seq_id_key((uint32_t) field_order_kv->key);

            nlohmann::json document;
            const Option<bool> & document_op = get_document_from_store(seq_id_key, document);

            if(!document_op.ok()) {
                LOG(ERROR) << "Document fetch error. " << document_op.error();
                continue;
            }

            nlohmann::json wrapper_doc;
            wrapper_doc["highlights"] = nlohmann::json::array();
            std::vector<highlight_t> highlights;
            StringUtils string_utils;

            // find out if fields have to be highlighted fully
            std::vector<std::string> fields_highlighted_fully_vec;
            spp::sparse_hash_set<std::string> fields_highlighted_fully;
            StringUtils::split(highlight_full_fields, fields_highlighted_fully_vec, ",");

            for(std::string & highlight_full_field: fields_highlighted_fully_vec) {
                StringUtils::trim(highlight_full_field);
                fields_highlighted_fully.emplace(highlight_full_field);
            }

            for(const std::string & field_name: search_fields) {
                // should not pick excluded field for highlighting
                if(exclude_fields.count(field_name) > 0) {
                    continue;
                }

                field search_field = search_schema.at(field_name);
                if(query != "*" && (search_field.type == field_types::STRING ||
                                    search_field.type == field_types::STRING_ARRAY)) {

                    bool highlighted_fully = (fields_highlighted_fully.find(field_name) != fields_highlighted_fully.end());
                    highlight_t highlight;
                    highlight_result(search_field, searched_queries, field_order_kv, document,
                                     string_utils, snippet_threshold, highlight_affix_num_tokens,
                                     highlighted_fully, highlight);

                    if(!highlight.snippets.empty()) {
                        highlights.push_back(highlight);
                    }
                }
            }

            std::sort(highlights.begin(), highlights.end());

            for(const auto & highlight: highlights) {
                nlohmann::json h_json = nlohmann::json::object();
                h_json["field"] = highlight.field;
                bool highlight_fully = (fields_highlighted_fully.find(highlight.field) != fields_highlighted_fully.end());

                if(!highlight.indices.empty()) {
                    h_json["indices"] = highlight.indices;
                    h_json["snippets"] = highlight.snippets;
                    if(highlight_fully) {
                        h_json["values"] = highlight.values;
                    }

                } else {
                    h_json["snippet"] = highlight.snippets[0];
                    if(highlight_fully) {
                        h_json["value"] = highlight.values[0];
                    }
                }

                wrapper_doc["highlights"].push_back(h_json);
            }

            //wrapper_doc["seq_id"] = (uint32_t) field_order_kv->key;

            prune_document(document, include_fields, exclude_fields);
            wrapper_doc["document"] = document;
            wrapper_doc["text_match"] = field_order_kv->match_score;

            if(field_order_kv->match_score == 0) {
                wrapper_doc["curated"] = true;
            }

            hits_array.push_back(wrapper_doc);
        }

        if(group_limit) {
            const auto& document = group_hits["hits"][0]["document"];

            group_hits["group_key"] = nlohmann::json::array();
            for(const auto& field_name: group_by_fields) {
                if(document.count(field_name) != 0) {
                    group_hits["group_key"].push_back(document[field_name]);
                }
            }

            result["grouped_hits"].push_back(group_hits);
        }
    }

    result["facet_counts"] = nlohmann::json::array();

    // populate facets
    for(const facet & a_facet: facets) {
        nlohmann::json facet_result = nlohmann::json::object();
        facet_result["field_name"] = a_facet.field_name;
        facet_result["counts"] = nlohmann::json::array();

        std::vector<std::pair<int64_t, facet_count_t>> facet_hash_counts;
        for (const auto & kv : a_facet.result_map) {
            facet_hash_counts.emplace_back(kv);
        }

        // keep only top K facets
        auto max_facets = std::min(max_facet_values, facet_hash_counts.size());
        std::nth_element(facet_hash_counts.begin(), facet_hash_counts.begin() + max_facets,
                         facet_hash_counts.end(), Collection::facet_count_compare);


        std::vector<std::string> facet_query_tokens;
        StringUtils::split(facet_query.query, facet_query_tokens, " ");

        std::vector<facet_value_t> facet_values;

        for(size_t fi = 0; fi < max_facets; fi++) {
            // remap facet value hash with actual string
            auto & kv = facet_hash_counts[fi];
            auto & facet_count = kv.second;

            // fetch actual facet value from representative doc id
            const std::string& seq_id_key = get_seq_id_key((uint32_t) facet_count.doc_id);
            nlohmann::json document;
            const Option<bool> & document_op = get_document_from_store(seq_id_key, document);

            if(!document_op.ok()) {
                LOG(ERROR) << "Facet fetch error. " << document_op.error();
                continue;
            }

            std::string value;
            facet_value_to_string(a_facet, facet_count, document, value);

            std::vector<std::string> tokens;
            StringUtils::split(value, tokens, " ");
            std::stringstream highlightedss;

            // invert query_pos -> token_pos
            spp::sparse_hash_map<uint32_t, uint32_t> token_query_pos;
            for(auto qtoken_pos: facet_count.query_token_pos) {
                token_query_pos.emplace(qtoken_pos.second.pos, qtoken_pos.first);
            }

            for(size_t i = 0; i < tokens.size(); i++) {
                if(i != 0) {
                    highlightedss << " ";
                }

                if(token_query_pos.count(i) != 0) {
                    size_t query_token_len = facet_query_tokens[token_query_pos[i]].size();
                    // handle query token being larger than actual token (typo correction)
                    query_token_len = std::min(query_token_len, tokens[i].size());
                    const std::string & unmarked = tokens[i].substr(query_token_len, std::string::npos);
                    highlightedss << "<mark>" + tokens[i].substr(0, query_token_len) + "</mark>" + unmarked;
                } else {
                    highlightedss << tokens[i];
                }
            }

            facet_value_t facet_value = {value, highlightedss.str(), facet_count.count};
            facet_values.emplace_back(facet_value);
        }

        std::stable_sort(facet_values.begin(), facet_values.end(), Collection::facet_count_str_compare);

        for(const auto & facet_count: facet_values) {
            nlohmann::json facet_value_count = nlohmann::json::object();
            const std::string & value = facet_count.value;

            facet_value_count["value"] = value;
            facet_value_count["highlighted"] = facet_count.highlighted;
            facet_value_count["count"] = facet_count.count;
            facet_result["counts"].push_back(facet_value_count);
        }

        // add facet value stats
        facet_result["stats"] = nlohmann::json::object();
        if(a_facet.stats.fvcount != 0) {
            facet_result["stats"]["min"] = a_facet.stats.fvmin;
            facet_result["stats"]["max"] = a_facet.stats.fvmax;
            facet_result["stats"]["sum"] = a_facet.stats.fvsum;
            facet_result["stats"]["avg"] = (a_facet.stats.fvsum / a_facet.stats.fvcount);
        }

        result["facet_counts"].push_back(facet_result);
    }

    // free search params
    for(Index* index: indices) {
        delete index->search_params;
    }

    result["request_params"] = nlohmann::json::object();;
    result["request_params"]["per_page"] = per_page;
    result["request_params"]["q"] = query;

    //long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    //!LOG(INFO) << "Time taken for result calc: " << timeMillis << "us";
    //!store->print_memory_usage();
    return result;
}


void Collection::populate_result_kvs(Topster *topster, std::vector<std::vector<KV *>> &result_kvs) const {
    if(topster->distinct) {
        for(auto &group_topster_entry: topster->group_kv_map) {
            Topster* group_topster = group_topster_entry.second;
            const std::vector<KV*> group_kvs(group_topster->kvs, group_topster->kvs+group_topster->size);
            result_kvs.emplace_back(group_kvs);
        }

    } else {
        for(uint32_t t = 0; t < topster->size; t++) {
            KV* kv = topster->getKV(t);
            result_kvs.push_back({kv});
        }
    }
}

void Collection::aggregate_topster(size_t query_index, Topster &topster, Topster *index_topster) const {
    if(index_topster->distinct) {
        for(auto &group_topster_entry: index_topster->group_kv_map) {
            Topster* group_topster = group_topster_entry.second;
            const std::vector<KV*> group_kvs(group_topster->kvs, group_topster->kvs+group_topster->size);
            for(KV* kv: group_kvs) {
                kv->query_index += query_index;
                topster.add(kv);
            }
        }

    } else {
        for(uint32_t t = 0; t < index_topster->size; t++) {
            KV* kv = index_topster->getKV(t);
            kv->query_index += query_index;
            topster.add(kv);
        }
    }
}

void Collection::facet_value_to_string(const facet &a_facet, const facet_count_t &facet_count,
                                       const nlohmann::json &document, std::string &value) {

    if(facet_schema.at(a_facet.field_name).type == field_types::STRING) {
        value =  document[a_facet.field_name];
    } else if(facet_schema.at(a_facet.field_name).type == field_types::STRING_ARRAY) {
        value = document[a_facet.field_name][facet_count.array_pos];
    } else if(facet_schema.at(a_facet.field_name).type == field_types::INT32) {
        int32_t raw_val = document[a_facet.field_name].get<int32_t>();
        value = std::to_string(raw_val);
    } else if(facet_schema.at(a_facet.field_name).type == field_types::INT32_ARRAY) {
        int32_t raw_val = document[a_facet.field_name][facet_count.array_pos].get<int32_t>();
        value = std::to_string(raw_val);
    } else if(facet_schema.at(a_facet.field_name).type == field_types::INT64) {
        int64_t raw_val = document[a_facet.field_name].get<int64_t>();
        value = std::to_string(raw_val);
    } else if(facet_schema.at(a_facet.field_name).type == field_types::INT64_ARRAY) {
        int64_t raw_val = document[a_facet.field_name][facet_count.array_pos].get<int64_t>();
        value = std::to_string(raw_val);
    } else if(facet_schema.at(a_facet.field_name).type == field_types::FLOAT) {
        float raw_val = document[a_facet.field_name].get<float>();
        value = std::to_string(raw_val);
        value.erase ( value.find_last_not_of('0') + 1, std::string::npos ); // remove trailing zeros
    } else if(facet_schema.at(a_facet.field_name).type == field_types::FLOAT_ARRAY) {
        float raw_val = document[a_facet.field_name][facet_count.array_pos].get<float>();
        value = std::to_string(raw_val);
        value.erase ( value.find_last_not_of('0') + 1, std::string::npos );  // remove trailing zeros
    } else if(facet_schema.at(a_facet.field_name).type == field_types::BOOL) {
        value = std::to_string(document[a_facet.field_name].get<bool>());
        value = (value == "1") ? "true" : "false";
    } else if(facet_schema.at(a_facet.field_name).type == field_types::BOOL_ARRAY) {
        value = std::to_string(document[a_facet.field_name][facet_count.array_pos].get<bool>());
        value = (value == "1") ? "true" : "false";
    }
}

void Collection::highlight_result(const field &search_field,
                                  const std::vector<std::vector<art_leaf *>> &searched_queries,
                                  const KV* field_order_kv, const nlohmann::json & document,
                                  StringUtils & string_utils,
                                  const size_t snippet_threshold,
                                  const size_t highlight_affix_num_tokens,
                                  bool highlighted_fully,
                                  highlight_t & highlight) {

    std::vector<uint32_t*> leaf_to_indices;
    std::vector<art_leaf *> query_suggestion;

    for (const art_leaf *token_leaf : searched_queries[field_order_kv->query_index]) {
        // Must search for the token string fresh on that field for the given document since `token_leaf`
        // is from the best matched field and need not be present in other fields of a document.
        Index* index = indices[field_order_kv->key % num_memory_shards];
        art_leaf *actual_leaf = index->get_token_leaf(search_field.name, &token_leaf->key[0], token_leaf->key_len);
        if(actual_leaf != nullptr) {
            query_suggestion.push_back(actual_leaf);
            std::vector<uint16_t> positions;
            uint32_t doc_index = actual_leaf->values->ids.indexOf(field_order_kv->key);
            auto doc_indices = new uint32_t[1];
            doc_indices[0] = doc_index;
            leaf_to_indices.push_back(doc_indices);
        }
    }

    if(query_suggestion.empty()) {
        // none of the tokens from the query were found on this field
        free_leaf_indices(leaf_to_indices);
        return ;
    }

    // positions in the field of each token in the query
    std::unordered_map<size_t, std::vector<std::vector<uint16_t>>> array_token_positions;
    Index::populate_token_positions(query_suggestion, leaf_to_indices, 0, array_token_positions);

    std::vector<match_index_t> match_indices;

    for(const auto& kv: array_token_positions) {
        const std::vector<std::vector<uint16_t>>& token_positions = kv.second;
        size_t array_index = kv.first;

        if(token_positions.empty()) {
            continue;
        }

        const Match & this_match = Match(field_order_kv->key, token_positions);
        uint64_t this_match_score = this_match.get_match_score(1, field_order_kv->field_id);
        match_indices.emplace_back(this_match, this_match_score, array_index);

        /*LOG(INFO) << "doc_id: " << document["id"]   << ", words_present: " << size_t(this_match.words_present)
                                               << ", match_score: " << this_match_score
                                               << ", match.distance: " << size_t(this_match.distance);*/
    }

    if(match_indices.empty()) {
        // none of the tokens from the query were found on this field
        free_leaf_indices(leaf_to_indices);
        return ;
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

        std::vector<size_t> token_indices;
        spp::sparse_hash_set<std::string> token_hits;

        for(size_t i = 0; i < match.offsets.size(); i++) {
            if(match.offsets[i].offset != MAX_DISPLACEMENT) {
                size_t token_index = (size_t)(match.offsets[i].offset);
                token_indices.push_back(token_index);
                if(token_index >= tokens.size()) {
                    LOG(ERROR) << "Highlight token index " << token_index << " is greater than length of store field.";
                    continue;
                }
                std::string token = tokens[token_index];
                string_utils.unicode_normalize(token);
                token_hits.insert(token);
            }
        }

        auto minmax = std::minmax_element(token_indices.begin(), token_indices.end());

        size_t prefix_length = highlight_affix_num_tokens;
        size_t suffix_length = highlight_affix_num_tokens + 1;

        // For longer strings, pick surrounding tokens within 4 tokens of min_index and max_index for the snippet
        const size_t start_index = (tokens.size() <= snippet_threshold) ? 0 :
                                   std::max(0, (int)(*(minmax.first) - prefix_length));

        const size_t end_index = (tokens.size() <= snippet_threshold) ? tokens.size() :
                                 std::min((int)tokens.size(), (int)(*(minmax.second) + suffix_length));

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

        if(highlighted_fully) {
            std::stringstream value_stream;
            for(size_t value_index = 0; value_index < tokens.size(); value_index++) {
                if(value_index != 0) {
                    value_stream << " ";
                }

                std::string token = tokens[value_index];
                string_utils.unicode_normalize(token);

                if(token_hits.count(token) != 0) {
                    value_stream << "<mark>" + tokens[value_index] + "</mark>";
                } else {
                    value_stream << tokens[value_index];
                }
            }

            highlight.values.push_back(value_stream.str());
        }
    }

    highlight.field = search_field.name;
    highlight.match_score = match_indices[0].match_score;

    free_leaf_indices(leaf_to_indices);
}

void Collection::free_leaf_indices(std::vector<uint32_t*>& leaf_to_indices) const {
    for(uint32_t* leaf_indices: leaf_to_indices) {
        delete [] leaf_indices;
    }
}

bool Collection::doc_exists(const std::string & id) {
    return store->contains(get_doc_id_key(id));
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

    uint32_t seq_id = (uint32_t) std::stoul(seq_id_str);

    std::string parsed_document;
    StoreStatus doc_status = store->get(get_seq_id_key(seq_id), parsed_document);

    if(doc_status == StoreStatus::NOT_FOUND) {
        LOG(ERROR) << "Sequence ID exists, but document is missing for id: " << id;
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

void Collection::remove_document(const nlohmann::json & document, const uint32_t seq_id, bool remove_from_store) {
    const std::string& id = document["id"];

    Index* index = indices[seq_id % num_memory_shards];
    index->remove(seq_id, document);
    num_documents -= 1;

    if(remove_from_store) {
        store->remove(get_doc_id_key(id));
        store->remove(get_seq_id_key(seq_id));
    }
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

    uint32_t seq_id = (uint32_t) std::stoul(seq_id_str);

    std::string parsed_document;
    StoreStatus doc_status = store->get(get_seq_id_key(seq_id), parsed_document);

    if(doc_status == StoreStatus::NOT_FOUND) {
        LOG(ERROR) << "Sequence ID exists, but document is missing for id: " << id;
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

    remove_document(document, seq_id, remove_from_store);
    return Option<std::string>(id);
}

Option<uint32_t> Collection::add_override(const override_t & override) {
    if(overrides.count("id") != 0) {
        return Option<uint32_t>(409, "There is already another entry with that `id`.");
    }

    bool inserted = store->insert(Collection::get_override_key(name, override.id), override.to_json().dump());
    if(!inserted) {
        return Option<uint32_t>(500, "Error while storing the override on disk.");
    }

    overrides.emplace(override.id, override);
    return Option<uint32_t>(200);
}

Option<uint32_t> Collection::remove_override(const std::string & id) {
    if(overrides.count(id) != 0) {
        bool removed = store->remove(Collection::get_override_key(name, id));
        if(!removed) {
            return Option<uint32_t>(500, "Error while deleting the override from disk.");
        }
        overrides.erase(id);
        return Option<uint32_t>(200);
    }

    return Option<uint32_t>(404, "Could not find that `id`.");
}

size_t Collection::get_num_memory_shards() {
    return num_memory_shards;
}

uint32_t Collection::get_seq_id_from_key(const std::string & key) {
    // last 4 bytes of the key would be the serialized version of the sequence id
    std::string serialized_seq_id = key.substr(key.length() - 4);
    return StringUtils::deserialize_uint32_t(serialized_seq_id);
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

uint64_t Collection::get_created_at() {
    return created_at;
}

size_t Collection::get_num_documents() {
    return num_documents;
}

uint32_t Collection::get_collection_id() {
    return collection_id;
}

Option<uint32_t> Collection::doc_id_to_seq_id(const std::string & doc_id) {
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

std::string Collection::get_override_key(const std::string & collection_name, const std::string & override_id) {
    return std::string(COLLECTION_OVERRIDE_PREFIX) + "_" + collection_name + "_" + override_id;
}

std::string Collection::get_seq_id_collection_prefix() {
    return std::to_string(collection_id) + "_" + std::string(SEQ_ID_PREFIX);
}

std::string Collection::get_default_sorting_field() {
    return default_sorting_field;
}

Option<bool> Collection::get_document_from_store(const std::string &seq_id_key, nlohmann::json & document) {
    std::string json_doc_str;
    StoreStatus json_doc_status = store->get(seq_id_key, json_doc_str);

    if(json_doc_status != StoreStatus::FOUND) {
        return Option<bool>(500, "Could not locate the JSON document for sequence ID: " + seq_id_key);
    }

    try {
        document = nlohmann::json::parse(json_doc_str);
    } catch(...) {
        return Option<bool>(500, "Error while parsing stored document with sequence ID: " + seq_id_key);
    }

    return Option<bool>(true);
}

const std::vector<Index *> &Collection::_get_indexes() const {
    return indices;
}
