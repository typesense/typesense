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
#include <tokenizer.h>
#include <collection_manager.h>
#include <h3api.h>
#include <regex>
#include <list>
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
                       const float max_memory_ratio, const std::string& fallback_field_type):
        name(name), collection_id(collection_id), created_at(created_at),
        next_seq_id(next_seq_id), store(store),
        fields(fields), default_sorting_field(default_sorting_field),
        num_memory_shards(num_memory_shards),
        max_memory_ratio(max_memory_ratio),
        fallback_field_type(fallback_field_type), dynamic_fields({}),
        indices(init_indices()) {

    this->num_documents = 0;
}

Collection::~Collection() {
    std::unique_lock lock(mutex);

    for(size_t i = 0; i < indices.size(); i++) {
        delete indices[i];
    }
}

uint32_t Collection::get_next_seq_id() {
    std::shared_lock lock(mutex);
    store->increment(get_next_seq_id_key(name), 1);
    return next_seq_id++;
}

Option<doc_seq_id_t> Collection::to_doc(const std::string & json_str, nlohmann::json& document,
                                        const index_operation_t& operation,
                                        const DIRTY_VALUES dirty_values,
                                        const std::string& id) {
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

nlohmann::json Collection::get_summary_json() const {
    std::shared_lock lock(mutex);

    nlohmann::json json_response;

    json_response["name"] = name;
    json_response["num_memory_shards"] = num_memory_shards.load();
    json_response["num_documents"] = num_documents.load();
    json_response["created_at"] = created_at.load();

    nlohmann::json fields_arr;

    for(const field & coll_field: fields) {
        nlohmann::json field_json;
        field_json[fields::name] = coll_field.name;
        field_json[fields::type] = coll_field.type;
        field_json[fields::facet] = coll_field.facet;
        field_json[fields::optional] = coll_field.optional;
        field_json[fields::index] = coll_field.index;

        fields_arr.push_back(field_json);
    }

    json_response["fields"] = fields_arr;
    json_response["default_sorting_field"] = default_sorting_field;
    return json_response;
}

Option<nlohmann::json> Collection::add(const std::string & json_str,
                                       const index_operation_t& operation, const std::string& id,
                                       const DIRTY_VALUES& dirty_values) {
    nlohmann::json document;
    std::vector<std::string> json_lines = {json_str};
    const nlohmann::json& res = add_many(json_lines, document, operation, id, dirty_values);

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

nlohmann::json Collection::add_many(std::vector<std::string>& json_lines, nlohmann::json& document,
                                    const index_operation_t& operation, const std::string& id,
                                    const DIRTY_VALUES& dirty_values) {
    //LOG(INFO) << "Memory ratio. Max = " << max_memory_ratio << ", Used = " << SystemMetrics::used_memory_ratio();
    std::vector<std::vector<index_record>> iter_batch;

    for(size_t i = 0; i < num_memory_shards; i++) {
        iter_batch.emplace_back(std::vector<index_record>());
    }

    const size_t index_batch_size = 1000;
    size_t num_indexed = 0;
    //bool exceeds_memory_limit = false;

    for(size_t i=0; i < json_lines.size(); i++) {
        const std::string & json_line = json_lines[i];
        Option<doc_seq_id_t> doc_seq_id_op = to_doc(json_line, document, operation, dirty_values, id);

        const uint32_t seq_id = doc_seq_id_op.ok() ? doc_seq_id_op.get().seq_id : 0;
        index_record record(i, seq_id, document, operation, dirty_values);

        // NOTE: we overwrite the input json_lines with result to avoid memory pressure

        record.is_update = false;

        if(!doc_seq_id_op.ok()) {
            record.index_failure(doc_seq_id_op.code(), doc_seq_id_op.error());
        } else {
            record.is_update = !doc_seq_id_op.get().is_new;
            if(record.is_update) {
                get_document_from_store(get_seq_id_key(seq_id), record.old_doc);
            }

            // if `fallback_field_type` or `dynamic_fields` is enabled, update schema first before indexing
            if(!fallback_field_type.empty() || !dynamic_fields.empty()) {
                Option<bool> schema_change_op = check_and_update_schema(record.doc, dirty_values);
                if(!schema_change_op.ok()) {
                    record.index_failure(schema_change_op.code(), schema_change_op.error());
                }
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
                // to return the document for the single doc add cases
                if(iter_batch[i_index].size() == 1) {
                    const auto& rec = iter_batch[i_index][0];
                    document = rec.is_update ? rec.new_doc : rec.doc;
                }
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
                        LOG(ERROR) << "Update to disk failed. Will restore old document";
                        remove_document(index_record.new_doc, index_record.seq_id, false);
                        index_in_memory(index_record.old_doc, index_record.seq_id, index_record.operation, index_record.dirty_values);
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
                        LOG(ERROR) << "Write to disk failed. Will restore old document";
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

            json_out[index_record.position] = res.dump(-1, ' ', false,
                                                       nlohmann::detail::error_handler_t::ignore);
        }
    }
}

Option<uint32_t> Collection::index_in_memory(nlohmann::json &document, uint32_t seq_id,
                                             const index_operation_t op, const DIRTY_VALUES& dirty_values) {
    std::unique_lock lock(mutex);

    Option<uint32_t> validation_op = Index::validate_index_in_memory(document, seq_id, default_sorting_field,
                                                                     search_schema, facet_schema, op,
                                                                     fallback_field_type, dirty_values);

    if(!validation_op.ok()) {
        return validation_op;
    }

    Index* index = indices[seq_id % num_memory_shards];
    index->index_in_memory(document, seq_id, default_sorting_field, op);

    num_documents += 1;
    return Option<>(200);
}

size_t Collection::par_index_in_memory(std::vector<std::vector<index_record>> & iter_batch,
                                       std::vector<size_t>& indexed_counts) {
    std::unique_lock lock(mutex);

    size_t num_processed = 0;
    std::mutex m_process;
    std::condition_variable cv_process;

    std::vector<size_t> num_indexed_vec(indices.size());

    for(size_t index_id = 0; index_id < indices.size(); index_id++) {
        Index* index = indices[index_id];
        CollectionManager::get_instance().get_thread_pool()->enqueue(
        [index, index_id, &num_indexed_vec, &iter_batch, this, &m_process, &num_processed, &cv_process]() {
            size_t num_indexed = Index::batch_memory_index(index, std::ref(iter_batch[index_id]), default_sorting_field,
                                      search_schema, facet_schema, fallback_field_type);
            std::unique_lock<std::mutex> lock(m_process);
            num_indexed_vec[index_id] = num_indexed;
            num_processed++;
            cv_process.notify_one();
        });
    }

    const size_t num_indices = indices.size();
    std::unique_lock<std::mutex> lock_process(m_process);
    cv_process.wait(lock_process, [&](){ return num_processed == num_indices; });

    size_t num_indexed = 0;

    for(size_t index_id = 0; index_id < indices.size(); index_id++) {
        num_documents += num_indexed_vec[index_id];
        num_indexed += num_indexed_vec[index_id];
        indexed_counts[index_id] = num_indexed_vec[index_id];
    }

    return num_indexed;
}

void Collection::prune_document(nlohmann::json &document, const spp::sparse_hash_set<std::string>& include_fields,
                                const spp::sparse_hash_set<std::string>& exclude_fields) {
    auto it = document.begin();
    for(; it != document.end(); ) {
        if (exclude_fields.count(it.key()) != 0 ||
           (!include_fields.empty() && include_fields.count(it.key()) == 0)) {
            it = document.erase(it);
        } else {
            ++it;
        }
    }
}

void Collection::curate_results(std::string query,
                                const std::map<size_t, std::vector<std::string>>& pinned_hits,
                                const std::vector<std::string>& hidden_hits,
                                std::map<size_t, std::vector<uint32_t>>& include_ids,
                                std::vector<uint32_t> & excluded_ids,
                                bool enable_overrides) const {
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

    if(enable_overrides) {
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

Option<nlohmann::json> Collection::search(const std::string & query, const std::vector<std::string>& search_fields,
                                  const std::string & simple_filter_query, const std::vector<std::string>& facet_fields,
                                  const std::vector<sort_by> & sort_fields, const std::vector<uint32_t>& num_typos,
                                  const size_t per_page, const size_t page,
                                  token_ordering token_order, const std::vector<bool>& prefixes,
                                  const size_t drop_tokens_threshold,
                                  const spp::sparse_hash_set<std::string> & include_fields,
                                  const spp::sparse_hash_set<std::string> & exclude_fields,
                                  const size_t max_facet_values,
                                  const std::string & simple_facet_query,
                                  const size_t snippet_threshold,
                                  const size_t highlight_affix_num_tokens,
                                  const std::string& highlight_full_fields,
                                  size_t typo_tokens_threshold,
                                  const std::string& pinned_hits_str,
                                  const std::string& hidden_hits_str,
                                  const std::vector<std::string>& group_by_fields,
                                  const size_t group_limit,
                                  const std::string& highlight_start_tag,
                                  const std::string& highlight_end_tag,
                                  std::vector<size_t> query_by_weights,
                                  size_t limit_hits,
                                  bool prioritize_exact_match,
                                  bool pre_segmented_query,
                                  bool enable_overrides,
                                  const std::string& highlight_fields) const {

    std::shared_lock lock(mutex);

    if(query != "*" && search_fields.empty()) {
        return Option<nlohmann::json>(400, "No search fields specified for the query.");
    }

    if(!search_fields.empty() && !query_by_weights.empty() && search_fields.size() != query_by_weights.size()) {
        return Option<nlohmann::json>(400, "Number of weights in `query_by_weights` does not match "
                                           "number of `query_by` fields.");
    }

    if(!group_by_fields.empty() && (group_limit == 0 || group_limit > GROUP_LIMIT_MAX)) {
        return Option<nlohmann::json>(400, "Value of `group_limit` must be between 1 and " +
                                      std::to_string(GROUP_LIMIT_MAX) + ".");
    }

    if(!search_fields.empty() && search_fields.size() != num_typos.size()) {
        if(num_typos.size() != 1) {
            return Option<nlohmann::json>(400, "Number of weights in `num_typos` does not match "
                                               "number of `query_by` fields.");
        }
    }

    if(!search_fields.empty() && search_fields.size() != prefixes.size()) {
        if(prefixes.size() != 1) {
            return Option<nlohmann::json>(400, "Number of prefix values in `prefix` does not match "
                                               "number of `query_by` fields.");
        }
    }

    std::vector<uint32_t> excluded_ids;
    std::map<size_t, std::vector<uint32_t>> include_ids; // position => list of IDs
    std::map<size_t, std::vector<std::string>> pinned_hits;

    Option<bool> pinned_hits_op = parse_pinned_hits(pinned_hits_str, pinned_hits);

    if(!pinned_hits_op.ok()) {
        return Option<nlohmann::json>(400, pinned_hits_op.error());
    }

    std::vector<std::string> hidden_hits;
    StringUtils::split(hidden_hits_str, hidden_hits, ",");

    curate_results(query, pinned_hits, hidden_hits, include_ids, excluded_ids, enable_overrides);

    /*for(auto& kv: include_ids) {
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

    // process weights for search fields
    std::vector<search_field_t> weighted_search_fields;
    size_t max_weight = 100;

    if(query_by_weights.empty()) {
        max_weight = search_fields.size();
        for(size_t i=1; i <= search_fields.size(); i++) {
            query_by_weights.push_back((max_weight - i) + 1);
        }
    } else {
        max_weight = *std::max_element(query_by_weights.begin(), query_by_weights.end());
    }

    for(size_t i=0; i < search_fields.size(); i++) {
        const auto& search_field = search_fields[i];
        const auto priority = (max_weight - query_by_weights[i]) + 1;
        const auto weight = query_by_weights[i] + 1;
        weighted_search_fields.push_back({search_field, priority, weight});
    }

    std::vector<facet> facets;

    // validate search fields
    for(const std::string & field_name: search_fields) {
        if(search_schema.count(field_name) == 0) {
            std::string error = "Could not find a field named `" + field_name + "` in the schema.";
            return Option<nlohmann::json>(404, error);
        }

        field search_field = search_schema.at(field_name);

        if(!search_field.index) {
            std::string error = "Field `" + field_name + "` is marked as a non-indexed field in the schema.";
            return Option<nlohmann::json>(404, error);
        }

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

    std::vector<filter> filters;
    Option<bool> parse_filter_op = parse_filter_query(simple_filter_query, filters);
    if(!parse_filter_op.ok()) {
        return Option<nlohmann::json>(parse_filter_op.code(), parse_filter_op.error());
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
    facet_query_t facet_query = {"", ""};

    if(!simple_facet_query.empty()) {
        size_t found_colon_index = simple_facet_query.find(':');

        if(found_colon_index == std::string::npos) {
            std::string error = "Facet query must be in the `facet_field: value` format.";
            return Option<nlohmann::json>(400, error);
        }

        if(facet_fields.empty()) {
            std::string error = "The `facet_query` parameter is supplied without a `facet_by` parameter.";
            return Option<nlohmann::json>(400, error);
        }

        std::string&& facet_query_fname = simple_facet_query.substr(0, found_colon_index);
        StringUtils::trim(facet_query_fname);

        std::string&& facet_query_value = simple_facet_query.substr(found_colon_index+1, std::string::npos);
        StringUtils::trim(facet_query_value);

        if(facet_query_value.empty()) {
            // empty facet value, we will treat it as no facet query
            facet_query = {"", ""};
        } else {
            // facet query field must be part of facet fields requested
            facet_query = { StringUtils::trim(facet_query_fname), facet_query_value };
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
    }

    // validate sort fields and standardize

    std::vector<sort_by> sort_fields_std;

    for(const sort_by& _sort_field: sort_fields) {
        sort_by sort_field_std(_sort_field.name, _sort_field.order);

        if(sort_field_std.name.back() == ')') {
            // check if this is a geo field
            size_t paran_start = 0;
            while(paran_start < sort_field_std.name.size() && sort_field_std.name[paran_start] != '(') {
                paran_start++;
            }

            const std::string& actual_field_name = sort_field_std.name.substr(0, paran_start);

            if(sort_schema.count(actual_field_name) == 0) {
                std::string error = "Could not find a field named `" + actual_field_name + "` in the schema for sorting.";
                return Option<nlohmann::json>(404, error);
            }

            const std::string& geo_coordstr = sort_field_std.name.substr(paran_start+1, sort_field_std.name.size() - paran_start - 2);

            // e.g. geopoint_field(lat1, lng1, exclude_radius: 10 miles)

            std::vector<std::string> geo_parts;
            StringUtils::split(geo_coordstr, geo_parts, ",");

            std::string error = "Bad syntax for geopoint sorting field `" + actual_field_name + "`";

            if(geo_parts.size() != 2 && geo_parts.size() != 3) {
                return Option<nlohmann::json>(400, error);
            }

            if(!StringUtils::is_float(geo_parts[0]) || !StringUtils::is_float(geo_parts[1])) {
                return Option<nlohmann::json>(400, error);
            }

            if(geo_parts.size() == 3) {
                // try to parse the exclude radius option
                bool is_exclude_option = false;

                if(StringUtils::begins_with(geo_parts[2], sort_field_const::exclude_radius)) {
                    is_exclude_option = true;
                } else if(StringUtils::begins_with(geo_parts[2], sort_field_const::precision)) {
                    is_exclude_option = false;
                } else {
                    return Option<nlohmann::json>(400, error);
                }

                std::vector<std::string> param_parts;
                StringUtils::split(geo_parts[2], param_parts, ":");

                if(param_parts.size() != 2) {
                    return Option<nlohmann::json>(400, error);
                }

                std::vector<std::string> param_value_parts;
                StringUtils::split(param_parts[1], param_value_parts, " ");

                if(param_value_parts.size() != 2) {
                    return Option<nlohmann::json>(400, error);
                }

                if(!StringUtils::is_float(param_value_parts[0])) {
                    return Option<nlohmann::json>(400, error);
                }

                int32_t value_meters;

                if(param_value_parts[1] == "km") {
                    value_meters = std::stof(param_value_parts[0]) * 1000;
                } else if(param_value_parts[1] == "mi") {
                    value_meters = std::stof(param_value_parts[0]) * 1609.34;
                } else {
                    return Option<nlohmann::json>(400, "Sort field's parameter "
                                                       "unit must be either `km` or `mi`.");
                }

                if(value_meters <= 0) {
                    return Option<nlohmann::json>(400, "Sort field's parameter must be a positive number.");
                }

                if(is_exclude_option) {
                    sort_field_std.exclude_radius = value_meters;
                } else {
                    sort_field_std.geo_precision = value_meters;
                }
            }

            double lat = std::stod(geo_parts[0]);
            double lng = std::stod(geo_parts[1]);
            int64_t lat_lng = GeoPoint::pack_lat_lng(lat, lng);
            sort_field_std.name = actual_field_name;
            sort_field_std.geopoint = lat_lng;
        }

        if(sort_field_std.name != sort_field_const::text_match && sort_schema.count(sort_field_std.name) == 0) {
            std::string error = "Could not find a field named `" + sort_field_std.name + "` in the schema for sorting.";
            return Option<nlohmann::json>(404, error);
        }

        StringUtils::toupper(sort_field_std.order);

        if(sort_field_std.order != sort_field_const::asc && sort_field_std.order != sort_field_const::desc) {
            std::string error = "Order for field` " + sort_field_std.name + "` should be either ASC or DESC.";
            return Option<nlohmann::json>(400, error);
        }

        sort_fields_std.emplace_back(sort_field_std);
    }

    /*
      1. Empty: [match_score, dsf] upstream
      2. ONE  : [usf, match_score]
      3. TWO  : [usf1, usf2, match_score]
      4. THREE: do nothing
    */
    if(sort_fields_std.empty()) {
        sort_fields_std.emplace_back(sort_field_const::text_match, sort_field_const::desc);
        if(!default_sorting_field.empty()) {
            sort_fields_std.emplace_back(default_sorting_field, sort_field_const::desc);
        } else {
            sort_fields_std.emplace_back(sort_field_const::seq_id, sort_field_const::desc);
        }
    }

    bool found_match_score = false;
    for(const auto & sort_field : sort_fields_std) {
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

    if((page * per_page) > limit_hits) {
        std::string message = "Only upto " + std::to_string(limit_hits) + " hits can be fetched. " +
                "Ensure that `page` and `per_page` parameters are within this range.";
        return Option<nlohmann::json>(422, message);
    }

    size_t max_hits = 250;

    // ensure that `max_hits` never exceeds number of documents in collection
    if(search_fields.size() <= 1 || query == "*") {
        max_hits = std::min(std::max((page * per_page), max_hits), get_num_documents());
    } else {
        max_hits = std::min(std::max((page * per_page), max_hits), get_num_documents());
    }

    if(token_order == NOT_SET) {
        if(default_sorting_field.empty()) {
            token_order = FREQUENCY;
        } else {
            token_order = MAX_SCORE;
        }
    }

    std::vector<std::vector<art_leaf*>> searched_queries;  // search queries used for generating the results
    std::vector<std::vector<KV*>> raw_result_kvs;
    std::vector<std::vector<KV*>> override_result_kvs;

    size_t total_found = 0;
    spp::sparse_hash_set<uint64_t> groups_processed;  // used to calculate total_found for grouped query

    //LOG(INFO) << "Num indices used for querying: " << indices.size();
    std::vector<query_tokens_t> field_query_tokens;

    if(search_fields.size() == 0) {
        // has to be a wildcard query
        field_query_tokens.emplace_back(query_tokens_t{});
        parse_search_query(query, field_query_tokens[0].q_include_tokens, field_query_tokens[0].q_exclude_tokens, "",
                           false);
    } else {
        for(size_t i = 0; i < search_fields.size(); i++) {
            const auto& search_field = search_fields[i];
            field_query_tokens.emplace_back(query_tokens_t{});

            const std::string & field_locale = search_schema.at(search_field).locale;
            parse_search_query(query, field_query_tokens[i].q_include_tokens, field_query_tokens[i].q_exclude_tokens,
                               field_locale, pre_segmented_query);

            // get synonyms
            std::vector<std::vector<std::string>> q_synonyms;
            synonym_reduction(field_query_tokens[i].q_include_tokens, field_query_tokens[i].q_synonyms);
        }
    }

    // search all indices
    std::vector<search_args*> search_args_vec;
    size_t num_processed = 0;
    std::mutex m_process;
    std::condition_variable cv_process;

    size_t index_id = 0;
    for(Index* index: indices) {
        search_args* search_params = new search_args(field_query_tokens, weighted_search_fields,
                                   filters, facets, index_to_included_ids[index_id],
                                   index_to_excluded_ids[index_id],
                                   sort_fields_std, facet_query, num_typos, max_facet_values, max_hits,
                                   per_page, page, token_order, prefixes,
                                   drop_tokens_threshold, typo_tokens_threshold,
                                   group_by_fields, group_limit, default_sorting_field, prioritize_exact_match);

        search_args_vec.push_back(search_params);

        CollectionManager::get_instance().get_thread_pool()->enqueue(
                [index, search_params, &m_process, &num_processed, &cv_process]() {
            index->run_search(search_params);
            std::unique_lock<std::mutex> lock(m_process);
            num_processed++;
            cv_process.notify_one();
        });

        index_id++;
    }

    const size_t num_indices = indices.size();
    std::unique_lock<std::mutex> lock_process(m_process);
    cv_process.wait(lock_process, [&](){ return num_processed == num_indices; });

    // for grouping we have to re-aggregate

    const size_t topster_size = std::max((size_t)1, max_hits);
    Topster topster(topster_size, group_limit);
    Topster curated_topster(topster_size, group_limit);

    for(const search_args* search_params: search_args_vec) {
        aggregate_topster(searched_queries.size(), topster, search_params->topster);
        aggregate_topster(searched_queries.size(), curated_topster, search_params->curated_topster);

        searched_queries.insert(searched_queries.end(), search_params->searched_queries.begin(),
                                search_params->searched_queries.end());

        for(size_t fi = 0; fi < search_params->facets.size(); fi++) {
            auto & this_facet = search_params->facets[fi];
            auto & acc_facet = facets[fi];

            for(auto & facet_kv: this_facet.result_map) {
                if(search_params->group_limit) {
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
                search_params->groups_processed.begin(),
                search_params->groups_processed.end()
            );
        } else {
            total_found += search_params->all_result_ids_len;
        }
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
    while(raw_results_index < raw_result_kvs.size()) {
        if(override_kv_index < override_result_kvs.size()) {
            size_t result_position = result_group_kvs.size() + 1;
            uint64_t override_position = override_result_kvs[override_kv_index][0]->distinct_key;
            if(result_position == override_position) {
                override_result_kvs[override_kv_index][0]->match_score_index = CURATED_RECORD_IDENTIFIER;
                result_group_kvs.push_back(override_result_kvs[override_kv_index]);
                override_kv_index++;
                continue;
            }
        }

        result_group_kvs.push_back(raw_result_kvs[raw_results_index]);
        raw_results_index++;
    }

    while(override_kv_index < override_result_kvs.size()) {
        override_result_kvs[override_kv_index][0]->match_score_index = CURATED_RECORD_IDENTIFIER;
        result_group_kvs.push_back({override_result_kvs[override_kv_index]});
        override_kv_index++;
    }

    const long start_result_index = (page - 1) * per_page;

    // `end_result_index` could be -1 when max_hits is 0
    const long end_result_index = std::min((page * per_page), std::min(max_hits, result_group_kvs.size())) - 1;

    nlohmann::json result = nlohmann::json::object();

    result["found"] = total_found;
    result["out_of"] = num_documents.load();

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

            std::vector<std::string> fields_highlighted_vec;
            std::vector<size_t> fields_highlighted_indices;
            if(highlight_fields.empty()) {
                for(size_t i = 0; i < search_fields.size(); i++) {
                    const auto& field_name = search_fields[i];
                    // should not pick excluded field for highlighting
                    if(exclude_fields.count(field_name) > 0) {
                        continue;
                    }

                    fields_highlighted_vec.emplace_back(field_name);
                    fields_highlighted_indices.push_back(i);
                }
            } else {
                if(query != "*") {
                    StringUtils::split(highlight_fields, fields_highlighted_vec, ",");
                    for(size_t i = 0; i < fields_highlighted_vec.size(); i++) {
                        fields_highlighted_indices.push_back(0);
                    }
                }
            }

            for(std::string & highlight_full_field: fields_highlighted_fully_vec) {
                fields_highlighted_fully.emplace(highlight_full_field);
            }

            for(size_t i = 0; i < fields_highlighted_vec.size(); i++) {
                const std::string& field_name = fields_highlighted_vec[i];
                const std::vector<std::string>& q_tokens = field_query_tokens[fields_highlighted_indices[i]].q_include_tokens;

                field search_field = search_schema.at(field_name);
                if(query != "*" && (search_field.type == field_types::STRING ||
                                    search_field.type == field_types::STRING_ARRAY)) {

                    bool highlighted_fully = (fields_highlighted_fully.find(field_name) != fields_highlighted_fully.end());
                    highlight_t highlight;
                    highlight_result(search_field, searched_queries, q_tokens, field_order_kv, document,
                                     string_utils, snippet_threshold, highlight_affix_num_tokens,
                                     highlighted_fully, highlight_start_tag, highlight_end_tag, highlight);

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
                    h_json["matched_tokens"] = highlight.matched_tokens;
                    h_json["indices"] = highlight.indices;
                    h_json["snippets"] = highlight.snippets;
                    if(highlight_fully) {
                        h_json["values"] = highlight.values;
                    }
                } else {
                    h_json["matched_tokens"] = highlight.matched_tokens[0];
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

            if(field_order_kv->match_score_index == CURATED_RECORD_IDENTIFIER) {
                wrapper_doc["curated"] = true;
            } else {
                wrapper_doc["text_match"] = field_order_kv->scores[field_order_kv->match_score_index];
            }

            nlohmann::json geo_distances;

            for(size_t sort_field_index = 0; sort_field_index < sort_fields_std.size(); sort_field_index++) {
                const auto& sort_field = sort_fields_std[sort_field_index];
                if(sort_field.geopoint != 0) {
                    geo_distances[sort_field.name] = std::abs(field_order_kv->scores[sort_field_index]);
                }
            }

            if(!geo_distances.empty()) {
                wrapper_doc["geo_distance_meters"] = geo_distances;
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
            bool facet_found = facet_value_to_string(a_facet, facet_count, document, value);

            if(!facet_found) {
                continue;
            }

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
                    highlightedss << highlight_start_tag <<
                                    tokens[i].substr(0, query_token_len) <<
                                    highlight_end_tag << unmarked;
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
    for(auto search_params: search_args_vec) {
        delete search_params;
    }

    result["request_params"] = nlohmann::json::object();;
    result["request_params"]["collection_name"] = name;
    result["request_params"]["per_page"] = per_page;
    result["request_params"]["q"] = query;

    //long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    //!LOG(INFO) << "Time taken for result calc: " << timeMillis << "us";
    //!store->print_memory_usage();
    return Option<nlohmann::json>(result);
}

void Collection::parse_search_query(const std::string &query, std::vector<std::string>& q_include_tokens,
                                    std::vector<std::string>& q_exclude_tokens,
                                    const std::string& locale, const bool already_segmented) const {
    if(query == "*") {
        q_exclude_tokens = {};
        q_include_tokens = {query};
    } else {
        std::vector<std::string> tokens;

        if(already_segmented) {
            StringUtils::split(query, tokens, " ");
        } else {
            Tokenizer(query, true, false, locale, {'-'}).tokenize(tokens);
        }

        bool exclude_operator_prior = false;

        for(auto& token: tokens) {
            if(token[0] == '-') {
                exclude_operator_prior = true;
                token = token.substr(1);
            } else {
                // remove hyphens (added earlier for supporting negation syntax)
                // to maintain parity with indexing which currently removing special symbols
                token.erase(std::remove(token.begin(), token.end(), '-'), token.end());
            }

            if(exclude_operator_prior) {
                q_exclude_tokens.push_back(token);
                exclude_operator_prior = false;
            } else {
                q_include_tokens.push_back(token);
            }
        }

        if(q_include_tokens.empty()) {
            // this can happen if the only query token is an exclusion token
            q_include_tokens.emplace_back("*");
        }
    }
}

void Collection::populate_result_kvs(Topster *topster, std::vector<std::vector<KV *>> &result_kvs) {
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

void Collection::aggregate_topster(size_t query_index, Topster& agg_topster, Topster* index_topster) {
    if(index_topster->distinct) {
        for(auto &group_topster_entry: index_topster->group_kv_map) {
            Topster* group_topster = group_topster_entry.second;
            for(const auto& map_kv: group_topster->kv_map) {
                map_kv.second->query_index += query_index;
                agg_topster.add(map_kv.second);
            }
        }
    } else {
        for(const auto& map_kv: index_topster->kv_map) {
            map_kv.second->query_index += query_index;
            agg_topster.add(map_kv.second);
        }
    }
}

Option<bool> Collection::get_filter_ids(const std::string & simple_filter_query,
                                    std::vector<std::pair<size_t, uint32_t*>>& index_ids) {
    std::shared_lock lock(mutex);

    std::vector<filter> filters;
    Option<bool> filter_op = parse_filter_query(simple_filter_query, filters);

    if(!filter_op.ok()) {
        return filter_op;
    }

    for(auto& index: indices) {
        uint32_t* filter_ids = nullptr;
        size_t filter_ids_len = index->do_filtering(&filter_ids, filters);
        index_ids.emplace_back(filter_ids_len, filter_ids);
    }

    return Option<bool>(true);
}

bool Collection::facet_value_to_string(const facet &a_facet, const facet_count_t &facet_count,
                                       const nlohmann::json &document, std::string &value) const {

    if(document.count(a_facet.field_name) == 0) {
        if(search_schema.at(a_facet.field_name).optional) {
            return false;
        }

        LOG(ERROR) << "Could not find field " << a_facet.field_name << " in document during faceting.";
        LOG(ERROR) << "Facet field type: " << facet_schema.at(a_facet.field_name).type;
        LOG(ERROR) << "Actual document: " << document;
        return false;
    }

    if(facet_schema.at(a_facet.field_name).is_array()) {
        size_t array_sz = document[a_facet.field_name].size();
        if(facet_count.array_pos >= array_sz) {
            LOG(ERROR) << "Facet field array size " << array_sz << " lesser than array pos " <<  facet_count.array_pos
                       << " for facet field " << a_facet.field_name;
            LOG(ERROR) << "Facet field type: " << facet_schema.at(a_facet.field_name).type;
            LOG(ERROR) << "Actual document: " << document;
            return false;
        }
    }

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
        value = StringUtils::float_to_str(raw_val);
        value.erase ( value.find_last_not_of('0') + 1, std::string::npos ); // remove trailing zeros
    } else if(facet_schema.at(a_facet.field_name).type == field_types::FLOAT_ARRAY) {
        float raw_val = document[a_facet.field_name][facet_count.array_pos].get<float>();
        value = StringUtils::float_to_str(raw_val);
        value.erase ( value.find_last_not_of('0') + 1, std::string::npos );  // remove trailing zeros
    } else if(facet_schema.at(a_facet.field_name).type == field_types::BOOL) {
        value = std::to_string(document[a_facet.field_name].get<bool>());
        value = (value == "1") ? "true" : "false";
    } else if(facet_schema.at(a_facet.field_name).type == field_types::BOOL_ARRAY) {
        value = std::to_string(document[a_facet.field_name][facet_count.array_pos].get<bool>());
        value = (value == "1") ? "true" : "false";
    }

    return true;
}

void Collection::highlight_result(const field &search_field,
                                  const std::vector<std::vector<art_leaf *>> &searched_queries,
                                  const std::vector<std::string>& q_tokens,
                                  const KV* field_order_kv, const nlohmann::json & document,
                                  StringUtils & string_utils,
                                  const size_t snippet_threshold,
                                  const size_t highlight_affix_num_tokens,
                                  bool highlighted_fully,
                                  const std::string& highlight_start_tag,
                                  const std::string& highlight_end_tag,
                                  highlight_t & highlight) const {

    if(searched_queries.size() <= field_order_kv->query_index) {
        return ;
    }

    std::vector<uint32_t*> leaf_to_indices;
    std::vector<art_leaf*> query_suggestion;
    std::set<std::string> query_suggestion_tokens;

    for (const art_leaf *token_leaf : searched_queries[field_order_kv->query_index]) {
        // Must search for the token string fresh on that field for the given document since `token_leaf`
        // is from the best matched field and need not be present in other fields of a document.
        Index* index = indices[field_order_kv->key % num_memory_shards];
        art_leaf *actual_leaf = index->get_token_leaf(search_field.name, &token_leaf->key[0], token_leaf->key_len);

        //LOG(INFO) << "field: " << search_field.name << ", key: " << token_leaf->key;

        if(actual_leaf != nullptr) {
            query_suggestion.push_back(actual_leaf);
            std::string token(reinterpret_cast<char*>(actual_leaf->key), actual_leaf->key_len-1);
            query_suggestion_tokens.insert(token);
            std::vector<uint16_t> positions;
            uint32_t doc_index = actual_leaf->values->ids.indexOf(field_order_kv->key);
            auto doc_indices = new uint32_t[1];
            doc_indices[0] = doc_index;
            leaf_to_indices.push_back(doc_indices);
        }
    }

    if(query_suggestion.size() != q_tokens.size()) {
        // can happen for compound query matched across 2 fields when some tokens are dropped
        for(const std::string& q_token: q_tokens) {
            if(query_suggestion_tokens.count(q_token) != 0) {
                continue;
            }

            Index* index = indices[field_order_kv->key % num_memory_shards];
            art_leaf *actual_leaf = index->get_token_leaf(search_field.name,
                                                          reinterpret_cast<const unsigned char *>(q_token.c_str()),
                                                          q_token.size() + 1);
            if(actual_leaf != nullptr) {
                std::vector<uint16_t> positions;
                uint32_t doc_index = actual_leaf->values->ids.indexOf(field_order_kv->key);
                if(doc_index != actual_leaf->values->ids.getLength()) {
                    auto doc_indices = new uint32_t[1];
                    doc_indices[0] = doc_index;
                    leaf_to_indices.push_back(doc_indices);
                    query_suggestion.push_back(actual_leaf);
                }
            }
        }
    }

    if(query_suggestion.empty()) {
        // none of the tokens from the query were found on this field
        free_leaf_indices(leaf_to_indices);
        return ;
    }

    //LOG(INFO) << "Document ID: " << document["id"];

    // positions in the field of each token in the query
    std::unordered_map<size_t, std::vector<token_positions_t>> array_token_positions;
    Index::populate_token_positions(query_suggestion, leaf_to_indices, 0, array_token_positions);

    std::vector<match_index_t> match_indices;

    for(const auto& kv: array_token_positions) {
        const std::vector<token_positions_t>& token_positions = kv.second;
        size_t array_index = kv.first;

        if(token_positions.empty()) {
            continue;
        }

        const Match & this_match = Match(field_order_kv->key, token_positions, true, true);
        uint64_t this_match_score = this_match.get_match_score(1);
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
        std::sort(match_indices[index].match.offsets.begin(), match_indices[index].match.offsets.end());
        const auto& match_index = match_indices[index];
        const Match& match = match_index.match;

        size_t last_valid_offset = 0;
        for (auto token_offset : match.offsets) {
            if(token_offset.offset != MAX_DISPLACEMENT) {
                last_valid_offset = token_offset.offset;
            } else {
                break;
            }
        }

        std::string text = (search_field.type == field_types::STRING) ? document[search_field.name] :
                           document[search_field.name][match_index.index];
        Tokenizer tokenizer(text, true, false, search_field.locale);

        if(search_field.locale == "ko") {
            text = string_utils.unicode_nfkd(text);
        }

        // need an ordered map here to ensure that it is ordered by the key (start offset)
        std::map<size_t, size_t> token_offsets;

        size_t match_offset_index = 0;
        std::string raw_token;
        spp::sparse_hash_set<std::string> token_hits;  // used to identify repeating tokens
        size_t raw_token_index = 0, tok_start = 0, tok_end = 0;

        // based on `highlight_affix_num_tokens`
        size_t snippet_start_offset = 0, snippet_end_offset = (text.empty() ? 0 : text.size() - 1);

        // window used to locate the starting offset for snippet on the text
        std::list<size_t> snippet_start_window;

        highlight.matched_tokens.emplace_back();
        std::vector<std::string>& matched_tokens = highlight.matched_tokens.back();

        while(tokenizer.next(raw_token, raw_token_index, tok_start, tok_end)) {
            if(token_offsets.empty()) {
                if(snippet_start_window.size() == highlight_affix_num_tokens + 1) {
                    snippet_start_window.pop_front();
                }

                snippet_start_window.push_back(tok_start);
            }

            if (token_hits.count(raw_token) != 0 ||
                (match_offset_index < match.offsets.size() &&
                 match.offsets[match_offset_index].offset == raw_token_index)) {

                token_offsets.emplace(tok_start, tok_end);
                token_hits.insert(raw_token);

                // to skip over duplicate tokens in the query
                do {
                    match_offset_index++;
                } while(match_offset_index < match.offsets.size() &&
                        match.offsets[match_offset_index - 1].offset == match.offsets[match_offset_index].offset);

                if(token_offsets.size() == 1) {
                    snippet_start_offset = snippet_start_window.front();
                }
            }

            if(raw_token_index == last_valid_offset + highlight_affix_num_tokens) {
                // register end of highlight snippet
                snippet_end_offset = tok_end;
            }

            if(raw_token_index == snippet_threshold - 1 &&
               raw_token_index >= last_valid_offset + highlight_affix_num_tokens) {
                // since we have already crossed snippet threshold, we can break now
                break;
            }
        }

        if(token_offsets.empty()) {
            continue;
        }

        if(raw_token_index + 1 < snippet_threshold) {
            // fully highlight field whose token size is less than given snippet threshold
            snippet_start_offset = 0;
            snippet_end_offset = text.size() - 1;
        }

        // with `token_index_offsets` we have a list of ranges to target for highlighting

        auto offset_it = token_offsets.begin();
        std::stringstream highlighted_text;

        for(size_t i = snippet_start_offset; i <= snippet_end_offset; i++) {
            if(offset_it != token_offsets.end()) {
                if (i == offset_it->first) {
                    highlighted_text << highlight_start_tag;
                    matched_tokens.push_back(text.substr(i, (offset_it->second - i) + 1));
                }

                if (i == offset_it->second) {
                    highlighted_text << text[i];
                    highlighted_text << highlight_end_tag;
                    offset_it++;
                    continue;
                }
            }

            highlighted_text << text[i];
        }

        highlight.snippets.push_back(highlighted_text.str());
        if(search_field.type == field_types::STRING_ARRAY) {
            highlight.indices.push_back(match_index.index);
        }

        if(highlighted_fully) {
            std::stringstream value_stream;
            offset_it = token_offsets.begin();

            for(size_t i = 0; i < text.size(); i++) {
                if(offset_it != token_offsets.end()) {
                    if (i == offset_it->first) {
                        value_stream << highlight_start_tag;
                    }

                    if (i == offset_it->second) {
                        value_stream << text[i];
                        value_stream << highlight_end_tag;
                        offset_it++;
                        continue;
                    }
                }

                value_stream << text[i];
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

Option<nlohmann::json> Collection::get(const std::string & id) const {
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

    {
        std::unique_lock lock(mutex);

        Index* index = indices[seq_id % num_memory_shards];
        index->remove(seq_id, document, false);
        num_documents -= 1;
    }

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

Option<bool> Collection::remove_if_found(uint32_t seq_id, const bool remove_from_store) {
    std::string parsed_document;
    StoreStatus doc_status = store->get(get_seq_id_key(seq_id), parsed_document);

    if(doc_status == StoreStatus::NOT_FOUND) {
        return Option<bool>(false);
    }

    if(doc_status == StoreStatus::ERROR) {
        return Option<bool>(500, "Error while fetching the document with seq id: " +
                            std::to_string(seq_id));
    }

    nlohmann::json document;
    try {
        document = nlohmann::json::parse(parsed_document);
    } catch(...) {
        return Option<bool>(500, "Error while parsing stored document.");
    }

    remove_document(document, seq_id, remove_from_store);
    return Option<bool>(true);
}

Option<uint32_t> Collection::add_override(const override_t & override) {
    bool inserted = store->insert(Collection::get_override_key(name, override.id), override.to_json().dump());
    if(!inserted) {
        return Option<uint32_t>(500, "Error while storing the override on disk.");
    }

    std::unique_lock lock(mutex);
    overrides[override.id] = override;
    return Option<uint32_t>(200);
}

Option<uint32_t> Collection::remove_override(const std::string & id) {
    if(overrides.count(id) != 0) {
        bool removed = store->remove(Collection::get_override_key(name, id));
        if(!removed) {
            return Option<uint32_t>(500, "Error while deleting the override from disk.");
        }

        std::unique_lock lock(mutex);
        overrides.erase(id);
        return Option<uint32_t>(200);
    }

    return Option<uint32_t>(404, "Could not find that `id`.");
}

size_t Collection::get_num_memory_shards() {
    return num_memory_shards.load();
}

uint32_t Collection::get_seq_id_from_key(const std::string & key) {
    // last 4 bytes of the key would be the serialized version of the sequence id
    std::string serialized_seq_id = key.substr(key.length() - 4);
    return StringUtils::deserialize_uint32_t(serialized_seq_id);
}

std::string Collection::get_next_seq_id_key(const std::string & collection_name) {
    return std::string(COLLECTION_NEXT_SEQ_PREFIX) + "_" + collection_name;
}

std::string Collection::get_seq_id_key(uint32_t seq_id) const {
    // We can't simply do std::to_string() because we want to preserve the byte order.
    // & 0xFF masks all but the lowest eight bits.
    const std::string & serialized_id = StringUtils::serialize_uint32_t(seq_id);
    return get_seq_id_collection_prefix() + "_" + serialized_id;
}

std::string Collection::get_doc_id_key(const std::string & doc_id) const {
    return std::to_string(collection_id) + "_" + DOC_ID_PREFIX + "_" + doc_id;
}

std::string Collection::get_name() const {
    std::shared_lock lock(mutex);
    return name;
}

uint64_t Collection::get_created_at() const {
    return created_at.load();
}

size_t Collection::get_num_documents() const {
    return num_documents.load();
}

uint32_t Collection::get_collection_id() const {
    return collection_id.load();
}

Option<uint32_t> Collection::doc_id_to_seq_id(const std::string & doc_id) const {
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
    std::shared_lock lock(mutex);

    std::vector<std::string> facet_fields_copy;
    for(auto it = facet_schema.begin(); it != facet_schema.end(); ++it) {
        facet_fields_copy.push_back(it->first);
    }

    return facet_fields_copy;
}

std::vector<field> Collection::get_sort_fields() {
    std::shared_lock lock(mutex);

    std::vector<field> sort_fields_copy;
    for(auto it = sort_schema.begin(); it != sort_schema.end(); ++it) {
        sort_fields_copy.push_back(it->second);
    }

    return sort_fields_copy;
}

std::vector<field> Collection::get_fields() {
    std::shared_lock lock(mutex);
    return fields;
}

std::vector<field> Collection::get_dynamic_fields() {
    std::shared_lock lock(mutex);
    return dynamic_fields;
}

std::unordered_map<std::string, field> Collection::get_schema() {
    std::shared_lock lock(mutex);
    return search_schema;
};

std::string Collection::get_meta_key(const std::string & collection_name) {
    return std::string(COLLECTION_META_PREFIX) + "_" + collection_name;
}

std::string Collection::get_override_key(const std::string & collection_name, const std::string & override_id) {
    return std::string(COLLECTION_OVERRIDE_PREFIX) + "_" + collection_name + "_" + override_id;
}

std::string Collection::get_synonym_key(const std::string & collection_name, const std::string & synonym_id) {
    return std::string(COLLECTION_SYNONYM_PREFIX) + "_" + collection_name + "_" + synonym_id;
}

std::string Collection::get_seq_id_collection_prefix() const {
    return std::to_string(collection_id) + "_" + std::string(SEQ_ID_PREFIX);
}

std::string Collection::get_default_sorting_field() {
    std::shared_lock lock(mutex);
    return default_sorting_field;
}

Option<bool> Collection::get_document_from_store(const uint32_t& seq_id, nlohmann::json& document) const {
    std::string json_doc_str;
    StoreStatus json_doc_status = store->get(get_seq_id_key(seq_id), json_doc_str);

    if(json_doc_status != StoreStatus::FOUND) {
        return Option<bool>(500, "Could not locate the JSON document for sequence ID: " + std::to_string(seq_id));
    }

    try {
        document = nlohmann::json::parse(json_doc_str);
    } catch(...) {
        return Option<bool>(500, "Error while parsing stored document with sequence ID: " + std::to_string(seq_id));
    }

    return Option<bool>(true);
}

Option<bool> Collection::get_document_from_store(const std::string &seq_id_key, nlohmann::json & document) const {
    std::string json_doc_str;
    StoreStatus json_doc_status = store->get(seq_id_key, json_doc_str);

    if(json_doc_status != StoreStatus::FOUND) {
        const std::string& seq_id = std::to_string(get_seq_id_from_key(seq_id_key));
        return Option<bool>(500, "Could not locate the JSON document for sequence ID: " + seq_id);
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

Option<bool> Collection::parse_geopoint_filter_value(std::string& raw_value,
                                                const std::string& format_err_msg,
                                                std::string& processed_filter_val,
                                                NUM_COMPARATOR& num_comparator) {

    num_comparator = LESS_THAN_EQUALS;

    if(!(raw_value[0] == '(' && raw_value[raw_value.size() - 1] == ')')) {
        return Option<bool>(400, format_err_msg);
    }

    std::vector<std::string> filter_values;
    auto raw_val_without_paran = raw_value.substr(1, raw_value.size() - 2);
    StringUtils::split(raw_val_without_paran, filter_values, ",");

    // we will end up with: "10.45 34.56 2 km" or a geo polygon

    if(filter_values.size() < 3) {
        return Option<bool>(400, format_err_msg);
    }

    // do validation: format should match either a point + radius or polygon

    size_t num_floats = 0;
    for(const auto& fvalue: filter_values) {
        if(StringUtils::is_float(fvalue)) {
            num_floats++;
        }
    }

    bool is_polygon = (num_floats == filter_values.size());
    if(!is_polygon) {
        // we have to ensure that this is a point + radius match
        if(!StringUtils::is_float(filter_values[0]) || !StringUtils::is_float(filter_values[1])) {
            return Option<bool>(400, format_err_msg);
        }
    }

    if(is_polygon) {
        processed_filter_val = raw_val_without_paran;
    } else {
        // point + radius
        std::vector<std::string> dist_values;
        StringUtils::split(filter_values[2], dist_values, " ");

        if(dist_values.size() != 2) {
            return Option<bool>(400, format_err_msg);
        }

        if(dist_values[1] != "km" && dist_values[1] != "mi") {
            return Option<bool>(400, "Unit must be either `km` or `mi`.");
        }

        processed_filter_val = filter_values[0] + ", " + filter_values[1] + ", " + // co-ords
                               dist_values[0] + ", " +  dist_values[1];           // X km
    }

   return Option<bool>(true);
}

Option<bool> Collection::parse_filter_query(const std::string& simple_filter_query,
                                            std::vector<filter>& filters) const {

    std::vector<filter> exclude_filters;  // to ensure that they go last in the list of filters

    std::vector<std::string> filter_blocks;
    StringUtils::split(simple_filter_query, filter_blocks, "&&");

    for(const std::string & filter_block: filter_blocks) {
        // split into [field_name, value]
        size_t found_index = filter_block.find(':');

        if(found_index == std::string::npos) {
            return Option<bool>(400, "Could not parse the filter query.");
        }

        std::string&& field_name = filter_block.substr(0, found_index);
        StringUtils::trim(field_name);

        if(search_schema.count(field_name) == 0) {
            return Option<bool>(404, "Could not find a filter field named `" + field_name + "` in the schema.");
        }

        field _field = search_schema.at(field_name);
        std::string&& raw_value = filter_block.substr(found_index+1, std::string::npos);
        StringUtils::trim(raw_value);
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

                f = {field_name, {}, {}};

                for(std::string & filter_value: filter_values) {
                    Option<NUM_COMPARATOR> op_comparator = filter::extract_num_comparator(filter_value);
                    if(!op_comparator.ok()) {
                        return Option<bool>(400, "Error with filter field `" + _field.name + "`: " + op_comparator.error());
                    }

                    if(op_comparator.get() == RANGE_INCLUSIVE) {
                        // split the value around range operator to extract bounds
                        std::vector<std::string> range_values;
                        StringUtils::split(filter_value, range_values, filter::RANGE_OPERATOR());
                        for(const std::string& range_value: range_values) {
                            auto validate_op = filter::validate_numerical_filter_value(_field, range_value);
                            if(!validate_op.ok()) {
                                return validate_op;
                            }

                            f.values.push_back(range_value);
                            f.comparators.push_back(op_comparator.get());
                        }
                    } else {
                        auto validate_op = filter::validate_numerical_filter_value(_field, filter_value);
                        if(!validate_op.ok()) {
                            return validate_op;
                        }

                        f.values.push_back(filter_value);
                        f.comparators.push_back(op_comparator.get());
                    }
                }

            } else {
                Option<NUM_COMPARATOR> op_comparator = filter::extract_num_comparator(raw_value);
                if(!op_comparator.ok()) {
                    return Option<bool>(400, "Error with filter field `" + _field.name + "`: " + op_comparator.error());
                }

                if(op_comparator.get() == RANGE_INCLUSIVE) {
                    // split the value around range operator to extract bounds
                    std::vector<std::string> range_values;
                    StringUtils::split(raw_value, range_values, filter::RANGE_OPERATOR());

                    f.field_name = field_name;
                    for(const std::string& range_value: range_values) {
                        auto validate_op = filter::validate_numerical_filter_value(_field, range_value);
                        if(!validate_op.ok()) {
                            return validate_op;
                        }

                        f.values.push_back(range_value);
                        f.comparators.push_back(op_comparator.get());
                    }
                } else {
                    auto validate_op = filter::validate_numerical_filter_value(_field, raw_value);
                    if(!validate_op.ok()) {
                        return validate_op;
                    }
                    f = {field_name, {raw_value}, {op_comparator.get()}};
                }
            }
        } else if(_field.is_bool()) {
            NUM_COMPARATOR bool_comparator = EQUALS;
            size_t filter_value_index = 0;

            if(raw_value[0] == '=') {
                bool_comparator = EQUALS;
                while(++filter_value_index < raw_value.size() && raw_value[filter_value_index] == ' ');
            } else if(raw_value.size() >= 2 && raw_value[0] == '!' && raw_value[1] == '=') {
                bool_comparator = NOT_EQUALS;
                filter_value_index++;
                while(++filter_value_index < raw_value.size() && raw_value[filter_value_index] == ' ');
            }

            if(filter_value_index != 0) {
                raw_value = raw_value.substr(filter_value_index);
            }

            if(filter_value_index == raw_value.size()) {
                return Option<bool>(400, "Error with filter field `" + _field.name +
                                         "`: Filter value cannot be empty.");
            }

            if(raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
                std::vector<std::string> filter_values;
                StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, ",");
                f = {field_name, {}, {}};

                for(std::string & filter_value: filter_values) {
                    if(filter_value != "true" && filter_value != "false") {
                        return Option<bool>(400, "Values of filter field `" + _field.name +
                                                           "`: must be `true` or `false`.");
                    }

                    filter_value = (filter_value == "true") ? "1" : "0";
                    f.values.push_back(filter_value);
                    f.comparators.push_back(bool_comparator);
                }
            } else {
                if(raw_value != "true" && raw_value != "false") {
                    return Option<bool>(400, "Value of filter field `" + _field.name + "` must be `true` or `false`.");
                }

                std::string bool_value = (raw_value == "true") ? "1" : "0";
                f = {field_name, {bool_value}, {bool_comparator}};
            }

        } else if(_field.is_geopoint()) {
            f = {field_name, {}, {}};

            const std::string& format_err_msg = "Value of filter field `" + _field.name +
                                                "`: must be in the `(-44.50, 170.29, 0.75 km)` or "
                                                "(56.33, -65.97, 23.82, -127.82) format.";

            NUM_COMPARATOR num_comparator;

            // could be a single value or a list
            if(raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
                std::vector<std::string> filter_values;
                StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, "),");

                for(std::string& filter_value: filter_values) {
                    filter_value += ")";
                    std::string processed_filter_val;
                    auto parse_op = parse_geopoint_filter_value(filter_value, format_err_msg, processed_filter_val, num_comparator);

                    if(!parse_op.ok()) {
                        return parse_op;
                    }

                    f.values.push_back(processed_filter_val);
                    f.comparators.push_back(num_comparator);
                }
            } else {
                // single value, e.g. (10.45, 34.56, 2 km)
                std::string processed_filter_val;
                auto parse_op = parse_geopoint_filter_value(raw_value, format_err_msg, processed_filter_val, num_comparator);

                if(!parse_op.ok()) {
                    return parse_op;
                }

                f.values.push_back(processed_filter_val);
                f.comparators.push_back(num_comparator);
            }

        } else if(_field.is_string()) {
            size_t filter_value_index = 0;
            NUM_COMPARATOR str_comparator = CONTAINS;

            if(raw_value[0] == '=') {
                if(!_field.facet) {
                    // EQUALS filtering on string is possible only on facet fields
                    return Option<bool>(400, "To perform exact filtering, filter field `" +
                                                       _field.name + "` must be a facet field.");
                }

                // string filter should be evaluated in strict "equals" mode
                str_comparator = EQUALS;
                while(++filter_value_index < raw_value.size() && raw_value[filter_value_index] == ' ');
            } else if(raw_value.size() >= 2 && raw_value[0] == '!' && raw_value[1] == '=') {
                if(!_field.facet) {
                    // EXCLUDE filtering on string is possible only on facet fields
                    return Option<bool>(400, "To perform exclude filtering, filter field `" +
                                             _field.name + "` must be a facet field.");
                }

                str_comparator = NOT_EQUALS;
                filter_value_index++;
                while(++filter_value_index < raw_value.size() && raw_value[filter_value_index] == ' ');
            }

            if(filter_value_index == raw_value.size()) {
                return Option<bool>(400, "Error with filter field `" + _field.name +
                                         "`: Filter value cannot be empty.");
            }

            if(raw_value[filter_value_index] == '[' && raw_value[raw_value.size() - 1] == ']') {
                std::vector<std::string> filter_values;
                StringUtils::split_to_values(raw_value.substr(filter_value_index+1, raw_value.size() - filter_value_index - 2), filter_values);
                f = {field_name, filter_values, {str_comparator}};
            } else {
                f = {field_name, {raw_value.substr(filter_value_index)}, {str_comparator}};
            }
        } else {
            return Option<bool>(400, "Error with filter field `" + _field.name +
                                "`: Unidentified field data type, see docs for supported data types.");
        }

        if(f.comparators.size() > 0 && f.comparators.front() == NOT_EQUALS) {
            exclude_filters.push_back(f);
        } else {
            filters.push_back(f);
        }
    }

    filters.insert( filters.end(), exclude_filters.begin(), exclude_filters.end() );

    return Option<bool>(true);
}

Option<bool> Collection::parse_pinned_hits(const std::string& pinned_hits_str,
                                           std::map<size_t, std::vector<std::string>>& pinned_hits) {
    if(!pinned_hits_str.empty()) {
        std::vector<std::string> pinned_hits_strs;
        StringUtils::split(pinned_hits_str, pinned_hits_strs, ",");

        for(const std::string & pinned_hits_part: pinned_hits_strs) {
            std::vector<std::string> expression_parts;
            int64_t index = pinned_hits_part.size() - 1;
            while(index >= 0 && pinned_hits_part[index] != ':') {
                index--;
            }

            if(index == 0) {
                return Option<bool>(false, "Pinned hits are not in expected format.");
            }

            std::string pinned_id = pinned_hits_part.substr(0, index);
            std::string pinned_pos = pinned_hits_part.substr(index+1);

            if(!StringUtils::is_positive_integer(pinned_pos)) {
                return Option<bool>(false, "Pinned hits are not in expected format.");
            }

            int position = std::stoi(pinned_pos);
            if(position == 0) {
                return Option<bool>(false, "Pinned hits must start from position 1.");
            }

            pinned_hits[position].emplace_back(pinned_id);
        }
    }

    return Option<bool>(true);
}

void Collection::synonym_reduction(const std::vector<std::string>& tokens,
                                   std::vector<std::vector<std::string>>& results) const {
    std::set<uint64_t> processed_syn_hashes;
    synonym_reduction_internal(tokens, tokens.size(), 0, processed_syn_hashes, results);
}

Option<bool> Collection::add_synonym(const synonym_t& synonym) {
    if(synonym_definitions.count(synonym.id) != 0) {
        // first we have to delete existing entries so we can upsert
        Option<bool> rem_op = remove_synonym(synonym.id);
        if(!rem_op.ok()) {
            return rem_op;
        }
    }

    std::unique_lock write_lock(mutex);
    synonym_definitions[synonym.id] = synonym;

    if(!synonym.root.empty()) {
        uint64_t root_hash = synonym_t::get_hash(synonym.root);
        synonym_index[root_hash].emplace_back(synonym.id);
    } else {
        for(const auto & syn_tokens : synonym.synonyms) {
            uint64_t syn_hash = synonym_t::get_hash(syn_tokens);
            synonym_index[syn_hash].emplace_back(synonym.id);
        }
    }

    write_lock.unlock();

    bool inserted = store->insert(Collection::get_synonym_key(name, synonym.id), synonym.to_json().dump());
    if(!inserted) {
        return Option<bool>(500, "Error while storing the synonym on disk.");
    }

    return Option<bool>(true);
}

bool Collection::get_synonym(const std::string& id, synonym_t& synonym) {
    std::shared_lock lock(mutex);

    if(synonym_definitions.count(id) != 0) {
        synonym = synonym_definitions[id];
        return true;
    }

    return false;
}

void Collection::synonym_reduction_internal(const std::vector<std::string>& tokens,
                                            size_t start_window_size, size_t start_index_pos,
                                            std::set<uint64_t>& processed_syn_hashes,
                                            std::vector<std::vector<std::string>>& results) const {

    bool recursed = false;

    for(size_t window_len = start_window_size; window_len > 0; window_len--) {
        for(size_t start_index = start_index_pos; start_index+window_len-1 < tokens.size(); start_index++) {
            std::vector<uint64_t> syn_hashes;
            uint64_t syn_hash = 1;

            for(size_t i = start_index; i < start_index+window_len; i++) {
                uint64_t token_hash = StringUtils::hash_wy(tokens[i].c_str(), tokens[i].size());

                if(i == start_index) {
                    syn_hash = token_hash;
                } else {
                    syn_hash = Index::hash_combine(syn_hash, token_hash);
                }

                syn_hashes.push_back(token_hash);
            }

            const auto& syn_itr = synonym_index.find(syn_hash);

            if(syn_itr != synonym_index.end() && processed_syn_hashes.count(syn_hash) == 0) {
                // tokens in this window match a synonym: reconstruct tokens and rerun synonym mapping against matches
                const auto& syn_ids = syn_itr->second;

                for(const auto& syn_id: syn_ids) {
                    const auto &syn_def = synonym_definitions.at(syn_id);

                    for (const auto &syn_def_tokens: syn_def.synonyms) {
                        std::vector<std::string> new_tokens;

                        for (size_t i = 0; i < start_index; i++) {
                            new_tokens.push_back(tokens[i]);
                        }

                        std::vector<uint64_t> syn_def_hashes;
                        uint64_t syn_def_hash = 1;

                        for (size_t i = 0; i < syn_def_tokens.size(); i++) {
                            const auto &syn_def_token = syn_def_tokens[i];
                            new_tokens.push_back(syn_def_token);
                            uint64_t token_hash = StringUtils::hash_wy(syn_def_token.c_str(),
                                                                       syn_def_token.size());

                            if (i == 0) {
                                syn_def_hash = token_hash;
                            } else {
                                syn_def_hash = Index::hash_combine(syn_def_hash, token_hash);
                            }

                            syn_def_hashes.push_back(token_hash);
                        }

                        if (syn_def_hash == syn_hash) {
                            // skip over token matching itself in the group
                            continue;
                        }

                        for (size_t i = start_index + window_len; i < tokens.size(); i++) {
                            new_tokens.push_back(tokens[i]);
                        }

                        processed_syn_hashes.emplace(syn_def_hash);
                        processed_syn_hashes.emplace(syn_hash);

                        for (uint64_t h: syn_def_hashes) {
                            processed_syn_hashes.emplace(h);
                        }

                        for (uint64_t h: syn_hashes) {
                            processed_syn_hashes.emplace(h);
                        }

                        recursed = true;
                        synonym_reduction_internal(new_tokens, window_len, start_index, processed_syn_hashes, results);
                    }
                }
            }
        }

        // reset it because for the next window we have to start from scratch
        start_index_pos = 0;
    }

    if(!recursed && !processed_syn_hashes.empty()) {
        results.emplace_back(tokens);
    }
}

Option<bool> Collection::remove_synonym(const std::string &id) {
    std::unique_lock lock(mutex);
    const auto& syn_iter = synonym_definitions.find(id);

    if(syn_iter != synonym_definitions.end()) {
        bool removed = store->remove(Collection::get_synonym_key(name, id));
        if(!removed) {
            return Option<bool>(500, "Error while deleting the synonym from disk.");
        }

        const auto& synonym = syn_iter->second;
        if(!synonym.root.empty()) {
            uint64_t root_hash = synonym_t::get_hash(synonym.root);
            synonym_index.erase(root_hash);
        } else {
            for(const auto & syn_tokens : synonym.synonyms) {
                uint64_t syn_hash = synonym_t::get_hash(syn_tokens);
                synonym_index.erase(syn_hash);
            }
        }

        synonym_definitions.erase(id);
        return Option<bool>(true);
    }

    return Option<bool>(404, "Could not find that `id`.");
}

spp::sparse_hash_map<std::string, synonym_t> Collection::get_synonyms() {
    std::shared_lock lock(mutex);
    return synonym_definitions;
}

Option<bool> Collection::check_and_update_schema(nlohmann::json& document, const DIRTY_VALUES& dirty_values) {
    std::unique_lock lock(mutex);

    std::vector<field> new_fields;

    auto kv = document.begin();
    while(kv != document.end()) {
        // we will not index the special "id" key
        if (search_schema.count(kv.key()) == 0 && kv.key() != "id") {
            const std::string &fname = kv.key();
            field new_field(fname, field_types::STRING, false, true);
            std::string field_type;
            bool parseable;

            bool found_dynamic_field = false;

            // check against dynamic field definitions
            for(const auto& dynamic_field: dynamic_fields) {
                if(std::regex_match (kv.key(), std::regex(dynamic_field.name))) {
                    new_field = dynamic_field;
                    new_field.name = fname;
                    found_dynamic_field = true;
                    break;
                }
            }

            if(!found_dynamic_field && fallback_field_type.empty()) {
                // we will not auto detect schema for non-dynamic fields if auto detection is not enabled
                kv++;
                continue;
            }

            // detect the actual type
            if(fallback_field_type.empty() || fallback_field_type == field_types::AUTO ||
               field_types::is_string_or_array(fallback_field_type)) {

                parseable = field::get_type(kv.value(), field_type);
                if(!parseable) {
                    if(dirty_values == DIRTY_VALUES::REJECT || dirty_values == DIRTY_VALUES::COERCE_OR_REJECT) {
                        return Option<bool>(400, "Type of field `" + kv.key() + "` is invalid.");
                    } else {
                        // DROP or COERCE_OR_DROP
                        kv = document.erase(kv);
                        continue;
                    }
                }

                new_field.type = field_type;

                if (field_types::is_string_or_array(fallback_field_type)) {
                    // Supporting single/array field detection only for strings, as it does not seem to be too useful for
                    // other field types.
                    if (new_field.is_array()) {
                        new_field.type = field_types::STRING_ARRAY;
                    } else {
                        new_field.type = field_types::STRING;
                    }
                }
            } else {
                new_field.type = fallback_field_type;
            }

            if(!new_field.index) {
                kv++;
                continue;
            }

            search_schema.emplace(new_field.name, new_field);
            fields.emplace_back(new_field);
            new_fields.emplace_back(new_field);
            if (new_field.is_sortable()) {
                sort_schema.emplace(new_field.name, new_field);
            }

            if(new_field.is_facet()) {
                facet_schema.emplace(new_field.name, new_field);
            }
        }

        kv++;
    }

    if(!new_fields.empty()) {
        // we should persist changes to fields in store
        std::string coll_meta_json;
        StoreStatus status = store->get(Collection::get_meta_key(name), coll_meta_json);

        if(status != StoreStatus::FOUND) {
            return Option<bool>(500, "Could not fetch collection meta from store.");
        }

        nlohmann::json collection_meta;

        try {
            collection_meta = nlohmann::json::parse(coll_meta_json);
            nlohmann::json fields_json = nlohmann::json::array();;

            Option<bool> fields_json_op = field::fields_to_json_fields(fields, default_sorting_field, fields_json);

            if(!fields_json_op.ok()) {
                return Option<bool>(fields_json_op.code(), fields_json_op.error());
            }

            collection_meta[COLLECTION_SEARCH_FIELDS_KEY] = fields_json;
            bool persisted = store->insert(Collection::get_meta_key(name), collection_meta.dump());

            if(!persisted) {
                return Option<bool>(500, "Could not persist collection meta to store.");
            }

            for(auto index: indices) {
                index->refresh_schemas(new_fields);
            }

        } catch(...) {
            return Option<bool>(500, "Unable to parse collection meta.");
        }
    }

    return Option<bool>(true);
}

std::vector<Index *> Collection::init_indices() {
    std::vector<Index*> index_list;

    for(const field& field: fields) {
        if(field.is_dynamic()) {
            // regexp fields are treated as dynamic fields
            dynamic_fields.push_back(field);
            continue;
        }

        search_schema.emplace(field.name, field);

        if(field.is_facet()) {
            facet_schema.emplace(field.name, field);
        }

        if(field.is_sortable()) {
            sort_schema.emplace(field.name, field);
        }
    }

    for(size_t i = 0; i < num_memory_shards; i++) {
        Index* index = new Index(name+std::to_string(i), search_schema, facet_schema, sort_schema);
        index_list.push_back(index);
    }

    return index_list;
}

DIRTY_VALUES Collection::parse_dirty_values_option(std::string& dirty_values) const {
    std::shared_lock lock(mutex);

    StringUtils::toupper(dirty_values);
    auto dirty_values_op = magic_enum::enum_cast<DIRTY_VALUES>(dirty_values);
    DIRTY_VALUES dirty_values_action;

    if(dirty_values_op.has_value()) {
        dirty_values_action = dirty_values_op.value();
    } else {
        dirty_values_action = (fallback_field_type.empty() && dynamic_fields.empty()) ?
                              DIRTY_VALUES::REJECT : DIRTY_VALUES::COERCE_OR_REJECT;
    }

    return dirty_values_action;
}
