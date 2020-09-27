#include <regex>
#include <chrono>
#include <thread>
#include "typesense_server_utils.h"
#include "core_api.h"
#include "string_utils.h"
#include "collection.h"
#include "collection_manager.h"
#include "system_metrics.h"
#include "logger.h"

bool handle_authentication(std::map<std::string, std::string>& req_params, const route_path& rpath,
                           const std::string& auth_key) {
    CollectionManager & collectionManager = CollectionManager::get_instance();

    std::string collection = "*";

    if(req_params.count("collection") != 0) {
        collection = req_params.at("collection");
    }

    if(rpath.handler == get_health) {
        // health endpoint requires no authentication
        return true;
    }

    return collectionManager.auth_key_matches(auth_key, rpath.action, collection, req_params);
}

bool get_collections(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::vector<Collection*> collections = collectionManager.get_collections();
    nlohmann::json json_response = nlohmann::json::array();

    for(Collection* collection: collections) {
        nlohmann::json collection_json = collection->get_summary_json();
        json_response.push_back(collection_json);
    }

    res.set_200(json_response.dump());
    return true;
}

bool post_create_collection(http_req & req, http_res & res) {
    const char* NUM_MEMORY_SHARDS = "num_memory_shards";
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req.body);
    } catch(const std::exception& e) {
        //LOG(ERROR) << "JSON error: " << e.what();
        res.set_400("Bad JSON.");
        return false;
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();

    // validate presence of mandatory fields

    if(req_json.count("name") == 0) {
        res.set_400("Parameter `name` is required.");
        return false;
    }

    if(req_json.count(NUM_MEMORY_SHARDS) == 0) {
        req_json[NUM_MEMORY_SHARDS] = CollectionManager::DEFAULT_NUM_MEMORY_SHARDS;
    }

    if(req_json.count("fields") == 0) {
        res.set_400("Parameter `fields` is required.");
        return false;
    }

    const char* DEFAULT_SORTING_FIELD = "default_sorting_field";

    if(req_json.count(DEFAULT_SORTING_FIELD) == 0) {
        res.set_400("Parameter `default_sorting_field` is required.");
        return false;
    }

    if(!req_json[DEFAULT_SORTING_FIELD].is_string()) {
        res.set_400(std::string("`") + DEFAULT_SORTING_FIELD +
                    "` should be a string. It should be the name of an int32/float field.");
        return false;
    }

    if(!req_json[NUM_MEMORY_SHARDS].is_number_unsigned()) {
        res.set_400(std::string("`") + NUM_MEMORY_SHARDS + "` should be a positive integer.");
        return false;
    }

    if(req_json[NUM_MEMORY_SHARDS].get<size_t>() == 0) {
        res.set_400(std::string("`") + NUM_MEMORY_SHARDS + "` should be a positive integer.");
        return false;
    }

    if(collectionManager.get_collection(req_json["name"]) != nullptr) {
        res.set_409("Collection with name `" + req_json["name"].get<std::string>() + "` already exists.");
        return false;
    }

    // field specific validation

    std::vector<field> fields;

    if(!req_json["fields"].is_array() || req_json["fields"].size() == 0) {
        res.set_400("Wrong format for `fields`. It should be an array like: "
                    "[{\"name\": \"<field_name>\", \"type\": \"<field_type>\"}]");
        return false;
    }

    for(nlohmann::json & field_json: req_json["fields"]) {
        if(!field_json.is_object() ||
            field_json.count(fields::name) == 0 || field_json.count(fields::type) == 0 ||
            !field_json.at(fields::name).is_string() || !field_json.at(fields::type).is_string()) {

            res.set_400("Wrong format for `fields`. It should be an array of objects containing "
                        "`name`, `type` and optionally, `facet` properties.");
            return false;
        }

        if(field_json.count("facet") != 0 && !field_json.at(fields::facet).is_boolean()) {
            res.set_400(std::string("The `facet` property of the field `") +
                        field_json.at(fields::name).get<std::string>() + "` should be a boolean.");
            return false;
        }

        if(field_json.count("facet") == 0) {
            field_json["facet"] = false;
        }

        if(field_json.count("optional") == 0) {
            field_json["optional"] = false;
        }

        fields.emplace_back(
            field(field_json["name"], field_json["type"], field_json["facet"], field_json["optional"])
        );
    }

    const std::string & default_sorting_field = req_json[DEFAULT_SORTING_FIELD].get<std::string>();
    const Option<Collection*> & collection_op =
            collectionManager.create_collection(req_json["name"], req_json[NUM_MEMORY_SHARDS].get<size_t>(),
            fields, default_sorting_field);

    if(collection_op.ok()) {
        nlohmann::json json_response = collection_op.get()->get_summary_json();
        res.set_201(json_response.dump());
        return true;
    }

    res.set(collection_op.code(), collection_op.error());
    return false;
}

bool del_drop_collection(http_req & req, http_res & res) {
    std::string doc_id = req.params["id"];
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(!collection) {
        res.set_404();
        return false;
    }

    nlohmann::json collection_json = collection->get_summary_json();
    Option<bool> drop_result = collectionManager.drop_collection(req.params["collection"]);

    if(!drop_result.ok()) {
        res.set(drop_result.code(), drop_result.error());
        return false;
    }

    res.set_200(collection_json.dump());
    return true;
}

bool get_debug(http_req & req, http_res & res) {
    nlohmann::json result;
    result["version"] = server->get_version();

    uint64_t state = server->node_state();
    result["state"] = state;

    res.set_200(result.dump());
    return true;
}

bool get_health(http_req & req, http_res & res) {
    nlohmann::json result;
    bool alive = server->is_alive();
    result["ok"] = alive;
    if(alive) {
        res.set_body(200, result.dump());
    } else {
        res.set_body(503, result.dump());
    }

    return alive;
}


bool get_metrics_json(http_req &req, http_res &res) {
    nlohmann::json result;

    CollectionManager & collectionManager = CollectionManager::get_instance();
    const std::string & data_dir_path = collectionManager.get_store()->get_state_dir_path();

    SystemMetrics sys_metrics;
    sys_metrics.get(data_dir_path, result);

    res.set_body(200, result.dump(2));
    return true;
}


bool get_log_sequence(http_req &req, http_res &res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    const uint64_t seq_num = collectionManager.get_store()->get_latest_seq_number();
    res.content_type_header = "text/plain; charset=utf8";
    res.set_body(200, std::to_string(seq_num));
    return true;
}

bool get_search(http_req & req, http_res & res) {
    auto begin = std::chrono::high_resolution_clock::now();

    const char *NUM_TYPOS = "num_typos";
    const char *PREFIX = "prefix";
    const char *DROP_TOKENS_THRESHOLD = "drop_tokens_threshold";
    const char *TYPO_TOKENS_THRESHOLD = "typo_tokens_threshold";
    const char *FILTER = "filter_by";
    const char *QUERY = "q";
    const char *QUERY_BY = "query_by";
    const char *SORT_BY = "sort_by";

    const char *FACET_BY = "facet_by";
    const char *FACET_QUERY = "facet_query";
    const char *MAX_FACET_VALUES = "max_facet_values";

    const char *GROUP_BY = "group_by";
    const char *GROUP_LIMIT = "group_limit";

    const char *PER_PAGE = "per_page";
    const char *PAGE = "page";
    const char *CALLBACK = "callback";
    const char *RANK_TOKENS_BY = "rank_tokens_by";
    const char *INCLUDE_FIELDS = "include_fields";
    const char *EXCLUDE_FIELDS = "exclude_fields";

    const char *PINNED_HITS = "pinned_hits";
    const char *HIDDEN_HITS = "hidden_hits";

    // strings under this length will be fully highlighted, instead of showing a snippet of relevant portion
    const char *SNIPPET_THRESHOLD = "snippet_threshold";

    // list of fields which will be highlighted fully without snippeting
    const char *HIGHLIGHT_FULL_FIELDS = "highlight_full_fields";

    if(req.params.count(NUM_TYPOS) == 0) {
        req.params[NUM_TYPOS] = "2";
    }

    if(req.params.count(PREFIX) == 0) {
        req.params[PREFIX] = "true";
    }

    if(req.params.count(DROP_TOKENS_THRESHOLD) == 0) {
        req.params[DROP_TOKENS_THRESHOLD] = std::to_string(Index::DROP_TOKENS_THRESHOLD);
    }

    if(req.params.count(TYPO_TOKENS_THRESHOLD) == 0) {
        req.params[TYPO_TOKENS_THRESHOLD] = std::to_string(Index::TYPO_TOKENS_THRESHOLD);
    }

    if(req.params.count(QUERY) == 0) {
        res.set_400(std::string("Parameter `") + QUERY + "` is required.");
        return false;
    }

    if(req.params.count(MAX_FACET_VALUES) == 0) {
        req.params[MAX_FACET_VALUES] = "10";
    }

    if(req.params.count(FACET_QUERY) == 0) {
        req.params[FACET_QUERY] = "";
    }

    if(req.params.count(SNIPPET_THRESHOLD) == 0) {
        req.params[SNIPPET_THRESHOLD] = "30";
    }

    if(req.params.count(HIGHLIGHT_FULL_FIELDS) == 0) {
        req.params[HIGHLIGHT_FULL_FIELDS] = "";
    }

    if(req.params.count(PER_PAGE) == 0) {
        if(req.params[FACET_QUERY].empty()) {
            req.params[PER_PAGE] = "10";
        } else {
            // for facet query we will set per_page to zero if it is not explicitly overridden
            req.params[PER_PAGE] = "0";
        }
    }

    if(req.params.count(PAGE) == 0) {
        req.params[PAGE] = "1";
    }

    if(req.params.count(INCLUDE_FIELDS) == 0) {
        req.params[INCLUDE_FIELDS] = "";
    }

    if(req.params.count(EXCLUDE_FIELDS) == 0) {
        req.params[EXCLUDE_FIELDS] = "";
    }

    if(req.params.count(GROUP_BY) == 0) {
        req.params[GROUP_BY] = "";
    }

    if(req.params.count(GROUP_LIMIT) == 0) {
        if(req.params[GROUP_BY] != "") {
            req.params[GROUP_LIMIT] = "3";
        } else {
            req.params[GROUP_LIMIT] = "0";
        }
    }

    if(!StringUtils::is_uint32_t(req.params[DROP_TOKENS_THRESHOLD])) {
        res.set_400("Parameter `" + std::string(DROP_TOKENS_THRESHOLD) + "` must be an unsigned integer.");
        return false;
    }

    if(!StringUtils::is_uint32_t(req.params[TYPO_TOKENS_THRESHOLD])) {
        res.set_400("Parameter `" + std::string(TYPO_TOKENS_THRESHOLD) + "` must be an unsigned integer.");
        return false;
    }

    if(!StringUtils::is_uint32_t(req.params[NUM_TYPOS])) {
        res.set_400("Parameter `" + std::string(NUM_TYPOS) + "` must be an unsigned integer.");
        return false;
    }

    if(!StringUtils::is_uint32_t(req.params[PER_PAGE])) {
        res.set_400("Parameter `" + std::string(PER_PAGE) + "` must be an unsigned integer.");
        return false;
    }

    if(!StringUtils::is_uint32_t(req.params[PAGE])) {
        res.set_400("Parameter `" + std::string(PAGE) + "` must be an unsigned integer.");
        return false;
    }

    if(!StringUtils::is_uint32_t(req.params[MAX_FACET_VALUES])) {
        res.set_400("Parameter `" + std::string(MAX_FACET_VALUES) + "` must be an unsigned integer.");
        return false;
    }

    if(!StringUtils::is_uint32_t(req.params[SNIPPET_THRESHOLD])) {
        res.set_400("Parameter `" + std::string(SNIPPET_THRESHOLD) + "` must be an unsigned integer.");
        return false;
    }

    if(!StringUtils::is_uint32_t(req.params[GROUP_LIMIT])) {
        res.set_400("Parameter `" + std::string(GROUP_LIMIT) + "` must be an unsigned integer.");
        return false;
    }

    std::string filter_str = req.params.count(FILTER) != 0 ? req.params[FILTER] : "";

    std::vector<std::string> search_fields;
    StringUtils::split(req.params[QUERY_BY], search_fields, ",");

    std::vector<std::string> facet_fields;
    StringUtils::split(req.params[FACET_BY], facet_fields, ",");

    std::vector<std::string> include_fields_vec;
    StringUtils::split(req.params[INCLUDE_FIELDS], include_fields_vec, ",");

    std::vector<std::string> exclude_fields_vec;
    StringUtils::split(req.params[EXCLUDE_FIELDS], exclude_fields_vec, ",");

    spp::sparse_hash_set<std::string> include_fields(include_fields_vec.begin(), include_fields_vec.end());
    spp::sparse_hash_set<std::string> exclude_fields(exclude_fields_vec.begin(), exclude_fields_vec.end());

    std::vector<std::string> group_by_fields;
    StringUtils::split(req.params[GROUP_BY], group_by_fields, ",");

    std::vector<sort_by> sort_fields;
    if(req.params.count(SORT_BY) != 0) {
        std::vector<std::string> sort_field_strs;
        StringUtils::split(req.params[SORT_BY], sort_field_strs, ",");

        if(sort_field_strs.size() > 2) {
            res.set_400("Only upto 2 sort fields are allowed.");
            return false;
        }

        for(const std::string & sort_field_str: sort_field_strs) {
            std::vector<std::string> expression_parts;
            StringUtils::split(sort_field_str, expression_parts, ":");

            if(expression_parts.size() != 2) {
                res.set_400(std::string("Parameter `") + SORT_BY + "` is malformed.");
                return false;
            }

            StringUtils::toupper(expression_parts[1]);
            sort_fields.emplace_back(expression_parts[0], expression_parts[1]);
        }
    }

    std::map<size_t, std::vector<std::string>> pinned_hits;

    if(req.params.count(PINNED_HITS) != 0) {
        std::vector<std::string> pinned_hits_strs;
        StringUtils::split(req.params[PINNED_HITS], pinned_hits_strs, ",");

        for(const std::string & pinned_hits_str: pinned_hits_strs) {
            std::vector<std::string> expression_parts;
            StringUtils::split(pinned_hits_str, expression_parts, ":");

            if(expression_parts.size() != 2) {
                res.set_400(std::string("Parameter `") + PINNED_HITS + "` is malformed.");
                return false;
            }
            
            if(!StringUtils::is_positive_integer(expression_parts[1])) {
                res.set_400(std::string("Parameter `") + PINNED_HITS + "` is malformed.");
                return false;
            }

            int position = std::stoi(expression_parts[1]);
            if(position == 0) {
                res.set_400(std::string("Parameter `") + PINNED_HITS + "` is malformed.");
                return false;
            }

            pinned_hits[position].emplace_back(expression_parts[0]);
        }
    }

    std::vector<std::string> hidden_hits;
    if(req.params.count(HIDDEN_HITS) != 0) {
        StringUtils::split(req.params[HIDDEN_HITS], hidden_hits, ",");
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    bool prefix = (req.params[PREFIX] == "true");
    const size_t drop_tokens_threshold = (size_t) std::stoi(req.params[DROP_TOKENS_THRESHOLD]);
    const size_t typo_tokens_threshold = (size_t) std::stoi(req.params[TYPO_TOKENS_THRESHOLD]);

    if(req.params.count(RANK_TOKENS_BY) == 0) {
        req.params[RANK_TOKENS_BY] = "DEFAULT_SORTING_FIELD";
    }

    StringUtils::toupper(req.params[RANK_TOKENS_BY]);
    token_ordering token_order = (req.params[RANK_TOKENS_BY] == "DEFAULT_SORTING_FIELD") ? MAX_SCORE : FREQUENCY;

    Option<nlohmann::json> result_op = collection->search(req.params[QUERY], search_fields, filter_str, facet_fields,
                                                          sort_fields, std::stoi(req.params[NUM_TYPOS]),
                                                          static_cast<size_t>(std::stol(req.params[PER_PAGE])),
                                                          static_cast<size_t>(std::stol(req.params[PAGE])),
                                                          token_order, prefix, drop_tokens_threshold,
                                                          include_fields, exclude_fields,
                                                          static_cast<size_t>(std::stol(req.params[MAX_FACET_VALUES])),
                                                          req.params[FACET_QUERY],
                                                          static_cast<size_t>(std::stol(req.params[SNIPPET_THRESHOLD])),
                                                          req.params[HIGHLIGHT_FULL_FIELDS],
                                                          typo_tokens_threshold,
                                                          pinned_hits,
                                                          hidden_hits,
                                                          group_by_fields,
                                                          static_cast<size_t>(std::stol(req.params[GROUP_LIMIT]))
                                                          );

    uint64_t timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::high_resolution_clock::now() - begin).count();


    if(!result_op.ok()) {
        const std::string & json_res_body = (req.params.count(CALLBACK) == 0) ? result_op.error() :
                                            (req.params[CALLBACK] + "(" + result_op.error() + ");");
        res.set(result_op.code(), json_res_body);
        return false;
    }

    nlohmann::json result = result_op.get();
    result["search_time_ms"] = timeMillis;
    result["page"] = std::stoi(req.params[PAGE]);
    const std::string & results_json_str = result.dump();

    //struct rusage r_usage;
    //getrusage(RUSAGE_SELF,&r_usage);
    //LOG(INFO) << "Memory usage: " << r_usage.ru_maxrss;

    if(req.params.count(CALLBACK) == 0) {
        res.set_200(results_json_str);
    } else {
        res.set_200(req.params[CALLBACK] + "(" + results_json_str + ");");
    }

    //LOG(INFO) << "Time taken: " << timeMillis << "ms";
    return true;
}

bool get_collection_summary(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    nlohmann::json json_response = collection->get_summary_json();
    res.set_200(json_response.dump());

    return true;
}

bool get_export_documents(http_req & req, http_res & res) {
    // NOTE: this is a streaming response end-point so this handler will be called multiple times
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        req.last_chunk_aggregate = true;
        res.final = true;
        res.set_404();
        HttpServer::stream_response(req, res);
        return false;
    }

    const std::string seq_id_prefix = collection->get_seq_id_collection_prefix();
    rocksdb::Iterator* it = nullptr;

    if(req.data == nullptr) {
        it = collectionManager.get_store()->get_iterator();
        it->Seek(seq_id_prefix);
        req.data = it;
    } else {
        it = static_cast<rocksdb::Iterator*>(req.data);
    }

    if(it->Valid() && it->key().ToString().compare(0, seq_id_prefix.size(), seq_id_prefix) == 0) {
        res.body = it->value().ToString();
        it->Next();

        // append a new line character if there is going to be one more record to send
        if(it->Valid() && it->key().ToString().compare(0, seq_id_prefix.size(), seq_id_prefix) == 0) {
            res.body += "\n";
            req.last_chunk_aggregate = false;
            res.final = false;
        } else {
            req.last_chunk_aggregate = true;
            res.final = true;
            delete it;
            req.data = nullptr;
        }
    }

    res.content_type_header = "application/octet-stream";
    res.status_code = 200;

    HttpServer::stream_response(req, res);
    return true;
}

bool post_import_documents(http_req& req, http_res& res) {
    //LOG(INFO) << "post_import_documents";
    //LOG(INFO) << "req.first_chunk=" << req.first_chunk_aggregate << ", last_chunk=" << req.last_chunk_aggregate;
    const char *BATCH_SIZE = "batch_size";

    if(req.params.count(BATCH_SIZE) == 0) {
        req.params[BATCH_SIZE] = "40";
    }

    if(!StringUtils::is_uint32_t(req.params[BATCH_SIZE])) {
        req.last_chunk_aggregate = true;
        res.final = true;
        res.set_400("Parameter `" + std::string(BATCH_SIZE) + "` must be a positive integer.");
        HttpServer::stream_response(req, res);
        return false;
    }

    const size_t IMPORT_BATCH_SIZE = std::stoi(req.params[BATCH_SIZE]);

    if(IMPORT_BATCH_SIZE == 0) {
        res.set_400("Parameter `" + std::string(BATCH_SIZE) + "` must be a positive integer.");
        return false;
    }

    if(req.body_index == 0) {
        // will log for every major chunk of request body
        //LOG(INFO) << "Import, req.body.size=" << req.body.size() << ", batch_size=" << IMPORT_BATCH_SIZE;
        //int nminusten_pos = std::max(0, int(req.body.size())-10);
        //LOG(INFO) << "Last 10 chars: " << req.body.substr(nminusten_pos);
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        req.last_chunk_aggregate = true;
        res.final = true;
        res.set_404();
        HttpServer::stream_response(req, res);
        return false;
    }

    //LOG(INFO) << "Import, " << "req.body_index=" << req.body_index << ", req.body.size: " << req.body.size();
    //LOG(INFO) << "req body %: " << (float(req.body_index)/req.body.size())*100;

    std::vector<std::string> json_lines;
    req.body_index = StringUtils::split(req.body, json_lines, "\n", false, req.body_index, IMPORT_BATCH_SIZE);

    //LOG(INFO) << "json_lines.size before: " << json_lines.size() << ", req.body_index: " << req.body_index;

    bool stream_proceed = false;  // default state

    if(req.body_index == req.body.size()) {
        // body has been consumed fully, see whether we can fetch more request body
        req.body_index = 0;
        stream_proceed = true;

        if(req.last_chunk_aggregate) {
            //LOG(INFO) << "req.last_chunk_aggregate is true";
            req.body = "";
        } else {
            if(!json_lines.empty()) {
                // check if req.body had complete last record
                bool complete_document;

                try {
                    nlohmann::json document = nlohmann::json::parse(json_lines.back());
                    complete_document = document.is_object();
                } catch(const std::exception& e) {
                    complete_document = false;
                }

                if(!complete_document) {
                    // eject partial record
                    req.body = json_lines.back();
                    json_lines.pop_back();
                } else {
                    req.body = "";
                }
            }
        }
    }

    //LOG(INFO) << "json_lines.size after: " << json_lines.size() << ", stream_proceed: " << stream_proceed;
    //LOG(INFO) << "json_lines.size: " << json_lines.size() << ", req.stream_state: " << req.stream_state;

    // When only one partial record arrives as a chunk, an empty body is pushed to response stream
    bool single_partial_record_body = (json_lines.empty() && !req.body.empty());
    std::stringstream response_stream;

    //LOG(INFO) << "single_partial_record_body: " << single_partial_record_body;

    if(!single_partial_record_body) {
        nlohmann::json json_res = collection->add_many(json_lines);
        //const std::string& import_summary_json = json_res.dump();
        //response_stream << import_summary_json << "\n";

        for (size_t i = 0; i < json_lines.size(); i++) {
            if(i == json_lines.size()-1 && req.body_index == req.body.size() && req.last_chunk_aggregate) {
                // indicates last record of last batch
                response_stream << json_lines[i];
            } else {
                response_stream << json_lines[i] << "\n";
            }
        }
    }

    res.content_type_header = "text/plain; charset=utf8";
    res.status_code = 200;
    res.body += response_stream.str();

    if(stream_proceed) {
        res.final = req.last_chunk_aggregate;
        HttpServer::stream_response(req, res);
    } else {
        // push handler back onto the event loop: we must process the next batch without blocking the event loop
        server->defer_processing(req, res, 1);
    }

    return true;
}

bool post_add_document(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    Option<nlohmann::json> inserted_doc_op = collection->add(req.body);

    if(!inserted_doc_op.ok()) {
        res.set(inserted_doc_op.code(), inserted_doc_op.error());
        return false;
    }

    res.set_201(inserted_doc_op.get().dump());
    return true;
}

bool put_upsert_document(http_req & req, http_res & res) {
    std::string doc_id = req.params["id"];

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    Option<nlohmann::json> upserted_doc_op = collection->add(req.body, true, doc_id);

    if(!upserted_doc_op.ok()) {
        res.set(upserted_doc_op.code(), upserted_doc_op.error());
        return false;
    }

    res.set_201(upserted_doc_op.get().dump());
    return true;
}

bool get_fetch_document(http_req & req, http_res & res) {
    std::string doc_id = req.params["id"];

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);
    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    Option<nlohmann::json> doc_option = collection->get(doc_id);

    if(!doc_option.ok()) {
        res.set(doc_option.code(), doc_option.error());
        return false;
    }

    res.set_200(doc_option.get().dump());
    return true;
}

bool del_remove_document(http_req & req, http_res & res) {
    std::string doc_id = req.params["id"];

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);
    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    Option<nlohmann::json> doc_option = collection->get(doc_id);

    if(!doc_option.ok()) {
        res.set(doc_option.code(), doc_option.error());
        return false;
    }

    Option<std::string> deleted_id_op = collection->remove(doc_id);

    if(!deleted_id_op.ok()) {
        res.set(deleted_id_op.code(), deleted_id_op.error());
        return false;
    }

    nlohmann::json doc = doc_option.get();
    res.set_200(doc.dump());
    return true;
}


bool get_aliases(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    const spp::sparse_hash_map<std::string, std::string> & symlinks = collectionManager.get_symlinks();
    nlohmann::json res_json = nlohmann::json::object();
    res_json["aliases"] = nlohmann::json::array();

    for(const auto & symlink_collection: symlinks) {
        nlohmann::json symlink;
        symlink["name"] = symlink_collection.first;
        symlink["collection_name"] = symlink_collection.second;
        res_json["aliases"].push_back(symlink);
    }

    res.set_200(res_json.dump());
    return true;
}

bool get_alias(http_req & req, http_res & res) {
    const std::string & alias = req.params["alias"];
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Option<std::string> collection_name_op = collectionManager.resolve_symlink(alias);

    if(!collection_name_op.ok()) {
        res.set_404();
        return false;
    }

    nlohmann::json res_json;
    res_json["name"] = alias;
    res_json["collection_name"] = collection_name_op.get();

    res.set_200(res_json.dump());
    return true;
}

bool put_upsert_alias(http_req & req, http_res & res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req.body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res.set_400("Bad JSON.");
        return false;
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    const std::string & alias = req.params["alias"];

    const char* COLLECTION_NAME = "collection_name";

    if(req_json.count(COLLECTION_NAME) == 0) {
        res.set_400(std::string("Parameter `") + COLLECTION_NAME + "` is required.");
        return false;
    }

    Option<bool> success_op = collectionManager.upsert_symlink(alias, req_json[COLLECTION_NAME]);
    if(!success_op.ok()) {
        res.set_500(success_op.error());
        return false;
    }

    req_json["name"] = alias;
    res.set_200(req_json.dump());
    return true;
}

bool del_alias(http_req & req, http_res & res) {
    const std::string & alias = req.params["alias"];
    CollectionManager & collectionManager = CollectionManager::get_instance();

    Option<std::string> collection_name_op = collectionManager.resolve_symlink(alias);
    if(!collection_name_op.ok()) {
        res.set_404();
        return false;
    }

    Option<bool> delete_op = collectionManager.delete_symlink(alias);

    if(!delete_op.ok()) {
        res.set_500(delete_op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["name"] = alias;
    res_json["collection_name"] = collection_name_op.get();
    res.set_200(res_json.dump());
    return true;
}

bool get_overrides(http_req &req, http_res &res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection *collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    nlohmann::json res_json;
    res_json["overrides"] = nlohmann::json::array();

    std::map<std::string, override_t> overrides = collection->get_overrides();
    for(const auto & kv: overrides) {
        nlohmann::json override = kv.second.to_json();
        res_json["overrides"].push_back(override);
    }

    res.set_200(res_json.dump());
    return true;
}

bool get_override(http_req &req, http_res &res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection *collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    std::string override_id = req.params["id"];

    std::map<std::string, override_t> overrides = collection->get_overrides();

    if(overrides.count(override_id) != 0) {
        nlohmann::json override = overrides[override_id].to_json();
        res.set_200(override.dump());
        return false;
    }

    res.set_404();
    return true;
}

bool put_override(http_req &req, http_res &res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection *collection = collectionManager.get_collection(req.params["collection"]);

    std::string override_id = req.params["id"];

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req.body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res.set_400("Bad JSON.");
        return false;
    }

    // validate format of req_json
    if(
            !req_json.is_object() ||
            (req_json.count("rule") == 0) ||
            (req_json["rule"].count("query") == 0 || req_json["rule"].count("match") == 0) ||
            (req_json.count("includes") == 0 && req_json.count("excludes") == 0)
            ) {
        res.set_400("Bad JSON.");
        return false;
    }

    req_json["id"] = override_id;

    override_t override(req_json);
    Option<uint32_t> add_op = collection->add_override(override);

    if(!add_op.ok()) {
        res.set(add_op.code(), add_op.error());
        return false;
    }

    res.set_200(req_json.dump());
    return true;
}

bool del_override(http_req &req, http_res &res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection *collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    Option<uint32_t> rem_op = collection->remove_override(req.params["id"]);
    if(!rem_op.ok()) {
        res.set(rem_op.code(), rem_op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["id"] = req.params["id"];

    res.set_200(res_json.dump());
    return true;
}

bool get_keys(http_req &req, http_res &res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    AuthManager &auth_manager = collectionManager.getAuthManager();

    const Option<std::vector<api_key_t>>& keys_op = auth_manager.list_keys();
    if(!keys_op.ok()) {
        res.set(keys_op.code(), keys_op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["keys"] = nlohmann::json::array();

    const std::vector<api_key_t>& keys = keys_op.get();
    for(const auto & key: keys) {
        nlohmann::json key_obj = key.to_json();
        key_obj["value_prefix"] = key_obj["value"];
        key_obj.erase("value");
        res_json["keys"].push_back(key_obj);
    }

    res.set_200(res_json.dump());
    return true;
}

bool post_create_key(http_req &req, http_res &res) {
    //LOG(INFO) << "post_create_key";

    CollectionManager & collectionManager = CollectionManager::get_instance();
    AuthManager &auth_manager = collectionManager.getAuthManager();

    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req.body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res.set_400("Bad JSON.");
        return false;
    }


    const Option<uint32_t>& validate_op = api_key_t::validate(req_json);
    if(!validate_op.ok()) {
        res.set(validate_op.code(), validate_op.error());
        return false;
    }

    const std::string &rand_key = req.metadata;

    api_key_t api_key(
        rand_key,
        req_json["description"].get<std::string>(),
        req_json["actions"].get<std::vector<std::string>>(),
        req_json["collections"].get<std::vector<std::string>>()
    );

    const Option<api_key_t>& api_key_op = auth_manager.create_key(api_key);
    if(!api_key_op.ok()) {
        res.set(api_key_op.code(), api_key_op.error());
        return false;
    }

    res.set_201(api_key_op.get().to_json().dump());
    return true;
}

bool get_key(http_req &req, http_res &res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    AuthManager &auth_manager = collectionManager.getAuthManager();

    const std::string& key_id_str = req.params["id"];
    uint32_t key_id = (uint32_t) std::stoul(key_id_str);

    const Option<api_key_t>& key_op = auth_manager.get_key(key_id);

    if(!key_op.ok()) {
        res.set(key_op.code(), key_op.error());
        return false;
    }

    nlohmann::json key_obj = key_op.get().to_json();
    key_obj["value_prefix"] = key_obj["value"];
    key_obj.erase("value");

    res.set_200(key_obj.dump());
    return true;
}

bool del_key(http_req &req, http_res &res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    AuthManager &auth_manager = collectionManager.getAuthManager();

    const std::string& key_id_str = req.params["id"];
    uint32_t key_id = (uint32_t) std::stoul(key_id_str);

    const Option<api_key_t> &del_op = auth_manager.remove_key(key_id);

    if(!del_op.ok()) {
        res.set(del_op.code(), del_op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["id"] = del_op.get().id;

    res.set_200(res_json.dump());
    return true;
}

bool raft_write_send_response(void *data) {
    //LOG(INFO) << "raft_write_send_response called";
    AsyncIndexArg* index_arg = static_cast<AsyncIndexArg*>(data);
    std::unique_ptr<AsyncIndexArg> index_arg_guard(index_arg);

    bool async_res = false;

    if(index_arg->req->route_hash == static_cast<uint64_t>(ROUTE_CODES::NOT_FOUND)) {
        // route not found
        index_arg->res->set_400("Not found.");
    } else if(index_arg->req->route_hash != static_cast<uint64_t>(ROUTE_CODES::ALREADY_HANDLED)) {
        route_path* found_rpath = nullptr;
        bool route_found = server->get_route(index_arg->req->route_hash, &found_rpath);
        if(route_found) {
            async_res = found_rpath->async_res;
            found_rpath->handler(*index_arg->req, *index_arg->res);
        } else {
            index_arg->res->set_404();
        }
    }

    //LOG(INFO) << "raft_write_send_response, async_res=" << async_res;

    // only handle synchronous responses as async ones are handled by their handlers
    if(!async_res) {
        // send response and return control back to raft replication thread
        //LOG(INFO) << "raft_write_send_response: sending response";
        server->send_response(index_arg->req, index_arg->res);
    }

    return true;
}
