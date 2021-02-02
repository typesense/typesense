#include <regex>
#include <chrono>
#include <thread>
#include <app_metrics.h>
#include "typesense_server_utils.h"
#include "core_api.h"
#include "string_utils.h"
#include "collection.h"
#include "collection_manager.h"
#include "system_metrics.h"
#include "logger.h"
#include "core_api_utils.h"

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

index_operation_t get_index_operation(const std::string& action) {
    if(action == "create") {
        return CREATE;
    } else if(action == "update") {
        return UPDATE;
    } else if(action == "upsert") {
        return UPSERT;
    }

    return CREATE;
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

bool post_health(http_req & req, http_res & res) {
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

bool get_stats_json(http_req &req, http_res &res) {
    nlohmann::json result;
    AppMetrics::get_instance().get("requests_per_second", "latency_ms", result);

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
    std::string results_json_str;
    Option<bool> search_op = CollectionManager::do_search(req.params, results_json_str);

    if(!search_op.ok()) {
        if(search_op.code() == 404) {
            res.set_404();
        } else {
            res.set(search_op.code(), search_op.error());
        }

        return false;
    }

    res.set_200(results_json_str);

    return true;
}

bool post_multi_search(http_req& req, http_res& res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req.body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res.set_400("Bad JSON.");
        return false;
    }

    if(req_json.count("searches") == 0) {
        res.set_400("Missing `searches` array.");
        return false;
    }

    if(!req_json["searches"].is_array()) {
        res.set_400("Missing `searches` array.");
        return false;
    }

    const char* LIMIT_MULTI_SEARCHES = "limit_multi_searches";
    size_t limit_multi_searches = 50;

    if(req.params.count(LIMIT_MULTI_SEARCHES) != 0 && StringUtils::is_uint32_t(req.params[LIMIT_MULTI_SEARCHES])) {
        limit_multi_searches = std::stoi(req.params[LIMIT_MULTI_SEARCHES]);
    }

    if(req_json["searches"].size() > limit_multi_searches) {
        res.set_400(std::string("Number of multi searches exceeds `") + LIMIT_MULTI_SEARCHES + "` parameter.");
        return false;
    }

    auto orig_req_params = req.params;

    nlohmann::json response;
    response["results"] = nlohmann::json::array();

    // we have to ensure that `req_json` is a flat <string, string> map
    for(auto& search: req_json["searches"]) {
        if(!search.is_object()) {
            res.set_400("The value of `searches` must be an array of objects.");
            return false;
        }

        req.params = orig_req_params;

        for(auto& item: search.items()) {
            bool populated = AuthManager::populate_req_params(req.params, item);
            if(!populated) {
                res.set_400("One or more search parameters are malformed.");
                return false;
            }
        }

        std::string results_json_str;
        Option<bool> search_op = CollectionManager::do_search(req.params, results_json_str);

        if(search_op.ok()) {
            response["results"].push_back(nlohmann::json::parse(results_json_str));
        } else {
            nlohmann::json err_res;
            err_res["error"] = search_op.error();
            err_res["code"] = search_op.code();
            response["results"].push_back(err_res);
        }
    }

    res.set_200(response.dump());
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
    res.set_200(json_response.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore));

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
    const char *ACTION = "action";

    if(req.params.count(BATCH_SIZE) == 0) {
        req.params[BATCH_SIZE] = "40";
    }

    if(req.params.count(ACTION) == 0) {
        req.params[ACTION] = "create";
    }

    if(!StringUtils::is_uint32_t(req.params[BATCH_SIZE])) {
        req.last_chunk_aggregate = true;
        res.final = true;
        res.set_400("Parameter `" + std::string(BATCH_SIZE) + "` must be a positive integer.");
        HttpServer::stream_response(req, res);
        return false;
    }

    if(req.params[ACTION] != "create" && req.params[ACTION] != "update" && req.params[ACTION] != "upsert") {
        req.last_chunk_aggregate = true;
        res.final = true;
        res.set_400("Parameter `" + std::string(ACTION) + "` must be a create|update|upsert.");
        HttpServer::stream_response(req, res);
        return false;
    }

    const size_t IMPORT_BATCH_SIZE = std::stoi(req.params[BATCH_SIZE]);

    if(IMPORT_BATCH_SIZE == 0) {
        req.last_chunk_aggregate = true;
        res.final = true;
        res.set_400("Parameter `" + std::string(BATCH_SIZE) + "` must be a positive integer.");
        HttpServer::stream_response(req, res);
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

    const index_operation_t operation = get_index_operation(req.params[ACTION]);

    if(!single_partial_record_body) {
        nlohmann::json document;
        nlohmann::json json_res = collection->add_many(json_lines, document, operation);
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
    const char *ACTION = "action";
    if(req.params.count(ACTION) == 0) {
        req.params[ACTION] = "create";
    }

    if(req.params[ACTION] != "create" && req.params[ACTION] != "update" && req.params[ACTION] != "upsert") {
        res.set_400("Parameter `" + std::string(ACTION) + "` must be a create|update|upsert.");
        return false;
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    const index_operation_t operation = get_index_operation(req.params[ACTION]);
    Option<nlohmann::json> inserted_doc_op = collection->add(req.body, operation);

    if(!inserted_doc_op.ok()) {
        res.set(inserted_doc_op.code(), inserted_doc_op.error());
        return false;
    }

    res.set_201(inserted_doc_op.get().dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore));
    return true;
}

bool patch_update_document(http_req & req, http_res & res) {
    std::string doc_id = req.params["id"];

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    Option<nlohmann::json> upserted_doc_op = collection->add(req.body, index_operation_t::UPDATE, doc_id);

    if(!upserted_doc_op.ok()) {
        res.set(upserted_doc_op.code(), upserted_doc_op.error());
        return false;
    }

    res.set_201(upserted_doc_op.get().dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore));
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

    res.set_200(doc_option.get().dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore));
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
    res.set_200(doc.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore));
    return true;
}

bool del_remove_documents(http_req & req, http_res & res) {
    // defaults: will get overridden later if needed
    res.content_type_header = "application/json";
    res.status_code = 200;

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

    const char *BATCH_SIZE = "batch_size";
    const char *FILTER_BY = "filter_by";

    if(req.params.count(BATCH_SIZE) == 0) {
        req.params[BATCH_SIZE] = "40";
    }

    if(req.params.count(FILTER_BY) == 0) {
        req.last_chunk_aggregate = true;
        res.final = true;
        res.set_400("Parameter `" + std::string(FILTER_BY) + "` must be provided.");
        HttpServer::stream_response(req, res);
        return false;
    }

    if(!StringUtils::is_uint32_t(req.params[BATCH_SIZE])) {
        req.last_chunk_aggregate = true;
        res.final = true;
        res.set_400("Parameter `" + std::string(BATCH_SIZE) + "` must be a positive integer.");
        HttpServer::stream_response(req, res);
        return false;
    }

    const size_t DELETE_BATCH_SIZE = std::stoi(req.params[BATCH_SIZE]);

    if(DELETE_BATCH_SIZE == 0) {
        req.last_chunk_aggregate = true;
        res.final = true;
        res.set_400("Parameter `" + std::string(BATCH_SIZE) + "` must be a positive integer.");
        HttpServer::stream_response(req, res);
        return false;
    }

    std::string simple_filter_query;

    if(req.params.count(FILTER_BY) != 0) {
        simple_filter_query = req.params[FILTER_BY];
    }

    deletion_state_t* deletion_state = nullptr;

    if(req.data == nullptr) {
        deletion_state = new deletion_state_t{};
        auto filter_ids_op = collection->get_filter_ids(simple_filter_query, deletion_state->index_ids);

        if(!filter_ids_op.ok()) {
            res.set(filter_ids_op.code(), filter_ids_op.error());
            req.last_chunk_aggregate = true;
            res.final = true;
            HttpServer::stream_response(req, res);
            return false;
        }

        for(size_t i=0; i<deletion_state->index_ids.size(); i++) {
            deletion_state->offsets.push_back(0);
        }
        deletion_state->collection = collection;
        deletion_state->num_removed = 0;
        req.data = deletion_state;
    } else {
        deletion_state = static_cast<deletion_state_t*>(req.data);
    }

    bool done = true;
    Option<bool> remove_op = stateful_remove_docs(deletion_state, DELETE_BATCH_SIZE, done);

    if(!remove_op.ok()) {
        res.set(remove_op.code(), remove_op.error());
        req.last_chunk_aggregate = true;
        res.final = true;
    } else {
        if(!done) {
            req.last_chunk_aggregate = false;
            res.final = false;
        } else {
            nlohmann::json response;
            response["num_deleted"] = deletion_state->num_removed;

            req.last_chunk_aggregate = true;
            req.data = nullptr;
            res.body = response.dump();
            res.final = true;
            delete deletion_state;
        }
    }

    HttpServer::stream_response(req, res);
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

    std::map<std::string, override_t>& overrides = collection->get_overrides();
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

    std::map<std::string, override_t>& overrides = collection->get_overrides();

    if(overrides.count(override_id) != 0) {
        nlohmann::json override = overrides[override_id].to_json();
        res.set_200(override.dump());
        return true;
    }

    res.set_404();
    return false;
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
    
    override_t override;
    Option<bool> parse_op = override_t::parse(req_json, override_id, override);
    if(!parse_op.ok()) {
        res.set(parse_op.code(), parse_op.error());
        return false;
    }
    
    Option<uint32_t> add_op = collection->add_override(override);

    if(!add_op.ok()) {
        res.set(add_op.code(), add_op.error());
        return false;
    }

    req_json["id"] = override.id;

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

    if(req_json.count("expires_at") == 0) {
        req_json["expires_at"] = api_key_t::FAR_FUTURE_TIMESTAMP;
    }

    api_key_t api_key(
        rand_key,
        req_json["description"].get<std::string>(),
        req_json["actions"].get<std::vector<std::string>>(),
        req_json["collections"].get<std::vector<std::string>>(),
        req_json["expires_at"].get<uint64_t>()
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

    res.set_200(res_json.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore));
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

bool post_snapshot(http_req& req, http_res& res) {
    const std::string SNAPSHOT_PATH = "snapshot_path";

    res.status_code = 201;
    res.content_type_header = "application/json";

    if(req.params.count(SNAPSHOT_PATH) == 0) {
        req.last_chunk_aggregate = true;
        res.final = true;
        res.set_400(std::string("Parameter `") + SNAPSHOT_PATH + "` is required.");
        HttpServer::stream_response(req, res);
        return false;
    }

    server->do_snapshot(req.params[SNAPSHOT_PATH], req, res);

    return true;
}

bool post_vote(http_req& req, http_res& res) {
    res.status_code = 200;
    res.content_type_header = "application/json";

    nlohmann::json response;
    response["success"] = server->trigger_vote();
    res.body = response.dump();

    return true;
}

bool post_config(http_req &req, http_res &res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req.body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res.set_400("Bad JSON.");
        return false;
    }

    if(req_json.count("log-slow-requests-time-ms") != 0) {
        if(!req_json["log-slow-requests-time-ms"].is_number_integer()) {
            res.set_400("Configuration `log-slow-requests-time-ms` must be an integer.");
            return false;
        }

        Config::get_instance().set_log_slow_requests_time_ms(req_json["log-slow-requests-time-ms"].get<int>());

        nlohmann::json response;
        response["success"] = true;
        res.set_201(response.dump());
    } else {
        res.set_400("Invalid configuration.");
    }

    return true;
}

bool get_synonyms(http_req &req, http_res &res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection *collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    nlohmann::json res_json;
    res_json["synonyms"] = nlohmann::json::array();

    auto& synonyms = collection->get_synonyms();
    for(const auto & kv: synonyms) {
        nlohmann::json synonym = kv.second.to_view_json();
        res_json["synonyms"].push_back(synonym);
    }

    res.set_200(res_json.dump());
    return true;
}

bool get_synonym(http_req &req, http_res &res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection *collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    std::string synonym_id = req.params["id"];

    synonym_t synonym;
    bool found = collection->get_synonym(synonym_id, synonym);

    if(found) {
        nlohmann::json synonym_json = synonym.to_view_json();
        res.set_200(synonym_json.dump());
        return true;
    }

    res.set_404();
    return false;
}

bool put_synonym(http_req &req, http_res &res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection *collection = collectionManager.get_collection(req.params["collection"]);

    std::string synonym_id = req.params["id"];

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    nlohmann::json syn_json;

    try {
        syn_json = nlohmann::json::parse(req.body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res.set_400("Bad JSON.");
        return false;
    }

    syn_json["id"] = synonym_id;

    synonym_t synonym;
    Option<bool> syn_op = synonym_t::parse(syn_json, synonym);

    if(!syn_op.ok()) {
        res.set(syn_op.code(), syn_op.error());
        return false;
    }

    Option<bool> upsert_op = collection->add_synonym(synonym);

    if(!upsert_op.ok()) {
        res.set(upsert_op.code(), upsert_op.error());
        return false;
    }

    res.set_200(syn_json.dump());
    return true;
}

bool del_synonym(http_req &req, http_res &res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection *collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.set_404();
        return false;
    }

    Option<bool> rem_op = collection->remove_synonym(req.params["id"]);
    if(!rem_op.ok()) {
        res.set(rem_op.code(), rem_op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["id"] = req.params["id"];

    res.set_200(res_json.dump());
    return true;
}
