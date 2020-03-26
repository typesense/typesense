#include <regex>
#include <chrono>
#include <thread>
#include <sys/resource.h>
#include "typesense_server_utils.h"
#include "core_api.h"
#include "string_utils.h"
#include "collection.h"
#include "collection_manager.h"
#include "logger.h"

nlohmann::json collection_summary_json(Collection *collection) {
    nlohmann::json json_response;

    json_response["name"] = collection->get_name();
    json_response["num_documents"] = collection->get_num_documents();
    json_response["created_at"] = collection->get_created_at();

    const std::vector<field> & coll_fields = collection->get_fields();
    nlohmann::json fields_arr;

    for(const field & coll_field: coll_fields) {
        nlohmann::json field_json;
        field_json[fields::name] = coll_field.name;
        field_json[fields::type] = coll_field.type;
        field_json[fields::facet] = coll_field.facet;
        fields_arr.push_back(field_json);
    }

    json_response["fields"] = fields_arr;
    json_response["default_sorting_field"] = collection->get_default_sorting_field();
    return json_response;
}

bool handle_authentication(const route_path & rpath, const std::string & auth_key) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    return rpath.handler == get_health || collectionManager.auth_key_matches(auth_key) ||
           (rpath.handler == get_search && collectionManager.search_only_auth_key_matches(auth_key));
}

bool get_collections(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::vector<Collection*> collections = collectionManager.get_collections();
    nlohmann::json json_response = nlohmann::json::array();

    for(Collection* collection: collections) {
        nlohmann::json collection_json = collection_summary_json(collection);
        json_response.push_back(collection_json);
    }

    res.send_200(json_response.dump());
    return true;
}

bool post_create_collection(http_req & req, http_res & res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req.body);
    } catch(const std::exception& e) {
        //LOG(ERROR) << "JSON error: " << e.what();
        res.send_400("Bad JSON.");
        return false;
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();

    // validate presence of mandatory fields

    if(req_json.count("name") == 0) {
        res.send_400("Parameter `name` is required.");
        return false;
    }

    if(req_json.count("fields") == 0) {
        res.send_400("Parameter `fields` is required.");
        return false;
    }

    const char* DEFAULT_SORTING_FIELD = "default_sorting_field";

    if(req_json.count(DEFAULT_SORTING_FIELD) == 0) {
        res.send_400("Parameter `default_sorting_field` is required.");
        return false;
    }

    if(!req_json[DEFAULT_SORTING_FIELD].is_string()) {
        res.send_400(std::string("`") + DEFAULT_SORTING_FIELD +
                            "` should be a string. It should be the name of an int32/float field.");
        return false;
    }

    if(collectionManager.get_collection(req_json["name"]) != nullptr) {
        res.send_409("Collection with name `" + req_json["name"].get<std::string>() + "` already exists.");
        return false;
    }

    // field specific validation

    std::vector<field> fields;

    if(!req_json["fields"].is_array() || req_json["fields"].size() == 0) {
        res.send_400("Wrong format for `fields`. It should be an array like: "
                            "[{\"name\": \"<field_name>\", \"type\": \"<field_type>\"}]");
        return false;
    }

    for(nlohmann::json & field_json: req_json["fields"]) {
        if(!field_json.is_object() ||
            field_json.count(fields::name) == 0 || field_json.count(fields::type) == 0 ||
            !field_json.at(fields::name).is_string() || !field_json.at(fields::type).is_string()) {

            res.send_400("Wrong format for `fields`. It should be an array of objects containing "
                                "`name`, `type` and optionally, `facet` properties.");
            return false;
        }

        if(field_json.count("facet") != 0 && !field_json.at(fields::facet).is_boolean()) {
            res.send_400(std::string("The `facet` property of the field `") +
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
            collectionManager.create_collection(req_json["name"], fields, default_sorting_field);

    if(collection_op.ok()) {
        nlohmann::json json_response = collection_summary_json(collection_op.get());
        res.send_201(json_response.dump());
        return true;
    }

    res.send(collection_op.code(), collection_op.error());
    return false;
}

bool del_drop_collection(http_req & req, http_res & res) {
    std::string doc_id = req.params["id"];
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(!collection) {
        res.send_404();
        return false;
    }

    nlohmann::json collection_json = collection_summary_json(collection);
    Option<bool> drop_result = collectionManager.drop_collection(req.params["collection"]);

    if(!drop_result.ok()) {
        res.send(drop_result.code(), drop_result.error());
        return false;
    }

    res.send_200(collection_json.dump());
    return true;
}

bool get_debug(http_req & req, http_res & res) {
    nlohmann::json result;
    result["version"] = server->get_version();
    res.send_200(result.dump());

    return true;
}

bool get_health(http_req & req, http_res & res) {
    nlohmann::json result;
    result["ok"] = true;
    res.send_200(result.dump());
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

    const char *MAX_HITS = "max_hits";
    const char *PER_PAGE = "per_page";
    const char *PAGE = "page";
    const char *CALLBACK = "callback";
    const char *RANK_TOKENS_BY = "rank_tokens_by";
    const char *INCLUDE_FIELDS = "include_fields";
    const char *EXCLUDE_FIELDS = "exclude_fields";

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
        res.send_400(std::string("Parameter `") + QUERY + "` is required.");
        return false;
    }

    if(req.params.count(QUERY_BY) == 0) {
        res.send_400(std::string("Parameter `") + QUERY_BY + "` is required.");
        return false;
    }

    if(req.params.count(MAX_FACET_VALUES) == 0) {
        req.params[MAX_FACET_VALUES] = "10";
    }

    if(req.params.count(FACET_QUERY) == 0) {
        req.params[FACET_QUERY] = "";
    }

    if(req.params.count(MAX_HITS) == 0) {
        // for facet query, let max hits be 0 if it is not explicitly set
        if(req.params[FACET_QUERY].empty()) {
            req.params[MAX_HITS] = "500";
        } else {
            req.params[MAX_HITS] = "0";
        }
    }

    if(req.params.count(SNIPPET_THRESHOLD) == 0) {
        req.params[SNIPPET_THRESHOLD] = "30";
    }

    if(req.params.count(HIGHLIGHT_FULL_FIELDS) == 0) {
        req.params[HIGHLIGHT_FULL_FIELDS] = "";
    }

    if(req.params.count(PER_PAGE) == 0) {
        req.params[PER_PAGE] = "10";
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

    if(!StringUtils::is_uint64_t(req.params[DROP_TOKENS_THRESHOLD])) {
        res.send_400("Parameter `" + std::string(DROP_TOKENS_THRESHOLD) + "` must be an unsigned integer.");
        return false;
    }

    if(!StringUtils::is_uint64_t(req.params[TYPO_TOKENS_THRESHOLD])) {
        res.send_400("Parameter `" + std::string(TYPO_TOKENS_THRESHOLD) + "` must be an unsigned integer.");
        return false;
    }

    if(!StringUtils::is_uint64_t(req.params[NUM_TYPOS])) {
        res.send_400("Parameter `" + std::string(NUM_TYPOS) + "` must be an unsigned integer.");
        return false;
    }

    if(!StringUtils::is_uint64_t(req.params[PER_PAGE])) {
        res.send_400("Parameter `" + std::string(PER_PAGE) + "` must be an unsigned integer.");
        return false;
    }

    if(!StringUtils::is_uint64_t(req.params[PAGE])) {
        res.send_400("Parameter `" + std::string(PAGE) + "` must be an unsigned integer.");
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

    std::vector<sort_by> sort_fields;
    if(req.params.count(SORT_BY) != 0) {
        std::vector<std::string> sort_field_strs;
        StringUtils::split(req.params[SORT_BY], sort_field_strs, ",");

        if(sort_field_strs.size() > 2) {
            res.send_400("Only upto 2 sort fields are allowed.");
            return false;
        }

        for(const std::string & sort_field_str: sort_field_strs) {
            std::vector<std::string> expression_parts;
            StringUtils::split(sort_field_str, expression_parts, ":");

            if(expression_parts.size() != 2) {
                res.send_400(std::string("Parameter `") + SORT_BY + "` is malformed.");
                return false;
            }

            StringUtils::toupper(expression_parts[1]);
            sort_fields.push_back(sort_by(expression_parts[0], expression_parts[1]));
        }
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.send_404();
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
                                                          static_cast<size_t>(std::stoi(req.params[PER_PAGE])),
                                                          static_cast<size_t>(std::stoi(req.params[PAGE])),
                                                          token_order, prefix, drop_tokens_threshold,
                                                          include_fields, exclude_fields,
                                                          static_cast<size_t>(std::stoi(req.params[MAX_FACET_VALUES])),
                                                          static_cast<size_t>(std::stoi(req.params[MAX_HITS])),
                                                          req.params[FACET_QUERY],
                                                          static_cast<size_t>(std::stoi(req.params[SNIPPET_THRESHOLD])),
                                                          req.params[HIGHLIGHT_FULL_FIELDS],
                                                          typo_tokens_threshold
                                                          );

    uint64_t timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::high_resolution_clock::now() - begin).count();


    if(!result_op.ok()) {
        const std::string & json_res_body = (req.params.count(CALLBACK) == 0) ? result_op.error() :
                                            (req.params[CALLBACK] + "(" + result_op.error() + ");");
        res.send(result_op.code(), json_res_body);
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
        res.send_200(results_json_str);
    } else {
        res.send_200(req.params[CALLBACK] + "(" + results_json_str + ");");
    }

    //LOG(INFO) << "Time taken: " << timeMillis << "ms";
    return true;
}

bool get_collection_summary(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.send_404();
        return false;
    }

    nlohmann::json json_response = collection_summary_json(collection);
    res.send_200(json_response.dump());

    return true;
}

bool collection_export_handler(http_req* req, http_res* res, void* data) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req->params["collection"]);

    if(!collection) {
        res->send_404();
        return false;
    }

    const std::string seq_id_prefix = collection->get_seq_id_collection_prefix();

    rocksdb::Iterator* it = reinterpret_cast<rocksdb::Iterator*>(data);

    if(it->Valid() && it->key().ToString().compare(0, seq_id_prefix.size(), seq_id_prefix) == 0) {
        res->body = it->value().ToString();
        res->final = false;
        it->Next();

        // apppend a new line character if there is going to be one more record to send
        if(it->Valid() && it->key().ToString().compare(0, seq_id_prefix.size(), seq_id_prefix) == 0) {
            res->body += "\n";
        }
    } else {
        res->body = "";
        res->final = true;
        delete it;
    }

    return true;
}

bool get_export_documents(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.send_404();
        server->send_message(SEND_RESPONSE_MSG, new request_response{&req, &res});
        return false;
    }

    const std::string seq_id_prefix = collection->get_seq_id_collection_prefix();

    rocksdb::Iterator* it = collectionManager.get_store()->get_iterator();
    it->Seek(seq_id_prefix);

    res.content_type_header = "application/octet-stream";
    res.status_code = 200;
    stream_response(collection_export_handler, req, res, (void *) it);
    return true;
}

bool post_add_document(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.send_404();
        return false;
    }

    Option<nlohmann::json> inserted_doc_op = collection->add(req.body);

    if(!inserted_doc_op.ok()) {
        res.send(inserted_doc_op.code(), inserted_doc_op.error());
        return false;
    }

    res.send_201(inserted_doc_op.get().dump());
    return true;
}

bool post_import_documents(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.send_404();
        return false;
    }

    Option<nlohmann::json> result = collection->add_many(req.body);

    if(!result.ok()) {
        res.send(result.code(), result.error());
        return false;
    }

    res.send_200(result.get().dump());
    return true;
}

bool get_fetch_document(http_req & req, http_res & res) {
    std::string doc_id = req.params["id"];

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);
    if(collection == nullptr) {
        res.send_404();
        return false;
    }

    Option<nlohmann::json> doc_option = collection->get(doc_id);

    if(!doc_option.ok()) {
        res.send(doc_option.code(), doc_option.error());
        return false;
    }

    res.send_200(doc_option.get().dump());
    return true;
}

bool del_remove_document(http_req & req, http_res & res) {
    std::string doc_id = req.params["id"];

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);
    if(collection == nullptr) {
        res.send_404();
        return false;
    }

    Option<nlohmann::json> doc_option = collection->get(doc_id);

    if(!doc_option.ok()) {
        res.send(doc_option.code(), doc_option.error());
        return false;
    }

    Option<std::string> deleted_id_op = collection->remove(doc_id);

    if(!deleted_id_op.ok()) {
        res.send(deleted_id_op.code(), deleted_id_op.error());
        return false;
    }

    nlohmann::json doc = doc_option.get();
    res.send_200(doc.dump());
    return true;
}

bool get_replication_updates(http_req & req, http_res & res) {
    // Could be heavy - spawn a new thread so we don't block the main thread
    std::thread response_thread([&]() {
        if(!StringUtils::is_uint64_t(req.params["seq_number"])) {
            res.send_400("The value of the parameter `seq_number` must be an unsigned integer.");
            return false;
        }

        const uint64_t MAX_UPDATES_TO_SEND = 10000;
        uint64_t seq_number = std::stoull(req.params["seq_number"]);

        CollectionManager & collectionManager = CollectionManager::get_instance();
        Store* store = collectionManager.get_store();
        Option<std::vector<std::string>*> updates_op = store->get_updates_since(seq_number, MAX_UPDATES_TO_SEND);
        if(!updates_op.ok()) {
            res.send(updates_op.code(), updates_op.error());
            server->send_message(SEND_RESPONSE_MSG, new request_response{&req, &res});
            return false;
        }

        nlohmann::json json_response;
        json_response["updates"] = nlohmann::json::array();

        std::vector<std::string> *updates = updates_op.get();
        for(const std::string & update: *updates) {
            json_response["updates"].push_back(StringUtils::base64_encode(update));
        }

        uint64_t latest_seq_num = store->get_latest_seq_number();
        json_response["latest_seq_num"] = latest_seq_num;

        res.send_200(json_response.dump());
        server->send_message(SEND_RESPONSE_MSG, new request_response{&req, &res});
        delete updates;
    });

    response_thread.detach();
    return true;
}

bool async_write_request(void *data) {
    //LOG(INFO) << "async_write_request called";
    AsyncIndexArg* index_arg = static_cast<AsyncIndexArg*>(data);
    std::unique_ptr<AsyncIndexArg> index_arg_guard(index_arg);

    if(index_arg->req->route_hash == static_cast<int>(ROUTE_CODES::NOT_FOUND)) {
        // route not found
        index_arg->res->send_400("Not found.");
    } else if(index_arg->req->route_hash != static_cast<int>(ROUTE_CODES::ALREADY_HANDLED)) {
        // call the underlying http handler
        route_path* found_rpath = nullptr;
        bool route_found = server->get_route(index_arg->req->route_hash, &found_rpath);
        if(route_found) {
            found_rpath->handler(*index_arg->req, *index_arg->res);
        } else {
            index_arg->res->send_404();
        }
    }

    if(index_arg->req->_req != nullptr) {
        // we have to return a response to the client
        server->send_response(index_arg->req, index_arg->res);
    }

    if(index_arg->promise != nullptr) {
        index_arg->promise->set_value(true);  // returns control back to caller
    }

    return true;
}
