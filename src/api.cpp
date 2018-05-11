#include <regex>
#include <chrono>
#include <thread>
#include <sys/resource.h>
#include "api.h"
#include "string_utils.h"
#include "collection.h"
#include "collection_manager.h"
#include "logger.h"

nlohmann::json collection_summary_json(Collection *collection) {
    nlohmann::json json_response;

    json_response["name"] = collection->get_name();
    json_response["num_documents"] = collection->get_num_documents();

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

    return collectionManager.auth_key_matches(auth_key) ||
           (rpath.handler == get_search && collectionManager.search_only_auth_key_matches(auth_key));
}

void get_collections(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::vector<Collection*> collections = collectionManager.get_collections();
    nlohmann::json json_response = nlohmann::json::array();

    for(Collection* collection: collections) {
        nlohmann::json collection_json = collection_summary_json(collection);
        json_response.push_back(collection_json);
    }

    res.send_200(json_response.dump());
}

void post_create_collection(http_req & req, http_res & res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req.body);
    } catch(const std::exception& e) {
        LOG(ERR) << "JSON error: " << e.what();
        return res.send_400("Bad JSON.");
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();

    // validate presence of mandatory fields

    if(req_json.count("name") == 0) {
        return res.send_400("Parameter `name` is required.");
    }

    if(req_json.count("fields") == 0) {
        return res.send_400("Parameter `fields` is required.");
    }

    const char* DEFAULT_SORTING_FIELD = "default_sorting_field";

    if(req_json.count(DEFAULT_SORTING_FIELD) == 0) {
        return res.send_400("Parameter `default_sorting_field` is required.");
    }

    if(!req_json[DEFAULT_SORTING_FIELD].is_string()) {
        return res.send_400(std::string("`") + DEFAULT_SORTING_FIELD +
                            "` should be a string. It should be the name of an int32/float field.");
    }

    if(collectionManager.get_collection(req_json["name"]) != nullptr) {
        return res.send_409("Collection with name `" + req_json["name"].get<std::string>() + "` already exists.");
    }

    // field specific validation

    std::vector<field> fields;

    if(!req_json["fields"].is_array() || req_json["fields"].size() == 0) {
        return res.send_400("Wrong format for `fields`. It should be an array like: "
                            "[{\"name\": \"<field_name>\", \"type\": \"<field_type>\"}]");
    }

    for(nlohmann::json & field_json: req_json["fields"]) {
        if(!field_json.is_object() ||
            field_json.count(fields::name) == 0 || field_json.count(fields::type) == 0 ||
            !field_json.at(fields::name).is_string() || !field_json.at(fields::type).is_string()) {

            return res.send_400("Wrong format for `fields`. It should be an array of objects containing "
                                "`name`, `type` and optionally, `facet` properties.");
        }

        if(field_json.count("facet") != 0 && !field_json.at(fields::facet).is_boolean()) {
            return res.send_400(std::string("The `facet` property of the field `") +
                                field_json.at(fields::name).get<std::string>() + "` should be a boolean.");
        }

        if(field_json.count("facet") == 0) {
            field_json["facet"] = false;
        }

        fields.push_back(
            field(field_json["name"], field_json["type"], field_json["facet"])
        );
    }

    const std::string & default_sorting_field = req_json[DEFAULT_SORTING_FIELD].get<std::string>();
    const Option<Collection*> & collection_op =
            collectionManager.create_collection(req_json["name"], fields, default_sorting_field);

    if(collection_op.ok()) {
        nlohmann::json json_response = collection_summary_json(collection_op.get());
        return res.send_201(json_response.dump());
    }

    return res.send(collection_op.code(), collection_op.error());
}

void del_drop_collection(http_req & req, http_res & res) {
    std::string doc_id = req.params["id"];
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(!collection) {
        return res.send_404();
    }

    nlohmann::json collection_json = collection_summary_json(collection);
    Option<bool> drop_result = collectionManager.drop_collection(req.params["collection"]);

    if(!drop_result.ok()) {
        return res.send(drop_result.code(), drop_result.error());
    }

    res.send_200(collection_json.dump());
}

void get_debug(http_req & req, http_res & res) {
    nlohmann::json result;
    result["version"] = TYPESENSE_VERSION;
    res.send_200(result.dump());
}

void get_search(http_req & req, http_res & res) {
    auto begin = std::chrono::high_resolution_clock::now();

    const char *NUM_TYPOS = "num_typos";
    const char *PREFIX = "prefix";
    const char *DROP_TOKENS_THRESHOLD = "drop_tokens_threshold";
    const char *FILTER = "filter_by";
    const char *QUERY = "q";
    const char *QUERY_BY = "query_by";
    const char *SORT_BY = "sort_by";
    const char *FACET_BY = "facet_by";
    const char *PER_PAGE = "per_page";
    const char *PAGE = "page";
    const char *CALLBACK = "callback";
    const char *RANK_TOKENS_BY = "rank_tokens_by";
    const char *INCLUDE_FIELDS = "include_fields";
    const char *EXCLUDE_FIELDS = "exclude_fields";

    if(req.params.count(NUM_TYPOS) == 0) {
        req.params[NUM_TYPOS] = "2";
    }

    if(req.params.count(PREFIX) == 0) {
        req.params[PREFIX] = "true";
    }

    if(req.params.count(DROP_TOKENS_THRESHOLD) == 0) {
        req.params[DROP_TOKENS_THRESHOLD] = std::to_string(Index::DROP_TOKENS_THRESHOLD);
    }

    if(req.params.count(QUERY) == 0) {
        return res.send_400(std::string("Parameter `") + QUERY + "` is required.");
    }

    if(req.params.count(QUERY_BY) == 0) {
        return res.send_400(std::string("Parameter `") + QUERY_BY + "` is required.");
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
        return res.send_400("Parameter `" + std::string(DROP_TOKENS_THRESHOLD) + "` must be an unsigned integer.");
    }

    if(!StringUtils::is_uint64_t(req.params[NUM_TYPOS])) {
        return res.send_400("Parameter `" + std::string(NUM_TYPOS) + "` must be an unsigned integer.");
    }

    if(!StringUtils::is_uint64_t(req.params[PER_PAGE])) {
        return res.send_400("Parameter `" + std::string(PER_PAGE) + "` must be an unsigned integer.");
    }

    if(!StringUtils::is_uint64_t(req.params[PAGE])) {
        return res.send_400("Parameter `" + std::string(PAGE) + "` must be an unsigned integer.");
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
            return res.send_400("Only upto 2 sort fields are allowed.");
        }

        for(const std::string & sort_field_str: sort_field_strs) {
            std::vector<std::string> expression_parts;
            StringUtils::split(sort_field_str, expression_parts, ":");

            if(expression_parts.size() != 2) {
                return res.send_400(std::string("Parameter `") + SORT_BY + "` is malformed.");
            }

            StringUtils::toupper(expression_parts[1]);
            sort_fields.push_back(sort_by(expression_parts[0], expression_parts[1]));
        }
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        return res.send_404();
    }

    bool prefix = (req.params[PREFIX] == "true");
    const size_t drop_tokens_threshold = (size_t) std::stoi(req.params[DROP_TOKENS_THRESHOLD]);

    if(req.params.count(RANK_TOKENS_BY) == 0) {
        req.params[RANK_TOKENS_BY] = "DEFAULT_SORTING_FIELD";
    }

    StringUtils::toupper(req.params[RANK_TOKENS_BY]);
    token_ordering token_order = (req.params[RANK_TOKENS_BY] == "DEFAULT_SORTING_FIELD") ? MAX_SCORE : FREQUENCY;

    Option<nlohmann::json> result_op = collection->search(req.params[QUERY], search_fields, filter_str, facet_fields,
                                               sort_fields, std::stoi(req.params[NUM_TYPOS]),
                                               std::stoi(req.params[PER_PAGE]), std::stoi(req.params[PAGE]),
                                               token_order, prefix, drop_tokens_threshold,
                                               include_fields, exclude_fields);

    uint64_t timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::high_resolution_clock::now() - begin).count();


    if(!result_op.ok()) {
        const std::string & json_res_body = (req.params.count(CALLBACK) == 0) ? result_op.error() :
                                            (req.params[CALLBACK] + "(" + result_op.error() + ");");
        return res.send(result_op.code(), json_res_body);
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
}

void get_collection_summary(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        return res.send_404();
    }

    nlohmann::json json_response = collection_summary_json(collection);
    res.send_200(json_response.dump());
}

void collection_export_handler(http_req* req, http_res* res, void* data) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req->params["collection"]);

    if(!collection) {
        return res->send_404();
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
}

void get_collection_export(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        res.send_404();
        res.server->send_message(SEND_RESPONSE_MSG, new request_response{&req, &res});
        return ;
    }

    const std::string seq_id_prefix = collection->get_seq_id_collection_prefix();

    rocksdb::Iterator* it = collectionManager.get_store()->get_iterator();
    it->Seek(seq_id_prefix);

    res.content_type_header = "application/octet-stream";
    res.status_code = 200;
    res.server->stream_response(collection_export_handler, req, res, (void *) it);
}

void post_add_document(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        return res.send_404();
    }

    Option<nlohmann::json> inserted_doc_op = collection->add(req.body);

    if(!inserted_doc_op.ok()) {
        res.send(inserted_doc_op.code(), inserted_doc_op.error());
    } else {
        res.send_201(inserted_doc_op.get().dump());
    }
}

void get_fetch_document(http_req & req, http_res & res) {
    std::string doc_id = req.params["id"];

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);
    if(collection == nullptr) {
        return res.send_404();
    }

    Option<nlohmann::json> doc_option = collection->get(doc_id);

    if(!doc_option.ok()) {
        res.send(doc_option.code(), doc_option.error());
    } else {
        res.send_200(doc_option.get().dump());
    }
}

void del_remove_document(http_req & req, http_res & res) {
    std::string doc_id = req.params["id"];

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);
    if(collection == nullptr) {
        return res.send_404();
    }

    Option<nlohmann::json> doc_option = collection->get(doc_id);

    if(!doc_option.ok()) {
        return res.send(doc_option.code(), doc_option.error());
    }

    Option<std::string> deleted_id_op = collection->remove(doc_id);

    if(!deleted_id_op.ok()) {
        res.send(deleted_id_op.code(), deleted_id_op.error());
    } else {
        nlohmann::json doc = doc_option.get();
        res.send_200(doc.dump());
    }
}

void get_replication_updates(http_req & req, http_res & res) {
    // Could be heavy - spawn a new thread so we don't block the main thread
    std::thread response_thread([&]() {
        if(!StringUtils::is_uint64_t(req.params["seq_number"])) {
            return res.send_400("The value of the parameter `seq_number` must be an unsigned integer.");
        }

        const uint64_t MAX_UPDATES_TO_SEND = 10000;
        uint64_t seq_number = std::stoull(req.params["seq_number"]);

        CollectionManager & collectionManager = CollectionManager::get_instance();
        Store* store = collectionManager.get_store();
        Option<std::vector<std::string>*> updates_op = store->get_updates_since(seq_number, MAX_UPDATES_TO_SEND);
        if(!updates_op.ok()) {
            res.send(updates_op.code(), updates_op.error());
            res.server->send_message(SEND_RESPONSE_MSG, new request_response{&req, &res});
            return ;
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
        res.server->send_message(SEND_RESPONSE_MSG, new request_response{&req, &res});
        delete updates;
    });

    response_thread.detach();
}

void on_send_response(void *data) {
    request_response* req_res = static_cast<request_response*>(data);
    req_res->response->server->send_response(req_res->req, req_res->response);
    delete req_res;
}