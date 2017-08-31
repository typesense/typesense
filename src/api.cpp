#include <regex>
#include <chrono>
#include <sys/resource.h>
#include "api.h"
#include "string_utils.h"
#include "collection.h"
#include "collection_manager.h"

void get_collections(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::vector<Collection*> collections = collectionManager.get_collections();
    nlohmann::json json_response;
    json_response["data"] = nlohmann::json::array();

    for(Collection* collection: collections) {
        nlohmann::json collection_map;
        collection_map["name"] = collection->get_name();
        json_response["data"].push_back(collection_map);
    }

    res.send_200(json_response.dump());
}

void post_create_collection(http_req & req, http_res & res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req.body);
    } catch(...) {
        return res.send_400("Bad JSON.");
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();

    // validate presence of mandatory fields

    if(req_json.count("name") == 0) {
        return res.send_400("Parameter `name` is required.");
    }

    if(req_json.count("search_fields") == 0) {
        return res.send_400("Parameter `search_fields` is required.");
    }

    if(req_json.count("sort_fields") == 0) {
        return res.send_400("Parameter `sort_fields` is required.");
    }

    if(collectionManager.get_collection(req_json["name"]) != nullptr) {
        return res.send_409("Collection with name `" + req_json["name"].get<std::string>() + "` already exists.");
    }

    // field specific validation

    std::vector<field> search_fields;

    if(!req_json["search_fields"].is_array() || req_json["search_fields"].size() == 0) {
        return res.send_400("Wrong format for `search_fields`. It should be an array like: "
                            "[{\"name\": \"<field_name>\", \"type\": \"<field_type>\"}]");
    }

    for(const nlohmann::json & search_field_json: req_json["search_fields"]) {
        if(!search_field_json.is_object() ||
            search_field_json.count(fields::name) == 0 || search_field_json.count(fields::type) == 0 ||
            !search_field_json.at(fields::name).is_string() || !search_field_json.at(fields::type).is_string()) {

            return res.send_400("Wrong format for `search_fields`. It should be an array like: "
                                "[{\"name\": \"<field_name>\", \"type\": \"<field_type>\"}]");
        }

        search_fields.push_back(field(search_field_json["name"], search_field_json["type"]));
    }

    std::vector<field> facet_fields;

    if(req_json.count("facet_fields") != 0) {
        if(!req_json["facet_fields"].is_array()) {
            return res.send_400("Wrong format for `facet_fields`. It should be an array like: "
                                        "[{\"name\": \"<field_name>\", \"type\": \"<field_type>\"}]");
        }

        for(const nlohmann::json & facet_field_json: req_json["facet_fields"]) {
            if(!facet_field_json.is_object() ||
               facet_field_json.count(fields::name) == 0 || facet_field_json.count(fields::type) == 0 ||
               !facet_field_json.at(fields::name).is_string() || !facet_field_json.at(fields::type).is_string()) {

                return res.send_400("Wrong format for `facet_fields`. It should be an array like: "
                                            "[{\"name\": \"<field_name>\", \"type\": \"<field_type>\"}]");
            }

            facet_fields.push_back(field(facet_field_json["name"], facet_field_json["type"]));
        }
    }

    std::vector<field> sort_fields;

    if(!req_json["sort_fields"].is_array() || req_json["sort_fields"].size() == 0) {
        return res.send_400("Wrong format for `sort_fields`. It should be an array like: "
                                    "[{\"name\": \"<field_name>\", \"type\": \"<field_type>\"}]");
    }

    for(const nlohmann::json & sort_field_json: req_json["sort_fields"]) {
        if(!sort_field_json.is_object() ||
           sort_field_json.count(fields::name) == 0 || sort_field_json.count(fields::type) == 0 ||
           !sort_field_json.at(fields::name).is_string() ||
           !sort_field_json.at(fields::type).is_string()) {

            return res.send_400("Wrong format for `sort_fields`. It should be an array like: "
                                        "[{\"name\": \"<field_name>\", \"type\": \"<field_type>\"}]");
        }

        if(sort_field_json["type"] != "INT32" && sort_field_json["type"] != "INT64") {
            return res.send_400("Sort field `" + sort_field_json["name"].get<std::string>()  + "` must be a number.");
        }

        sort_fields.push_back(field(sort_field_json["name"], sort_field_json["type"]));
    }

    const char* PREFIX_RANKING_FIELD = "prefix_ranking_field";
    std::string token_ranking_field = "";

    if(req_json.count(PREFIX_RANKING_FIELD) != 0) {
        if(!req_json[PREFIX_RANKING_FIELD].is_string()) {
            return res.send_400(std::string("Wrong format for `") + PREFIX_RANKING_FIELD + "`. It should be the name of an unsigned INT32 field.");
        }

        token_ranking_field = req_json[PREFIX_RANKING_FIELD].get<std::string>();
    }

    collectionManager.create_collection(req_json["name"], search_fields, facet_fields, sort_fields, token_ranking_field);
    res.send_201(req.body);
}

void del_drop_collection(http_req & req, http_res & res) {
    std::string doc_id = req.params["id"];

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Option<bool> drop_result = collectionManager.drop_collection(req.params["collection"]);

    if(!drop_result.ok()) {
        return res.send(drop_result.code(), drop_result.error());
    }

    nlohmann::json json_response;
    json_response["collection"] = req.params["collection"];
    res.send_200(json_response.dump());
}

void get_search(http_req & req, http_res & res) {
    auto begin = std::chrono::high_resolution_clock::now();

    const char *NUM_TYPOS = "num_typos";
    const char *PREFIX = "prefix";
    const char *FILTER = "filter_by";
    const char *SEARCH_BY = "search_by";
    const char *SORT_BY = "sort_by";
    const char *FACET_BY = "facet_by";
    const char *PER_PAGE = "per_page";
    const char *PAGE = "page";
    const char *CALLBACK = "callback";
    const char *SORT_PREFIXES_BY = "sort_prefixes_by";

    if(req.params.count(NUM_TYPOS) == 0) {
        req.params[NUM_TYPOS] = "2";
    }

    if(req.params.count(PREFIX) == 0) {
        req.params[PREFIX] = "false";
    }

    if(req.params.count(SEARCH_BY) == 0) {
        return res.send_400(std::string("Parameter `") + SEARCH_BY + "` is required.");
    }

    if(req.params.count(PER_PAGE) == 0) {
        req.params[PER_PAGE] = "10";
    }

    if(req.params.count(PAGE) == 0) {
        req.params[PAGE] = "1";
    }

    std::string filter_str = req.params.count(FILTER) != 0 ? req.params[FILTER] : "";

    std::vector<std::string> search_fields;
    StringUtils::split(req.params[SEARCH_BY], search_fields, ",");

    std::vector<std::string> facet_fields;
    StringUtils::split(req.params[FACET_BY], facet_fields, "&&");

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

    if(req.params.count(SORT_PREFIXES_BY) == 0) {
        if(prefix && !collection->get_token_ranking_field().empty()) {
            req.params[SORT_PREFIXES_BY] = "PREFIX_SORT_FIELD";
        } else {
            req.params[SORT_PREFIXES_BY] = "TERM_FREQUENCY";
        }
    }

    StringUtils::toupper(req.params[SORT_PREFIXES_BY]);
    token_ordering token_order = (req.params[SORT_PREFIXES_BY] == "PREFIX_SORT_FIELD") ? MAX_SCORE : FREQUENCY;

    Option<nlohmann::json> result_op = collection->search(req.params["q"], search_fields, filter_str, facet_fields,
                                               sort_fields, std::stoi(req.params[NUM_TYPOS]),
                                               std::stoi(req.params[PER_PAGE]), std::stoi(req.params[PAGE]),
                                               token_order, prefix);

    uint64_t timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::high_resolution_clock::now() - begin).count();


    if(!result_op.ok()) {
        const std::string & json_res_body = (req.params.count(CALLBACK) == 0) ? result_op.error() :
                                            (req.params[CALLBACK] + "(" + result_op.error() + ");");
        return res.send(result_op.code(), json_res_body);
    }

    nlohmann::json result = result_op.get();
    result["took_ms"] = timeMillis;
    const std::string & results_json_str = result.dump();

    //struct rusage r_usage;
    //getrusage(RUSAGE_SELF,&r_usage);
    //std::cout << "Memory usage: " << r_usage.ru_maxrss << std::endl;

    if(req.params.count(CALLBACK) == 0) {
        res.send_200(results_json_str);
    } else {
        res.send_200(req.params[CALLBACK] + "(" + results_json_str + ");");
    }

    std::cout << "Time taken: " << timeMillis << "ms" << std::endl;
}

void get_collection_summary(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        return res.send_404();
    }

    nlohmann::json json_response;

    json_response["name"] = collection->get_name();
    json_response["num_documents"] = collection->get_num_documents();
    res.send_200(json_response.dump());
}

void post_add_document(http_req & req, http_res & res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        return res.send_404();
    }

    Option<std::string> inserted_id_op = collection->add(req.body);

    if(!inserted_id_op.ok()) {
        res.send(inserted_id_op.code(), inserted_id_op.error());
    } else {
        nlohmann::json json_response;
        json_response["id"] = inserted_id_op.get();
        res.send_201(json_response.dump());
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

    Option<std::string> deleted_id_op = collection->remove(doc_id);

    if(!deleted_id_op.ok()) {
        res.send(deleted_id_op.code(), deleted_id_op.error());
    } else {
        nlohmann::json json_response;
        json_response["id"] = deleted_id_op.get();
        res.send_200(json_response.dump());
    }
}

void get_replication_updates(http_req &req, http_res &res) {
    if(!StringUtils::is_uint64_t(req.params["seq_number"])) {
        return res.send_400("The value of the parameter `seq_number` must be an unsigned integer.");
    }

    const uint64_t MAX_UPDATES_TO_SEND = 2000;
    uint64_t seq_number = std::stoull(req.params["seq_number"]);

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Store* store = collectionManager.get_store();
    Option<std::vector<std::string>*> updates_op = store->get_updates_since(seq_number, MAX_UPDATES_TO_SEND);
    if(!updates_op.ok()) {
        return res.send(updates_op.code(), updates_op.error());
    }

    nlohmann::json json_response;
    json_response["updates"] = nlohmann::json::array();

    std::vector<std::string> *updates = updates_op.get();
    for(const std::string & update: *updates) {
        json_response["updates"].push_back(StringUtils::base64_encode(update));
    }

    res.send_200(json_response.dump());
}