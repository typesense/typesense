#include <regex>
#include <chrono>
#include <sys/resource.h>
#include "api.h"
#include "string_utils.h"
#include "collection.h"
#include "collection_manager.h"

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

    collectionManager.create_collection(req_json["name"], search_fields, facet_fields, sort_fields);
    res.send_201(req.body);
}

void get_search(http_req & req, http_res & res) {
    const char *NUM_TYPOS = "num_typos";
    const char *PREFIX = "prefix";
    const char *FILTER = "filter_by";
    const char *SEARCH_BY = "search_by";
    const char *SORT_BY = "sort_by";
    const char *FACET_BY = "facet_by";
    const char *TOKEN_ORDERING = "token_ordering";

    if(req.params.count(NUM_TYPOS) == 0) {
        req.params[NUM_TYPOS] = "2";
    }

    if(req.params.count(PREFIX) == 0) {
        req.params[PREFIX] = "false";
    }

    if(req.params.count(TOKEN_ORDERING) == 0) {
        req.params[TOKEN_ORDERING] = "FREQUENCY";
    }

    if(req.params.count(SEARCH_BY) == 0) {
        return res.send_400(std::string("Parameter `") + SEARCH_BY + "` is required.");
    }

    std::string filter_str = req.params.count(FILTER) != 0 ? req.params[FILTER] : "";

    token_ordering token_order = (req.params[TOKEN_ORDERING] == "MAX_SCORE") ? MAX_SCORE : FREQUENCY;

    std::vector<std::string> search_fields;
    StringUtils::split(req.params[SEARCH_BY], search_fields, ",");

    std::vector<std::string> facet_fields;
    StringUtils::split(req.params[FACET_BY], facet_fields, ",");

    std::vector<sort_field> sort_fields;
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
            sort_fields.push_back(sort_field(expression_parts[0], expression_parts[1]));
        }
    }

    auto begin = std::chrono::high_resolution_clock::now();

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection* collection = collectionManager.get_collection(req.params["collection"]);

    if(collection == nullptr) {
        return res.send_404();
    }

    nlohmann::json result = collection->search(req.params["q"], search_fields, filter_str, facet_fields,
                                               sort_fields, std::stoi(req.params[NUM_TYPOS]), 100,
                                               token_order, false);
    const std::string & json_str = result.dump();
    //std::cout << "JSON:" << json_str << std::endl;
    struct rusage r_usage;
    getrusage(RUSAGE_SELF,&r_usage);

    //std::cout << "Memory usage: " << r_usage.ru_maxrss << std::endl;
    res.send_200(json_str);

    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    std::cout << "Time taken: " << timeMillis << "us" << std::endl;
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