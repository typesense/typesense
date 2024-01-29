#include <chrono>
#include <thread>
#include <app_metrics.h>
#include <regex>
#include <analytics_manager.h>
#include "typesense_server_utils.h"
#include "core_api.h"
#include "string_utils.h"
#include "collection.h"
#include "collection_manager.h"
#include "system_metrics.h"
#include "logger.h"
#include "core_api_utils.h"
#include "lru/lru.hpp"
#include "ratelimit_manager.h"
#include "event_manager.h"
#include "http_proxy.h"
#include "include/stopwords_manager.h"
#include "conversation_manager.h"
#include "conversation_model_manager.h"
#include "conversation_model.h"

using namespace std::chrono_literals;

std::shared_mutex mutex;
LRU::Cache<uint64_t, cached_res_t> res_cache;

std::atomic<bool> alter_in_progress = false;

void init_api(uint32_t cache_num_entries) {
    res_cache.capacity(cache_num_entries);
}

void set_alter_in_progress(bool in_progress) {
    alter_in_progress = in_progress;
}

bool get_alter_in_progress() {
    return alter_in_progress;
}

bool handle_authentication(std::map<std::string, std::string>& req_params,
                           std::vector<nlohmann::json>& embedded_params_vec,
                           const std::string& body,
                           const route_path& rpath,
                           const std::string& req_auth_key) {
    CollectionManager & collectionManager = CollectionManager::get_instance();

    std::vector<collection_key_t> collections;

    get_collections_for_auth(req_params, body, rpath, req_auth_key, collections, embedded_params_vec);

    if(rpath.handler == get_health) {
        // health endpoint requires no authentication
        return true;
    }

    if(collections.size() != embedded_params_vec.size()) {
        LOG(ERROR) << "Impossible error: size of collections and embedded_params_vec don't match, "
                   << "collections.size: " << collections.size()
                   << ", embedded_params_vec.size: " << embedded_params_vec.size();
        return false;
    }

    return collectionManager.auth_key_matches(req_auth_key, rpath.action, collections, req_params, embedded_params_vec);
}

void stream_response(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    if(!res->is_alive) {
        // underlying request is dead or this is a raft log playback
        return ;
    }

    // wait for previous chunk to finish (if any)
    res->wait();

    auto req_res = new async_req_res_t(req, res, true);
    server->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
}

void defer_processing(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res, size_t timeout_ms) {
    defer_processing_t* defer = new defer_processing_t(req, res, timeout_ms, server);
    //LOG(INFO) << "core_api req " << req.get() << ", use count: " << req.use_count();
    server->get_message_dispatcher()->send_message(HttpServer::DEFER_PROCESSING_MESSAGE, defer);
}

// we cannot return errors here because that will end up as auth failure and won't convey
// bad schema errors
void get_collections_for_auth(std::map<std::string, std::string>& req_params,
                                      const string& body,
                                      const route_path& rpath, const string& req_auth_key,
                                      std::vector<collection_key_t>& collections,
                                      std::vector<nlohmann::json>& embedded_params_vec) {

    if(rpath.handler == post_multi_search) {
        nlohmann::json req_obj;

        // If a `preset` parameter is present, we've to only load a pre-existing search configuration
        // and ignore the actual request body.
        auto preset_it = req_params.find("preset");
        if(preset_it != req_params.end()) {
            CollectionManager::get_instance().get_preset(preset_it->second, req_obj);
        } else {
            req_obj = nlohmann::json::parse(body, nullptr, false);
        }

        if(!req_obj.is_discarded() && req_obj.count("searches") != 0  && req_obj["searches"].is_array()) {
            for(auto& el : req_obj["searches"]) {
                if(el.is_object()) {
                    std::string coll_name;
                    if(el.count("collection") != 0 && el["collection"].is_string()) {
                        coll_name = el["collection"].get<std::string>();
                    } else if(req_params.count("collection") != 0) {
                        coll_name = req_params["collection"];
                    } else {
                        // if preset exists, that should be the lowest priority
                        if(el.count("preset") != 0) {
                            nlohmann::json preset_obj;
                            auto preset_op = CollectionManager::get_instance().
                                    get_preset(el["preset"].get<std::string>(), preset_obj);
                            if(preset_op.ok() && preset_obj.count("collection") != 0  &&
                                preset_obj["collection"].is_string()) {
                                coll_name = preset_obj["collection"].get<std::string>();
                            }
                        }
                    }

                    const std::string& access_key = (el.count("x-typesense-api-key") != 0 &&
                                                     el["x-typesense-api-key"].is_string()) ?
                                                    el["x-typesense-api-key"].get<std::string>() :
                                                    req_auth_key;

                    collections.emplace_back(coll_name, access_key);
                    embedded_params_vec.emplace_back(nlohmann::json::object());
                } else {
                    collections.emplace_back("", req_auth_key);
                    embedded_params_vec.emplace_back(nlohmann::json::object());
                }
            }
        } else {
            LOG(ERROR) << "Multi search request body is malformed, body: " << body;
        }
    } else {
        if(rpath.handler == post_create_collection) {
            nlohmann::json obj = nlohmann::json::parse(body, nullptr, false);

            if(obj.is_discarded()) {
                LOG(ERROR) << "Create collection request body is malformed.";
            }

            else if(obj.count("name") != 0 && obj["name"].is_string()) {
                collections.emplace_back(obj["name"].get<std::string>(), req_auth_key);
                embedded_params_vec.emplace_back(nlohmann::json::object());
            }

        } else if(req_params.count("collection") != 0) {
            collections.emplace_back(req_params.at("collection"), req_auth_key);
            embedded_params_vec.emplace_back(nlohmann::json::object());
        }
    }

    if(collections.empty()) {
        collections.emplace_back("", req_auth_key);
        embedded_params_vec.emplace_back(nlohmann::json::object());
    }
}

index_operation_t get_index_operation(const std::string& action) {
    if(action == "create") {
        return CREATE;
    } else if(action == "update") {
        return UPDATE;
    } else if(action == "upsert") {
        return UPSERT;
    } else if(action == "emplace") {
        return EMPLACE;
    }

    return CREATE;
}

bool get_collections(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    nlohmann::json json_response = collectionManager.get_collection_summaries();
    res->set_200(json_response.dump());
    return true;
}

bool post_create_collection(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const std::exception& e) {
        //LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    const std::string SRC_COLL_NAME = "src_name";

    /*if(res->is_alive && req_json.is_object() && req_json.count("enable_nested_fields") == 0) {
        // This patch ensures that nested fields are only enabled for collections created on Typesense versions
        // which support nested fields. This ensures that ".*" schema does not end up duplicating fields on
        // manually flattened collection schemas that also contain nested versions for convenience.
        // TO BE ENABLED WHEN READY!
        // req_json["enable_nested_fields"] = true;
    }*/

    CollectionManager& collectionManager = CollectionManager::get_instance();
    const Option<Collection*> &collection_op = req->params.count(SRC_COLL_NAME) != 0 ?
               collectionManager.clone_collection(req->params[SRC_COLL_NAME], req_json) :
               CollectionManager::create_collection(req_json);

    if(collection_op.ok()) {
        nlohmann::json json_response = collection_op.get()->get_summary_json();
        res->set_201(json_response.dump());
        return true;
    }

    res->set(collection_op.code(), collection_op.error());
    return false;
}

bool patch_update_collection(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const std::exception& e) {
        //LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        alter_in_progress = false;
        return false;
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        res->set_404();
        alter_in_progress = false;
        return false;
    }

    auto alter_op = collection->alter(req_json);
    if(!alter_op.ok()) {
        res->set(alter_op.code(), alter_op.error());
        alter_in_progress = false;
        return false;
    }

    alter_in_progress = false;
    res->set_200(req_json.dump());
    return true;
}

bool del_drop_collection(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    bool compact_store = false;

    if(req->params.count("compact_store") != 0) {
        compact_store = (req->params["compact_store"] == "true");
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Option<nlohmann::json> drop_op = collectionManager.drop_collection(req->params["collection"], true, compact_store);

    if(!drop_op.ok()) {
        res->set(drop_op.code(), drop_op.error());
        return false;
    }

    res->set_200(drop_op.get().dump());
    return true;
}

bool get_debug(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json result;
    result["version"] = server->get_version();

    uint64_t state = server->node_state();
    result["state"] = state;

    res->set_200(result.dump());
    return true;
}

bool get_health(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json result;
    bool alive = server->is_alive();
    result["ok"] = alive;

    auto resource_error = cached_resource_stat_t::get_instance().get_out_of_resource_error();
    if (resource_error != cached_resource_stat_t::resource_check_t::OK) {
        result["resource_error"] = std::string(magic_enum::enum_name(resource_error));
    }

    if(alive) {
        res->set_body(200, result.dump());
    } else {
        res->set_body(503, result.dump());
    }

    return alive;
}

bool post_health(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json result;
    bool alive = server->is_alive();
    result["ok"] = alive;

    if(alive) {
        res->set_body(200, result.dump());
    } else {
        res->set_body(503, result.dump());
    }

    return alive;
}

bool get_metrics_json(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json result;

    CollectionManager & collectionManager = CollectionManager::get_instance();
    const std::string & data_dir_path = collectionManager.get_store()->get_state_dir_path();

    SystemMetrics sys_metrics;
    sys_metrics.get(data_dir_path, result);

    res->set_body(200, result.dump(2));
    return true;
}

bool get_stats_json(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json result;
    AppMetrics::get_instance().get("requests_per_second", "latency_ms", result);
    result["pending_write_batches"] = server->get_num_queued_writes();

    res->set_body(200, result.dump(2));
    return true;
}

bool get_status(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json status = server->node_status();
    res->set_body(200, status.dump());
    return true;
}

uint64_t hash_request(const std::shared_ptr<http_req>& req) {
    std::stringstream ss;
    ss << req->route_hash << req->body;

    for(auto& kv: req->params) {
        if(kv.first != "use_cache") {
            ss << kv.second;
        }
    }

    const std::string& req_str = ss.str();
    return StringUtils::hash_wy(req_str.c_str(), req_str.size());
}

bool get_search(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const auto use_cache_it = req->params.find("use_cache");
    bool use_cache = (use_cache_it != req->params.end()) && (use_cache_it->second == "1" || use_cache_it->second == "true");
    uint64_t req_hash = 0;

    if(use_cache) {
        // cache enabled, let's check if request is already in the cache
        req_hash = hash_request(req);

        //LOG(INFO) << "req_hash = " << req_hash;

        std::unique_lock lock(mutex);
        auto hit_it = res_cache.find(req_hash);
        if(hit_it != res_cache.end()) {
            //LOG(INFO) << "Result found in cache.";
            const auto& cached_value = hit_it.value();

            // we still need to check that TTL has not expired
            uint32_t ttl = cached_value.ttl;
            uint64_t seconds_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::high_resolution_clock::now() - cached_value.created_at).count();

            if(seconds_elapsed < cached_value.ttl) {
                res->set_content(cached_value.status_code, cached_value.content_type_header, cached_value.body, true);
                return true;
            }

            // Result found in cache but ttl has lapsed.
            res_cache.erase(req_hash);
        }
    }

    if(req->embedded_params_vec.empty()) {
        res->set_500("Embedded params is empty.");
        return false;
    }

    std::string results_json_str;
    Option<bool> search_op = CollectionManager::do_search(req->params, req->embedded_params_vec[0],
                                                          results_json_str, req->conn_ts);

    if(!search_op.ok()) {
        res->set(search_op.code(), search_op.error());
        if(search_op.code() == 408) {
            req->overloaded = true;
        }
        return false;
    }

    res->set_200(results_json_str);

    // we will cache only successful requests
    if(use_cache) {
        //LOG(INFO) << "Adding to cache, key = " << req_hash;
        auto now = std::chrono::high_resolution_clock::now();
        const auto cache_ttl_it = req->params.find("cache_ttl");
        uint32_t cache_ttl = 60;
        if(cache_ttl_it != req->params.end() && StringUtils::is_int32_t(cache_ttl_it->second)) {
            cache_ttl = std::stoul(cache_ttl_it->second);
        }

        cached_res_t cached_res;
        cached_res.load(res->status_code, res->content_type_header, res->body, now, cache_ttl, req_hash);

        std::unique_lock lock(mutex);
        res_cache.insert(req_hash, cached_res);
    }

    return true;
}

bool post_multi_search(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const auto use_cache_it = req->params.find("use_cache");
    bool use_cache = (use_cache_it != req->params.end()) && (use_cache_it->second == "1" || use_cache_it->second == "true");
    uint64_t req_hash = 0;

    if(use_cache) {
        // cache enabled, let's check if request is already in the cache
        req_hash = hash_request(req);

        //LOG(INFO) << "req_hash = " << req_hash;

        std::unique_lock lock(mutex);
        auto hit_it = res_cache.find(req_hash);
        if(hit_it != res_cache.end()) {
            //LOG(INFO) << "Result found in cache.";
            const auto& cached_value = hit_it.value();

            // we still need to check that TTL has not expired
            uint32_t ttl = cached_value.ttl;
            uint64_t seconds_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::high_resolution_clock::now() - cached_value.created_at).count();

            if(seconds_elapsed < cached_value.ttl) {
                res->set_content(cached_value.status_code, cached_value.content_type_header, cached_value.body, true);
                return true;
            }

            // Result found in cache but ttl has lapsed.
            res_cache.erase(req_hash);
        }
    }

    nlohmann::json req_json;

    const auto preset_it = req->params.find("preset");
    if(preset_it != req->params.end()) {
        CollectionManager::get_instance().get_preset(preset_it->second, req_json);
    } else {
        try {
            req_json = nlohmann::json::parse(req->body);
        } catch(const std::exception& e) {
            LOG(ERROR) << "JSON error: " << e.what();
            res->set_400("Bad JSON.");
            return false;
        }
    }

    if(req_json.count("searches") == 0) {
        res->set_400("Missing `searches` array.");
        return false;
    }

    if(!req_json["searches"].is_array()) {
        res->set_400("Missing `searches` array.");
        return false;
    }

    if(req->embedded_params_vec.empty()) {
        res->set_400("Missing embedded params array.");
        return false;
    }

    auto orig_req_params = req->params;
    const char* LIMIT_MULTI_SEARCHES = "limit_multi_searches";
    size_t limit_multi_searches = 50;

    if(orig_req_params.count(LIMIT_MULTI_SEARCHES) != 0 &&
        StringUtils::is_uint32_t(orig_req_params[LIMIT_MULTI_SEARCHES])) {
        limit_multi_searches = std::stoi(orig_req_params[LIMIT_MULTI_SEARCHES]);
    }

    const auto& first_embedded_param = req->embedded_params_vec[0];
    if(first_embedded_param.count(LIMIT_MULTI_SEARCHES) != 0 && first_embedded_param[LIMIT_MULTI_SEARCHES].is_number_integer()) {
        limit_multi_searches = first_embedded_param[LIMIT_MULTI_SEARCHES].get<size_t>();
    }

    if(req_json["searches"].size() > limit_multi_searches) {
        res->set_400(std::string("Number of multi searches exceeds `") + LIMIT_MULTI_SEARCHES + "` parameter.");
        return false;
    }

    nlohmann::json response;
    response["results"] = nlohmann::json::array();

    nlohmann::json& searches = req_json["searches"];

    if(searches.size() != req->embedded_params_vec.size()) {
        LOG(ERROR) << "Embedded params parsing error: length does not match multi search array, searches.size(): "
                   << searches.size() << ", embedded_params_vec.size: " << req->embedded_params_vec.size()
                   << ", req_body: " << req->body;
        res->set_500("Embedded params parsing error.");
        return false;
    }

    // Get API key and IP
    if(!req->metadata.empty()) {
        auto api_key_ip_op = get_api_key_and_ip(req->metadata);
        if(!api_key_ip_op.ok()) {
            res->set(api_key_ip_op.code(), api_key_ip_op.error());
            return false;
        }
        const auto& api_key_ip = api_key_ip_op.get();
        auto rate_limit_manager = RateLimitManager::getInstance();

        // Check rate limiting first before doing any search, don't want to waste time if we're rate limited
        for(size_t i = 0; i < searches.size(); i++) {
            if(RateLimitManager::getInstance()->is_rate_limited({RateLimitedEntityType::api_key, api_key_ip.first}, {RateLimitedEntityType::ip, api_key_ip.second})) {
                res->set(429, "Rate limit exceeded or blocked");
                return false;
            }
        }
    }

    bool conversation = orig_req_params["conversation"] == "true";
    bool conversation_history = orig_req_params.find("conversation_id") != orig_req_params.end();
    std::string common_query;

    if(!conversation && conversation_history) {
        res->set_400("`conversation_id` can only be used if `conversation` is enabled.");
        return false;
    }

    if(conversation) {
        if(orig_req_params.find("q") == orig_req_params.end()) {
            res->set_400("`q` parameter has to be common for all searches if conversation is enabled. Please set `q` as a query parameter in the request, instead of inside the POST body");
            return false;
        }

        if(orig_req_params.find("conversation_model_id") == orig_req_params.end()) {
            res->set_400("`conversation_model_id` is needed if conversation is enabled.");
            return false;
        }

        const std::string& conversation_model_id = orig_req_params["conversation_model_id"];
        auto conversation_model = ConversationModelManager::get_model(conversation_model_id);

        if(!conversation_model.ok()) {
            res->set_400("`conversation_model_id` is invalid.");
            return false;
        }

        if(conversation_history) {
            std::string conversation_id = orig_req_params["conversation_id"];

            auto conversation_history = ConversationManager::get_instance().get_conversation(conversation_id);

            if(!conversation_history.ok()) {
                res->set_400("`conversation_id` is invalid.");
                return false;
            }
        }

        common_query = orig_req_params["q"];

        if(conversation_history) {
            const std::string& conversation_model_id = orig_req_params["conversation_model_id"];
            auto conversation_id = orig_req_params["conversation_id"];
            auto conversation_model = ConversationModelManager::get_model(conversation_model_id).get();
            auto conversation_history = ConversationManager::get_instance().get_conversation(conversation_id).get();
            auto generate_standalone_q = ConversationModel::get_standalone_question(conversation_history, common_query, conversation_model);

            if(!generate_standalone_q.ok()) {
                res->set_400(generate_standalone_q.error());
                return false;
            }

            orig_req_params["q"] = generate_standalone_q.get();
        }
    }

    for(size_t i = 0; i < searches.size(); i++) {
        auto& search_params = searches[i];

        if(!search_params.is_object()) {
            res->set_400("The value of `searches` must be an array of objects.");
            return false;
        }

        req->params = orig_req_params;

        for(auto& search_item: search_params.items()) {
            if(search_item.key() == "cache_ttl") {
                // cache ttl can be applied only from an embedded key: cannot be a multi search param
                continue;
            }

            if(conversation && search_item.key() == "q") {
                // q is common for all searches
                res->set_400("`q` parameter cannot be used in POST body if `conversation` is enabled. Please set `q` as a query parameter in the request, instead of inside the POST body");
                return false;
            }

            if(conversation && search_item.key() == "conversation_model_id") {
                // conversation_model_id is common for all searches
                res->set_400("`conversation_model_id` cannot be used in POST body. Please set `conversation_model_id` as a query parameter in the request, instead of inside the POST body");
                return false;
            }

            if(conversation && search_item.key() == "conversation_id") {
                // conversation_id is common for all searches
                res->set_400("`conversation_id` cannot be used in POST body. Please set `conversation_id` as a query parameter in the request, instead of inside the POST body");
                return false;
            }

            if(search_item.key() == "conversation") {
                res->set_400("`conversation` cannot be used in POST body. Please set `conversation` as a query parameter in the request, instead of inside the POST body");
                return false;
            }

            // overwrite = false since req params will contain embedded params and so has higher priority
            bool populated = AuthManager::add_item_to_params(req->params, search_item, false);
            if(!populated) {
                res->set_400("One or more search parameters are malformed.");
                return false;
            }
        }

        if(req->params.count("conversation") != 0) {
            req->params.erase("conversation");
        }

        if(req->params.count("conversation_id") != 0) {
            req->params.erase("conversation_id");
        }

        if(req->params.count("conversation_model_id") != 0) {
            req->params.erase("conversation_model_id");
        }

        std::string results_json_str;
        Option<bool> search_op = CollectionManager::do_search(req->params, req->embedded_params_vec[i],
                                                              results_json_str, req->conn_ts);

        if(search_op.ok()) {
            auto results_json = nlohmann::json::parse(results_json_str);
            if(conversation) {
                results_json["request_params"]["q"] = common_query;
            }
            response["results"].push_back(results_json);
        } else {
            if(search_op.code() == 408) {
                res->set(search_op.code(), search_op.error());
                req->overloaded = true;
                return false;
            }
            nlohmann::json err_res;
            err_res["error"] = search_op.error();
            err_res["code"] = search_op.code();
            response["results"].push_back(err_res);
        }
    }

    if(conversation) {
        nlohmann::json result_docs_arr = nlohmann::json::array();
        int res_index = 0;
        for(const auto& result : response["results"]) {
            nlohmann::json result_docs = nlohmann::json::array();

            std::vector<std::string> vector_fields;

            auto collection = CollectionManager::get_instance().get_collection(searches[res_index]["collection"].get<std::string>());
            auto search_schema = collection->get_schema();

            for(const auto& field : search_schema) {
                if(field.type == field_types::FLOAT_ARRAY) {
                    vector_fields.push_back(field.name);
                }
            }

            if(result.contains("grouped_hits")) {
                for(const auto& grouped_hit : result["grouped_hits"]) {
                    for(const auto& hit : grouped_hit["hits"]) {
                        auto doc = hit["document"];
                        for(const auto& vector_field : vector_fields) {
                            if(doc.contains(vector_field)) {
                                doc.erase(vector_field);
                            }
                        }
                        result_docs.push_back(doc);
                    }
                }
            }
            else {
                for(const auto& hit : result["hits"]) {
                    auto doc = hit["document"];
                    for(const auto& vector_field : vector_fields) {
                        if(doc.contains(vector_field)) {
                            doc.erase(vector_field);
                        }
                    }
                    result_docs.push_back(doc);
                }
            }

            result_docs_arr.push_back(result_docs);
        }

        // We have to pop a document from the search result with max size
        // Until we do not exceed MAX_TOKENS limit
        while(ConversationManager::get_instance().get_token_count(result_docs_arr) > ConversationManager::get_instance().MAX_TOKENS) {
            // sort the result_docs_arr by size descending
            std::sort(result_docs_arr.begin(), result_docs_arr.end(), [](const auto& a, const auto& b) {
                return a.size() > b.size();
            });

            // pop the last element from first array
            if(result_docs_arr.size() > 0 && result_docs_arr[0].size() > 0) {
                result_docs_arr[0].erase(result_docs_arr[0].size() - 1);
            }
        }


        // Make result_docs_arr 1D
        nlohmann::json result_docs = nlohmann::json::array();
        for(const auto& result_doc : result_docs_arr) {
            for(const auto& doc : result_doc) {
                result_docs.push_back(doc);
            }
        }

        const std::string& conversation_model_id = orig_req_params["conversation_model_id"];
        auto conversation_model = ConversationModelManager::get_model(conversation_model_id).get();

        auto prompt = req->params["q"];

        auto answer_op = ConversationModel::get_answer(result_docs.dump(0), prompt, conversation_model);

        if(!answer_op.ok()) {
            res->set_400(answer_op.error());
            return false;
        }

        response["conversation"] = nlohmann::json::object();
        response["conversation"]["query"] = common_query;
        response["conversation"]["answer"] = answer_op.get();

        auto formatted_question_op = ConversationModel::format_question(common_query, conversation_model);
        if(!formatted_question_op.ok()) {
            res->set_400(formatted_question_op.error());
            return false;
        }


        auto formatted_answer_op = ConversationModel::format_answer(answer_op.get(), conversation_model);
        if(!formatted_answer_op.ok()) {
            res->set_400(formatted_answer_op.error());
            return false;
        }

        std::vector<std::string> exclude_fields;
        StringUtils::split(req->params["exclude_fields"], exclude_fields, ",");
        bool exclude_conversation_history = std::find(exclude_fields.begin(), exclude_fields.end(), "conversation_history") != exclude_fields.end();

        if(conversation_history) {
            std::string conversation_id = orig_req_params["conversation_id"];
            ConversationManager::get_instance().append_conversation(conversation_id, formatted_question_op.get());
            ConversationManager::get_instance().append_conversation(conversation_id, formatted_answer_op.get());
            auto get_conversation_op = ConversationManager::get_instance().get_conversation(conversation_id);
            if(!get_conversation_op.ok()) {
                res->set_400(get_conversation_op.error());
                return false;
            }

            auto conversation_history = get_conversation_op.get();
            if(!exclude_conversation_history) {
                response["conversation"]["conversation_history"] = conversation_history;
            }
            response["conversation"]["conversation_id"] = conversation_id;
        } else {
            nlohmann::json conversation_history = nlohmann::json::array();
            conversation_history.push_back(formatted_question_op.get());
            conversation_history.push_back(formatted_answer_op.get());

            auto create_conversation_op = ConversationManager::get_instance().create_conversation(conversation_history);
            if(!create_conversation_op.ok()) {
                res->set_400(create_conversation_op.error());
                return false;
            }

            auto get_conversation_op = ConversationManager::get_instance().get_conversation(create_conversation_op.get());
            if(!get_conversation_op.ok()) {
                res->set_400(get_conversation_op.error());
                return false;
            }
            if(!exclude_conversation_history) {
                response["conversation"]["conversation_history"] = get_conversation_op.get();
            }
            response["conversation"]["conversation_id"] = create_conversation_op.get();
        }

    }

    res->set_200(response.dump());

    // we will cache only successful requests
    if(use_cache) {
        //LOG(INFO) << "Adding to cache, key = " << req_hash;
        auto now = std::chrono::high_resolution_clock::now();
        const auto cache_ttl_it = req->params.find("cache_ttl");
        uint32_t cache_ttl = 60;
        if(cache_ttl_it != req->params.end() && StringUtils::is_int32_t(cache_ttl_it->second)) {
            cache_ttl = std::stoul(cache_ttl_it->second);
        }

        cached_res_t cached_res;
        cached_res.load(res->status_code, res->content_type_header, res->body, now, cache_ttl, req_hash);

        std::unique_lock lock(mutex);
        res_cache.insert(req_hash, cached_res);
    }

    return true;
}

bool get_collection_summary(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager& collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    nlohmann::json json_response = collection->get_summary_json();
    res->set_200(json_response.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore));

    return true;
}

bool get_export_documents(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    // NOTE: this is a streaming response end-point so this handler will be called multiple times
    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        req->last_chunk_aggregate = true;
        res->final = true;
        res->set_404();
        stream_response(req, res);
        return false;
    }

    const char* FILTER_BY = "filter_by";
    const char* INCLUDE_FIELDS = "include_fields";
    const char* EXCLUDE_FIELDS = "exclude_fields";
    const char* BATCH_SIZE = "batch_size";

    export_state_t* export_state = nullptr;

    const std::string seq_id_prefix = collection->get_seq_id_collection_prefix();

    if(req->data == nullptr) {
        export_state = new export_state_t();

        // destruction of data is managed by req destructor
        req->data = export_state;

        std::string simple_filter_query;
        spp::sparse_hash_set<std::string> exclude_fields;
        spp::sparse_hash_set<std::string> include_fields;

        if(req->params.count(FILTER_BY) != 0) {
            simple_filter_query = req->params[FILTER_BY];
        }

        if(req->params.count(INCLUDE_FIELDS) != 0) {
            std::vector<std::string> include_fields_vec;
            StringUtils::split(req->params[INCLUDE_FIELDS], include_fields_vec, ",");
            include_fields = spp::sparse_hash_set<std::string>(include_fields_vec.begin(), include_fields_vec.end());
        }

        if(req->params.count(EXCLUDE_FIELDS) != 0) {
            std::vector<std::string> exclude_fields_vec;
            StringUtils::split(req->params[EXCLUDE_FIELDS], exclude_fields_vec, ",");
            exclude_fields = spp::sparse_hash_set<std::string>(exclude_fields_vec.begin(), exclude_fields_vec.end());
        }

        collection->populate_include_exclude_fields_lk(include_fields, exclude_fields,
                                                      export_state->include_fields, export_state->exclude_fields);

        if(req->params.count(BATCH_SIZE) != 0 && StringUtils::is_uint32_t(req->params[BATCH_SIZE])) {
            export_state->export_batch_size = std::stoul(req->params[BATCH_SIZE]);
        }

        if(simple_filter_query.empty()) {
            export_state->iter_upper_bound_key = collection->get_seq_id_collection_prefix() + "`";  // cannot inline this
            export_state->iter_upper_bound = new rocksdb::Slice(export_state->iter_upper_bound_key);
            export_state->it = collectionManager.get_store()->scan(seq_id_prefix, export_state->iter_upper_bound);
        } else {
            filter_result_t filter_result;
            auto filter_ids_op = collection->get_filter_ids(simple_filter_query, filter_result);

            if(!filter_ids_op.ok()) {
                res->set(filter_ids_op.code(), filter_ids_op.error());
                req->last_chunk_aggregate = true;
                res->final = true;
                stream_response(req, res);
                return false;
            }

            export_state->index_ids.emplace_back(filter_result.count, filter_result.docs);
            filter_result.docs = nullptr;

            for(size_t i=0; i<export_state->index_ids.size(); i++) {
                export_state->offsets.push_back(0);
            }
            export_state->res_body = &res->body;
            export_state->collection = collection.get();
        }
    } else {
        export_state = dynamic_cast<export_state_t*>(req->data);
    }

    if(export_state->it != nullptr) {
        rocksdb::Iterator* it = export_state->it;
        size_t batch_counter = 0;
        std::string().swap(res->body);

        while(it->Valid() && it->key().ToString().compare(0, seq_id_prefix.size(), seq_id_prefix) == 0) {
            if(export_state->include_fields.empty() && export_state->exclude_fields.empty()) {
                res->body += it->value().ToString();
            } else {
                nlohmann::json doc = nlohmann::json::parse(it->value().ToString());
                Collection::prune_doc(doc, export_state->include_fields, export_state->exclude_fields);
                res->body += doc.dump();
            }

            it->Next();

            // append a new line character if there is going to be one more record to send
            if(it->Valid() && it->key().ToString().compare(0, seq_id_prefix.size(), seq_id_prefix) == 0) {
                res->body += "\n";
                req->last_chunk_aggregate = false;
                res->final = false;
            } else {
                req->last_chunk_aggregate = true;
                res->final = true;
            }

            batch_counter++;
            if(batch_counter == export_state->export_batch_size) {
                break;
            }
        }
    } else {
        bool done;
        stateful_export_docs(export_state, export_state->export_batch_size, done);

        if(!done) {
            req->last_chunk_aggregate = false;
            res->final = false;
        } else {
            req->last_chunk_aggregate = true;
            res->final = true;
        }
    }

    res->content_type_header = "text/plain; charset=utf-8";
    res->status_code = 200;

    stream_response(req, res);
    return true;
}

bool post_import_documents(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    //LOG(INFO) << "Import, req->body_index=" << req->body_index << ", body size: " << req->body.size();
    //LOG(INFO) << "req->first_chunk=" << req->first_chunk_aggregate << ", last_chunk=" << req->last_chunk_aggregate;

    const char *BATCH_SIZE = "batch_size";
    const char *ACTION = "action";
    const char *DIRTY_VALUES = "dirty_values";
    const char *RETURN_DOC = "return_doc";
    const char *RETURN_ID = "return_id";
    const char *REMOTE_EMBEDDING_BATCH_SIZE = "remote_embedding_batch_size";
    const char *REMOTE_EMBEDDING_TIMEOUT_MS = "remote_embedding_timeout_ms";
    const char *REMOTE_EMBEDDING_NUM_TRIES = "remote_embedding_num_tries";

    if(req->params.count(BATCH_SIZE) == 0) {
        req->params[BATCH_SIZE] = "40";
    }

    if(req->params.count(REMOTE_EMBEDDING_BATCH_SIZE) == 0) {
        req->params[REMOTE_EMBEDDING_BATCH_SIZE] = "200";
    }

    if(req->params.count(ACTION) == 0) {
        req->params[ACTION] = "create";
    }

    if(req->params.count(DIRTY_VALUES) == 0) {
        req->params[DIRTY_VALUES] = "";  // set it empty as default will depend on `index_all_fields`
    }

    if(req->params.count(RETURN_DOC) == 0) {
        req->params[RETURN_DOC] = "false";
    }

    if(req->params.count(RETURN_ID) == 0) {
        req->params[RETURN_ID] = "false";
    }

    if(!StringUtils::is_uint32_t(req->params[BATCH_SIZE])) {
        res->final = true;
        res->set_400("Parameter `" + std::string(BATCH_SIZE) + "` must be a positive integer.");
        stream_response(req, res);
        return false;
    }

    if(req->params[ACTION] != "create" && req->params[ACTION] != "update" && req->params[ACTION] != "upsert" &&
       req->params[ACTION] != "emplace") {
        res->final = true;
        res->set_400("Parameter `" + std::string(ACTION) + "` must be a create|update|upsert.");
        stream_response(req, res);
        return false;
    }

    if(req->params[RETURN_DOC] != "true" && req->params[RETURN_DOC] != "false") {
        res->final = true;
        res->set_400("Parameter `" + std::string(RETURN_DOC) + "` must be a true|false.");
        stream_response(req, res);
        return false;
    }

    if(req->params[RETURN_ID] != "true" && req->params[RETURN_ID] != "false") {
        res->final = true;
        res->set_400("Parameter `" + std::string(RETURN_ID) + "` must be a true|false.");
        stream_response(req, res);
        return false;
    }

    if(req->params.count(REMOTE_EMBEDDING_TIMEOUT_MS) == 0) {
        req->params[REMOTE_EMBEDDING_TIMEOUT_MS] = "60000";
    }

    if(req->params.count(REMOTE_EMBEDDING_NUM_TRIES) == 0) {
        req->params[REMOTE_EMBEDDING_NUM_TRIES] = "2";
    }

    const size_t IMPORT_BATCH_SIZE = std::stoi(req->params[BATCH_SIZE]);
    const size_t REMOTE_EMBEDDING_BATCH_SIZE_VAL = std::stoi(req->params[REMOTE_EMBEDDING_BATCH_SIZE]);
    const size_t REMOTE_EMBEDDING_TIMEOUT_MS_VAL = std::stoi(req->params[REMOTE_EMBEDDING_TIMEOUT_MS]);
    const size_t REMOTE_EMBEDDING_NUM_TRIES_VAL = std::stoi(req->params[REMOTE_EMBEDDING_NUM_TRIES]);

    if(IMPORT_BATCH_SIZE == 0) {
        res->final = true;
        res->set_400("Parameter `" + std::string(BATCH_SIZE) + "` must be a positive integer.");
        stream_response(req, res);
        return false;
    }

    if(REMOTE_EMBEDDING_BATCH_SIZE_VAL == 0) {
        res->final = true;
        res->set_400("Parameter `" + std::string(REMOTE_EMBEDDING_BATCH_SIZE) + "` must be a positive integer.");
        stream_response(req, res);
        return false;
    }

    if(REMOTE_EMBEDDING_TIMEOUT_MS_VAL == 0) {
        res->final = true;
        res->set_400("Parameter `" + std::string(REMOTE_EMBEDDING_TIMEOUT_MS) + "` must be a positive integer.");
        stream_response(req, res);
        return false;
    }

    if(REMOTE_EMBEDDING_NUM_TRIES_VAL == 0) {
        res->final = true;
        res->set_400("Parameter `" + std::string(REMOTE_EMBEDDING_NUM_TRIES) + "` must be a positive integer.");
        stream_response(req, res);
        return false;
    }

    if(req->body_index == 0) {
        // will log for every major chunk of request body
        //LOG(INFO) << "Import, req->body.size=" << req->body.size() << ", batch_size=" << IMPORT_BATCH_SIZE;
        //int nminusten_pos = std::max(0, int(req->body.size())-10);
        //LOG(INFO) << "Last 10 chars: " << req->body.substr(nminusten_pos);
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        //LOG(INFO) << "collection == nullptr, for collection: " << req->params["collection"];
        res->final = true;
        res->set_404();
        stream_response(req, res);
        return false;
    }

    //LOG(INFO) << "Import, " << "req->body_index=" << req->body_index << ", req->body.size: " << req->body.size();
    //LOG(INFO) << "req body %: " << (float(req->body_index)/req->body.size())*100;

    std::vector<std::string> json_lines;
    StringUtils::split(req->body, json_lines, "\n", false, false);

    //LOG(INFO) << "json_lines.size before: " << json_lines.size() << ", req->body_index: " << req->body_index;

    if(req->last_chunk_aggregate) {
        //LOG(INFO) << "req->last_chunk_aggregate is true";
        req->body = "";
    } else {
        if(!json_lines.empty()) {
            // check if req->body had complete last record
            bool complete_document;

            try {
                nlohmann::json document = nlohmann::json::parse(json_lines.back());
                complete_document = document.is_object();
            } catch(const std::exception& e) {
                complete_document = false;
            }

            if(!complete_document) {
                // eject partial record
                req->body = json_lines.back();
                json_lines.pop_back();
            } else {
                req->body = "";
            }
        }
    }

    //LOG(INFO) << "json_lines.size after: " << json_lines.size() << ", stream_proceed: " << stream_proceed;
    //LOG(INFO) << "json_lines.size: " << json_lines.size() << ", req->res_state: " << req->res_state;

    // When only one partial record arrives as a chunk, an empty body is pushed to response stream
    bool single_partial_record_body = (json_lines.empty() && !req->body.empty());
    std::stringstream response_stream;

    //LOG(INFO) << "single_partial_record_body: " << single_partial_record_body;

    const index_operation_t operation = get_index_operation(req->params[ACTION]);

    if(!single_partial_record_body) {
        nlohmann::json document;

        const auto& dirty_values = collection->parse_dirty_values_option(req->params[DIRTY_VALUES]);
        const bool& return_doc = req->params[RETURN_DOC] == "true";
        const bool& return_id = req->params[RETURN_ID] == "true";
        nlohmann::json json_res = collection->add_many(json_lines, document, operation, "",
                                                       dirty_values, return_doc, return_id, REMOTE_EMBEDDING_BATCH_SIZE_VAL, REMOTE_EMBEDDING_TIMEOUT_MS_VAL, REMOTE_EMBEDDING_NUM_TRIES_VAL);
        //const std::string& import_summary_json = json_res->dump();
        //response_stream << import_summary_json << "\n";

        for (size_t i = 0; i < json_lines.size(); i++) {
            bool res_start = (res->status_code == 0) && (i == 0);

            if(res_start) {
                // indicates first import result to be streamed
                response_stream << json_lines[i];
            } else {
                response_stream << "\n" << json_lines[i];
            }
        }
    }

    res->content_type_header = "text/plain; charset=utf-8";
    res->status_code = 200;
    res->body = response_stream.str();

    res->final.store(req->last_chunk_aggregate);
    stream_response(req, res);

    return true;
}

bool post_add_document(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const char *ACTION = "action";
    const char *DIRTY_VALUES_PARAM = "dirty_values";

    if(req->params.count(ACTION) == 0) {
        req->params[ACTION] = "create";
    }

    if(req->params[ACTION] != "create" && req->params[ACTION] != "update" && req->params[ACTION] != "upsert" &&
       req->params[ACTION] != "emplace") {
        res->set_400("Parameter `" + std::string(ACTION) + "` must be a create|update|upsert.");
        return false;
    }

    if(req->params.count(DIRTY_VALUES_PARAM) == 0) {
        req->params[DIRTY_VALUES_PARAM] = "";  // set it empty as default will depend on whether schema is enabled
    }


    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    const index_operation_t operation = get_index_operation(req->params[ACTION]);
    const auto& dirty_values = collection->parse_dirty_values_option(req->params[DIRTY_VALUES_PARAM]);

    size_t remote_embedding_timeout_ms = 60000;
    size_t remote_embedding_num_tries = 2;

    if(req->params.count("remote_embedding_timeout_ms") != 0) {
        remote_embedding_timeout_ms = std::stoul(req->params["remote_embedding_timeout_ms"]);
    }

    if(req->params.count("remote_embedding_num_tries") != 0) {
        remote_embedding_num_tries = std::stoul(req->params["remote_embedding_num_tries"]);
    }

    nlohmann::json document;
    std::vector<std::string> json_lines = {req->body};
    const nlohmann::json& inserted_doc_op = collection->add_many(json_lines, document, operation, "", dirty_values,
                                                                 false, false, 200, remote_embedding_timeout_ms,
                                                                 remote_embedding_num_tries);

    if(!inserted_doc_op["success"].get<bool>()) {
        nlohmann::json res_doc;

        try {
            res_doc = nlohmann::json::parse(json_lines[0]);
        } catch(const std::exception& e) {
            LOG(ERROR) << "JSON error: " << e.what();
            res->set_400("Bad JSON.");
            return false;
        }

        res->status_code = res_doc["code"].get<size_t>();
        // erase keys from res_doc except error and embedding_error
        for(auto it = res_doc.begin(); it != res_doc.end(); ) {
            if(it.key() != "error" && it.key() != "embedding_error") {
                it = res_doc.erase(it);
            } else {
                ++it;
            }
        }

        // rename error to message if not empty and exists
        if(res_doc.count("error") != 0 && !res_doc["error"].get<std::string>().empty()) {
            res_doc["message"] = res_doc["error"];
            res_doc.erase("error");
        }

        res->body = res_doc.dump();
        return false;
    }

    res->set_201(document.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore));
    return true;
}

bool patch_update_document(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    std::string doc_id = req->params["id"];

    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    const char* DIRTY_VALUES_PARAM = "dirty_values";

    if(req->params.count(DIRTY_VALUES_PARAM) == 0) {
        req->params[DIRTY_VALUES_PARAM] = "";  // set it empty as default will depend on whether schema is enabled
    }

    const auto& dirty_values = collection->parse_dirty_values_option(req->params[DIRTY_VALUES_PARAM]);
    Option<nlohmann::json> upserted_doc_op = collection->add(req->body, index_operation_t::UPDATE, doc_id, dirty_values);

    if(!upserted_doc_op.ok()) {
        res->set(upserted_doc_op.code(), upserted_doc_op.error());
        return false;
    }

    res->set_200(upserted_doc_op.get().dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore));
    return true;
}

bool patch_update_documents(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const char *FILTER_BY = "filter_by";
    std::string filter_query;
    if(req->params.count(FILTER_BY) == 0) {
        res->set_400("Parameter `" + std::string(FILTER_BY) + "` must be provided.");
        return false;
    } else {
        filter_query = req->params[FILTER_BY];
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);
    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    const char* DIRTY_VALUES_PARAM = "dirty_values";
    if(req->params.count(DIRTY_VALUES_PARAM) == 0) {
        req->params[DIRTY_VALUES_PARAM] = "";  // set it empty as default will depend on whether schema is enabled
    }

    search_stop_us = UINT64_MAX; // Filtering shouldn't timeout during update operation.
    auto update_op = collection->update_matching_filter(filter_query, req->body, req->params[DIRTY_VALUES_PARAM]);
    if(update_op.ok()) {
        res->set_200(update_op.get().dump());
    } else {
        res->set(update_op.code(), update_op.error());
    }

    return update_op.ok();
}

bool get_fetch_document(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    std::string doc_id = req->params["id"];

    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);
    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    Option<nlohmann::json> doc_option = collection->get(doc_id);

    if(!doc_option.ok()) {
        res->set(doc_option.code(), doc_option.error());
        return false;
    }

    res->set_200(doc_option.get().dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore));
    return true;
}

bool del_remove_document(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    std::string doc_id = req->params["id"];

    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);
    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    Option<nlohmann::json> doc_option = collection->get(doc_id);

    if(!doc_option.ok()) {
        res->set(doc_option.code(), doc_option.error());
        return false;
    }

    Option<std::string> deleted_id_op = collection->remove(doc_id);

    if(!deleted_id_op.ok()) {
        res->set(deleted_id_op.code(), deleted_id_op.error());
        return false;
    }

    nlohmann::json doc = doc_option.get();
    res->set_200(doc.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore));
    return true;
}

bool del_remove_documents(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    // defaults: will get overridden later if needed
    res->content_type_header = "application/json";
    res->status_code = 200;

    // NOTE: this is a streaming response end-point so this handler will be called multiple times
    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        req->last_chunk_aggregate = true;
        res->final = true;
        res->set_404();
        stream_response(req, res);
        return false;
    }

    const char *BATCH_SIZE = "batch_size";
    const char *FILTER_BY = "filter_by";
    const char *TOP_K_BY = "top_k_by";

    if(req->params.count(TOP_K_BY) != 0) {
        std::vector<std::string> parts;
        StringUtils::split(req->params[TOP_K_BY], parts, ":");

        if(parts.size() != 2 || !StringUtils::is_uint32_t(parts[1])) {
            req->last_chunk_aggregate = true;
            res->final = true;
            res->set_400("The `top_k_by` parameter is not valid.");
            stream_response(req, res);
            return false;
        }

        const std::string& field_name = parts[0];
        const size_t k = std::stoull(parts[1]);
        auto op = collection->truncate_after_top_k(field_name, k);

        req->last_chunk_aggregate = true;
        res->final = true;

        if(!op.ok()) {
            res->set_500(op.error());
            stream_response(req, res);
            return false;
        }

        res->set_200(R"({"ok": true})");
        stream_response(req, res);
        return true;
    }

    if(req->params.count(BATCH_SIZE) == 0) {
        req->params[BATCH_SIZE] = "1000000000"; // 1 Billion
    }

    if(req->params.count(FILTER_BY) == 0) {
        req->last_chunk_aggregate = true;
        res->final = true;
        res->set_400("Parameter `" + std::string(FILTER_BY) + "` must be provided.");
        stream_response(req, res);
        return false;
    }

    if(!StringUtils::is_uint32_t(req->params[BATCH_SIZE])) {
        req->last_chunk_aggregate = true;
        res->final = true;
        res->set_400("Parameter `" + std::string(BATCH_SIZE) + "` must be a positive integer.");
        stream_response(req, res);
        return false;
    }

    const size_t DELETE_BATCH_SIZE = std::stoi(req->params[BATCH_SIZE]);

    if(DELETE_BATCH_SIZE == 0) {
        req->last_chunk_aggregate = true;
        res->final = true;
        res->set_400("Parameter `" + std::string(BATCH_SIZE) + "` must be a positive integer.");
        stream_response(req, res);
        return false;
    }

    std::string simple_filter_query;

    if(req->params.count(FILTER_BY) != 0) {
        simple_filter_query = req->params[FILTER_BY];
    }

    deletion_state_t* deletion_state = nullptr;

    if(req->data == nullptr) {
        deletion_state = new deletion_state_t{};
        // destruction of data is managed by req destructor
        req->data = deletion_state;

        search_stop_us = UINT64_MAX; // Filtering shouldn't timeout during delete operation.
        filter_result_t filter_result;
        auto filter_ids_op = collection->get_filter_ids(simple_filter_query, filter_result);

        if(!filter_ids_op.ok()) {
            res->set(filter_ids_op.code(), filter_ids_op.error());
            req->last_chunk_aggregate = true;
            res->final = true;
            stream_response(req, res);
            return false;
        }

        deletion_state->index_ids.emplace_back(filter_result.count, filter_result.docs);
        filter_result.docs = nullptr;

        for(size_t i=0; i<deletion_state->index_ids.size(); i++) {
            deletion_state->offsets.push_back(0);
        }
        deletion_state->collection = collection.get();
        deletion_state->num_removed = 0;
    } else {
        deletion_state = dynamic_cast<deletion_state_t*>(req->data);
    }

    bool done = true;
    Option<bool> remove_op = stateful_remove_docs(deletion_state, DELETE_BATCH_SIZE, done);

    //LOG(INFO) << "Deletion batch size: " << DELETE_BATCH_SIZE << ", done: " << done;

    if(!remove_op.ok()) {
        res->set(remove_op.code(), remove_op.error());
        req->last_chunk_aggregate = true;
        res->final = true;
    } else {
        if(!done) {
            req->last_chunk_aggregate = false;
            res->final = false;
        } else {
            nlohmann::json response;
            response["num_deleted"] = deletion_state->num_removed;

            req->last_chunk_aggregate = true;
            res->body = response.dump();
            res->final = true;
        }
    }

    if(res->final) {
        stream_response(req, res);
    } else {
        defer_processing(req, res, 0);
    }

    return true;
}

bool get_aliases(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
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

    res->set_200(res_json.dump());
    return true;
}

bool get_alias(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const std::string & alias = req->params["alias"];
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Option<std::string> collection_name_op = collectionManager.resolve_symlink(alias);

    if(!collection_name_op.ok()) {
        res->set_404();
        return false;
    }

    nlohmann::json res_json;
    res_json["name"] = alias;
    res_json["collection_name"] = collection_name_op.get();

    res->set_200(res_json.dump());
    return true;
}

bool put_upsert_alias(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    const std::string & alias = req->params["alias"];

    const char* COLLECTION_NAME = "collection_name";

    if(req_json.count(COLLECTION_NAME) == 0) {
        res->set_400(std::string("Parameter `") + COLLECTION_NAME + "` is required.");
        return false;
    }

    Option<bool> success_op = collectionManager.upsert_symlink(alias, req_json[COLLECTION_NAME]);
    if(!success_op.ok()) {
        res->set_500(success_op.error());
        return false;
    }

    req_json["name"] = alias;
    res->set_200(req_json.dump());
    return true;
}

bool del_alias(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const std::string & alias = req->params["alias"];
    CollectionManager & collectionManager = CollectionManager::get_instance();

    Option<std::string> collection_name_op = collectionManager.resolve_symlink(alias);
    if(!collection_name_op.ok()) {
        res->set_404();
        return false;
    }

    Option<bool> delete_op = collectionManager.delete_symlink(alias);

    if(!delete_op.ok()) {
        res->set_500(delete_op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["name"] = alias;
    res_json["collection_name"] = collection_name_op.get();
    res->set_200(res_json.dump());
    return true;
}

bool get_overrides(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    nlohmann::json res_json;
    res_json["overrides"] = nlohmann::json::array();

    const std::map<std::string, override_t>& overrides = collection->get_overrides();
    for(const auto & kv: overrides) {
        nlohmann::json override = kv.second.to_json();
        res_json["overrides"].push_back(override);
    }

    res->set_200(res_json.dump());
    return true;
}

bool get_override(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    std::string override_id = req->params["id"];

    const std::map<std::string, override_t>& overrides = collection->get_overrides();

    if(overrides.count(override_id) != 0) {
        nlohmann::json override = overrides.at(override_id).to_json();
        res->set_200(override.dump());
        return true;
    }

    res->set_404();
    return false;
}

bool put_override(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    std::string override_id = req->params["id"];

    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }
    
    override_t override;
    Option<bool> parse_op = override_t::parse(req_json, override_id, override, "",
                                              collection->get_symbols_to_index(),
                                              collection->get_token_separators());
    if(!parse_op.ok()) {
        res->set(parse_op.code(), parse_op.error());
        return false;
    }
    
    Option<uint32_t> add_op = collection->add_override(override);

    if(!add_op.ok()) {
        res->set(add_op.code(), add_op.error());
        return false;
    }

    req_json["id"] = override.id;

    res->set_200(req_json.dump());
    return true;
}

bool del_override(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    Option<uint32_t> rem_op = collection->remove_override(req->params["id"]);
    if(!rem_op.ok()) {
        res->set(rem_op.code(), rem_op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["id"] = req->params["id"];

    res->set_200(res_json.dump());
    return true;
}

bool get_keys(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    AuthManager &auth_manager = collectionManager.getAuthManager();

    const Option<std::vector<api_key_t>>& keys_op = auth_manager.list_keys();
    if(!keys_op.ok()) {
        res->set(keys_op.code(), keys_op.error());
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

    res->set_200(res_json.dump());
    return true;
}

bool post_create_key(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    //LOG(INFO) << "post_create_key";

    CollectionManager & collectionManager = CollectionManager::get_instance();
    AuthManager &auth_manager = collectionManager.getAuthManager();

    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }


    const Option<uint32_t>& validate_op = api_key_t::validate(req_json);
    if(!validate_op.ok()) {
        res->set(validate_op.code(), validate_op.error());
        return false;
    }

    if(req_json.count("expires_at") == 0) {
        req_json["expires_at"] = api_key_t::FAR_FUTURE_TIMESTAMP;
    }

    if(req_json.count("autodelete") == 0) {
        req_json["autodelete"] = false;
    }

    const std::string &rand_key = (req_json.count("value") != 0) ?
            req_json["value"].get<std::string>() : req->metadata;

    api_key_t api_key(
        rand_key,
        req_json["description"].get<std::string>(),
        req_json["actions"].get<std::vector<std::string>>(),
        req_json["collections"].get<std::vector<std::string>>(),
        req_json["expires_at"].get<uint64_t>(),
        req_json["autodelete"].get<bool>()
    );

    const Option<api_key_t>& api_key_op = auth_manager.create_key(api_key);
    if(!api_key_op.ok()) {
        res->set(api_key_op.code(), api_key_op.error());
        return false;
    }

    res->set_201(api_key_op.get().to_json().dump());
    return true;
}

bool get_key(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    AuthManager &auth_manager = collectionManager.getAuthManager();

    const std::string& key_id_str = req->params["id"];
    uint32_t key_id = (uint32_t) std::stoul(key_id_str);

    const Option<api_key_t>& key_op = auth_manager.get_key(key_id);

    if(!key_op.ok()) {
        res->set(key_op.code(), key_op.error());
        return false;
    }

    nlohmann::json key_obj = key_op.get().to_json();
    key_obj["value_prefix"] = key_obj["value"];
    key_obj.erase("value");

    res->set_200(key_obj.dump());
    return true;
}

bool del_key(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    AuthManager &auth_manager = collectionManager.getAuthManager();

    const std::string& key_id_str = req->params["id"];
    uint32_t key_id = (uint32_t) std::stoul(key_id_str);

    const Option<api_key_t> &del_op = auth_manager.remove_key(key_id);

    if(!del_op.ok()) {
        res->set(del_op.code(), del_op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["id"] = del_op.get().id;

    res->set_200(res_json.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore));
    return true;
}

bool post_snapshot(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const std::string SNAPSHOT_PATH = "snapshot_path";

    res->status_code = 201;
    res->content_type_header = "application/json";

    if(req->params.count(SNAPSHOT_PATH) == 0) {
        req->last_chunk_aggregate = true;
        res->final = true;
        res->set_400(std::string("Parameter `") + SNAPSHOT_PATH + "` is required.");
        stream_response(req, res);
        return false;
    }

    server->do_snapshot(req->params[SNAPSHOT_PATH], req, res);

    return true;
}

bool post_vote(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    res->status_code = 200;
    res->content_type_header = "application/json";

    nlohmann::json response;
    response["success"] = server->trigger_vote();
    res->body = response.dump();

    return true;
}

bool post_config(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    auto config_update_op = Config::get_instance().update_config(req_json);

    if(!config_update_op.ok()) {
        res->set(config_update_op.code(), config_update_op.error());
    } else {
        nlohmann::json response;
        response["success"] = true;
        res->set_201(response.dump());
    }

    return true;
}

bool post_clear_cache(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    {
        std::unique_lock lock(mutex);
        res_cache.clear();
    }

    nlohmann::json response;
    response["success"] = true;
    res->set_200(response.dump());

    return true;
}

bool post_compact_db(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager& collectionManager = CollectionManager::get_instance();
    rocksdb::Status status = collectionManager.get_store()->compact_all();

    nlohmann::json response;
    response["success"] = status.ok();

    if(!status.ok()) {
        response["error"] = "Error code: " + std::to_string(status.code());
        res->set_500(response.dump());
    } else {
        res->set_200(response.dump());
    }

    return true;
}

bool post_reset_peers(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    res->status_code = 200;
    res->content_type_header = "application/json";

    nlohmann::json response;
    response["success"] = server->reset_peers();
    res->body = response.dump();

    return true;
}

bool get_synonyms(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    nlohmann::json res_json;
    res_json["synonyms"] = nlohmann::json::array();

    const auto& synonyms = collection->get_synonyms();
    for(const auto & kv: synonyms) {
        nlohmann::json synonym = kv.second.to_view_json();
        res_json["synonyms"].push_back(synonym);
    }

    res->set_200(res_json.dump());
    return true;
}

bool get_synonym(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    std::string synonym_id = req->params["id"];

    synonym_t synonym;
    bool found = collection->get_synonym(synonym_id, synonym);

    if(found) {
        nlohmann::json synonym_json = synonym.to_view_json();
        res->set_200(synonym_json.dump());
        return true;
    }

    res->set_404();
    return false;
}

bool put_synonym(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    std::string synonym_id = req->params["id"];

    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    nlohmann::json syn_json;

    try {
        syn_json = nlohmann::json::parse(req->body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    if(!syn_json.is_object()) {
        res->set_400("Bad JSON.");
        return false;
    }

    // These checks should be inside `add_synonym` but older versions of Typesense wrongly persisted
    // `root` as an array, so we have to do it here so that on-disk synonyms are loaded properly
    if(syn_json.count("root") != 0 && !syn_json["root"].is_string()) {
        res->set_400("Key `root` should be a string.");
        return false;
    }

    if(syn_json.count("synonyms") && syn_json["synonyms"].is_array()) {
        if(syn_json["synonyms"].empty()) {
            res->set_400("Could not find a valid string array of `synonyms`");
            return false;
        }

        for(const auto& synonym: syn_json["synonyms"]) {
            if (!synonym.is_string() || synonym.empty()) {
                res->set_400("Could not find a valid string array of `synonyms`");
                return false;
            }
        }
    }

    syn_json["id"] = synonym_id;
    Option<bool> upsert_op = collection->add_synonym(syn_json);

    if(!upsert_op.ok()) {
        res->set(upsert_op.code(), upsert_op.error());
        return false;
    }

    res->set_200(syn_json.dump());
    return true;
}

bool del_synonym(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    auto collection = collectionManager.get_collection(req->params["collection"]);

    if(collection == nullptr) {
        res->set_404();
        return false;
    }

    Option<bool> rem_op = collection->remove_synonym(req->params["id"]);
    if(!rem_op.ok()) {
        res->set(rem_op.code(), rem_op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["id"] = req->params["id"];

    res->set_200(res_json.dump());
    return true;
}

bool is_doc_import_route(uint64_t route_hash) {
    route_path* rpath;
    bool found = server->get_route(route_hash, &rpath);
    return found && (rpath->handler == post_import_documents);
}

bool is_doc_write_route(uint64_t route_hash) {
    route_path* rpath;
    bool found = server->get_route(route_hash, &rpath);
    return found && (rpath->handler == post_add_document || rpath->handler == patch_update_document);
}

bool is_doc_del_route(uint64_t route_hash) {
    route_path* rpath;
    bool found = server->get_route(route_hash, &rpath);
    return found && (rpath->handler == del_remove_document || rpath->handler == del_remove_documents);
}

bool get_presets(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    CollectionManager & collectionManager = CollectionManager::get_instance();
    const spp::sparse_hash_map<std::string, nlohmann::json> & presets = collectionManager.get_presets();
    nlohmann::json res_json = nlohmann::json::object();
    res_json["presets"] = nlohmann::json::array();

    for(const auto& preset_kv: presets) {
        nlohmann::json preset;
        preset["name"] = preset_kv.first;
        preset["value"] = preset_kv.second;
        res_json["presets"].push_back(preset);
    }

    res->set_200(res_json.dump());
    return true;
}

bool get_preset(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const std::string & preset_name = req->params["name"];
    CollectionManager & collectionManager = CollectionManager::get_instance();

    nlohmann::json preset;
    Option<bool> preset_op = collectionManager.get_preset(preset_name, preset);

    if(!preset_op.ok()) {
        res->set(preset_op.code(), preset_op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["name"] = preset_name;
    res_json["value"] = preset;

    res->set_200(res_json.dump());
    return true;
}

bool put_upsert_preset(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    const std::string & preset_name = req->params["name"];

    const char* PRESET_VALUE = "value";

    if(req_json.count(PRESET_VALUE) == 0) {
        res->set_400(std::string("Parameter `") + PRESET_VALUE + "` is required.");
        return false;
    }

    Option<bool> success_op = collectionManager.upsert_preset(preset_name, req_json[PRESET_VALUE]);
    if(!success_op.ok()) {
        res->set_500(success_op.error());
        return false;
    }

    req_json["name"] = preset_name;

    res->set_200(req_json.dump());
    return true;
}

bool del_preset(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const std::string & preset_name = req->params["name"];
    CollectionManager & collectionManager = CollectionManager::get_instance();

    nlohmann::json preset;
    Option<bool> preset_op = collectionManager.get_preset(preset_name, preset);
    if(!preset_op.ok()) {
        res->set(preset_op.code(), preset_op.error());
        return false;
    }

    Option<bool> delete_op = collectionManager.delete_preset(preset_name);

    if(!delete_op.ok()) {
        res->set_500(delete_op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["name"] = preset_name;
    res_json["value"] = preset;
    res->set_200(res_json.dump());
    return true;
}

bool get_stopwords(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    StopwordsManager& stopwordManager = StopwordsManager::get_instance();
    const spp::sparse_hash_map<std::string, stopword_struct_t>& stopwords = stopwordManager.get_stopwords();
    nlohmann::json res_json = nlohmann::json::object();
    res_json["stopwords"] = nlohmann::json::array();

    for(const auto& stopwords_kv: stopwords) {
        auto stopword = stopwords_kv.second.to_json();
        res_json["stopwords"].push_back(stopword);
    }

    res->set_200(res_json.dump());
    return true;
}

bool get_stopword(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const std::string & stopword_name = req->params["name"];
    StopwordsManager& stopwordManager = StopwordsManager::get_instance();

    stopword_struct_t stopwordStruct;
    Option<bool> stopword_op = stopwordManager.get_stopword(stopword_name, stopwordStruct);

    if(!stopword_op.ok()) {
        res->set(stopword_op.code(), stopword_op.error());
        return false;
    }

    nlohmann::json res_json;

    res_json["stopwords"] = stopwordStruct.to_json();

    res->set_200(res_json.dump());
    return true;
}

bool put_upsert_stopword(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    StopwordsManager& stopwordManager = StopwordsManager::get_instance();
    const std::string & stopword_name = req->params["name"];

    Option<bool> success_op = stopwordManager.upsert_stopword(stopword_name, req_json, true);
    if(!success_op.ok()) {
        res->set(success_op.code(), success_op.error());
        return false;
    }

    req_json["id"] = stopword_name;

    res->set_200(req_json.dump());
    return true;
}

bool del_stopword(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const std::string & stopword_name = req->params["name"];
    StopwordsManager& stopwordManager = StopwordsManager::get_instance();

    Option<bool> delete_op = stopwordManager.delete_stopword(stopword_name);

    if(!delete_op.ok()) {
        res->set(delete_op.code(), delete_op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["id"] = stopword_name;

    res->set_200(res_json.dump());
    return true;
}

bool get_rate_limits(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    RateLimitManager* rateLimitManager = RateLimitManager::getInstance();

    res->set_200(rateLimitManager->get_all_rules_json().dump());
    return true;
}

bool get_rate_limit(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    RateLimitManager* rateLimitManager = RateLimitManager::getInstance();
    // Convert param id to uint64_t
    if(!StringUtils::is_uint32_t(req->params["id"])) {
        res->set_400("{\"message\": \"Invalid ID\"}");
        return false;
    }
    uint64_t id = std::stoull(req->params["id"]);
    const auto& rule_option = rateLimitManager->find_rule_by_id(id);

    if(!rule_option.ok()) {
        res->set(rule_option.code(), rule_option.error());
        return false;
    }

    res->set_200(rule_option.get().dump());
    return true;
}

bool put_rate_limit(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    RateLimitManager* rateLimitManager = RateLimitManager::getInstance();
    nlohmann::json req_json;
    if(!StringUtils::is_uint32_t(req->params["id"])) {
        res->set_400("{\"message\": \"Invalid ID\"}");
        return false;
    }
    uint64_t id = std::stoull(req->params["id"]);

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const nlohmann::json::parse_error& e) {
        res->set_400("Invalid JSON");
        return false;
    }

    const auto& edit_rule_result = rateLimitManager->edit_rule(id, req_json);

    if(!edit_rule_result.ok()) {
        res->set(edit_rule_result.code(), edit_rule_result.error());
        return false;
    }

    res->set_200(edit_rule_result.get().dump());
    return true;

}

bool del_rate_limit(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    RateLimitManager* rateLimitManager = RateLimitManager::getInstance();
    if(!StringUtils::is_uint32_t(req->params["id"])) {
        res->set_400("{\"message\": \"Invalid ID\"}");
        return false;
    }
    uint64_t id = std::stoull(req->params["id"]);
    const auto& rule_option = rateLimitManager->find_rule_by_id(id);

    if(!rule_option.ok()) {
        res->set(rule_option.code(), rule_option.error());
        return false;
    }

    rateLimitManager->delete_rule_by_id(id);
    nlohmann::json res_json;
    res_json["id"] = id;
    res->set_200(res_json.dump());
    return true;
}

bool post_rate_limit(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    RateLimitManager* rateLimitManager = RateLimitManager::getInstance();
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch (const std::exception & e) {
        res->set_400("Bad JSON.");
        return false;
    }

    auto add_rule_result = rateLimitManager->add_rule(req_json);

    if(!add_rule_result.ok()) {
        res->set(add_rule_result.code(), add_rule_result.error());
        return false;
    }

    res->set_200(add_rule_result.get().dump());
    return true;
}

bool get_active_throttles(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    RateLimitManager* rateLimitManager = RateLimitManager::getInstance();
    res->set_200(rateLimitManager->get_throttled_entities_json().dump());
    return true;
}

bool del_throttle(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    RateLimitManager* rateLimitManager = RateLimitManager::getInstance();
    if(!StringUtils::is_uint32_t(req->params["id"])) {
        res->set_400("{\"message\": \"Invalid ID\"}");
        return false;
    }
    uint64_t id = std::stoull(req->params["id"]);
    bool res_ = rateLimitManager->delete_ban_by_id(id);
    if(!res_) {
        res->set_400("{\"message\": \"Invalid ID\"}");
        return false;
    }
    nlohmann::json res_json;
    res_json["id"] = id;
    res->set_200(res_json.dump());
    return true;
}

bool del_exceed(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    RateLimitManager* rateLimitManager = RateLimitManager::getInstance();
    if(!StringUtils::is_uint32_t(req->params["id"])) {
        res->set_400("{\"message\": \"Invalid ID\"}");
        return false;
    }
    uint64_t id = std::stoull(req->params["id"]);
    bool res_ = rateLimitManager->delete_throttle_by_id(id);
    if(!res_) {
        res->set_400("{\"message\": \"Invalid ID\"}");
        return false;
    }
    nlohmann::json res_json;
    res_json["id"] = id;
    res->set_200(res_json.dump());
    return true;
}

bool get_limit_exceed_counts(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    RateLimitManager* rateLimitManager = RateLimitManager::getInstance();
    res->set_200(rateLimitManager->get_exceeded_entities_json().dump());
    return true;
}

Option<std::pair<std::string,std::string>> get_api_key_and_ip(const std::string& metadata) {
    // format <length of api_key>:<api_key><ip>
    // length of api_key is a uint32_t
    if(metadata.size() < 10) {
        if(metadata.size() >= 2 && metadata[0] == '0' && metadata[1] == ':') {
            // e.g. "0:0.0.0.0" (when api key is not provided at all)
            std::string ip = metadata.substr(metadata.find(":") + 1);
            return Option<std::pair<std::string,std::string>>(std::make_pair("", ip));
        }

        return Option<std::pair<std::string,std::string>>(400, "Invalid metadata");
    }

    if(metadata.find(":") == std::string::npos) {
        return Option<std::pair<std::string,std::string>>(400, "Invalid metadata");
    }

    std::string key_len_str = metadata.substr(0, metadata.find(":"));

    if(!StringUtils::is_uint32_t(key_len_str)) {
        return Option<std::pair<std::string,std::string>>(400, "Invalid metadata");
    }

    uint32_t api_key_length = static_cast<uint32_t>(std::stoul(key_len_str));

    if(metadata.size() < api_key_length + metadata.find(":") + 7) {
        return Option<std::pair<std::string,std::string>>(400, "Invalid metadata");
    }

    std::string api_key = metadata.substr(metadata.find(":") + 1, api_key_length);
    std::string ip = metadata.substr(metadata.find(":") + 1 + api_key_length);

    // validate IP address
    std::regex ip_pattern("\\b(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\b");
    if(!std::regex_match(ip, ip_pattern)) {
        return Option<std::pair<std::string,std::string>>(400, "Invalid metadata");
    }

    return Option<std::pair<std::string,std::string>>(std::make_pair(api_key, ip));
}

bool post_create_event(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    auto add_event_op = EventManager::get_instance().add_event(req_json, req->client_ip);
    if(add_event_op.ok()) {
        res->set_201(R"({"ok": true)");
        return true;
    }

    res->set_400(add_event_op.error());
    return false;
}

bool get_analytics_rules(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto rules_op = AnalyticsManager::get_instance().list_rules();

    if(!rules_op.ok()) {
        res->set(rules_op.code(), rules_op.error());
        return false;
    }

    res->set_200(rules_op.get().dump());
    return true;
}

bool get_analytics_rule(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto rules_op = AnalyticsManager::get_instance().get_rule(req->params["name"]);

    if(!rules_op.ok()) {
        res->set(rules_op.code(), rules_op.error());
        return false;
    }

    res->set_200(rules_op.get().dump());
    return true;
}

bool post_create_analytics_rules(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    auto op = AnalyticsManager::get_instance().create_rule(req_json, false, true);

    if(!op.ok()) {
        res->set(op.code(), op.error());
        return false;
    }

    res->set_201(req_json.dump());
    return true;
}

bool put_upsert_analytics_rules(const std::shared_ptr<http_req> &req, const std::shared_ptr<http_res> &res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    req_json["name"] = req->params["name"];
    auto op = AnalyticsManager::get_instance().create_rule(req_json, true, true);

    if(!op.ok()) {
        res->set(op.code(), op.error());
        return false;
    }

    res->set_200(req_json.dump());
    return true;
}

bool del_analytics_rules(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto op = AnalyticsManager::get_instance().remove_rule(req->params["name"]);
    if(!op.ok()) {
        res->set(op.code(), op.error());
        return false;
    }

    nlohmann::json res_json;
    res_json["name"] = req->params["name"];

    res->set_200(res_json.dump());
    return true;
}

bool post_proxy(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    HttpProxy& proxy = HttpProxy::get_instance();

    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const nlohmann::json::parse_error& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    std::string body, url, method;
    std::unordered_map<std::string, std::string> headers;

    if(req_json.count("url") == 0 || req_json.count("method") == 0) {
        res->set_400("Missing required fields.");
        return false;
    }

    if(!req_json["url"].is_string() || !req_json["method"].is_string() || req_json["url"].get<std::string>().empty() || req_json["method"].get<std::string>().empty()) {
        res->set_400("URL and method must be non-empty strings.");
        return false;
    }

    try {        
        if(req_json.count("body") != 0 && !req_json["body"].is_string()) {
            res->set_400("Body must be a string.");
            return false;
        }
        if(req_json.count("headers") != 0 && !req_json["headers"].is_object()) {
            res->set_400("Headers must be a JSON object.");
            return false;
        }
        if(req_json.count("body")) {
            body = req_json["body"].get<std::string>();
        }
        url = req_json["url"].get<std::string>();
        method = req_json["method"].get<std::string>();
        if(req_json.count("headers")) {
            headers = req_json["headers"].get<std::unordered_map<std::string, std::string>>();
        }
    } catch(const std::exception& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    auto response = proxy.send(url, method, body, headers);
    
    if(response.status_code != 200) {
        int code = response.status_code;
        res->set_body(code, response.body);
        return false;
    }

    res->set_200(response.body);
    return true;
}


bool get_conversation(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    std::string conversation_id = req->params["id"];

    auto conversation_op = ConversationManager::get_instance().get_conversation(conversation_id);

    if(!conversation_op.ok()) {
        res->set(conversation_op.code(), conversation_op.error());
        return false;
    }

    res->set_200(conversation_op.get().dump());
    return true;
}


bool del_conversation(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    std::string conversation_id = req->params["id"];

    auto conversation_op = ConversationManager::get_instance().delete_conversation(conversation_id);

    if(!conversation_op.ok()) {
        res->set(conversation_op.code(), conversation_op.error());
        return false;
    }

    res->set_200(conversation_op.get().dump());
    return true;
}

bool get_conversations(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto conversations_op = ConversationManager::get_instance().get_all_conversations();

    if(!conversations_op.ok()) {
        res->set(conversations_op.code(), conversations_op.error());
        return false;
    }

    res->set_200(conversations_op.get().dump());
    return true;
}

bool put_conversation(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    std::string conversation_id = req->params["id"];

    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const nlohmann::json::parse_error& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    req_json["id"] = conversation_id;

    auto conversation_op = ConversationManager::get_instance().update_conversation(req_json);

    if(!conversation_op.ok()) {
        res->set(conversation_op.code(), conversation_op.error());
        return false;
    }

    res->set_200(conversation_op.get().dump());
    return true;
}

bool post_conversation_model(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const nlohmann::json::parse_error& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    if(!req_json.is_object()) {
        res->set_400("Bad JSON.");
        return false;
    }

    auto add_model_op = ConversationModelManager::add_model(req_json);

    if(!add_model_op.ok()) {
        res->set(add_model_op.code(), add_model_op.error());
        return false;
    }

    auto model = add_model_op.get();
    Collection::hide_credential(model, "api_key");

    res->set_200(model.dump());
    return true;
}

bool get_conversation_model(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const std::string& model_id = req->params["id"];

    auto model_op = ConversationModelManager::get_model(model_id);

    if(!model_op.ok()) {
        res->set(model_op.code(), model_op.error());
        return false;
    }

    auto model = model_op.get();
    Collection::hide_credential(model, "api_key");

    res->set_200(model.dump());
    return true;
}

bool get_conversation_models(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto models_op = ConversationModelManager::get_all_models();

    if(!models_op.ok()) {
        res->set(models_op.code(), models_op.error());
        return false;
    }

    auto models = models_op.get();

    for(auto& model: models) {
        Collection::hide_credential(model, "api_key");
    }

    res->set_200(models.dump());
    return true;
}

bool del_conversation_model(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const std::string& model_id = req->params["id"];

    auto model_op = ConversationModelManager::delete_model(model_id);

    if(!model_op.ok()) {
        res->set(model_op.code(), model_op.error());
        return false;
    }

    auto model = model_op.get();

    Collection::hide_credential(model, "api_key");

    res->set_200(model.dump());
    return true;
}

bool put_conversation_model(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    const std::string& model_id = req->params["id"];

    nlohmann::json req_json;

    try {
        req_json = nlohmann::json::parse(req->body);
    } catch(const nlohmann::json::parse_error& e) {
        LOG(ERROR) << "JSON error: " << e.what();
        res->set_400("Bad JSON.");
        return false;
    }

    if(!req_json.is_object()) {
        res->set_400("Bad JSON.");
        return false;
    }

    auto model_op = ConversationModelManager::update_model(model_id, req_json);

    if(!model_op.ok()) {
        res->set(model_op.code(), model_op.error());
        return false;
    }

    auto model = model_op.get();

    Collection::hide_credential(model, "api_key");

    res->set_200(model.dump());
    return true;
}