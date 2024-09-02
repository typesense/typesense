#include "conversation_manager.h"
#include "logger.h"
#include <chrono>
#include "http_client.h"
#include "core_api.h"

Option<std::string> ConversationManager::add_conversation(const nlohmann::json& conversation, const nlohmann::json& model, const std::string& id) {
    std::unique_lock lock(conversations_mutex);
    if(!conversation.is_array()) {
        return Option<std::string>(400, "Conversation is not an array");
    }

    if(!id.empty()) {
        auto conversation_exists = check_conversation_exists(id);
        if(!conversation_exists.ok()) {
            return Option<std::string>(conversation_exists.code(), conversation_exists.error());
        }
    }

    std::string conversation_id = id.empty() ? sole::uuid4().str() : id;
    std::string history_collection = model["history_collection"].get<std::string>();

    auto collection = CollectionManager::get_instance().get_collection(history_collection).get();
    if(!collection) {
        return Option<std::string>(404, "Conversation store collection not found");
    }

    std::string body;
    
    for(const auto& message : conversation) {
       if(!message.is_object()) {
           return Option<std::string>(400, "Message is not an object");
       }

        // key is role, value is message
        const auto& message_it = message.items().begin();
        if(message_it == message.items().end()) {
            return Option<std::string>(400, "Message is empty");
        }

        if(!message_it.value().is_string()) {
            return Option<std::string>(400, "Role and message must be strings");
        }
         
        nlohmann::json message_json;
        message_json["conversation_id"] = conversation_id;
        message_json["role"] = message_it.key();
        message_json["message"] = message_it.value();
        message_json["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        message_json["model_id"] = model["id"];
        body += message_json.dump(-1) + "\n";
    }

    if(!raft_server) {
        auto req = std::make_shared<http_req>();
        auto resp = std::make_shared<http_res>(nullptr);

        req->params["action"] = "emplace";
        req->params["collection"] = history_collection;
        req->body = body;

        auto api_res = post_import_documents(req, resp);
        if(!api_res) {
            return Option<std::string>(resp->status_code, resp->body);
        }

        return Option<std::string>(conversation_id);
    }


    std::string leader_url = raft_server->get_leader_url();

    if(!leader_url.empty()) {
        std::string base_url = leader_url + "collections/" + history_collection;
        std::string res;
        std::string url = base_url + "/documents/import?action=emplace";
        std::map<std::string, std::string> res_headers;

        long status = HttpClient::post_response(url, body, res, res_headers, {}, 10*1000, true);

        if(status != 200) {
            LOG(ERROR) << "Error while creating conversation: " << res;
            LOG(ERROR) << "Status: " << status;
            return Option<std::string>(400, "Error while creating conversation");
        } else {
            return Option<std::string>(conversation_id);
        }
    } else {
        return Option<std::string>(500, "Leader URL is empty");
    }
}

Option<nlohmann::json> ConversationManager::get_conversation(const std::string& conversation_id) {

    auto collection_op = get_history_collection(conversation_id);
    if(!collection_op.ok()) {
        return Option<nlohmann::json>(collection_op.code(), collection_op.error());
    }
    auto collection = collection_op.get();

    nlohmann::json res;
    size_t total = 0;
    std::vector<sort_by> sort_by_vec = {{"timestamp", sort_field_const::asc}};
    auto search_res = collection->search("*", {}, "conversation_id:" + conversation_id, {}, sort_by_vec, {}, 250);
    if(!search_res.ok()) {
        return Option<nlohmann::json>(400, "Error while searching conversation store: " + search_res.error());
    }
    auto search_res_json = search_res.get();
    total = search_res_json["found"].get<uint32_t>();
    if(total == 0) {
        return Option<nlohmann::json>(404, "Conversation not found");
    }

    res["conversation"] = nlohmann::json::array();
    for(auto& hit : search_res_json["hits"]) {
        nlohmann::json message;
        message[hit["document"]["role"]] = hit["document"]["message"];
        res["conversation"].push_back(message);
    }

    // swap every two elements
    for(size_t i = 0; i < res["conversation"].size() - 1; i += 2) {
        res["conversation"].at(i).swap(res["conversation"].at(i + 1));
    }


    res["id"] = conversation_id;
    res["last_updated"] = (search_res_json["hits"].size() > 0) ? search_res_json["hits"][search_res_json["hits"].size() - 1]["document"]["timestamp"].get<uint32_t>() : 0;

    if(total > 250) {
        while(total > 0) {
            search_res = collection->search("*", {}, "conversation_id:" + conversation_id, {}, sort_by_vec, {}, 250, search_res_json["page"].get<uint32_t>() + 1);
            if(!search_res.ok()) {
                return Option<nlohmann::json>(400, "Error while searching conversation store: " + search_res.error());
            }
            search_res_json = search_res.get();
            for(auto& hit : search_res_json["hits"]) {
                nlohmann::json message;
                message[hit["document"]["role"]] = hit["document"]["message"];
                res["conversation"].push_back(message);
            }
            res["last_updated"] = search_res_json["hits"][search_res_json["hits"].size() - 1]["document"]["timestamp"];
            total -= search_res_json["hits"].size();
        }
    }

    return Option<nlohmann::json>(res);
}

// pop front elements until the conversation is less than MAX_TOKENS
Option<nlohmann::json> ConversationManager::truncate_conversation(nlohmann::json conversation, size_t limit) {
    if(!conversation.is_array()) {
        return Option<nlohmann::json>(400, "Conversation history is not an array");
    }
    if(limit <= 0) {
        return Option<nlohmann::json>(400, "Limit must be positive integer");
    }
    while(conversation.dump(0).size() > limit) {
        // pop front element from json array
        try {
            conversation.erase(0);
        } catch (std::exception& e) {
            return Option<nlohmann::json>(400, "Conversation history is not an array");
        }
    }

    return Option<nlohmann::json>(conversation);
}

Option<nlohmann::json> ConversationManager::delete_conversation_unsafe(const std::string& conversation_id) {
    auto conversation_exists = check_conversation_exists(conversation_id);
    if(!conversation_exists.ok()) {
        return Option<nlohmann::json>(conversation_exists.code(), conversation_exists.error());
    }

    auto history_collection_op = get_history_collection(conversation_id);
    if(!history_collection_op.ok()) {
        return Option<nlohmann::json>(history_collection_op.code(), history_collection_op.error());
    }

    auto history_collection = history_collection_op.get()->get_name();

    if(!raft_server) {
        auto req = std::make_shared<http_req>();
        auto resp = std::make_shared<http_res>(nullptr);

        req->params["filter_by"] = "conversation_id:" + conversation_id;
        req->params["collection"] = history_collection;

        auto api_res = del_remove_documents(req, resp);
        if(!api_res) {
            return Option<nlohmann::json>(resp->status_code, resp->body);
        }

        nlohmann::json res_json;
        res_json["id"] = conversation_id;
        return Option<nlohmann::json>(res_json);
    }

    auto leader_url = raft_server->get_leader_url();

    if(leader_url.empty()) {
        return Option<nlohmann::json>(500, "Leader URL is empty");
    }

    std::string base_url = leader_url + "collections/" + history_collection;
    std::string res;
    std::string url = base_url + "/documents?filter_by=conversation_id:" + conversation_id;
    std::map<std::string, std::string> res_headers;

    long status = HttpClient::delete_response(url, res, res_headers, 10*1000, true);

    if(status != 200) {
        LOG(ERROR) << "Error while deleting conversation: " << res;
        LOG(ERROR) << "Status: " << status;
        return Option<nlohmann::json>(400, "Error while deleting conversation");
    } else {
        nlohmann::json res_json;
        res_json["conversation_id"] = conversation_id;
        return Option<nlohmann::json>(res_json);
    }
}

Option<nlohmann::json> ConversationManager::delete_conversation(const std::string& conversation_id) {
    std::unique_lock lock(conversations_mutex);
    return delete_conversation_unsafe(conversation_id); 
} 

Option<bool> ConversationManager::init(ReplicationState* raft_server) {

    if(raft_server == nullptr) {
        return Option<bool>(400, "Raft server is null");
    }

    this->raft_server = raft_server;

    return Option<bool>(true);
}   

void ConversationManager::clear_expired_conversations() {
    std::unique_lock lock(conversations_mutex);
    // Only leader can delete expired conversations
    if(raft_server && !raft_server->is_leader()) {
        return;
    }

    auto models_op = ConversationModelManager::get_all_models();
    if(!models_op.ok()) {
        LOG(ERROR) << "Error while getting conversation models: " << models_op.error();
        return;
    }

    auto models = models_op.get();

    for(auto& model : models) {
        if(model.count("history_collection") == 0) {
            continue;
        }

        auto history_collection = model["history_collection"].get<std::string>();
        auto ttl = model["ttl"].get<uint64_t>();

        auto collection = CollectionManager::get_instance().get_collection(history_collection).get();
        std::string filter_by_str = "timestamp:<" + std::to_string(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count() - ttl + TTL_OFFSET) + "&&model_id:=" + model["id"].get<std::string>();
        if(raft_server) {
            
            std::string res;
            std::map<std::string, std::string> res_headers;
            std::string url  = raft_server->get_leader_url() + "collections/" + history_collection + "/documents?filter_by=" + filter_by_str;
            auto res_code = HttpClient::get_instance().delete_response(url, res, res_headers, 10*1000, true);

            if(res_code != 200) {
                LOG(ERROR) << "Error while deleting expired conversations: " << res;
                LOG(ERROR) << "Status: " << res_code;
            }
        } else {
            std::shared_ptr<http_req> req = std::make_shared<http_req>();
            std::shared_ptr<http_res> resp = std::make_shared<http_res>(nullptr);
            req->params["collection"] = history_collection;
            req->params["filter_by"] = filter_by_str;
            auto api_res = del_remove_documents(req, resp);

            if(!api_res) {
                LOG(ERROR) << "Error while deleting expired conversations: " << resp->body;
            }

        }

    }
}


void ConversationManager::run() {
    while(!quit) {
        std::unique_lock lock(conversations_mutex);
        cv.wait_for(lock, std::chrono::seconds(60), [&] { return quit.load(); });

        if(quit) {
            return;
        }

        lock.unlock();
        clear_expired_conversations();
    }
}

void ConversationManager::stop() {
    quit = true;
    cv.notify_all();
}



Option<bool> ConversationManager::validate_conversation_store_schema(Collection* collection) {
    const auto& schema = collection->get_schema();

    if(schema.count("conversation_id") == 0) {
        return Option<bool>(400, "Schema is missing `conversation_id` field");
    }

    if(schema.count("role") == 0) {
        return Option<bool>(400, "Schema is missing `role` field");
    }

    if(schema.count("message") == 0) {
        return Option<bool>(400, "Schema is missing `message` field");
    }

    if(schema.count("timestamp") == 0) {
        return Option<bool>(400, "Schema is missing `timestamp` field");
    }

    if(schema.at("conversation_id").type != field_types::STRING) {
        return Option<bool>(400, "`conversation_id` field must be a string");
    }

    if(schema.at("role").type != field_types::STRING) {
        return Option<bool>(400, "`role` field must be a string");
    }

    if(schema.at("message").type != field_types::STRING) {
        return Option<bool>(400, "`message` field must be a string");
    }

    if(schema.at("timestamp").type != field_types::INT32) {
        return Option<bool>(400, "`timestamp` field must be an integer");
    }

    if(!schema.at("timestamp").sort) {
        return Option<bool>(400, "`timestamp` field must be a sort field");
    }

    if(schema.count("model_id") == 0) {
        return Option<bool>(400, "Schema is missing `model_id` field");
    }

    if(schema.at("model_id").type != field_types::STRING) {
        return Option<bool>(400, "`model_id` field must be a string");
    }

    return Option<bool>(true);
}


Option<bool> ConversationManager::check_conversation_exists(const std::string& conversation_id) {
    auto collection_op = get_history_collection(conversation_id);
    if(!collection_op.ok()) {
        return Option<bool>(collection_op.code(), collection_op.error());
    }

    auto collection = collection_op.get();

    nlohmann::json res;
    size_t total = 0;
    auto search_res = collection->search("*", {}, "conversation_id:" + conversation_id, {}, {}, {}, 250);
    if(!search_res.ok()) {
        return Option<bool>(400, "Error while searching conversation store: " + search_res.error());
    }
    auto search_res_json = search_res.get();
    total = search_res_json["found"].get<uint32_t>();
    if(total == 0) {
        return Option<bool>(404, "Conversation not found");
    }

    return Option<bool>(true);
}


Option<Collection*> ConversationManager::get_history_collection(const std::string& conversation_id) {

    auto history_collections = ConversationModelManager::get_history_collections();
    for(auto& collection : history_collections) {
        auto collection_ptr = CollectionManager::get_instance().get_collection(collection).get();
        if(!collection_ptr) {
            continue;
        }

        auto search_res = collection_ptr->search("*", {}, "conversation_id:" + conversation_id, {}, {}, {}, 1);
        if(!search_res.ok()) {
            continue;
        }

        auto search_res_json = search_res.get();
        if(search_res_json["found"].get<uint32_t>() > 0) {
            return Option<Collection*>(collection_ptr);
        }
    }

    return Option<Collection*>(404, "Conversation not found");
}

Option<bool> ConversationManager::validate_conversation_store_collection(const std::string& collection) {
    auto collection_ptr = CollectionManager::get_instance().get_collection(collection).get();
    if(!collection_ptr) {
        return Option<bool>(404, "Collection not found");
    }

    auto validate_op = validate_conversation_store_schema(collection_ptr);
    if(!validate_op.ok()) {
        return Option<bool>(validate_op.code(), validate_op.error());
    }

    return Option<bool>(true);
}
