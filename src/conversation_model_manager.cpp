#include "conversation_model_manager.h"
#include "conversation_model.h"
#include "conversation_manager.h"

Option<nlohmann::json> ConversationModelManager::get_model(const std::string& model_id) {
    std::shared_lock lock(models_mutex);
    auto it = models.find(model_id);
    if (it == models.end()) {
        return Option<nlohmann::json>(404, "Model not found");
    }

    return Option<nlohmann::json>(it->second);
}

Option<nlohmann::json> ConversationModelManager::add_model(nlohmann::json model, const std::string& model_id) {
    std::unique_lock lock(models_mutex);

    return add_model_unsafe(model, model_id);
}

Option<nlohmann::json> ConversationModelManager::add_model_unsafe(nlohmann::json model, const std::string& model_id) {
    auto validate_res = ConversationModel::validate_model(model);
    if (!validate_res.ok()) {
        return Option<nlohmann::json>(validate_res.code(), validate_res.error());
    }


    model["id"] = model_id.empty() ? sole::uuid4().str() : model_id;

    auto model_key = get_model_key(model_id);
    bool insert_op = store->insert(model_key, model.dump(0));
    if(!insert_op) {
        return Option<nlohmann::json>(500, "Error while inserting model into the store");
    }

    models[model_id] = model;


    ConversationManager::get_instance().add_conversation_collection(model["conversation_collection"]);
    return Option<nlohmann::json>(model);
}

Option<nlohmann::json> ConversationModelManager::delete_model(const std::string& model_id) {
    std::unique_lock lock(models_mutex);

    return delete_model_unsafe(model_id);
}

Option<nlohmann::json> ConversationModelManager::delete_model_unsafe(const std::string& model_id) {
    auto it = models.find(model_id);
    if (it == models.end()) {
        return Option<nlohmann::json>(404, "Model not found");
    }

    nlohmann::json model = it->second;

    auto model_key = get_model_key(model_id);
    bool delete_op = store->remove(model_key);
    
    if(model.count("conversation_collection") != 0) {
        ConversationManager::get_instance().remove_conversation_collection(model["conversation_collection"].get<std::string>());
    }
    models.erase(it);
    return Option<nlohmann::json>(model);
}

Option<nlohmann::json> ConversationModelManager::get_all_models() {
    std::shared_lock lock(models_mutex);
    nlohmann::json models_json = nlohmann::json::array();
    for (auto& [id, model] : models) {
        models_json.push_back(model);
    }

    return Option<nlohmann::json>(models_json);
}

Option<nlohmann::json> ConversationModelManager::update_model(const std::string& model_id, nlohmann::json model) {
    std::unique_lock lock(models_mutex);
    auto validate_res = ConversationModel::validate_model(model);
    if (!validate_res.ok()) {
        return Option<nlohmann::json>(validate_res.code(), validate_res.error());
    }

    auto it = models.find(model_id);
    if (it == models.end()) {
        return Option<nlohmann::json>(404, "Model not found");
    }

    model["id"] = model_id;

    auto model_key = get_model_key(model_id);
    bool insert_op = store->insert(model_key, model.dump(0));
    if(!insert_op) {
        return Option<nlohmann::json>(500, "Error while inserting model into the store");
    }

    if(it->second["conversation_collection"] != model["conversation_collection"]) {
        ConversationManager::get_instance().remove_conversation_collection(it->second["conversation_collection"]);
        ConversationManager::get_instance().add_conversation_collection(model["conversation_collection"]);
    }

    models[model_id] = model;

    return Option<nlohmann::json>(model);
}

Option<int> ConversationModelManager::init(Store* store) {
    std::unique_lock lock(models_mutex);
    ConversationModelManager::store = store;

    std::vector<std::string> model_strs;
    store->scan_fill(std::string(MODEL_KEY_PREFIX) + "_", std::string(MODEL_KEY_PREFIX) + "`", model_strs);

    int loaded_models = 0;
    for(auto& model_str : model_strs) {
        nlohmann::json model_json = nlohmann::json::parse(model_str);
        std::string model_id = model_json["id"];
        // Migrate cloudflare models to new namespace convention, change namespace from `cf` to `cloudflare`
        if(EmbedderManager::get_model_namespace(model_json["model_name"]) == "cf") {
            auto delete_op = delete_model(model_id);
            if(!delete_op.ok()) {
                return Option<int>(delete_op.code(), delete_op.error());
            }
            model_json["model_name"] = "cloudflare/" + EmbedderManager::get_model_name_without_namespace(model_json["model_name"]);
            auto add_res = add_model(model_json, model_id);
            if(!add_res.ok()) {
                return Option<int>(add_res.code(), add_res.error());
            }
        }

        
        
        // Migrate models that don't have a conversation collection
        if(model_json.count("conversation_collection") == 0) {
            auto delete_op = delete_model_unsafe(model_id);
            if(!delete_op.ok()) {
                return Option<int>(delete_op.code(), delete_op.error());
            }
            auto migrate_op = migrate_model(model_json);
            if(!migrate_op.ok()) {
                return Option<int>(migrate_op.code(), migrate_op.error());
            }
            model_json = migrate_op.get();
        }

        models[model_id] = model_json;
        ConversationManager::get_instance().add_conversation_collection(model_json["conversation_collection"].get<std::string>());
        loaded_models++;
    }

    return Option<int>(loaded_models);
}

const std::string ConversationModelManager::get_model_key(const std::string& model_id) {
    return std::string(MODEL_KEY_PREFIX) + "_" + model_id;
}


Option<Collection*> ConversationModelManager::get_default_conversation_collection() {
    int64_t time_epoch;
    if(DEFAULT_CONVERSATION_COLLECTION_SUFFIX == 0) {
        time_epoch = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        DEFAULT_CONVERSATION_COLLECTION_SUFFIX = time_epoch;
    } else {
        time_epoch = DEFAULT_CONVERSATION_COLLECTION_SUFFIX;
    }
    
    std::string collection_id = "default_conversation_history_" + std::to_string(time_epoch);
    nlohmann::json schema_json = R"({
        "fields": [
            {
                "name": "conversation_id",
                "type": "string",
                "facet": true
            },
            {
                "name": "role",
                "type": "string"
            },
            {
                "name": "message",
                "type": "string"
            },
            {
                "name": "timestamp",
                "type": "int32",
                "sort": true
            }
        ]
    })"_json;
    schema_json["name"] = collection_id;

    auto get_res = CollectionManager::get_instance().get_collection(collection_id).get();
    if(get_res) {
        return Option<Collection*>(get_res);
    }

    auto create_res = CollectionManager::get_instance().create_collection(schema_json);
    if(!create_res.ok()) {
        return Option<Collection*>(create_res.code(), create_res.error());
    }
    return Option<Collection*>(create_res.get());
}

Option<nlohmann::json> ConversationModelManager::migrate_model(nlohmann::json model) {
    auto model_id = model["id"];
    auto default_collection = get_default_conversation_collection();
    if(!default_collection.ok()) {
        return Option<nlohmann::json>(default_collection.code(), default_collection.error());
    }
    model["conversation_collection"] = default_collection.get()->get_name();
    auto add_res = add_model_unsafe(model, model_id);
    if(!add_res.ok()) {
        return Option<nlohmann::json>(add_res.code(), add_res.error());
    }
    return Option<nlohmann::json>(model);
}
