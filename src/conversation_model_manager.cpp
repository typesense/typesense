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

Option<bool> ConversationModelManager::add_model(nlohmann::json& model, const std::string& model_id,
                                                 const bool write_to_disk) {
    std::unique_lock lock(models_mutex);

    if (models.find(model_id) != models.end()) {
        return Option<bool>(409, "Model already exists");
    }

    model["id"] = model_id.empty() ? sole::uuid4().str() : model_id;

    if(model.count("ttl") == 0) {
        model["ttl"] = (uint64_t)(60 * 60 * 24);
    }

    auto validate_res = ConversationModel::validate_model(model);
    if (!validate_res.ok()) {
        return Option<bool>(validate_res.code(), validate_res.error());
    }

    models[model["id"]] = model;

    if(write_to_disk) {
        auto model_key = get_model_key(model["id"]);
        bool insert_op = store->insert(model_key, model.dump(0));
        if(!insert_op) {
            return Option<bool>(500, "Error while inserting model into the store");
        }
    }

    return Option<bool>(true);
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
    if(!delete_op) {
        return Option<nlohmann::json>(500, "Error while deleting model from the store");
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
    auto it = models.find(model_id);
    if (it == models.end()) {
        return Option<nlohmann::json>(404, "Model not found");
    }

    nlohmann::json model_copy = it->second;

    for (auto& [key, value] : model.items()) {
        model_copy[key] = value;
    }

    auto validate_res = ConversationModel::validate_model(model_copy);
    if (!validate_res.ok()) {
        return Option<nlohmann::json>(validate_res.code(), validate_res.error());
    }

    auto model_key = get_model_key(model_id);
    bool insert_op = store->insert(model_key, model_copy.dump(0));
    if(!insert_op) {
        return Option<nlohmann::json>(500, "Error while inserting model into the store");
    }

    models[model_id] = model_copy;

    return Option<nlohmann::json>(model_copy);
}

Option<int> ConversationModelManager::init(Store* store) {
    ConversationModelManager::store = store;

    std::vector<std::string> model_strs;
    store->scan_fill(std::string(MODEL_KEY_PREFIX) + "_", std::string(MODEL_KEY_PREFIX) + "`", model_strs);

    if(!model_strs.empty()) {
        LOG(INFO) << "Found " << model_strs.size() << " conversation model(s).";
    }

    int loaded_models = 0;

    for(auto& model_str : model_strs) {
        nlohmann::json model_json = nlohmann::json::parse(model_str);
        const std::string& model_id = model_json["id"];

        // handle model format changes
        auto has_migration = migrate_model(model_json);

        // write to disk only when a migration has been done on model data
        auto add_op = add_model(model_json, model_id, has_migration);
        if(!add_op.ok()) {
            LOG(ERROR) << "Error while loading conversation model: " << model_id << ", error: " << add_op.error();
            continue;
        }

        loaded_models++;
    }

    return Option<int>(loaded_models);
}

const std::string ConversationModelManager::get_model_key(const std::string& model_id) {
    return std::string(MODEL_KEY_PREFIX) + "_" + model_id;
}

Option<Collection*> ConversationModelManager::create_default_history_collection(const std::string& model_id) {
    std::string collection_name = "ts_conversation_history_" + model_id;

    auto get_res = CollectionManager::get_instance().get_collection(collection_name).get();
    if(get_res) {
        return Option<Collection*>(get_res);
    }

    nlohmann::json schema_json = R"({
        "fields": [
            {
                "name": "conversation_id",
                "type": "string"
            },
            {
                "name": "role",
                "type": "string",
                "index": false
            },
            {
                "name": "message",
                "type": "string",
                "index": false
            },
            {
                "name": "timestamp",
                "type": "int32",
                "sort": true
            },
            {
                "name": "model_id",
                "type": "string"
            }
        ]
    })"_json;

    schema_json["name"] = collection_name;

    auto create_res = CollectionManager::get_instance().create_collection(schema_json);
    if(!create_res.ok()) {
        return Option<Collection*>(create_res.code(), create_res.error());
    }
    return Option<Collection*>(create_res.get());
}

bool ConversationModelManager::migrate_model(nlohmann::json& model) {
    // handles missing fields and format changes
    auto model_id = model["id"];
    bool has_model_change = false;

    // Migrate cloudflare models to new namespace convention, change namespace from `cf` to `cloudflare`
    if(EmbedderManager::get_model_namespace(model["model_name"]) == "cf") {
        model["model_name"] = "cloudflare/@cf/" + EmbedderManager::get_model_name_without_namespace(model["model_name"]);
        has_model_change = true;
    }

    if(model.count("history_collection") == 0) {
        auto default_collection_op = create_default_history_collection(model_id);
        if(!default_collection_op.ok()) {
            LOG(INFO) << "Error while creating default history collection for model " << model_id << ": "
                      << default_collection_op.error();
            return false;
        }

        model["history_collection"] = default_collection_op.get()->get_name();
        has_model_change = true;
    }

    if(model.count("ttl") == 0) {
        model["ttl"] = (uint64_t)(60 * 60 * 24);
        has_model_change = true;
    }

    return has_model_change;
}

std::unordered_set<std::string> ConversationModelManager::get_history_collections() {
    std::unordered_set<std::string> collections;
    for(auto& [id, model] : models) {
        if(model.find("history_collection") == model.end()) {
            continue;
        }
        collections.insert(model["history_collection"].get<std::string>());
    }
    return collections;
}