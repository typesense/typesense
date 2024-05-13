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
    auto it = models.find(model_id);
    if (it == models.end()) {
        return Option<nlohmann::json>(404, "Model not found");
    }

    nlohmann::json model = it->second;

    auto model_key = get_model_key(model_id);
    bool delete_op = store->remove(model_key);
    
    ConversationManager::get_instance().remove_conversation_collection(model["conversation_collection"]);
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
        models[model_id] = model_json;
        ConversationManager::get_instance().add_conversation_collection(model_json["conversation_collection"]);
        loaded_models++;
    }

    return Option<int>(loaded_models);
}

const std::string ConversationModelManager::get_model_key(const std::string& model_id) {
    return std::string(MODEL_KEY_PREFIX) + "_" + model_id;
}

Option<bool> ConversationModelManager::delete_models_With_conversation_collection(const std::string& collection) {
    std::unique_lock lock(models_mutex);
    std::vector<std::string> model_ids;
    for(auto& [model_id, model] : models) {
        if(model["conversation_collection"] == collection) {
            model_ids.push_back(model_id);
        }
    }

    for(auto& model_id : model_ids) {
        auto delete_res = delete_model(model_id);
        if(!delete_res.ok()) {
            return Option<bool>(delete_res.code(), delete_res.error());
        }
    }

    return Option<bool>(true);
}