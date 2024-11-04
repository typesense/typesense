#include "personalization_model_manager.h"
#include "sole.hpp"
#include <glog/logging.h>

Option<nlohmann::json> PersonalizationModelManager::get_model(const std::string& model_id) {
    std::shared_lock lock(models_mutex);
    auto it = models.find(model_id);
    if (it == models.end()) {
        return Option<nlohmann::json>(404, "Model not found");
    }
    return Option<nlohmann::json>(it->second);
}

Option<std::string> PersonalizationModelManager::add_model(nlohmann::json& model_json, std::string model_id, const bool write_to_disk, const std::string model_data) {
    std::unique_lock lock(models_mutex);

    if (models.find(model_id) != models.end()) {
        return Option<std::string>(409, "Model id already exists");
    }

    model_json["id"] = model_id.empty() ? sole::uuid4().str() : model_id;
    model_id = model_json["id"];
    model_json["model_path"] = PersonalizationModel::get_model_subdir(model_json["id"]);

    auto validate_op = PersonalizationModel::validate_model(model_json);
    if(!validate_op.ok()) {
        return Option<std::string>(validate_op.code(), validate_op.error());
    }

    if(write_to_disk) {
        auto model_key = get_model_key(model_json["id"]);
        auto create_op = PersonalizationModel::create_model(model_json["id"], model_json, model_data);
        if(!create_op.ok()) {
            return Option<std::string>(create_op.code(), create_op.error());
        }
        bool insert_op = store->insert(model_key, model_json.dump(0));
        if(!insert_op) {
            return Option<std::string>(500, "Error while inserting model into the store");
        }
    }
    models[model_json["id"]] = model_json;

    return Option<std::string>(model_id);
}

Option<int> PersonalizationModelManager::init(Store* store) {
    PersonalizationModelManager::store = store;

    std::vector<std::string> model_strs;
    store->scan_fill(std::string(MODEL_KEY_PREFIX) + "_", std::string(MODEL_KEY_PREFIX) + "`", model_strs);

    if(!model_strs.empty()) {
        LOG(INFO) << "Found " << model_strs.size() << " personalization model(s).";
    }

    int loaded_models = 0;

    for(auto& model_str : model_strs) {
        nlohmann::json model_json;
        try {
            model_json = nlohmann::json::parse(model_str);
        } catch (const nlohmann::json::parse_error& e) {
            LOG(ERROR) << "Error parsing model JSON: " << e.what();
            continue;
        }

        const std::string& model_id = model_json["id"];

        auto add_op = add_model(model_json, model_id, false);
        if(!add_op.ok()) {
            LOG(ERROR) << "Error while loading personalization model: " << model_id << ", error: " << add_op.error();
            continue;
        }

        loaded_models++;
    }

    return Option<int>(loaded_models);
}

Option<nlohmann::json> PersonalizationModelManager::delete_model(const std::string& model_id) {
    std::unique_lock lock(models_mutex);
    auto it = models.find(model_id);
    if (it == models.end()) {
        return Option<nlohmann::json>(404, "Model not found");
    }

    nlohmann::json model = it->second;

    auto delete_op = PersonalizationModel::delete_model(model_id);
    if(!delete_op.ok()) {
        return Option<nlohmann::json>(delete_op.code(), delete_op.error());
    }

    auto model_key = get_model_key(model_id);
    bool remove_op = store->remove(model_key);
    if(!remove_op) {
        return Option<nlohmann::json>(500, "Error while deleting model from the store");
    }
    
    models.erase(it);
    return Option<nlohmann::json>(model);
}

Option<nlohmann::json> PersonalizationModelManager::get_all_models() {
    std::shared_lock lock(models_mutex);
    nlohmann::json models_json = nlohmann::json::array();
    for (auto& [id, model] : models) {
        models_json.push_back(model);
    }
    return Option<nlohmann::json>(models_json);
}

const std::string PersonalizationModelManager::get_model_key(const std::string& model_id) {
    return std::string(MODEL_KEY_PREFIX) + "_" + model_id;
}

Option<nlohmann::json> PersonalizationModelManager::update_model(const std::string& model_id, nlohmann::json model, const std::string& model_data) {
    std::unique_lock lock(models_mutex);
    auto it = models.find(model_id);
    if (it == models.end()) {
        return Option<nlohmann::json>(404, "Model not found");
    }

    nlohmann::json model_copy = it->second;

    for (auto& [key, value] : model.items()) {
        model_copy[key] = value;
    }

    auto validate_res = PersonalizationModel::validate_model(model_copy);
    if (!validate_res.ok()) {
        return Option<nlohmann::json>(validate_res.code(), validate_res.error());
    }

    auto model_key = get_model_key(model_id);
    bool insert_op = store->insert(model_key, model_copy.dump(0));
    if(!insert_op) {
        return Option<nlohmann::json>(500, "Error while inserting model into the store");
    }

    auto update_op = PersonalizationModel::update_model(model_id, model_copy, model_data);
    if(!update_op.ok()) {
        return Option<nlohmann::json>(update_op.code(), update_op.error());
    }

    models[model_id] = model_copy;

    return Option<nlohmann::json>(model_copy);
}