#pragma once

#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <json.hpp>
#include "option.h"
#include "store.h"
#include "personalization_model.h"

class PersonalizationModelManager {
public:
    PersonalizationModelManager() = delete;
    PersonalizationModelManager(const PersonalizationModelManager&) = delete;
    PersonalizationModelManager(PersonalizationModelManager&&) = delete;
    PersonalizationModelManager& operator=(const PersonalizationModelManager&) = delete;

    static Option<nlohmann::json> get_model(const std::string& model_id);
    static Option<std::string> add_model(nlohmann::json& model,std::string model_id, const bool write_to_disk, const std::string model_data = "");
    static Option<nlohmann::json> delete_model(const std::string& model_id);
    static Option<nlohmann::json> get_all_models();
    static Option<nlohmann::json> update_model(const std::string& model_id, nlohmann::json model, const std::string& model_data);
    static Option<int> init(Store* store);

private:
    static inline std::unordered_map<std::string, nlohmann::json> models;
    static inline std::shared_mutex models_mutex;
    static inline Store* store;
    static constexpr const char* MODEL_KEY_PREFIX = "$PER";

    static const std::string get_model_key(const std::string& model_id);
};
