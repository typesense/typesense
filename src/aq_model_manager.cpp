#include <filesystem>
#include <map>
#include "vq_model_manager.h"
#include "http_client.h"


const std::string VQModelManager::get_model_url(const std::string& model_name) {
    return MODELS_REPO_URL + "voice_query/" + model_name + ".bin";
}

const std::string VQModelManager::get_config_url() {
    return MODELS_REPO_URL + "voice_query/models.json";
}


const std::string VQModelManager::get_model_namespace(const std::string& model_name) {
    // <namespace>/<model_name> if / is present in model_name
    if(model_name.find("/") != std::string::npos) {
        return model_name.substr(0, model_name.find("/"));
    } else {
        return "unknown";
    }
}

const Option<nlohmann::json> VQModelManager::get_config() {
    auto config_url = get_config_url();
    auto& client = HttpClient::get_instance();  
    std::string res;
    std::map<std::string, std::string> headers;
    auto response = client.get_response(config_url, res, headers);

    if (response != 200) {
        return Option<nlohmann::json>(400, "Failed to get model config file");
    }

    try {
        auto config = nlohmann::json::parse(res);

        return Option<nlohmann::json>(config);
    } catch (const std::exception& e) {
        return Option<nlohmann::json>(400, "Failed to parse model config file");
    }
}

const std::string VQModelManager::get_absolute_model_path(const std::string& model_name) {
    auto voice_query_home = EmbedderManager::get_model_dir();
    voice_query_home += voice_query_home.back() == '/' ? "" : "/";
    voice_query_home += "voice_query";
    std::filesystem::path path(voice_query_home);
    if (!std::filesystem::exists(path)) {
        std::filesystem::create_directory(path);
    }

    auto model_namespace = get_model_namespace(model_name);
    std::filesystem::path model_path(voice_query_home.back() == '/' ? voice_query_home + model_namespace : voice_query_home + "/" + model_namespace);
    if (!std::filesystem::exists(model_path)) {
        std::filesystem::create_directory(model_path);
    }
    return voice_query_home.back() == '/' ? voice_query_home + model_name + ".bin" : voice_query_home + "/" + model_name + ".bin";
}

Option<bool> VQModelManager::download_model(const std::string& model_name) {
    auto model_path = get_absolute_model_path(model_name);
    auto config = get_config();

    if (!config.ok()) {
        return Option<bool>(config.code(), config.error());
    }

    auto config_json = config.get();
    
    if (config_json.find(model_name + ".bin") == config_json.end()) {
        return Option<bool>(400, "Voice query model not found");
    }

    auto model_md5 = config_json[model_name + ".bin"].get<std::string>();

    if(EmbedderManager::check_md5(model_path, model_md5)) {
        return Option<bool>(true);
    }

    std::unique_lock<std::mutex> lock(download_mutex);
    auto model_url = get_model_url(model_name);
    auto& client = HttpClient::get_instance();
    auto response = client.download_file(model_url, model_path);
    LOG(INFO) << "Downloading model " << model_name << " from " << model_url << " to " << model_path;
    if (response != 200) {
        LOG(INFO) << response;
        return Option<bool>(400, "Failed to download voice query model");
    }

    return Option<bool>(true);
}

Option<std::shared_ptr<VQModel>> VQModelManager::validate_and_init_model(const std::string& model_name) {
    if(models.find(model_name) != models.end()) {
        return Option<std::shared_ptr<VQModel>>(models[model_name]);
    }

    auto model_namespace = get_model_namespace(model_name);
    if(model_namespace != "ts") {
        return Option<std::shared_ptr<VQModel>>(400, "Unknown model namespace");
    }
    
    auto model_name_without_namespace = get_model_name_without_namespace(model_name);

    auto download_res = download_model(model_name_without_namespace);

    if (!download_res.ok()) {
        return Option<std::shared_ptr<VQModel>>(download_res.code(), download_res.error());
    }

    auto model_path = get_absolute_model_path(model_name_without_namespace);
    auto model_inner_namespace = get_model_namespace(model_name_without_namespace);

    if (model_inner_namespace == "whisper") {
        auto whisper_ctx = WhisperModel::validate_and_load_model(model_path);
        if (!whisper_ctx) {
            return Option<std::shared_ptr<VQModel>>(400, "Failed to load voice query model");
        }
        auto whisper_model = std::shared_ptr<VQModel>(new WhisperModel(whisper_ctx, model_name));
        {
            std::unique_lock<std::shared_mutex> lock(models_mutex);
            models[model_name] = whisper_model;
        }
        return Option<std::shared_ptr<VQModel>>(whisper_model);
    } else {
        return Option<std::shared_ptr<VQModel>>(400, "Unknown model namespace");
    }
}

Option<std::shared_ptr<VQModel>> VQModelManager::get_model(const std::string& model_name) {
    std::shared_lock<std::shared_mutex> lock(models_mutex);
    auto model = models.find(model_name);
    if (model == models.end()) {
        return Option<std::shared_ptr<VQModel>>(400, "Voice query model not found");
    }
    return Option<std::shared_ptr<VQModel>>(model->second);
}

void VQModelManager::delete_model(const std::string& model_name) {
    std::unique_lock<std::shared_mutex> lock(models_mutex);
    auto model = models.find(model_name);
    if (model != models.end()) {
        models.erase(model);
    }
}

void VQModelManager::delete_all_models() {
    std::unique_lock<std::shared_mutex> lock(models_mutex);
    models.clear();
}

void VQModelManager::clear_unused_models() {
    std::unique_lock<std::shared_mutex> lock(models_mutex);
    for (auto it = models.begin(); it != models.end();) {
        if (it->second->get_collection_ref_count() == 0) {
            it = models.erase(it);
        } else {
            it++;
        }
    }
}

const std::string VQModelManager::get_model_name_without_namespace(const std::string& model_name) {
    if(model_name.find("/") != std::string::npos) {
        return model_name.substr(model_name.find("/") + 1);
    } else {
        return model_name;
    }
}

VQModelManager::~VQModelManager() {
    delete_all_models();
}