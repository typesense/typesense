#include "text_embedder_manager.h"


TextEmbedderManager& TextEmbedderManager::get_instance() {
    static TextEmbedderManager instance;
    return instance;
}

TextEmbedder* TextEmbedderManager::get_text_embedder(const nlohmann::json& model_config) {
    std::unique_lock<std::mutex> lock(text_embedders_mutex);
    const std::string& model_name = model_config.at("model_name");
    if(text_embedders[model_name] == nullptr) {
        if(model_config.count("api_key") == 0) {
            if(is_public_model(model_name)) {
                // download the model if it doesn't exist
                download_public_model(model_name);
            }
            text_embedders[model_name] = std::make_shared<TextEmbedder>(get_model_name_without_namespace(model_name));
        } else {
            text_embedders[model_name] = std::make_shared<TextEmbedder>(model_name, model_config.at("api_key").get<std::string>());
        }
    }
    return text_embedders[model_name].get();
}

void TextEmbedderManager::delete_text_embedder(const std::string& model_path) {
    std::unique_lock<std::mutex> lock(text_embedders_mutex);
    if (text_embedders.find(model_path) != text_embedders.end()) {
        text_embedders.erase(model_path);
    }
}

void TextEmbedderManager::delete_all_text_embedders() {
    std::unique_lock<std::mutex> lock(text_embedders_mutex);
    text_embedders.clear();
}

const TokenizerType TextEmbedderManager::get_tokenizer_type(const nlohmann::json& model_config) {
    if(model_config.find("model_type") == model_config.end()) {
        return TokenizerType::bert;
    } else {
        std::string tokenizer_type = model_config.at("model_type").get<std::string>();
        if(tokenizer_type == "distilbert") {
            return TokenizerType::distilbert;
        } else if(tokenizer_type == "xlm_roberta") {
            return TokenizerType::xlm_roberta;
        } else {
            return TokenizerType::bert;
        }
    }
}

const std::string TextEmbedderManager::get_indexing_prefix(const nlohmann::json& model_config) {
    std::string val;
    if(is_public_model(model_config["model_name"].get<std::string>())) {
        val = public_models[model_config["model_name"].get<std::string>()].indexing_prefix;
    } else {
        val = model_config.count("indexing_prefix") == 0 ? "" : model_config["indexing_prefix"].get<std::string>();
    }
    if(!val.empty()) {
        val += " ";
    }

    return val;
}

const std::string TextEmbedderManager::get_query_prefix(const nlohmann::json& model_config) {
    std::string val;
    if(is_public_model(model_config["model_name"].get<std::string>())) {
        val = public_models[model_config["model_name"].get<std::string>()].query_prefix;
    } else {
        val = model_config.count("query_prefix") == 0 ? "" : model_config["query_prefix"].get<std::string>();
    }
    if(!val.empty()) {
        val += " ";
    }

    return val;
}

void TextEmbedderManager::set_model_dir(const std::string& dir) {
    // create the directory if it doesn't exist
    if(!std::filesystem::exists(dir)) {
        std::filesystem::create_directories(dir);
    }
    model_dir = dir;
}

const std::string& TextEmbedderManager::get_model_dir() {
    return model_dir;
}

TextEmbedderManager::~TextEmbedderManager() {
    delete_all_text_embedders();
}

const std::string TextEmbedderManager::get_absolute_model_path(const std::string& model_name) {
    return get_model_subdir(model_name) + "/model.onnx";
}
const std::string TextEmbedderManager::get_absolute_vocab_path(const std::string& model_name, const std::string& vocab_file_name) {
    return get_model_subdir(model_name) + "/" + vocab_file_name;
}

const std::string TextEmbedderManager::get_absolute_config_path(const std::string& model_name) {
    return get_model_subdir(model_name) + "/config.json";
}

const bool TextEmbedderManager::check_md5(const std::string& file_path, const std::string& target_md5) {
    std::ifstream stream(file_path);
    if (stream.fail()) {
        return false;
    }
    unsigned char md5[MD5_DIGEST_LENGTH];
    std::stringstream ss,res;
    ss << stream.rdbuf();
    MD5((unsigned char*)ss.str().c_str(), ss.str().length(), md5);
    for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
        res << std::hex << (int)md5[i];
    }
    return res.str() == target_md5;
}
void TextEmbedderManager::download_public_model(const std::string& model_name) {
    HttpClient& httpClient = HttpClient::get_instance();
    auto model = public_models[model_name];
    auto actual_model_name = get_model_name_without_namespace(model_name);
    if(!check_md5(get_absolute_model_path(actual_model_name), model.model_md5)) {
        long res = httpClient.download_file(get_model_url(model), get_absolute_model_path(actual_model_name));
        if(res != 200) {
            LOG(INFO) << "Failed to download public model " << model_name << ": " << res;
        }
    }
    
    if(!check_md5(get_absolute_vocab_path(actual_model_name, model.vocab_file_name), model.vocab_md5)) {
        long res = httpClient.download_file(get_vocab_url(model), get_absolute_vocab_path(actual_model_name, model.vocab_file_name));
        if(res != 200) {
            LOG(INFO) << "Failed to download default vocab " << model_name << ": " << res;
        }
    }

}

const bool TextEmbedderManager::is_public_model(const std::string& model_name) {
    if(public_models.find(model_name) != public_models.end()) {
        return true;
    }

    auto model_namespace = get_namespace(model_name);
    if(!model_namespace.ok() || model_namespace.get() != "ts") {
        return false;
    }

    auto actual_model_name = get_model_name_without_namespace(model_name);
    auto model_config_op = get_public_model_config(actual_model_name);

    if(model_config_op.ok()) {
        auto config = model_config_op.get();
        config["model_name"] = actual_model_name;
        public_models[model_name] = text_embedding_model(config);
        return true;
    }

    return false;
}

const std::string TextEmbedderManager::get_model_subdir(const std::string& model_name) {
    if(model_dir.back() != '/') {
        // create subdir <model_name> if it doesn't exist
        if(!std::filesystem::exists(model_dir + "/" + model_name)) {
            std::filesystem::create_directories(model_dir + "/" + model_name);
        }
        return model_dir + "/" + model_name;
    } else {
        // create subdir <model_name> if it doesn't exist
        if(!std::filesystem::exists(model_dir + model_name)) {
            std::filesystem::create_directories(model_dir + model_name);
        }
        return model_dir + model_name;
    }
}

Option<std::string> TextEmbedderManager::get_namespace(const std::string& model_name) {
    // <namespace>/<model_name> if / is present in model_name
    if(model_name.find("/") != std::string::npos) {
        return Option<std::string>(model_name.substr(0, model_name.find("/")));
    } else {
        return Option<std::string>(404, "Namespace not found");
    }
}

const std::string TextEmbedderManager::get_model_name_without_namespace(const std::string& model_name) {
    // <namespace>/<model_name> if / is present in model_name
    if(model_name.find("/") != std::string::npos) {
        return model_name.substr(model_name.find("/") + 1);
    } else {
        return model_name;
    }
}

text_embedding_model::text_embedding_model(const nlohmann::json& json) {
    model_name = json.at("model_name").get<std::string>();
    model_md5 = json.at("model_md5").get<std::string>();
    vocab_file_name = json.at("vocab_file_name").get<std::string>();
    vocab_md5 = json.at("vocab_md5").get<std::string>();
    tokenizer_type = TextEmbedderManager::get_tokenizer_type(json);
    if(json.count("indexing_prefix") != 0) {
        indexing_prefix = json.at("indexing_prefix").get<std::string>();
    }
    if(json.count("query_prefix") != 0) {
        query_prefix = json.at("query_prefix").get<std::string>();
    }
}


Option<nlohmann::json> TextEmbedderManager::get_public_model_config(const std::string& model_name) {
    // check cache first
    if(std::filesystem::exists(get_absolute_config_path(model_name))) {
        std::ifstream config_file(get_absolute_config_path(model_name));
        nlohmann::json config;
        config_file >> config;
        config_file.close();
        return Option<nlohmann::json>(config);
    }

    auto actual_model_name = get_model_name_without_namespace(model_name);
    HttpClient& httpClient = HttpClient::get_instance();
    std::unordered_map<std::string, std::string> headers;
    headers["Accept"] = "application/json";
    std::map<std::string, std::string> response_headers;
    std::string response_body;
    long res = httpClient.get_response(MODELS_REPO_URL + actual_model_name + "/" + MODEL_CONFIG_FILE, response_body, response_headers, headers);

    if(res == 200 || res == 302) {
        // cache the config file
        auto config_file_path = get_absolute_config_path(actual_model_name);
        std::ofstream config_file(config_file_path);
        config_file << response_body;
        config_file.close();

        return Option<nlohmann::json>(nlohmann::json::parse(response_body));
    }

    return Option<nlohmann::json>(404, "Model not found");
}

const std::string TextEmbedderManager::get_model_url(const text_embedding_model& model) {
    return MODELS_REPO_URL + model.model_name + "/model.onnx";
}

const std::string TextEmbedderManager::get_vocab_url(const text_embedding_model& model) {
    return MODELS_REPO_URL + model.model_name + "/" + model.vocab_file_name;
}