#include "text_embedder_manager.h"


TextEmbedderManager& TextEmbedderManager::get_instance() {
    static TextEmbedderManager instance;
    return instance;
}

Option<bool> TextEmbedderManager::validate_and_init_model(const nlohmann::json& model_config, size_t& num_dims) {
    const std::string& model_name = model_config["model_name"].get<std::string>();

    if(is_remote_model(model_name)) {
        LOG(INFO) << "Validating and initializing remote model: " << model_name;
        return validate_and_init_remote_model(model_config, num_dims);
    } else {
        LOG(INFO) << "Validating and initializing local model: " << model_name;
        return validate_and_init_local_model(model_config, num_dims);
    }
}

Option<bool> TextEmbedderManager::validate_and_init_remote_model(const nlohmann::json& model_config,
                                                                 size_t& num_dims) {
    const std::string& model_name = model_config["model_name"].get<std::string>();
    auto model_namespace = TextEmbedderManager::get_model_namespace(model_name);

    if(model_namespace == "openai") {
        auto op = OpenAIEmbedder::is_model_valid(model_config, num_dims);
        if(!op.ok()) {
            return op;
        }
    } else if(model_namespace == "google") {
        auto op = GoogleEmbedder::is_model_valid(model_config, num_dims);
        if(!op.ok()) {
            return op;
        }
    } else if(model_namespace == "gcp") {
        auto op = GCPEmbedder::is_model_valid(model_config, num_dims);
        if(!op.ok()) {
            return op;
        }
    } else {
        return Option<bool>(400, "Invalid model namespace");
    }

    std::unique_lock<std::mutex> lock(text_embedders_mutex);
    std::string model_key = is_remote_model(model_name) ? RemoteEmbedder::get_model_key(model_config) : model_name;
    auto text_embedder_it = text_embedders.find(model_key);
    if(text_embedder_it == text_embedders.end()) {
        text_embedders.emplace(model_key, std::make_shared<TextEmbedder>(model_config, num_dims));
    }

    return Option<bool>(true);
}

Option<bool> TextEmbedderManager::validate_and_init_local_model(const nlohmann::json& model_config, size_t& num_dims) {
    const std::string& model_name = model_config["model_name"].get<std::string>();
    Option<bool> public_model_op = TextEmbedderManager::get_instance().init_public_model(model_name);

    if(!public_model_op.ok()) {
        return public_model_op;
    }

    std::string abs_path = TextEmbedderManager::get_absolute_model_path(
            TextEmbedderManager::get_model_name_without_namespace(model_name));

    if(!std::filesystem::exists(abs_path)) {
        LOG(ERROR) << "Model file not found: " << abs_path;
        return Option<bool>(400, "Model file not found");
    }

    bool is_public_model = public_model_op.get();

    if(!is_public_model) {
        if(!std::filesystem::exists(TextEmbedderManager::get_absolute_config_path(model_name))) {
            LOG(ERROR) << "Config file not found: " << TextEmbedderManager::get_absolute_config_path(model_name);
            return Option<bool>(400, "Config file not found");
        }
        std::ifstream config_file(TextEmbedderManager::get_absolute_config_path(model_name));
        nlohmann::json config;
        config_file >> config;
        if(config["model_type"].is_null() || config["vocab_file_name"].is_null()) {
            LOG(ERROR) << "Invalid config file: " << TextEmbedderManager::get_absolute_config_path(model_name);
            return Option<bool>(400, "Invalid config file");
        }

        if(!config["model_type"].is_string() || !config["vocab_file_name"].is_string()) {
            LOG(ERROR) << "Invalid config file: " << TextEmbedderManager::get_absolute_config_path(model_name);
            return Option<bool>(400, "Invalid config file");
        }

        if(!std::filesystem::exists(TextEmbedderManager::get_model_subdir(model_name) + "/" + config["vocab_file_name"].get<std::string>())) {
            LOG(ERROR) << "Vocab file not found: " << TextEmbedderManager::get_model_subdir(model_name) + "/" + config["vocab_file_name"].get<std::string>();
            return Option<bool>(400, "Vocab file not found");
        }

        if(config["model_type"].get<std::string>() != "bert" && config["model_type"].get<std::string>() != "xlm_roberta" && config["model_type"].get<std::string>() != "distilbert") {
            LOG(ERROR) << "Invalid model type: " << config["model_type"].get<std::string>();
            return Option<bool>(400, "Invalid model type");
        }
    }

    std::unique_lock<std::mutex> lock(text_embedders_mutex);
    auto text_embedder_it = text_embedders.find(model_name);

    if(text_embedder_it != text_embedders.end()) {
        num_dims = text_embedder_it->second->get_num_dim();
        return Option<bool>(true);
    }

    const std::shared_ptr<TextEmbedder>& embedder = std::make_shared<TextEmbedder>(
            get_model_name_without_namespace(model_name));

    auto validate_op = embedder->validate();
    if(!validate_op.ok()) {
        return validate_op;
    }

    num_dims = embedder->get_num_dim();
    text_embedders.emplace(model_name, embedder);
    return Option<bool>(true);
}

Option<TextEmbedder*> TextEmbedderManager::get_text_embedder(const nlohmann::json& model_config) {
    std::unique_lock<std::mutex> lock(text_embedders_mutex);
    const std::string& model_name = model_config.at("model_name");
    std::string model_key = is_remote_model(model_name) ? RemoteEmbedder::get_model_key(model_config) : model_name;
    auto text_embedder_it = text_embedders.find(model_key);

    if(text_embedder_it == text_embedders.end()) {
        return Option<TextEmbedder*>(404, "Text embedder was not found.");
    }

    return Option<TextEmbedder*>(text_embedder_it->second.get());
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
        std::unique_lock<std::mutex> lock(text_embedders_mutex);
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
        std::unique_lock<std::mutex> lock(text_embedders_mutex);
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
    // convert md5 to hex string with leading zeros
    for(int i = 0; i < MD5_DIGEST_LENGTH; i++) {
        res << std::hex << std::setfill('0') << std::setw(2) << (int)md5[i];
    }
    return res.str() == target_md5;
}

Option<bool> TextEmbedderManager::download_public_model(const text_embedding_model& model) {
    HttpClient& httpClient = HttpClient::get_instance();
    auto actual_model_name = get_model_name_without_namespace(model.model_name);
    if(!check_md5(get_absolute_model_path(actual_model_name), model.model_md5)) {
        long res = httpClient.download_file(get_model_url(model), get_absolute_model_path(actual_model_name));
        if(res != 200) {
            LOG(INFO) << "Failed to download public model: " << model.model_name;
            return Option<bool>(400, "Failed to download model file");
        }
    }
    
    if(!check_md5(get_absolute_vocab_path(actual_model_name, model.vocab_file_name), model.vocab_md5)) {
        long res = httpClient.download_file(get_vocab_url(model), get_absolute_vocab_path(actual_model_name, model.vocab_file_name));
        if(res != 200) {
            LOG(INFO) << "Failed to download default vocab for model: " << model.model_name;
            return Option<bool>(400, "Failed to download vocab file");
        }
    }

    return Option<bool>(true);
}

Option<bool> TextEmbedderManager::init_public_model(const std::string& model_name) {
    std::unique_lock<std::mutex> lock(text_embedders_mutex);
    if(public_models.find(model_name) != public_models.end()) {
        // model has already been initialized
        return Option<bool>(true);
    }

    auto model_namespace = get_namespace(model_name);
    if(!model_namespace.ok() || model_namespace.get() != "ts") {
        // not a public model
        return Option<bool>(false);
    }

    auto actual_model_name = get_model_name_without_namespace(model_name);
    auto model_config_op = get_public_model_config(actual_model_name);

    if(!model_config_op.ok()) {
        return Option<bool>(model_config_op.code(), model_config_op.error());
    }

    auto config = model_config_op.get();
    config["model_name"] = actual_model_name;

    auto model = text_embedding_model(config);

    auto download_op = TextEmbedderManager::get_instance().download_public_model(model);
    if (!download_op.ok()) {
        LOG(ERROR) << download_op.error();
        return Option<bool>(400, download_op.error());
    }

    public_models.emplace(model_name, text_embedding_model(config));
    return Option<bool>(true);;
}

bool TextEmbedderManager::is_public_model(const std::string& model_name) {
    std::unique_lock<std::mutex> lock(text_embedders_mutex);
    return public_models.find(model_name) != public_models.end();
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

const std::string TextEmbedderManager::get_model_namespace(const std::string& model_name) {
    if(model_name.find("/") != std::string::npos) {
        return model_name.substr(0, model_name.find("/"));
    } else {
        return "ts";
    }
}

bool TextEmbedderManager::is_remote_model(const std::string& model_name) {
    auto model_namespace = get_namespace(model_name);
    return model_namespace.ok() && (model_namespace.get() == "openai" || model_namespace.get() == "google" || model_namespace.get() == "gcp");
}
