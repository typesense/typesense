#include "natural_language_search_model_manager.h"
#include "natural_language_search_model.h"
#include "collection_manager.h"
#include "logger.h"
#include "string_utils.h"
#include "sole.hpp"
#include <unordered_set>

Option<nlohmann::json> NaturalLanguageSearchModelManager::get_model(const std::string& model_id) {
    std::shared_lock lock(models_mutex);
    auto it = models.find(model_id);
    if (it == models.end()) {
        return Option<nlohmann::json>(404, "Model not found");
    }
    return Option<nlohmann::json>(it->second);
}

Option<bool> NaturalLanguageSearchModelManager::add_model(nlohmann::json& model, const std::string& model_id, const bool write_to_disk) {
    std::unique_lock lock(models_mutex);

    if (models.find(model_id) != models.end()) {
        return Option<bool>(409, "Model already exists");
    }

    model["id"] = model_id.empty() ? sole::uuid4().str() : model_id;

    auto validate_res = NaturalLanguageSearchModel::validate_model(model);
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

Option<nlohmann::json> NaturalLanguageSearchModelManager::delete_model(const std::string& model_id) {
    std::unique_lock lock(models_mutex);
    return delete_model_unsafe(model_id);
}

Option<nlohmann::json> NaturalLanguageSearchModelManager::delete_model_unsafe(const std::string& model_id) {
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

Option<nlohmann::json> NaturalLanguageSearchModelManager::get_all_models() {
    std::shared_lock lock(models_mutex);
    nlohmann::json models_json = nlohmann::json::array();
    for (auto& [id, model] : models) {
        models_json.push_back(model);
    }

    return Option<nlohmann::json>(models_json);
}

Option<nlohmann::json> NaturalLanguageSearchModelManager::update_model(const std::string& model_id, nlohmann::json model) {
    std::unique_lock lock(models_mutex);
    auto it = models.find(model_id);
    if (it == models.end()) {
        return Option<nlohmann::json>(404, "Model not found");
    }

    nlohmann::json model_copy = it->second;

    for (auto& [key, value] : model.items()) {
        model_copy[key] = value;
    }

    auto validate_res = NaturalLanguageSearchModel::validate_model(model_copy);
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

Option<int> NaturalLanguageSearchModelManager::init(Store* store) {
    NaturalLanguageSearchModelManager::store = store;

    std::vector<std::string> model_strs;
    store->scan_fill(std::string(MODEL_KEY_PREFIX) + "_", std::string(MODEL_KEY_PREFIX) + "`", model_strs);

    if(!model_strs.empty()) {
        LOG(INFO) << "Found " << model_strs.size() << " natural language search model(s).";
    }

    int loaded_models = 0;

    for(auto& model_str : model_strs) {
        nlohmann::json model_json = nlohmann::json::parse(model_str);
        const std::string& model_id = model_json["id"];

        auto has_migration = migrate_model(model_json);
        auto add_op = add_model(model_json, model_id, has_migration);
        if(!add_op.ok()) {
            LOG(ERROR) << "Error while loading natural language search model: " << model_id 
                      << ", error: " << add_op.error();
            continue;
        }

        loaded_models++;
    }

    return Option<int>(loaded_models);
}

const std::string NaturalLanguageSearchModelManager::get_model_key(const std::string& model_id) {
    return std::string(MODEL_KEY_PREFIX) + "_" + model_id;
}

bool NaturalLanguageSearchModelManager::migrate_model(nlohmann::json& model) {
    bool has_model_change = false;
    return has_model_change;
}

Option<std::string> NaturalLanguageSearchModelManager::get_schema_prompt(const std::string& collection_name, uint64_t ttl_seconds) {
    if (ttl_seconds == 0) {
        return generate_schema_prompt(collection_name);
    }

    std::shared_lock lock(schema_prompts_mutex);
    auto it = schema_prompts.find(collection_name);
    if (it != schema_prompts.end()) {
        const auto& cached_entry = it.value();
        if (ttl_seconds > 0) {
            auto now = NaturalLanguageSearchModelManager::now();
            auto age_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                now - cached_entry.created_at).count();
            if (age_seconds <= static_cast<int64_t>(ttl_seconds)) {
                return Option<std::string>(cached_entry.prompt);
            }
        }
    }
    lock.unlock();
    return generate_schema_prompt(collection_name);
}

Option<std::string> NaturalLanguageSearchModelManager::generate_schema_prompt(const std::string& collection_name) {
    auto collection = CollectionManager::get_instance().get_collection(collection_name);
    if (collection == nullptr) {
        return Option<std::string>(404, "Collection not found");
    }
    Collection* coll = collection.get();
    auto search_schema = coll->get_schema();

    std::string schema_prompt;
    schema_prompt += "You are given the database schema structure below. ";
    schema_prompt += "Your task is to extract relevant SQL-like query parameters from the user's search query.\n\n";
    schema_prompt += "Database Schema:\n";
    schema_prompt += "Table fields are listed in the format: [Field Name] [Data Type] [Is Indexed] [Is Faceted] [Enum Values]\n\n";
    schema_prompt += "| Field Name | Data Type | Is Indexed | Is Faceted | Enum Values |\n";
    schema_prompt += "|------------|-----------|------------|------------|-------------|\n";

    std::unordered_map<std::string, std::vector<std::string>> field_facet_values;
    
    // Collect all string facetable fields
    std::vector<std::string> string_facet_fields;
    for (const auto& facet_field : coll->get_facet_fields()) {
        if (search_schema.count(facet_field) == 0) continue;

        const auto& field_type = search_schema.at(facet_field).type;
        bool is_string_type = (field_type == field_types::STRING || field_type == field_types::STRING_ARRAY);
        if (is_string_type) {
            string_facet_fields.push_back(facet_field);
        }
    }
    
    // Perform a single search query for all facetable fields
    if (!string_facet_fields.empty()) {
        auto results = coll->search("*", {}, "", string_facet_fields, {}, {0}, 0, 1,
          FREQUENCY, {false}, 0, spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>(), 20,
          "", 30, 4, "", Index::TYPO_TOKENS_THRESHOLD, "", "", {}, 3,
          "<mark>", "</mark>", {}, 1000000, true, false, true, "", false,
          6000*1000, 4, 7, fallback, 4, {off}, INT16_MAX, INT16_MAX, 2,
          false, false, "", true, 0, max_score, 20, 1000).get();

        if (results.contains("facet_counts") && results["facet_counts"].is_array()) {
            for (const auto& facet_result : results["facet_counts"]) {
                if (facet_result.contains("field_name") && facet_result["field_name"].is_string() &&
                    facet_result.contains("counts") && facet_result["counts"].is_array()) {
                    
                    std::string field_name = facet_result["field_name"].get<std::string>();
                    auto& values = field_facet_values[field_name];
                    
                    for (const auto& count : facet_result["counts"]) {
                        if (count.contains("value") && count["value"].is_string()) {
                            values.push_back(count["value"].get<std::string>());
                        }
                    }
                }
            }
        }
    }

    std::string schema_fields;
    for (auto it = search_schema.begin(); it != search_schema.end(); ++it) {
        const std::string& field_name = it.key();
        const auto& field = it.value();

        std::string enum_values;
        bool is_faceted = field.facet;
        bool is_string_type = (field.type == field_types::STRING || field.type == field_types::STRING_ARRAY);

        if (is_faceted && is_string_type) {
            auto facet_values_it = field_facet_values.find(field_name);
            if (facet_values_it != field_facet_values.end() && !facet_values_it->second.empty()) {
                enum_values = "[";
                const auto& values = facet_values_it->second;
                for (size_t i = 0; i < values.size() && i < 10; ++i) {
                    if (i > 0) enum_values += ", ";
                    enum_values += values[i];
                }
                if (values.size() > 10) enum_values += ", ...";
                enum_values += "]";
            } else {
                enum_values = "[Faceted field with unique values]";
            }
        } else {
            enum_values = "N/A";
        }

        schema_fields += "| " + field_name + " | " + field.type + " | "
                      + (field.index ? "Yes" : "No") + " | "
                      + (field.facet ? "Yes" : "No") + " | "
                      + enum_values + " |\n";
    }
    schema_prompt += schema_fields;

    schema_prompt += "\nInstructions:\n";
    schema_prompt += "1. Find all search terms that match fields in the schema.\n";
    schema_prompt += "2. Find filter values for faceted fields. Map user intent to the appropriate value when possible.\n";
    schema_prompt += "3. Ensure that filter terms are properly associated with their fields.\n";
    schema_prompt += "4. For faceted fields, use the example values to interpret user intent even if the exact value isn't specified.\n";
    schema_prompt += "5. Infer query parameters from context, even if not explicitly mentioned.\n";

    schema_prompt += "\nTypesense Query Syntax:\n";
    schema_prompt += "\nFiltering:\n";
    schema_prompt += "- Matching values: {fieldName}:{value} or {fieldName}:[value1,value2] for OR conditions\n";
    schema_prompt += "- Numeric filters: {fieldName}:[min..max] for ranges, or {fieldName}:>, {fieldName}:<, {fieldName}:>=, {fieldName}:<=, {fieldName}:=\n";
    schema_prompt += "- Multiple conditions: {condition1} && {condition2}\n";
    schema_prompt += "- OR conditions across fields: {fieldName1}:{value1} || {fieldName2}:{value2}\n";
    schema_prompt += "- Negation: {fieldName}:!= or {fieldName}:!=[value1,value2]\n";
    schema_prompt += "- For values with parentheses, surround with backticks: {fieldName}:`value (with parentheses)`\n";

    schema_prompt += "\nSorting:\n";
    schema_prompt += "- Format: {fieldName}:asc or {fieldName}:desc, maximum 3 fields\n";
    schema_prompt += "- Multiple sort fields: {fieldName1}:asc,{fieldName2}:desc\n";

    schema_prompt += "\nThe output should be in JSON format like this:\n";
    schema_prompt += "{\n";
    schema_prompt += "  \"q\": \"Include query only if both filter_by and sort_by are inadequate, remove any other text converted into filter_by or sort_by from the query\",\n";
    schema_prompt += "  \"filter_by\": \"typesense filter syntax explained above\",\n";
    schema_prompt += "  \"sort_by\": \"typesense sort syntax explained above\"\n";
    schema_prompt += "}\n";
    
    // LOG(INFO) << "Schema prompt for'" << collection_name << "': " << schema_prompt;
    
    std::unique_lock lock(schema_prompts_mutex);
    schema_prompts.insert(collection_name, SchemaPromptEntry(schema_prompt));

    return Option<std::string>(schema_prompt);
}
void NaturalLanguageSearchModelManager::clear_schema_prompt(const std::string& collection_name) {
    std::unique_lock lock(schema_prompts_mutex);
    schema_prompts.erase(collection_name);
}

void NaturalLanguageSearchModelManager::clear_all_schema_prompts() {
    std::unique_lock lock(schema_prompts_mutex);
    schema_prompts.clear();
}

bool NaturalLanguageSearchModelManager::has_cached_schema_prompt(const std::string& collection_name) {
    std::shared_lock lock(schema_prompts_mutex);
    return schema_prompts.contains(collection_name);
}

void NaturalLanguageSearchModelManager::init_schema_prompts_cache(uint32_t capacity) {
    std::unique_lock lock(schema_prompts_mutex);
    schema_prompts.capacity(capacity);
}

Option<nlohmann::json> NaturalLanguageSearchModelManager::process_natural_language_query(
    const std::string& nl_query, 
    const std::string& collection_name, 
    const std::string& nl_model_id,
    uint64_t prompt_cache_ttl_seconds) {
    nlohmann::json model_config;
    
    if (!nl_model_id.empty()) {
        auto model_op = get_model(nl_model_id);
        if (!model_op.ok()) {
            return Option<nlohmann::json>(model_op.code(), "Error getting natural language search model: " + model_op.error());
        }
        model_config = model_op.get();
    } else {
        auto models_op = get_all_models();
        if (!models_op.ok() || models_op.get().empty()) {
            return Option<nlohmann::json>(404, "No natural language search models found. Please configure at least one model.");
        }
        model_config = models_op.get()[0];
    }    
    auto schema_prompt_op = get_schema_prompt(collection_name, prompt_cache_ttl_seconds);
    if (!schema_prompt_op.ok()) {
        return Option<nlohmann::json>(schema_prompt_op.code(), "Error generating schema prompt: " + schema_prompt_op.error());
    }
    
    auto params_op = NaturalLanguageSearchModel::generate_search_params(nl_query, schema_prompt_op.get(), model_config);
    if (!params_op.ok()) {
        return Option<nlohmann::json>(params_op.code(), "Error generating search parameters: " + params_op.error());
    }
    return params_op;
}

Option<uint64_t> NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(std::map<std::string, std::string>& req_params, uint64_t schema_prompt_ttl_seconds) {

    std::string nl_query;
    bool has_nl_query = false;
    auto start_time = std::chrono::high_resolution_clock::now();

    if(req_params.count("nl_query") != 0 && req_params["nl_query"] == "true" && req_params.count("q") != 0 && !req_params["q"].empty()) {
        nl_query = req_params["q"];
        req_params["_original_nl_query"] = nl_query;
        has_nl_query = true;
    }

    if (!has_nl_query) {
        return Option<uint64_t>(400, "No nl_query found in either URL parameters or JSON body");
    }

    std::string collection_name = req_params.at("collection");

    auto params_op = process_natural_language_query(
        nl_query,
        collection_name,
        req_params.count("nl_model_id") > 0 ? req_params.at("nl_model_id") : "default",
        schema_prompt_ttl_seconds
    );

    if(!params_op.ok()) {
        req_params["error"] = params_op.error();
        req_params["_nl_processing_failed"] = "true";
        req_params["_fallback_q_used"] = "true";
        return Option<uint64_t>(400, params_op.error());
    }

    nlohmann::json generated_params = params_op.get();
    nlohmann::json _llm_generated_params = nlohmann::json::array();

    for(auto& param : generated_params.items()) {
        if(param.key() == "q") {
            _llm_generated_params.push_back("q");
            req_params["q"] = param.value();
        } else if(param.key() == "filter_by") {
            _llm_generated_params.push_back("filter_by");
            req_params["_original_llm_filter_by"] = param.value();
            req_params["llm_generated_filter_by"] = param.value();

            if(req_params.count("filter_by") != 0 && !req_params["filter_by"].empty()) {
                std::string existing_filter = req_params["filter_by"];
                std::string generated_filter = param.value().get<std::string>();

                StringUtils::trim(existing_filter);
                StringUtils::trim(generated_filter);

                if(!existing_filter.empty() && !generated_filter.empty()) {
                    req_params["filter_by"] = existing_filter + " && " + generated_filter;
                } else if(!existing_filter.empty()) {
                    req_params["filter_by"] = existing_filter;
                } else if(!generated_filter.empty()) {
                    req_params["filter_by"] = generated_filter;
                } else {
                    req_params["filter_by"] = "";
                }
            } else {
                req_params["filter_by"] = param.value();
            }
        } else if(param.key() == "sort_by") {
            req_params["sort_by"] = param.value();
            _llm_generated_params.push_back("sort_by");
        } else if(param.key() != "llm_response") {
            req_params[param.key()] = param.value();
            _llm_generated_params.push_back(param.key());
        }
    }

    req_params["processed_by_nl_model"] = "true";

    if(generated_params.count("llm_response") != 0) {
        req_params["llm_response_str"] = generated_params["llm_response"].dump();
        req_params["_llm_generated_params"] = _llm_generated_params.dump();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    uint64_t processing_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    return Option<uint64_t>(processing_time_ms);
}

nlohmann::json NaturalLanguageSearchModelManager::build_augmented_params(const std::map<std::string, std::string>* req_params) {
    const std::unordered_set<std::string> generated_params = {
        "q", "filter_by", "sort_by"
    };

    nlohmann::json augmented = nlohmann::json::object();

    for (const std::string& name : generated_params) {
        auto it = req_params->find(name);
        if (it != req_params->end()) {
            augmented[name] = it->second;
        }
    }

    for (const auto& [key, value] : *req_params) {
        if (key.empty() || key[0] == '_' || generated_params.count(key) == 0) {
            continue;
        }
        augmented[key] = value;
    }

    return augmented;
}

nlohmann::json NaturalLanguageSearchModelManager::build_generated_params(const std::map<std::string, std::string>* req_params) {
    nlohmann::json generated_params = nlohmann::json::object();

    std::unordered_set<std::string> generated_keys;
    if (auto it = req_params->find("_llm_generated_params"); it != req_params->end()) {
        for (const auto& name : nlohmann::json::parse(it->second)) {
            generated_keys.insert(name.get<std::string>());
        }
    }

    if (generated_keys.empty()) {
        generated_keys = {"q", "filter_by", "sort_by"};
    }

    auto copy_if_present = [&](const std::string& dest_key, const std::string& src_key) {
        auto it = req_params->find(src_key);
        if (it != req_params->end()) {
            generated_params[dest_key] = it->second;
        }
    };

    if (generated_keys.count("q")) {
        copy_if_present("q", "q");
    }
    if (generated_keys.count("filter_by")) {
        copy_if_present("filter_by", req_params->count("_original_llm_filter_by") ? "_original_llm_filter_by" : "llm_generated_filter_by");
    }
    if (generated_keys.count("sort_by")) {
        copy_if_present("sort_by", "sort_by");
    }

    for (const std::string& key : generated_keys) {
        if (generated_params.contains(key)) {
          continue;
        }
        copy_if_present(key, key);
    }

    return generated_params;
}

void NaturalLanguageSearchModelManager::add_nl_query_data_to_results(nlohmann::json& results_json, const std::map<std::string, std::string>* req_params, uint64_t nl_processing_time_ms, bool error) {

    if (req_params == nullptr) {
        return;
    }

    const bool has_nl_data =
        req_params->count("processed_by_nl_model") > 0 ||
        req_params->count("_llm_response") > 0      ||
        req_params->count("llm_response_str") > 0   ||
        req_params->count("_original_nl_query") > 0 ||
        req_params->count("_fallback_q_used") > 0 ||
        req_params->count("_nl_processing_failed") > 0;

    if (!has_nl_data) {
        return;
    }

    if (!results_json.contains("request_params") && !error) {
        results_json["request_params"] = nlohmann::json::object();
    }

    auto it = req_params->find("_original_nl_query");
    if (it == req_params->end()) {
        it = req_params->find("q");
    }
    if (it != req_params->end() && !error) {
        results_json["request_params"]["q"] = it->second;
        if (results_json.contains("search_time_ms")) {
            results_json["search_time_ms"] = results_json["search_time_ms"].get<uint64_t>() + nl_processing_time_ms;
        }
    }

    nlohmann::json parsed_nl_query = nlohmann::json::object();

    if (nl_processing_time_ms > 0) {
        parsed_nl_query["parse_time_ms"] = nl_processing_time_ms;
    }

    if (req_params->count("processed_by_nl_model") > 0) {
        parsed_nl_query["generated_params"] = build_generated_params(req_params);
    } else if (req_params->count("_nl_processing_failed") > 0) {
        parsed_nl_query["generated_params"] = nlohmann::json::object();
        if (req_params->count("error") > 0) {
            parsed_nl_query["error"] = req_params->at("error");
        }
    } else {
        parsed_nl_query["generated_params"] = nlohmann::json::object();
    }

    parsed_nl_query["augmented_params"] = build_augmented_params(req_params);

    bool debug_mode = false;
    if (auto it = req_params->find("nl_query_debug"); it != req_params->end()) {
        debug_mode = (it->second == "true" || it->second == "1");
    }

    if (debug_mode) {
        if (auto it = req_params->find("_llm_response"); it != req_params->end()) {
            parsed_nl_query["llm_response"] = it->second;
        } else if (auto it = req_params->find("llm_response_str"); it != req_params->end()) {
            try {
                parsed_nl_query["llm_response"] = nlohmann::json::parse(it->second);
            } catch (...) {
                parsed_nl_query["llm_response_str"] = it->second;
            }
        }
    }

    bool is_excluded = false;
    if (req_params->count("exclude_fields") > 0) {
      std::vector<std::string> exclude_fields;
      StringUtils::split(req_params->at("exclude_fields"), exclude_fields, ",");
      for (const auto& field : exclude_fields) {
        if (field == "parsed_nl_query") {
          is_excluded = true;
          break;
        }
      }
    }

    if (!parsed_nl_query.empty() && !is_excluded) {
        results_json["parsed_nl_query"] = std::move(parsed_nl_query);
    }
}

void NaturalLanguageSearchModelManager::dispose() {
    std::unique_lock lock(models_mutex);
    models.clear();
    schema_prompts.clear();
}

void NaturalLanguageSearchModelManager::reset_mock_time() {
    use_mock_time = false;
}

std::chrono::time_point<std::chrono::system_clock> NaturalLanguageSearchModelManager::now() {
    if (use_mock_time) {
        return mock_time_for_testing;
    }
    return std::chrono::system_clock::now();
}

void NaturalLanguageSearchModelManager::set_mock_time_for_testing(std::chrono::time_point<std::chrono::system_clock> mock_time) {
    mock_time_for_testing = mock_time;
    use_mock_time = true;
}

void NaturalLanguageSearchModelManager::advance_mock_time_for_testing(uint64_t seconds) {
    if (!use_mock_time) {
        mock_time_for_testing = std::chrono::system_clock::now();
        use_mock_time = true;
    }
    mock_time_for_testing += std::chrono::seconds(seconds);
}
