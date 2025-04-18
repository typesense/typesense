#include "natural_language_search_model_manager.h"
#include "natural_language_search_model.h"
#include "collection_manager.h"
#include "logger.h"
#include "sole.hpp"

Option<nlohmann::json> NaturalLanguageSearchModelManager::get_model(const std::string& model_id) {
    LOG(INFO) << "NLSM::get_model - looking for model with ID: " << model_id;
    std::shared_lock lock(models_mutex);
    
    LOG(INFO) << "NLSM::get_model - acquired shared lock";
    auto it = models.find(model_id);
    
    if (it == models.end()) {
        LOG(ERROR) << "NLSM::get_model - model not found with ID: " << model_id;
        return Option<nlohmann::json>(404, "Model not found");
    }
    
    LOG(INFO) << "NLSM::get_model - found model with ID: " << model_id;
    return Option<nlohmann::json>(it->second);
}

Option<bool> NaturalLanguageSearchModelManager::add_model(nlohmann::json& model, const std::string& model_id,
                                                 const bool write_to_disk) {
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

        // handle model format changes
        auto has_migration = migrate_model(model_json);

        // write to disk only when a migration has been done on model data
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
    // For future use
    bool has_model_change = false;


    return has_model_change;
}

Option<std::string> NaturalLanguageSearchModelManager::get_schema_prompt(const std::string& collection_name) {
    // Default TTL of 24 hours
    return get_schema_prompt(collection_name, DEFAULT_SCHEMA_PROMPT_TTL_SEC);
}

Option<std::string> NaturalLanguageSearchModelManager::get_schema_prompt(const std::string& collection_name, uint64_t ttl_seconds) {
    LOG(INFO) << "Getting schema prompt for collection: " << collection_name << " with TTL: " << ttl_seconds << " seconds";
    
    // If TTL is 0, bypass the cache
    if (ttl_seconds == 0) {
        LOG(INFO) << "TTL is 0, bypassing schema prompt cache for collection: " << collection_name;
        return generate_schema_prompt(collection_name);
    }
    
    // First, check if we have this prompt in the cache
    {
        std::shared_lock lock(schema_prompts_mutex);
        
        if (schema_prompts.contains(collection_name)) {
            auto cached_entry = schema_prompts.lookup(collection_name);
            
            // Check if the entry is expired
            if (ttl_seconds > 0) {
                auto now = NaturalLanguageSearchModelManager::now();
                auto age_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                    now - cached_entry.created_at).count();
                
                if (age_seconds > ttl_seconds) {
                    return Option<std::string>(404, "Schema prompt for collection `" + collection_name + "` has expired.");
                }
            }
            
            return Option<std::string>(cached_entry.prompt);
        }
    }
    
    LOG(INFO) << "Cache miss for schema prompt, generating for collection: " << collection_name;
    
    return generate_schema_prompt(collection_name);
}

// Helper method to generate a schema prompt and cache it
Option<std::string> NaturalLanguageSearchModelManager::generate_schema_prompt(const std::string& collection_name) {
    // Get the collection from the collection manager
    auto collection = CollectionManager::get_instance().get_collection(collection_name);
    
    if (collection == nullptr) {
        LOG(ERROR) << "Collection not found: " << collection_name;
        return Option<std::string>(404, "Collection not found");
    }
    
    std::string schema_prompt = "You are given the database schema structure below. ";
    schema_prompt += "Your task is to extract relevant SQL-like query parameters from the user's search query.\n\n";
    schema_prompt += "Database Schema:\n";
    schema_prompt += "Table fields are listed in the format: [Field Name] [Data Type] [Is Indexed] [Is Faceted] [Enum Values]\n\n";

    schema_prompt += "| Field Name | Data Type | Is Indexed | Is Faceted | Enum Values |\n";
    schema_prompt += "|------------|-----------|------------|------------|-------------|\n";

    std::string schema_fields = "";
    Collection* coll = collection.get();
    tsl::htrie_map<char, field> search_schema = coll->get_schema();
    
    // Get all faceted fields
    auto facet_fields = coll->get_facet_fields();
    
    // Store facet values for faceted fields
    std::unordered_map<std::string, std::vector<std::string>> field_facet_values;
    
    // Fetch facet values for each facet field by running a search with facets
    for (const auto& facet_field : facet_fields) {
        if (search_schema.count(facet_field) > 0 && 
            (search_schema.at(facet_field).type == field_types::STRING || 
             search_schema.at(facet_field).type == field_types::STRING_ARRAY)) {
            
            LOG(INFO) << "Fetching facet values for field: " << facet_field;
            
            // Create a search query to get facet values
            std::vector<std::string> query_fields = {facet_field};
            std::vector<std::string> facets = {facet_field};
            
            // Execute a wildcard search with facet field
            try {
                auto results = coll->search("*", query_fields, "", facets, {}, {0}, 0, 1, 
                                           FREQUENCY, {false}, 0, {}, {}, 20).get();
                
                if (results.contains("facet_counts") && results["facet_counts"].is_array()) {
                    for (const auto& facet_result : results["facet_counts"]) {
                        if (facet_result["field_name"] == facet_field && 
                            facet_result.contains("counts") && 
                            facet_result["counts"].is_array()) {
                            
                            std::vector<std::string> values;
                            for (const auto& count : facet_result["counts"]) {
                                if (count.contains("value") && count["value"].is_string()) {
                                    values.push_back(count["value"].get<std::string>());
                                }
                            }
                            
                            // Store the values
                            if (!values.empty()) {
                                field_facet_values[facet_field] = values;
                                LOG(INFO) << "Found " << values.size() << " facet values for field: " << facet_field;
                            }
                        }
                    }
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Error fetching facet values for field " << facet_field << ": " << e.what();
            }
        }
    }
    
    // Now build the schema fields with facet values
    for(auto it = search_schema.begin(); it != search_schema.end(); ++it) {
        std::string enum_values = "";
        bool is_faceted = it.value().facet;
        bool is_string_type = (it.value().type == field_types::STRING || 
                              it.value().type == field_types::STRING_ARRAY);
        
        if(is_faceted && is_string_type) {
            // Use actual facet values if available
            const std::string& field_name = it.key();
            auto facet_values_it = field_facet_values.find(field_name);
            
            if (facet_values_it != field_facet_values.end() && !facet_values_it->second.empty()) {
                // We have actual facet values
                enum_values = "[";
                for (size_t i = 0; i < facet_values_it->second.size() && i < 10; i++) {
                    if (i > 0) enum_values += ", ";
                    enum_values += facet_values_it->second[i];
                }
                
                if (facet_values_it->second.size() > 10) {
                    enum_values += ", ...";
                }
                
                enum_values += "]";
            } else {
                // No facet values found, provide a general description
                enum_values = "[Faceted field with unique values]";
            }
        } else {
            enum_values = "N/A";
        }

        schema_fields += "| " + it.key() + " | " + it.value().type + " | " + 
                        (it.value().index ? "Yes" : "No") + " | " + 
                        (it.value().facet ? "Yes" : "No") + " | " + 
                        enum_values + " |\n";
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

    // Log the complete schema prompt for debugging
    LOG(INFO) << "Schema prompt for natural language processing:\n" << schema_prompt;
    
    // Store the generated prompt in the cache
    {
        std::unique_lock lock(schema_prompts_mutex);
        schema_prompts.insert(collection_name, SchemaPromptEntry(schema_prompt));
        LOG(INFO) << "Cached schema prompt for collection: " << collection_name;
    }
    
    return Option<std::string>(schema_prompt);
}

void NaturalLanguageSearchModelManager::clear_schema_prompt(const std::string& collection_name) {
    std::unique_lock lock(schema_prompts_mutex);
    schema_prompts.erase(collection_name);
}

void NaturalLanguageSearchModelManager::clear_all_schema_prompts() {
    std::unique_lock lock(schema_prompts_mutex);
    schema_prompts.clear();
    LOG(INFO) << "Cleared all schema prompts from cache";
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
    
    LOG(INFO) << "NLSM::process_natural_language_query starting - query: " << nl_query 
              << ", collection: " << collection_name 
              << ", model_id: " << nl_model_id
              << ", prompt_cache_ttl: " << prompt_cache_ttl_seconds;
    
    // Get the model to use
    nlohmann::json model_config;
    
    if (!nl_model_id.empty() && nl_model_id != "default") {
        // Use specified model
        LOG(INFO) << "NLSM: Using specified model ID: " << nl_model_id;
        auto model_op = get_model(nl_model_id);
        LOG(INFO) << "NLSM: get_model call complete for ID: " << nl_model_id 
                  << ", success: " << model_op.ok();
        
        if (!model_op.ok()) {
            LOG(ERROR) << "NLSM: Error getting model: " << model_op.error();
            return Option<nlohmann::json>(model_op.code(), 
                                    "Error getting natural language search model: " + model_op.error());
        }
        LOG(INFO) << "NLSM: Successfully retrieved model with ID: " << nl_model_id;
        model_config = model_op.get();
    } else {
        // If default or no model specified, get all models and use the first one
        LOG(INFO) << "NLSM: No specific model ID provided, looking for default model";
        auto models_op = get_all_models();
        LOG(INFO) << "NLSM: get_all_models call complete, success: " << models_op.ok();
        
        if (!models_op.ok() || models_op.get().empty()) {
            LOG(ERROR) << "NLSM: No models found";
            return Option<nlohmann::json>(404, 
                                    "No natural language search models found. Please configure at least one model.");
        }
        LOG(INFO) << "NLSM: Using first available model";
        model_config = models_op.get()[0];
    }
    
    LOG(INFO) << "NLSM: Retrieved model config, model_name: " << model_config["model_name"].get<std::string>();
    
    // Get the schema prompt for the collection with the specified TTL
    LOG(INFO) << "NLSM: Getting schema prompt for collection: " << collection_name;
    auto schema_prompt_op = get_schema_prompt(collection_name, prompt_cache_ttl_seconds);
    LOG(INFO) << "NLSM: get_schema_prompt call complete, success: " << schema_prompt_op.ok();
    
    if (!schema_prompt_op.ok()) {
        LOG(ERROR) << "NLSM: Error generating schema prompt: " << schema_prompt_op.error();
        return Option<nlohmann::json>(schema_prompt_op.code(),
                                "Error generating schema prompt: " + schema_prompt_op.error());
    }
    
    LOG(INFO) << "NLSM: Successfully retrieved schema prompt for collection: " << collection_name;
    
    // Generate search parameters from natural language query
    LOG(INFO) << "NLSM: Calling NaturalLanguageSearchModel::generate_search_params";
    auto params_op = NaturalLanguageSearchModel::generate_search_params(
        nl_query, 
        schema_prompt_op.get(), 
        model_config
    );
    LOG(INFO) << "NLSM: NaturalLanguageSearchModel::generate_search_params call complete, success: " << params_op.ok();
    
    if (!params_op.ok()) {
        LOG(ERROR) << "NLSM: Error generating search parameters: " << params_op.error();
        return Option<nlohmann::json>(params_op.code(), 
                                "Error generating search parameters: " + params_op.error());
    }
    
    LOG(INFO) << "NLSM: Successfully generated search parameters";
    return params_op;
} 