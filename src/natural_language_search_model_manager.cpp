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

Option<uint64_t> NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(std::map<std::string, std::string>& req_params, nlohmann::json& search_obj, uint64_t schema_prompt_ttl_seconds) {
  std::string nl_query;
  bool has_nl_query = false;
  auto start_time = std::chrono::high_resolution_clock::now();

  if(req_params.count("nl_query") != 0) {
      LOG(INFO) << "Found nl_query in URL parameters: " << req_params["nl_query"];
      nl_query = req_params["nl_query"];
      has_nl_query = true;
  }

  if(search_obj.count("nl_query") != 0 && search_obj["nl_query"].is_string()) {
      LOG(INFO) << "Found nl_query in JSON body: " << search_obj["nl_query"].get<std::string>();
      nl_query = search_obj["nl_query"].get<std::string>();
      has_nl_query = true;
  }

  if (has_nl_query) {
      search_obj["_original_nl_query"] = nl_query;
      LOG(INFO) << "Storing original nl_query: " << nl_query;
  }

  if (!has_nl_query) {
      LOG(INFO) << "No nl_query found in either URL parameters or JSON body";
      return Option<uint64_t>(400, "No nl_query found in either URL parameters or JSON body");
  }

  LOG(INFO) << "Starting processing natural language query: " << nl_query;

  // Check for collection_name
  std::string collection_name;
  if(search_obj.count("collection") != 0 && search_obj["collection"].is_string()) {
      collection_name = search_obj["collection"].get<std::string>();
      LOG(INFO) << "Found collection_name in search_obj: " << collection_name;
  } else if(req_params.count("collection") != 0) {
      collection_name = req_params.at("collection");
      LOG(INFO) << "Found collection_name in req_params: " << collection_name;
  } else {
      LOG(ERROR) << "No collection_name found for natural language query";
      if(search_obj.count("error") == 0) {
          search_obj["error"] = "Collection name is required for natural language queries.";
      }
      return Option<uint64_t>(404, "No collection_name found for natural language query");
  }

  LOG(INFO) << "Calling NaturalLanguageSearchModelManager::process_natural_language_query";
  LOG(INFO) << "Parameters - nl_query: " << nl_query << ", collection_name: " << collection_name
            << ", nl_model_id: " << (req_params.count("nl_model_id") ? req_params.at("nl_model_id") : "default");

  auto params_op = process_natural_language_query(
      nl_query,
      collection_name,
      req_params.count("nl_model_id") ? req_params.at("nl_model_id") : "default",
      schema_prompt_ttl_seconds
  );

  LOG(INFO) << "Returned from NaturalLanguageSearchModelManager::process_natural_language_query";
  LOG(INFO) << "Status: " << (params_op.ok() ? "SUCCESS" : "FAILED with code: " + std::to_string(params_op.code()));

  if(!params_op.ok()) {
      LOG(ERROR) << "Error processing natural language query: " << params_op.error();
      if(search_obj.count("error") == 0) {
          search_obj["error"] = params_op.error();
      }

      // Mark that the NL processing failed
      search_obj["_nl_processing_failed"] = true;
      LOG(INFO) << "Marking NL processing as failed";

      // Set a fallback query if one doesn't exist
      if(search_obj.count("q") == 0 && req_params.count("q") == 0) {
          LOG(INFO) << "Setting fallback query parameter from nl_query";
          search_obj["q"] = nl_query;
          search_obj["_fallback_q_used"] = true;
          LOG(INFO) << "Marking that fallback q is being used";
      }

      return Option<uint64_t>(400, "Error processing natural language query");
  }

  LOG(INFO) << "Successfully processed natural language query, extracting search parameters";
  nlohmann::json generated_params = params_op.get();
  // Log the generated parameters
  LOG(INFO) << "Generated search parameters: " << generated_params.dump();

  // Create a list to track which parameters were actually generated by the LLM
  search_obj["_llm_generated_params"] = nlohmann::json::array();

  // Extract and set parameters if provided
  for(auto& param : generated_params.items()) {
      if(param.key() == "q") {
          LOG(INFO) << "Setting generated query parameter: " << param.value().dump();
          search_obj["q"] = param.value();
          // Track that q was generated by the LLM
          search_obj["_llm_generated_params"].push_back("q");
          // Also set the q parameter in the request params to fix the single search case
          LOG(INFO) << "Also setting 'q' in request params for compatibility";
          req_params["q"] = param.value();
      } else if(param.key() == "filter_by") {
          // Store the original LLM-generated filter_by before potentially combining it
          // This ensures we can report exactly what the LLM generated in the response
          search_obj["_original_llm_filter_by"] = param.value();
          LOG(INFO) << "Storing original LLM-generated filter_by: " << param.value().dump();
          // Also store in a field that won't be removed before validation
          search_obj["llm_generated_filter_by"] = param.value();
          LOG(INFO) << "Also storing in non-internal field llm_generated_filter_by for preservation";
          // Track that filter_by was generated by the LLM
          search_obj["_llm_generated_params"].push_back("filter_by");
          LOG(INFO) << "Added 'filter_by' to _llm_generated_params array";

          // Check if filter_by already exists in the search object
          if(search_obj.count("filter_by") != 0 && !search_obj["filter_by"].empty()) {
              std::string existing_filter = search_obj["filter_by"].get<std::string>();
              std::string generated_filter = param.value().get<std::string>();

              // Trim whitespace from both filters
              auto trim = [](std::string& s) {
                  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                      return !std::isspace(ch);
                  }));
                  s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) {
                      return !std::isspace(ch);
                  }).base(), s.end());
              };

              trim(existing_filter);
              trim(generated_filter);

              // Only combine if both filters are non-empty
              if(!existing_filter.empty() && !generated_filter.empty()) {
                  LOG(INFO) << "Concatenating existing filter_by (" << existing_filter
                            << ") with generated filter_by (" << generated_filter << ") using &&";
                  search_obj["filter_by"] = existing_filter + " && " + generated_filter;
              } else if(!existing_filter.empty()) {
                  LOG(INFO) << "Using only existing filter_by as generated filter is empty: " << existing_filter;
                  search_obj["filter_by"] = existing_filter;
              } else if(!generated_filter.empty()) {
                  LOG(INFO) << "Using only generated filter_by as existing filter is empty: " << generated_filter;
                  search_obj["filter_by"] = generated_filter;
              } else {
                  LOG(INFO) << "Both existing and generated filters are empty, setting filter_by to empty string";
                  search_obj["filter_by"] = "";
              }
          } else {
              LOG(INFO) << "Setting generated filter_by parameter: " << param.value().dump();
              search_obj["filter_by"] = param.value();
          }
      } else if(param.key() == "sort_by") {
          LOG(INFO) << "Setting generated sort_by parameter: " << param.value().dump();
          search_obj["sort_by"] = param.value();
          // Track that sort_by was generated by the LLM
          search_obj["_llm_generated_params"].push_back("sort_by");
      } else if(param.key() != "llm_response") {
          LOG(INFO) << "Setting other generated parameter " << param.key() << ": " << param.value().dump();
          search_obj[param.key()] = param.value();
          // Track that this parameter was generated by the LLM
          search_obj["_llm_generated_params"].push_back(param.key());
      }
  }

  // Mark as processed by natural language model
  search_obj["processed_by_nl_model"] = true;

  // Preserve LLM response for debugging if available
  if(generated_params.count("llm_response") != 0) {
      LOG(INFO) << "Preserving LLM response for debugging";
      // Convert the llm_response object to a string to avoid parameter validation issues
      if(generated_params["llm_response"].is_object()) {
          LOG(INFO) << "Converting llm_response object to string to avoid validation issues";
          search_obj["llm_response_str"] = generated_params["llm_response"].dump();
      } else {
          search_obj["llm_response_str"] = generated_params["llm_response"].dump();
      }
      // Keep the original object for internal use, but remove before validation
      search_obj["_llm_response"] = generated_params["llm_response"];

      // Note: The "generated_params" will be constructed in the add_nl_query_data_to_results helper
      // from the q, filter_by, and sort_by fields in search_obj
  }

  LOG(INFO) << "Completed natural language query processing";
  auto end_time = std::chrono::high_resolution_clock::now();
  uint64_t processing_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  LOG(INFO) << "NL query processing took " << processing_time_ms << "ms";
  return Option<uint64_t>(processing_time_ms);
}

// Helper function to build augmented_params from search_obj
nlohmann::json NaturalLanguageSearchModelManager::build_augmented_params(const nlohmann::json& search_obj) {
    LOG(INFO) << "Building augmented_params from search_obj";
    nlohmann::json augmented_params = nlohmann::json::object();

    // Include important search parameters that were actually used for the search
    const std::vector<std::string> important_params = {"q", "filter_by", "sort_by", "query_by", "prefix", "infix",
                                                       "exclude", "include", "limit", "offset", "page"};

    for(const auto& param_name : important_params) {
        if(search_obj.contains(param_name)) {
            augmented_params[param_name] = search_obj[param_name];
            LOG(INFO) << "Including " << param_name << " in augmented_params: " << search_obj[param_name].dump();
        }
    }

    // Include any other non-internal parameters from search_obj
    for(const auto& [key, value] : search_obj.items()) {
        // Skip parameters that are already added or internal (start with _)
        if(!key.empty() && key[0] != '_' &&
           augmented_params.find(key) == augmented_params.end() &&
           key != "error" && key != "processed_by_nl_model" &&
           key != "nl_query" && key != "nl_query_debug" &&
           key != "nl_model_id" && key != "llm_response_str" &&
           key != "llm_generated_filter_by") {

            augmented_params[key] = value;
            LOG(INFO) << "Including additional parameter " << key << " in augmented_params: " << value.dump();
        }
    }

    return augmented_params;
}

void NaturalLanguageSearchModelManager::add_nl_query_data_to_results(nlohmann::json& results_json, const nlohmann::json& search_obj,
                                 const std::map<std::string, std::string>* req_params,
                                 uint64_t nl_processing_time_ms) {

    // Check if search parameters were processed by NL model
    bool has_nl_data = search_obj.contains("processed_by_nl_model") ||
                        search_obj.contains("q") ||
                        search_obj.contains("filter_by") ||
                        search_obj.contains("sort_by") ||
                        search_obj.contains("_llm_response") ||
                        search_obj.contains("llm_response_str") ||
                        search_obj.contains("_original_nl_query");

    if (!has_nl_data) {
        LOG(INFO) << "No NL query data found in search_obj, skipping";
        return;
    }

    // Add nl_query to request_params if it exists
    if (search_obj.contains("_original_nl_query") && search_obj["_original_nl_query"].is_string()) {
        if (!results_json.contains("request_params")) {
            results_json["request_params"] = nlohmann::json::object();
        }
        LOG(INFO) << "Adding nl_query to request_params: " << search_obj["_original_nl_query"].get<std::string>();
        results_json["request_params"]["nl_query"] = search_obj["_original_nl_query"].get<std::string>();
    } else if (req_params != nullptr && req_params->count("nl_query") > 0) {
        if (!results_json.contains("request_params")) {
            results_json["request_params"] = nlohmann::json::object();
        }
        LOG(INFO) << "Adding nl_query from req_params to request_params: " << req_params->at("nl_query");
        results_json["request_params"]["nl_query"] = req_params->at("nl_query");
    } else if (search_obj.contains("nl_query") && search_obj["nl_query"].is_string()) {
        if (!results_json.contains("request_params")) {
            results_json["request_params"] = nlohmann::json::object();
        }
        LOG(INFO) << "Adding nl_query from search_obj to request_params: " << search_obj["nl_query"].get<std::string>();
        results_json["request_params"]["nl_query"] = search_obj["nl_query"].get<std::string>();
    }

    // Create a new parsed_nl_query object to hold all NL-related data
    nlohmann::json parsed_nl_query = nlohmann::json::object();

    // Add the parse_time_ms field to parsed_nl_query
    if (nl_processing_time_ms > 0) {
        parsed_nl_query["parse_time_ms"] = nl_processing_time_ms;
        LOG(INFO) << "Added parse_time_ms: " << nl_processing_time_ms << "ms to parsed_nl_query";
    }

    // Include generated parameters for search
    if(search_obj.contains("processed_by_nl_model")) {
        LOG(INFO) << "Adding generated_params to parsed_nl_query - LLM processing was successful";
        nlohmann::json generated_params = nlohmann::json::object();

        // Only include parameters that were actually generated by the LLM
        if(search_obj.contains("_llm_generated_params") && search_obj["_llm_generated_params"].is_array()) {
            LOG(INFO) << "Using _llm_generated_params to determine which parameters to include";
            LOG(INFO) << "Contents of _llm_generated_params: " << search_obj["_llm_generated_params"].dump();

            for(const auto& param_name : search_obj["_llm_generated_params"]) {
                if(param_name == "q" && search_obj.contains("q")) {
                    generated_params["q"] = search_obj["q"];
                    LOG(INFO) << "Including LLM-generated q parameter";
                } else if(param_name == "filter_by") {
                    // Only include the original LLM-generated filter_by in generated_params, never the combined one
                    if(search_obj.contains("_original_llm_filter_by")) {
                        generated_params["filter_by"] = search_obj["_original_llm_filter_by"];
                        LOG(INFO) << "Including LLM-generated filter_by parameter from _original_llm_filter_by";
                    } else if(search_obj.contains("llm_generated_filter_by")) {
                        // Use the preserved non-internal field if the internal one was removed
                        generated_params["filter_by"] = search_obj["llm_generated_filter_by"];
                        LOG(INFO) << "Including LLM-generated filter_by parameter from llm_generated_filter_by";
                    } else {
                        LOG(ERROR) << "Missing _original_llm_filter_by and llm_generated_filter_by even though filter_by is in _llm_generated_params";
                    }
                    // Deliberately not falling back to filter_by as that may contain user-provided filters
                } else if(param_name == "sort_by" && search_obj.contains("sort_by")) {
                    generated_params["sort_by"] = search_obj["sort_by"];
                    LOG(INFO) << "Including LLM-generated sort_by parameter";
                } else if(search_obj.contains(param_name)) {
                    // Include any other parameters that were generated by the LLM
                    generated_params[param_name] = search_obj[param_name];
                    LOG(INFO) << "Including other LLM-generated parameter: " << param_name.get<std::string>();
                }
            }
        } else {
            // Fallback to previous logic for backward compatibility
            LOG(INFO) << "No _llm_generated_params found, using fallback logic";

            if(search_obj.contains("q") && !search_obj.contains("_fallback_q_used")) {
                generated_params["q"] = search_obj["q"];
                LOG(INFO) << "Including q parameter (fallback logic)";
            }

            // Only include original LLM-generated filter_by in generated_params
            if(search_obj.contains("_original_llm_filter_by")) {
                generated_params["filter_by"] = search_obj["_original_llm_filter_by"];
                LOG(INFO) << "Including filter_by parameter from _original_llm_filter_by (fallback logic)";
            } else if(search_obj.contains("llm_generated_filter_by")) {
                // Use the preserved non-internal field if the internal one was removed
                generated_params["filter_by"] = search_obj["llm_generated_filter_by"];
                LOG(INFO) << "Including filter_by parameter from llm_generated_filter_by (fallback logic)";
            } else if(search_obj.contains("filter_by") && search_obj.contains("_llm_generated_params")) {
                // If we have filter_by in _llm_generated_params but no _original_llm_filter_by,
                // we'll use the filter_by value directly as a last resort
                for(const auto& param : search_obj["_llm_generated_params"]) {
                    if(param == "filter_by") {
                        generated_params["filter_by"] = search_obj["filter_by"];
                        LOG(INFO) << "Including filter_by parameter directly as last resort (fallback logic)";
                        break;
                    }
                }
            } else {
                LOG(INFO) << "No filter_by found to include in generated_params (fallback logic)";
            }

            if(search_obj.contains("sort_by")) {
                generated_params["sort_by"] = search_obj["sort_by"];
                LOG(INFO) << "Including sort_by parameter (fallback logic)";
            }
        }

        parsed_nl_query["generated_params"] = generated_params;

        // Add augmented_params containing the final set of search parameters after all combinations and overrides
        parsed_nl_query["augmented_params"] = NaturalLanguageSearchModelManager::build_augmented_params(search_obj);
    } else if(search_obj.contains("_nl_processing_failed")) {
        LOG(INFO) << "LLM processing failed, adding empty generated_params";
        parsed_nl_query["generated_params"] = nlohmann::json::object();

        // Include error message if available
        if(search_obj.contains("error")) {
            parsed_nl_query["error"] = search_obj["error"];
            LOG(INFO) << "Adding error message to parsed_nl_query: " << search_obj["error"].get<std::string>();
        }

        // Add augmented_params even in failure case
        parsed_nl_query["augmented_params"] = NaturalLanguageSearchModelManager::build_augmented_params(search_obj);
    } else {
        // For backward compatibility, still include generated_params in other cases
        // but it should be empty in error cases
        LOG(INFO) << "Adding default generated_params structure";
        parsed_nl_query["generated_params"] = nlohmann::json::object();

        // Add augmented_params in default case as well
        parsed_nl_query["augmented_params"] = NaturalLanguageSearchModelManager::build_augmented_params(search_obj);
    }

    // Check if debug mode is enabled (from search_obj or req_params)
    bool debug_mode = false;

    // Check in search_obj
    if (search_obj.contains("nl_query_debug") &&
        ((search_obj["nl_query_debug"].is_boolean() && search_obj["nl_query_debug"].get<bool>()) ||
         (search_obj["nl_query_debug"].is_string() &&
          (search_obj["nl_query_debug"].get<std::string>() == "true" ||
           search_obj["nl_query_debug"].get<std::string>() == "1")))) {
        debug_mode = true;
        LOG(INFO) << "NL query debug mode is enabled from search_obj";
    }

    // Also check in req_params if provided
    if (!debug_mode && req_params != nullptr && req_params->count("nl_query_debug") > 0) {
        const std::string& debug_val = req_params->at("nl_query_debug");
        if (debug_val == "true" || debug_val == "1") {
            debug_mode = true;
            LOG(INFO) << "NL query debug mode is enabled from req_params";
        }
    }

    // Include LLM response data only in debug mode
    if (debug_mode) {
        if(search_obj.contains("_llm_response")) {
            LOG(INFO) << "Including _llm_response in parsed_nl_query for debugging";
            parsed_nl_query["llm_response"] = search_obj["_llm_response"];
        } else if(search_obj.contains("llm_response_str")) {
            LOG(INFO) << "Including llm_response_str in parsed_nl_query for debugging";
            // Try to parse the string back to JSON
            try {
                parsed_nl_query["llm_response"] = nlohmann::json::parse(search_obj["llm_response_str"].get<std::string>());
                LOG(INFO) << "Successfully parsed llm_response_str as JSON";
            } catch(const std::exception& e) {
                LOG(INFO) << "Could not parse llm_response_str as JSON, using raw string";
                parsed_nl_query["llm_response_str"] = search_obj["llm_response_str"];
            }
        }
    }

    // Add parsed_nl_query to results if it has any data
    if (!parsed_nl_query.empty()) {
        results_json["parsed_nl_query"] = parsed_nl_query;
        LOG(INFO) << "Added parsed_nl_query to results: " << parsed_nl_query.dump();
    }

    LOG(INFO) << "Completed adding NL query data to results";
}

void NaturalLanguageSearchModelManager::dispose() {
    std::unique_lock lock(models_mutex);
    models.clear();
    schema_prompts.clear();
}