#include <store.h>
#include "field.h"
#include "magic_enum.hpp"
#include "embedder_manager.h"
#include <stack>
#include <collection_manager.h>
#include <regex>


Option<bool> field::json_field_to_field(bool enable_nested_fields, nlohmann::json& field_json,
                                        std::vector<field>& the_fields,
                                        string& fallback_field_type, size_t& num_auto_detect_fields) {

    if(field_json["name"] == "id") {
        // No field should exist with the name "id" as it is reserved for internal use
        // We cannot throw an error here anymore since that will break backward compatibility!
        LOG(WARNING) << "Collection schema cannot contain a field with name `id`. Ignoring field.";
        return Option<bool>(true);
    }

    if(!field_json.is_object() ||
       field_json.count(fields::name) == 0 || field_json.count(fields::type) == 0 ||
       !field_json.at(fields::name).is_string() || !field_json.at(fields::type).is_string()) {

        return Option<bool>(400, "Wrong format for `fields`. It should be an array of objects containing "
                                 "`name`, `type`, `optional` and `facet` properties.");
    }

    if(field_json.count("store") != 0 && !field_json.at("store").is_boolean()) {
        return Option<bool>(400, std::string("The `store` property of the field `") +
                                 field_json[fields::name].get<std::string>() + std::string("` should be a boolean."));
    }

    if(field_json.count("drop") != 0) {
        return Option<bool>(400, std::string("Invalid property `drop` on field `") +
                                 field_json[fields::name].get<std::string>() + std::string("`: it is allowed only "
                                                                                           "during schema update."));
    }

    if(field_json.count(fields::facet) != 0 && !field_json.at(fields::facet).is_boolean()) {
        return Option<bool>(400, std::string("The `facet` property of the field `") +
                                 field_json[fields::name].get<std::string>() + std::string("` should be a boolean."));
    }

    if(field_json.count(fields::optional) != 0 && !field_json.at(fields::optional).is_boolean()) {
        return Option<bool>(400, std::string("The `optional` property of the field `") +
                                 field_json[fields::name].get<std::string>() + std::string("` should be a boolean."));
    }

    if(field_json.count(fields::index) != 0 && !field_json.at(fields::index).is_boolean()) {
        return Option<bool>(400, std::string("The `index` property of the field `") +
                                 field_json[fields::name].get<std::string>() + std::string("` should be a boolean."));
    }

    if(field_json.count(fields::sort) != 0 && !field_json.at(fields::sort).is_boolean()) {
        return Option<bool>(400, std::string("The `sort` property of the field `") +
                                 field_json[fields::name].get<std::string>() + std::string("` should be a boolean."));
    }

    if(field_json.count(fields::infix) != 0 && !field_json.at(fields::infix).is_boolean()) {
        return Option<bool>(400, std::string("The `infix` property of the field `") +
                                 field_json[fields::name].get<std::string>() + std::string("` should be a boolean."));
    }

    if(field_json.count(fields::locale) != 0){
        if(!field_json.at(fields::locale).is_string()) {
            return Option<bool>(400, std::string("The `locale` property of the field `") +
                                     field_json[fields::name].get<std::string>() + std::string("` should be a string."));
        }

        if(!field_json[fields::locale].get<std::string>().empty() &&
           field_json[fields::locale].get<std::string>().size() != 2) {
            return Option<bool>(400, std::string("The `locale` value of the field `") +
                                     field_json[fields::name].get<std::string>() + std::string("` is not valid."));
        }
    }

    if (field_json.count(fields::reference) != 0 && !field_json.at(fields::reference).is_string()) {
        return Option<bool>(400, "Reference should be a string.");
    } else if (field_json.count(fields::reference) == 0) {
        field_json[fields::reference] = "";
    }

    if (field_json.count(fields::async_reference) == 0) {
        field_json[fields::async_reference] = false;
    } else if (!field_json.at(fields::async_reference).is_boolean()) {
        return Option<bool>(400, std::string("The `async_reference` property of the field `") +
                                 field_json[fields::name].get<std::string>() + std::string("` should be a boolean."));
    } else if (field_json[fields::async_reference].get<bool>() &&
                field_json[fields::reference].get<std::string>().empty()) {
        return Option<bool>(400, std::string("The `async_reference` property of the field `") +
                                 field_json[fields::name].get<std::string>() + std::string("` is only applicable if "
                                                                                           "`reference` is specified."));
    }

    if(field_json.count(fields::stem) != 0) {
        if(!field_json.at(fields::stem).is_boolean()) {
            return Option<bool>(400, std::string("The `stem` property of the field `") +
                                     field_json[fields::name].get<std::string>() + std::string("` should be a boolean."));
        }

        if(field_json[fields::stem] && field_json[fields::type] != field_types::STRING && field_json[fields::type] != field_types::STRING_ARRAY) {
            return Option<bool>(400, std::string("The `stem` property is only allowed for string and string[] fields."));
        }

        if(field_json[fields::stem].get<bool>()) {
            std::string locale;
            if(field_json.count(fields::locale) != 0) {
                locale = field_json[fields::locale].get<std::string>();
            }
            auto stem_validation = StemmerManager::get_instance().validate_language(locale);
            if(!stem_validation) {
                return Option<bool>(400, std::string("The `locale` value of the field `") +
                                         field_json[fields::name].get<std::string>() + std::string("` is not supported for stem."));
            }
        }
    } else {
        field_json[fields::stem] = false;
    }

    if (field_json.count(fields::range_index) != 0) {
        if (!field_json.at(fields::range_index).is_boolean()) {
            return Option<bool>(400, std::string("The `range_index` property of the field `") +
                                     field_json[fields::name].get<std::string>() +
                                     std::string("` should be a boolean."));
        }

        auto const& type = field_json["type"];
        if (field_json[fields::range_index] &&
            type != field_types::INT32 && type != field_types::INT32_ARRAY &&
            type != field_types::INT64 && type != field_types::INT64_ARRAY &&
            type != field_types::FLOAT && type != field_types::FLOAT_ARRAY) {
            return Option<bool>(400, std::string("The `range_index` property is only allowed for the numerical fields`"));
        }
    } else {
        field_json[fields::range_index] = false;
    }

    if(field_json["name"] == ".*") {
        if(field_json.count(fields::facet) == 0) {
            field_json[fields::facet] = false;
        }

        if(field_json.count(fields::optional) == 0) {
            field_json[fields::optional] = true;
        }

        if(field_json.count(fields::index) == 0) {
            field_json[fields::index] = true;
        }

        if(field_json.count(fields::locale) == 0) {
            field_json[fields::locale] = "";
        }

        if(field_json.count(fields::sort) == 0) {
            field_json[fields::sort] = false;
        }

        if(field_json.count(fields::infix) == 0) {
            field_json[fields::infix] = false;
        }

        if(field_json[fields::optional] == false) {
            return Option<bool>(400, "Field `.*` must be an optional field.");
        }

        if(field_json[fields::facet] == true) {
            return Option<bool>(400, "Field `.*` cannot be a facet field.");
        }

        if(field_json[fields::index] == false) {
            return Option<bool>(400, "Field `.*` must be an index field.");
        }

        if (!field_json[fields::reference].get<std::string>().empty()) {
            return Option<bool>(400, "Field `.*` cannot be a reference field.");
        }

        field fallback_field(field_json["name"], field_json["type"], field_json["facet"],
                             field_json["optional"], field_json[fields::index], field_json[fields::locale],
                             field_json[fields::sort], field_json[fields::infix]);

        if(fallback_field.has_valid_type()) {
            fallback_field_type = fallback_field.type;
            num_auto_detect_fields++;
        } else {
            return Option<bool>(400, "The `type` of field `.*` is invalid.");
        }

        the_fields.emplace_back(fallback_field);
        return Option<bool>(true);
    }

    if(field_json.count(fields::facet) == 0) {
        field_json[fields::facet] = false;
    }

    if(field_json.count(fields::index) == 0) {
        field_json[fields::index] = true;
    }

    if(field_json.count(fields::locale) == 0) {
        field_json[fields::locale] = "";
    }

    if(field_json.count(fields::store) == 0) {
        field_json[fields::store] = true;
    }

    if(field_json.count(fields::sort) == 0) {
        if(field_json["type"] == field_types::INT32 || field_json["type"] == field_types::INT64 ||
           field_json["type"] == field_types::FLOAT || field_json["type"] == field_types::BOOL ||
           field_json["type"] == field_types::GEOPOINT || field_json["type"] == field_types::GEOPOINT_ARRAY) {
            if((field_json.count(fields::num_dim) == 0) || (field_json[fields::facet])) {
                field_json[fields::sort] = true;
            } else {
                field_json[fields::sort] = false;
            }
        } else {
            field_json[fields::sort] = false;
        }
    } else if (!field_json[fields::sort].get<bool>() &&
                (field_json["type"] == field_types::GEOPOINT || field_json["type"] == field_types::GEOPOINT_ARRAY)) {
        return Option<bool>(400, std::string("The `sort` property of the field `") +=
                                 field_json[fields::name].get<std::string>() += "` having `" + field_json["type"].get<std::string>() +=
                                 "` type cannot be `false`. The sort index is used during GeoSearch.");
    }

    if(field_json.count(fields::infix) == 0) {
        field_json[fields::infix] = false;
    }

    if(field_json[fields::type] == field_types::OBJECT || field_json[fields::type] == field_types::OBJECT_ARRAY) {
        if(!enable_nested_fields) {
            return Option<bool>(400, "Type `object` or `object[]` can be used only when nested fields are enabled by "
                                     "setting` enable_nested_fields` to true.");
        }
    }

    if(field_json.count(fields::embed) != 0) {
        if(!field_json[fields::embed].is_object()) {
            return Option<bool>(400, "Property `" + fields::embed + "` must be an object.");
        }

        auto& embed_json = field_json[fields::embed];

        if(field_json[fields::embed].count(fields::from) == 0) {
            return Option<bool>(400, "Property `" + fields::embed + "` must contain a `" + fields::from + "` property.");
        }

        if(!field_json[fields::embed][fields::from].is_array()) {
            return Option<bool>(400, "Property `" + fields::embed + "." + fields::from + "` must be an array.");
        }

        if(field_json[fields::embed][fields::from].empty()) {
            return Option<bool>(400, "Property `" + fields::embed + "." + fields::from + "` must have at least one element.");
        }

        if(embed_json.count(fields::model_config) == 0) {
            return Option<bool>(400, "Property `" + fields::embed + "." + fields::model_config + "` not found.");
        }

        auto& model_config = embed_json[fields::model_config];

        if(model_config.count(fields::model_name) == 0) {
            return Option<bool>(400, "Property `" + fields::embed + "." + fields::model_config + "." + fields::model_name + "`not found");
        }

        if(!model_config[fields::model_name].is_string()) {
            return Option<bool>(400, "Property `" + fields::embed + "."  + fields::model_config + "." + fields::model_name + "` must be a string.");
        }

        if(model_config[fields::model_name].get<std::string>().empty()) {
            return Option<bool>(400, "Property `" + fields::embed + "." + fields::model_config + "." + fields::model_name + "` cannot be empty.");
        }

        if(model_config.count(fields::indexing_prefix) != 0) {
            if(!model_config[fields::indexing_prefix].is_string()) {
                return Option<bool>(400, "Property `" + fields::embed + "." + fields::model_config + "." + fields::indexing_prefix + "` must be a string.");
            }
        }

        if(model_config.count(fields::query_prefix) != 0) {
            if(!model_config[fields::query_prefix].is_string()) {
                return Option<bool>(400, "Property `" + fields::embed + "." + fields::model_config + "." + fields::query_prefix + "` must be a string.");
            }
        }

        for(auto& embed_from_field : field_json[fields::embed][fields::from]) {
            if(!embed_from_field.is_string()) {
                return Option<bool>(400, "Property `" + fields::embed + "." + fields::from + "` must contain only field names as strings.");
            }
        }
    }

    auto DEFAULT_VEC_DIST_METRIC = magic_enum::enum_name(vector_distance_type_t::cosine);

    if(field_json.count(fields::num_dim) == 0) {
        field_json[fields::num_dim] = 0;
        field_json[fields::vec_dist] = DEFAULT_VEC_DIST_METRIC;
    } else {
        if(!field_json[fields::num_dim].is_number_unsigned() || field_json[fields::num_dim] == 0) {
            return Option<bool>(400, "Property `" + fields::num_dim + "` must be a positive integer.");
        }

        if(field_json[fields::type] != field_types::FLOAT_ARRAY) {
            return Option<bool>(400, "Property `" + fields::num_dim + "` is only allowed on a float array field.");
        }

        if(field_json[fields::facet].get<bool>()) {
            return Option<bool>(400, "Property `" + fields::facet + "` is not allowed on a vector field.");
        }

        if(field_json[fields::sort].get<bool>()) {
            return Option<bool>(400, "Property `" + fields::sort + "` cannot be enabled on a vector field.");
        }

        if(field_json.count(fields::vec_dist) == 0) {
            field_json[fields::vec_dist] = DEFAULT_VEC_DIST_METRIC;
        } else {
            if(!field_json[fields::vec_dist].is_string()) {
                return Option<bool>(400, "Property `" + fields::vec_dist + "` must be a string.");
            }

            auto vec_dist_op = magic_enum::enum_cast<vector_distance_type_t>(field_json[fields::vec_dist].get<std::string>());
            if(!vec_dist_op.has_value()) {
                return Option<bool>(400, "Property `" + fields::vec_dist + "` is invalid.");
            }
        }
    }

    if(field_json.count(fields::hnsw_params) != 0) {
        if(!field_json[fields::hnsw_params].is_object()) {
            return Option<bool>(400, "Property `" + fields::hnsw_params + "` must be an object.");
        }

        if(field_json[fields::hnsw_params].count("ef_construction") != 0 &&
           (!field_json[fields::hnsw_params]["ef_construction"].is_number_unsigned() ||
            field_json[fields::hnsw_params]["ef_construction"] == 0)) {
            return Option<bool>(400, "Property `" + fields::hnsw_params + ".ef_construction` must be a positive integer.");
        }

        if(field_json[fields::hnsw_params].count("M") != 0 &&
           (!field_json[fields::hnsw_params]["M"].is_number_unsigned() ||
            field_json[fields::hnsw_params]["M"] == 0)) {
            return Option<bool>(400, "Property `" + fields::hnsw_params + ".M` must be a positive integer.");
        }

        // remove unrelated properties except for m ef_construction and M
        auto it = field_json[fields::hnsw_params].begin();
        while(it != field_json[fields::hnsw_params].end()) {
            if(it.key() != "max_elements" && it.key() != "ef_construction" && it.key() != "M" && it.key() != "ef") {
                it = field_json[fields::hnsw_params].erase(it);
            } else {
                ++it;
            }
        }

        if(field_json[fields::hnsw_params].count("ef_construction") == 0) {
            field_json[fields::hnsw_params]["ef_construction"] = 200;
        }

        if(field_json[fields::hnsw_params].count("M") == 0) {
            field_json[fields::hnsw_params]["M"] = 16;
        }
    } else {
        field_json[fields::hnsw_params] = R"({
                                            "M": 16,
                                            "ef_construction": 200
                                        })"_json;
    }

    if(field_json.count(fields::optional) == 0) {
        // dynamic type fields are always optional
        bool is_dynamic = field::is_dynamic(field_json[fields::name], field_json[fields::type]);
        field_json[fields::optional] = is_dynamic;
    }

    bool is_obj = field_json[fields::type] == field_types::OBJECT || field_json[fields::type] == field_types::OBJECT_ARRAY;
    bool is_regexp_name = field_json[fields::name].get<std::string>().find(".*") != std::string::npos;

    if (is_regexp_name && !field_json[fields::reference].get<std::string>().empty()) {
        return Option<bool>(400, "Wildcard field cannot have a reference.");
    }

    if(is_obj || (!is_regexp_name && enable_nested_fields &&
                   field_json[fields::name].get<std::string>().find('.') != std::string::npos)) {
        field_json[fields::nested] = true;
        field_json[fields::nested_array] = field::VAL_UNKNOWN;  // unknown, will be resolved during read
    } else {
        field_json[fields::nested] = false;
        field_json[fields::nested_array] = 0;
    }

    if(field_json[fields::type] == field_types::GEOPOINT && field_json[fields::sort] == false) {
        LOG(WARNING) << "Forcing geopoint field `" << field_json[fields::name].get<std::string>() << "` to be sortable.";
        field_json[fields::sort] = true;
    }

    auto vec_dist = magic_enum::enum_cast<vector_distance_type_t>(field_json[fields::vec_dist].get<std::string>()).value();

    if (!field_json[fields::reference].get<std::string>().empty()) {
        std::vector<std::string> tokens;
        StringUtils::split(field_json[fields::reference].get<std::string>(), tokens, ".");

        if (tokens.size() < 2) {
            return Option<bool>(400, "Invalid reference `" + field_json[fields::reference].get<std::string>()  + "`.");
        }

        tokens.clear();
        StringUtils::split(field_json[fields::name].get<std::string>(), tokens, ".");

        if (tokens.size() > 2) {
            return Option<bool>(400, "`" + field_json[fields::name].get<std::string>() + "` field cannot have a reference."
                                        " Only the top-level field of an object is allowed.");
        }
    }

    the_fields.emplace_back(
            field(field_json[fields::name], field_json[fields::type], field_json[fields::facet],
                  field_json[fields::optional], field_json[fields::index], field_json[fields::locale],
                  field_json[fields::sort], field_json[fields::infix], field_json[fields::nested],
                  field_json[fields::nested_array], field_json[fields::num_dim], vec_dist,
                  field_json[fields::reference], field_json[fields::embed], field_json[fields::range_index], 
                  field_json[fields::store], field_json[fields::stem], field_json[fields::hnsw_params],
                  field_json[fields::async_reference])
    );

    if (!field_json[fields::reference].get<std::string>().empty()) {
        // Add a reference helper field in the schema. It stores the doc id of the document it references to reduce the
        // computation while searching.
        auto f = field(field_json[fields::name].get<std::string>() + fields::REFERENCE_HELPER_FIELD_SUFFIX,
                       field_types::is_array(field_json[fields::type].get<std::string>()) ? field_types::INT64_ARRAY : field_types::INT64,
                       false, field_json[fields::optional], true);
        f.nested = field_json[fields::nested];
        the_fields.emplace_back(std::move(f));
    }

    return Option<bool>(true);
}

bool field::flatten_obj(nlohmann::json& doc, nlohmann::json& value, bool has_array, bool has_obj_array,
                        bool is_update, const field& the_field, const std::string& flat_name,
                        const std::unordered_map<std::string, field>& dyn_fields,
                        std::unordered_map<std::string, field>& flattened_fields) {
    if(value.is_object()) {
        has_obj_array = has_array;
        auto it = value.begin();
        while(it != value.end()) {
            const std::string& child_field_name = flat_name + "." + it.key();
            if(it.value().is_null()) {
                if(!has_array) {
                    // we don't want to push null values into an array because that's not valid
                    doc[child_field_name] = nullptr;
                }

                field flattened_field;
                flattened_field.name = child_field_name;
                flattened_field.type = field_types::NIL;
                flattened_fields[child_field_name] = flattened_field;

                if(!is_update) {
                    // update code path requires and takes care of null values
                    it = value.erase(it);
                } else {
                    it++;
                }
            } else {
                flatten_obj(doc, it.value(), has_array, has_obj_array, is_update, the_field, child_field_name,
                            dyn_fields, flattened_fields);
                it++;
            }
        }
    } else if(value.is_array()) {
        for(const auto& kv: value.items()) {
            flatten_obj(doc, kv.value(), true, has_obj_array, is_update, the_field, flat_name, dyn_fields, flattened_fields);
        }
    } else { // must be a primitive
        if(doc.count(flat_name) != 0 && flattened_fields.find(flat_name) == flattened_fields.end()) {
            return true;
        }

        std::string detected_type;
        bool found_dynamic_field = false;
        field dyn_field(the_field.name, field_types::STRING, false);

        for(auto dyn_field_it = dyn_fields.begin(); dyn_field_it != dyn_fields.end(); dyn_field_it++) {
            auto& dynamic_field = dyn_field_it->second;

            if(dynamic_field.is_auto() || dynamic_field.is_string_star()) {
                continue;
            }

            if(std::regex_match(flat_name, std::regex(dynamic_field.name))) {
                detected_type = dynamic_field.type;
                found_dynamic_field = true;
                dyn_field = dynamic_field;
                break;
            }
        }

        if(!found_dynamic_field) {
            if(!field::get_type(value, detected_type)) {
                return false;
            }

            if(std::isalnum(detected_type.back()) && has_array) {
                // convert singular type to multi valued type
                detected_type += "[]";
            }
        }

        if(has_array) {
            doc[flat_name].push_back(value);
        } else {
            doc[flat_name] = value;
        }

        field flattened_field = found_dynamic_field ? dyn_field : the_field;
        flattened_field.name = flat_name;
        flattened_field.type = detected_type;
        flattened_field.optional = true;
        flattened_field.nested = true;
        flattened_field.nested_array = has_obj_array;
        int sort_op = flattened_field.sort ? 1 : -1;
        int infix_op = flattened_field.infix ? 1 : -1;
        flattened_field.set_computed_defaults(sort_op, infix_op);
        flattened_fields[flat_name] = flattened_field;
    }

    return true;
}

Option<bool> field::flatten_field(nlohmann::json& doc, nlohmann::json& obj, const field& the_field,
                                  std::vector<std::string>& path_parts, size_t path_index,
                                  bool has_array, bool has_obj_array, bool is_update,
                                  const std::unordered_map<std::string, field>& dyn_fields,
                                  std::unordered_map<std::string, field>& flattened_fields) {
    if(path_index == path_parts.size()) {
        // end of path: check if obj matches expected type
        std::string detected_type;
        bool found_dynamic_field = false;

        for(auto dyn_field_it = dyn_fields.begin(); dyn_field_it != dyn_fields.end(); dyn_field_it++) {
            auto& dynamic_field = dyn_field_it->second;

            if(dynamic_field.is_auto() || dynamic_field.is_string_star()) {
                continue;
            }

            if(std::regex_match(the_field.name, std::regex(dynamic_field.name))) {
                detected_type = obj.is_object() ? field_types::OBJECT : dynamic_field.type;
                found_dynamic_field = true;
                break;
            }
        }

        if(!found_dynamic_field) {
            if(!field::get_type(obj, detected_type)) {
                if(obj.is_null() && the_field.optional) {
                    // null values are allowed only if field is optional
                    return Option<bool>(true);
                }

                return Option<bool>(400, "Field `" + the_field.name + "` has an incorrect type.");
            }

            if(std::isalnum(detected_type.back()) && has_array) {
                // convert singular type to multi valued type
                detected_type += "[]";
            }
        }

        has_obj_array = has_obj_array || ((detected_type == field_types::OBJECT) && has_array);

        // handle differences in detection of numerical types
        bool is_numericaly_valid = (detected_type != the_field.type) &&
            ( (detected_type == field_types::INT64 &&
                (the_field.type == field_types::INT32 || the_field.type == field_types::FLOAT)) ||

              (detected_type == field_types::INT64_ARRAY &&
                (the_field.type == field_types::INT32_ARRAY || the_field.type == field_types::FLOAT_ARRAY)) ||

              (detected_type == field_types::FLOAT_ARRAY && the_field.type == field_types::GEOPOINT_ARRAY) ||

              (detected_type == field_types::FLOAT_ARRAY && the_field.type == field_types::GEOPOINT && !has_obj_array) ||

              (detected_type == field_types::INT64_ARRAY && the_field.type == field_types::GEOPOINT && !has_obj_array) ||

              (detected_type == field_types::INT64_ARRAY && the_field.type == field_types::GEOPOINT_ARRAY)
           );

        if(detected_type == the_field.type || is_numericaly_valid) {
            if(the_field.is_object()) {
                flatten_obj(doc, obj, has_array, has_obj_array, is_update, the_field, the_field.name,
                            dyn_fields, flattened_fields);
            } else {
                if(doc.count(the_field.name) != 0 && flattened_fields.find(the_field.name) == flattened_fields.end()) {
                    return Option<bool>(true);
                }

                if(has_array) {
                    doc[the_field.name].push_back(obj);
                } else {
                    doc[the_field.name] = obj;
                }

                field flattened_field = the_field;
                flattened_field.type = detected_type;
                flattened_field.nested = (path_index > 1);
                flattened_field.nested_array = has_obj_array;
                flattened_fields[the_field.name] = flattened_field;
            }

            return Option<bool>(true);
        } else {
            if(has_obj_array && !the_field.is_array()) {
                return Option<bool>(400, "Field `" + the_field.name + "` has an incorrect type. "
                                                                      "Hint: field inside an array of objects must be an array type as well.");
            }

            return Option<bool>(400, "Field `" + the_field.name + "` has an incorrect type.");
        }
    }

    const std::string& fragment = path_parts[path_index];
    const auto& it = obj.find(fragment);

    if(it != obj.end()) {
        if(it.value().is_array()) {
            if(it.value().empty()) {
                return Option<bool>(404, "Field `" + the_field.name + "` not found.");
            }

            has_array = true;
            for(auto& ele: it.value()) {
                has_obj_array = has_obj_array || ele.is_object();
                Option<bool> op = flatten_field(doc, ele, the_field, path_parts, path_index + 1, has_array,
                                                has_obj_array, is_update, dyn_fields, flattened_fields);
                if(!op.ok()) {
                    return op;
                }
            }
            return Option<bool>(true);
        } else {
            return flatten_field(doc, it.value(), the_field, path_parts, path_index + 1, has_array, has_obj_array,
                                 is_update, dyn_fields, flattened_fields);
        }
    } else if(!the_field.optional) {
        return Option<bool>(404, "Field `" + the_field.name + "` not found.");
    }

    return Option<bool>(true);
}

Option<bool> field::flatten_doc(nlohmann::json& document,
                                const tsl::htrie_map<char, field>& nested_fields,
                                const std::unordered_map<std::string, field>& dyn_fields,
                                bool is_update, std::vector<field>& flattened_fields) {

    std::unordered_map<std::string, field> flattened_fields_map;

    for(auto& nested_field: nested_fields) {
        if(!nested_field.index) {
            continue;
        }

        std::vector<std::string> field_parts;
        StringUtils::split(nested_field.name, field_parts, ".");

        if(field_parts.size() > 1 && document.count(nested_field.name) != 0) {
            // skip explicitly present nested fields
            continue;
        }

        auto op = flatten_field(document, document, nested_field, field_parts, 0, false, false,
                                is_update, dyn_fields, flattened_fields_map);
        if(op.ok()) {
            continue;
        }

        if(op.code() == 404 && (is_update || nested_field.optional)) {
            continue;
        } else {
            return op;
        }
    }

    document[".flat"] = nlohmann::json::array();
    for(auto& kv: flattened_fields_map) {
        document[".flat"].push_back(kv.second.name);
        if(kv.second.type != field_types::NIL) {
            // not a real field so we won't add it
            flattened_fields.push_back(kv.second);
        }
    }

    return Option<bool>(true);
}

void field::compact_nested_fields(tsl::htrie_map<char, field>& nested_fields) {
    std::vector<std::string> nested_fields_vec;
    for(const auto& f: nested_fields) {
        nested_fields_vec.push_back(f.name);
    }

    for(auto& field_name: nested_fields_vec) {
        nested_fields.erase_prefix(field_name + ".");
    }
}

Option<bool> field::json_fields_to_fields(bool enable_nested_fields, nlohmann::json &fields_json, string &fallback_field_type,
                                          std::vector<field>& the_fields) {
    size_t num_auto_detect_fields = 0;
    const tsl::htrie_map<char, field> dummy_search_schema;

    for(size_t i = 0; i < fields_json.size(); i++) {
        nlohmann::json& field_json = fields_json[i];
        auto op = json_field_to_field(enable_nested_fields,
                                      field_json, the_fields, fallback_field_type, num_auto_detect_fields);
        if(!op.ok()) {
            return op;
        }

        if(!the_fields.empty() && !the_fields.back().embed.empty()) {
            auto validate_res = validate_and_init_embed_field(dummy_search_schema, field_json, fields_json, the_fields.back());
            if(!validate_res.ok()) {
                return validate_res;
            }
        }
    }

    if(num_auto_detect_fields > 1) {
        return Option<bool>(400,"There can be only one field named `.*`.");
    }

    return Option<bool>(true);
}

Option<bool> field::validate_and_init_embed_field(const tsl::htrie_map<char, field>& search_schema, nlohmann::json& field_json,
                                                  const nlohmann::json& fields_json,
                                                  field& the_field) {
    const std::string err_msg = "Property `" + fields::embed + "." + fields::from +
                                    "` can only refer to string, string array or image (for supported models) fields.";

    bool found_image_field = false;
    for(auto& field_name : field_json[fields::embed][fields::from].get<std::vector<std::string>>()) {

        auto embed_field = std::find_if(fields_json.begin(), fields_json.end(), [&field_name](const nlohmann::json& x) {
            return x["name"].get<std::string>() == field_name;
        });


        if(embed_field == fields_json.end()) {
            const auto& embed_field2 = search_schema.find(field_name);
            if (embed_field2 == search_schema.end()) {
                return Option<bool>(400, err_msg);
            } else if (embed_field2->type != field_types::STRING && embed_field2->type != field_types::STRING_ARRAY && embed_field2->type != field_types::IMAGE) {
                return Option<bool>(400, err_msg);
            }
        } else if((*embed_field)[fields::type] != field_types::STRING &&
                  (*embed_field)[fields::type] != field_types::STRING_ARRAY &&
                    (*embed_field)[fields::type] != field_types::IMAGE) {
            return Option<bool>(400, err_msg);
        }
    }

    const auto& model_config = field_json[fields::embed][fields::model_config];
    size_t num_dim = field_json[fields::num_dim].get<size_t>();
    auto res = EmbedderManager::get_instance().validate_and_init_model(model_config, num_dim);
    if(!res.ok()) {
        return Option<bool>(res.code(), res.error());
    }
    
    LOG(INFO) << "Model init done.";
    field_json[fields::num_dim] = num_dim;
    the_field.num_dim = num_dim;

    return Option<bool>(true);
}

Option<bool> field::fields_to_json_fields(const std::vector<field>& fields, const string& default_sorting_field,
                                          nlohmann::json& fields_json) {
    bool found_default_sorting_field = false;

    // Check for duplicates in field names
    std::map<std::string, std::vector<const field*>> unique_fields;

    for(const field & field: fields) {
        unique_fields[field.name].push_back(&field);

        if(field.name == "id") {
            continue;
        }

        nlohmann::json field_val;
        field_val[fields::name] = field.name;
        field_val[fields::type] = field.type;
        field_val[fields::facet] = field.facet;
        field_val[fields::optional] = field.optional;
        field_val[fields::index] = field.index;
        field_val[fields::sort] = field.sort;
        field_val[fields::infix] = field.infix;

        field_val[fields::locale] = field.locale;

        field_val[fields::store] = field.store;
        field_val[fields::stem] = field.stem;
        field_val[fields::range_index] = field.range_index;

        if(field.embed.count(fields::from) != 0) {
            field_val[fields::embed] = field.embed;
        }

        field_val[fields::nested] = field.nested;
        if(field.nested) {
            field_val[fields::nested_array] = field.nested_array;
        }

        if(field.num_dim > 0) {
            field_val[fields::num_dim] = field.num_dim;
            field_val[fields::vec_dist] = field.vec_dist == ip ? "ip" : "cosine";
        }

        if (!field.reference.empty()) {
            field_val[fields::reference] = field.reference;
            field_val[fields::async_reference] = field.is_async_reference;
        }

        fields_json.push_back(field_val);

        if(!field.has_valid_type()) {
            return Option<bool>(400, "Field `" + field.name +
                                     "` has an invalid data type `" + field.type +
                                     "`, see docs for supported data types.");
        }

        if(field.name == default_sorting_field && !field.is_sortable()) {
            return Option<bool>(400, "Default sorting field `" + default_sorting_field +
                                     "` is not a sortable type.");
        }

        if(field.name == default_sorting_field) {
            if(field.optional) {
                return Option<bool>(400, "Default sorting field `" + default_sorting_field +
                                         "` cannot be an optional field.");
            }

            if(field.is_geopoint()) {
                return Option<bool>(400, "Default sorting field cannot be of type geopoint.");
            }

            found_default_sorting_field = true;
        }

        if(field.is_dynamic() && !field.nested && !field.optional) {
            return Option<bool>(400, "Field `" + field.name + "` must be an optional field.");
        }

        if(field.name == ".*" && !field.index) {
            return Option<bool>(400, "Field `" + field.name + "` cannot be marked as non-indexable.");
        }

        if(!field.index && field.facet) {
            return Option<bool>(400, "Field `" + field.name + "` cannot be a facet since "
                                                              "it's marked as non-indexable.");
        }

        if(!field.is_sort_field() && field.sort) {
            return Option<bool>(400, "Field `" + field.name + "` cannot be a sortable field.");
        }
    }

    if(!default_sorting_field.empty() && !found_default_sorting_field) {
        return Option<bool>(400, "Default sorting field is defined as `" + default_sorting_field +
                                 "` but is not found in the schema.");
    }

    // check for duplicate field names in schema
    for(auto& fname_fields: unique_fields) {
        if(fname_fields.second.size() > 1) {
            // if there are more than 1 field with the same field name, then
            // a) only 1 field can be of static type
            // b) only 1 field can be of dynamic type
            size_t num_static = 0;
            size_t num_dynamic = 0;

            for(const field* f: fname_fields.second) {
                if(f->name == ".*" || f->is_dynamic()) {
                    num_dynamic++;
                } else {
                    num_static++;
                }
            }

            if(num_static != 0 && num_static > 1) {
                return Option<bool>(400, "There are duplicate field names in the schema.");
            }

            if(num_dynamic != 0 && num_dynamic > 1) {
                return Option<bool>(400, "There are duplicate field names in the schema.");
            }
        }
    }

    return Option<bool>(true);
}
