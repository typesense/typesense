#include "validator.h"

Option<uint32_t> validator_t::coerce_element(const field& a_field, nlohmann::json& document,
                                       nlohmann::json& doc_ele,
                                       const std::string& fallback_field_type,
                                       const DIRTY_VALUES& dirty_values) {

    const std::string& field_name = a_field.name;
    bool array_ele_erased = false;
    nlohmann::json::iterator dummy_iter;

    if(a_field.type == field_types::STRING && !doc_ele.is_string()) {
        Option<uint32_t> coerce_op = coerce_string(dirty_values, fallback_field_type, a_field, document, field_name, dummy_iter, false, array_ele_erased);
        if(!coerce_op.ok()) {
            return coerce_op;
        }
    } else if(a_field.type == field_types::INT32) {
        if(!doc_ele.is_number_integer()) {
            Option<uint32_t> coerce_op = coerce_int32_t(dirty_values, a_field, document, field_name, dummy_iter, false, array_ele_erased);
            if(!coerce_op.ok()) {
                return coerce_op;
            }
        }
    } else if(a_field.type == field_types::INT64 && !doc_ele.is_number_integer()) {
        Option<uint32_t> coerce_op = coerce_int64_t(dirty_values, a_field, document, field_name, dummy_iter, false, array_ele_erased);
        if(!coerce_op.ok()) {
            return coerce_op;
        }
    } else if(a_field.type == field_types::FLOAT && !doc_ele.is_number()) {
        // using `is_number` allows integer to be passed to a float field
        Option<uint32_t> coerce_op = coerce_float(dirty_values, a_field, document, field_name, dummy_iter, false, array_ele_erased);
        if(!coerce_op.ok()) {
            return coerce_op;
        }
    } else if(a_field.type == field_types::BOOL && !doc_ele.is_boolean()) {
        Option<uint32_t> coerce_op = coerce_bool(dirty_values, a_field, document, field_name, dummy_iter, false, array_ele_erased);
        if(!coerce_op.ok()) {
            return coerce_op;
        }
    } else if(a_field.type == field_types::GEOPOINT) {
        if(!doc_ele.is_array() || doc_ele.size() != 2) {
            return Option<>(400, "Field `" + field_name  + "` must be a 2 element array: [lat, lng].");
        }

        if(!(doc_ele[0].is_number() && doc_ele[1].is_number())) {
            // one or more elements is not a number, try to coerce
            Option<uint32_t> coerce_op = coerce_geopoint(dirty_values, a_field, document, field_name,
                                                         doc_ele[0], doc_ele[1],
                                                         dummy_iter, false, array_ele_erased);
            if(!coerce_op.ok()) {
                return coerce_op;
            }
        }
    } else if(a_field.is_array()) {
        if(!doc_ele.is_array()) {
            if(a_field.optional && (dirty_values == DIRTY_VALUES::DROP ||
                                    dirty_values == DIRTY_VALUES::COERCE_OR_DROP)) {
                document.erase(field_name);
                return Option<uint32_t>(200);
            } else {
                return Option<>(400, "Field `" + field_name  + "` must be an array.");
            }
        }

        nlohmann::json::iterator it = doc_ele.begin();

        // have to differentiate the geopoint[] type of a nested array object's geopoint[] vs a simple nested field
        // geopoint[] type of an array of objects field won't be an array of array
        if(a_field.nested && a_field.type == field_types::GEOPOINT_ARRAY && it != doc_ele.end() && it->is_number()) {
            if(!doc_ele.empty() && doc_ele.size() % 2 != 0) {
                return Option<>(400, "Nested field `" + field_name  + "` does not contain valid geopoint values.");
            }

            const auto& item = doc_ele;

            for(size_t ai = 0; ai < doc_ele.size(); ai+=2) {
                if(!(doc_ele[ai].is_number() && doc_ele[ai+1].is_number())) {
                    // one or more elements is not an number, try to coerce
                    Option<uint32_t> coerce_op = coerce_geopoint(dirty_values, a_field, document, field_name,
                                                                 doc_ele[ai], doc_ele[ai+1],
                                                                 it, true, array_ele_erased);
                    if(!coerce_op.ok()) {
                        return coerce_op;
                    }
                }

                it++;
            }

            return Option<uint32_t>(200);
        }

        if(a_field.type == field_types::FLOAT_ARRAY && a_field.num_dim != 0 && a_field.num_dim != doc_ele.size()) {
            return Option<uint32_t>(400, "Field `" + a_field.name + "` must have " +
                                    std::to_string(a_field.num_dim)  + " dimensions.");
        }

        for(; it != doc_ele.end(); ) {
            nlohmann::json& item = it.value();
            array_ele_erased = false;

            if (a_field.type == field_types::STRING_ARRAY && !item.is_string()) {
                Option<uint32_t> coerce_op = coerce_string(dirty_values, fallback_field_type, a_field, document, field_name, it, true, array_ele_erased);
                if (!coerce_op.ok()) {
                    return coerce_op;
                }
            } else if (a_field.type == field_types::INT32_ARRAY && !item.is_number_integer()) {
                Option<uint32_t> coerce_op = coerce_int32_t(dirty_values, a_field, document, field_name, it, true, array_ele_erased);
                if (!coerce_op.ok()) {
                    return coerce_op;
                }
            } else if (a_field.type == field_types::INT64_ARRAY && !item.is_number_integer()) {
                Option<uint32_t> coerce_op = coerce_int64_t(dirty_values, a_field, document, field_name, it, true, array_ele_erased);
                if (!coerce_op.ok()) {
                    return coerce_op;
                }
            } else if (a_field.type == field_types::FLOAT_ARRAY && !item.is_number()) {
                // we check for `is_number` to allow whole numbers to be passed into float fields
                Option<uint32_t> coerce_op = coerce_float(dirty_values, a_field, document, field_name, it, true, array_ele_erased);
                if (!coerce_op.ok()) {
                    return coerce_op;
                }
            } else if (a_field.type == field_types::BOOL_ARRAY && !item.is_boolean()) {
                Option<uint32_t> coerce_op = coerce_bool(dirty_values, a_field, document, field_name, it, true, array_ele_erased);
                if (!coerce_op.ok()) {
                    return coerce_op;
                }
            } else if (a_field.type == field_types::GEOPOINT_ARRAY) {
                if(!item.is_array() || item.size() != 2) {
                    return Option<>(400, "Field `" + field_name  + "` must contain 2 element arrays: [ [lat, lng],... ].");
                }

                if(!(item[0].is_number() && item[1].is_number())) {
                    // one or more elements is not a number, try to coerce
                    Option<uint32_t> coerce_op = coerce_geopoint(dirty_values, a_field, document, field_name,
                                                                 item[0], item[1],
                                                                 it, true, array_ele_erased);
                    if(!coerce_op.ok()) {
                        return coerce_op;
                    }
                }
            }

            if(!array_ele_erased) {
                // if it is erased, the iterator will be reassigned
                it++;
            }
        }
    }

    return Option<uint32_t>(200);
}

Option<uint32_t> validator_t::coerce_string(const DIRTY_VALUES& dirty_values, const std::string& fallback_field_type,
                                      const field& a_field, nlohmann::json &document,
                                      const std::string &field_name, nlohmann::json::iterator& array_iter,
                                      bool is_array, bool& array_ele_erased) {
    std::string suffix = is_array ? "an array of" : "a";
    auto& item = is_array ? array_iter.value() : document[field_name];

    if(dirty_values == DIRTY_VALUES::REJECT) {
        if(a_field.nested && item.is_array()) {
            return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                  "Hint: field inside an array of objects must be an array type as well.");
        }
        return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " string.");
    }

    if(dirty_values == DIRTY_VALUES::DROP) {
        if(!a_field.optional) {
            if(a_field.nested && item.is_array()) {
                return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                      "Hint: field inside an array of objects must be an array type as well.");
            }
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " string.");
        }

        if(!is_array) {
            document.erase(field_name);
        } else {
            array_iter = document[field_name].erase(array_iter);
            array_ele_erased = true;
        }
        return Option<uint32_t>(200);
    }

    // we will try to coerce the value to a string

    if (item.is_number_integer()) {
        item = std::to_string((int64_t)item);
    }

    else if(item.is_number_float()) {
        item = StringUtils::float_to_str((float)item);
    }

    else if(item.is_boolean()) {
        item = item == true ? "true" : "false";
    }

    else {
        if(dirty_values == DIRTY_VALUES::COERCE_OR_DROP) {
            if(!a_field.optional) {
                if(a_field.nested && item.is_array()) {
                    return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                         "Hint: field inside an array of objects must be an array type as well.");
                }
                return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " string.");
            }

            if(!is_array) {
                document.erase(field_name);
            } else {
                array_iter = document[field_name].erase(array_iter);
                array_ele_erased = true;
            }
        } else {
            // COERCE_OR_REJECT / non-optional + DROP
            if(a_field.nested && item.is_array()) {
                return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                      "Hint: field inside an array of objects must be an array type as well.");
            }
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " string.");
        }
    }

    return Option<>(200);
}

Option<uint32_t> validator_t::coerce_int32_t(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                       const std::string &field_name,
                                       nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased) {
    std::string suffix = is_array ? "an array of" : "an";
    auto& item = is_array ? array_iter.value() : document[field_name];

    if(dirty_values == DIRTY_VALUES::REJECT) {
        if(a_field.nested && item.is_array()) {
            return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                  "Hint: field inside an array of objects must be an array type as well.");
        }
        return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int32.");
    }

    if(dirty_values == DIRTY_VALUES::DROP) {
        if(!a_field.optional) {
            if(a_field.nested && item.is_array()) {
                return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                      "Hint: field inside an array of objects must be an array type as well.");
            }
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int32.");
        }

        if(!is_array) {
            document.erase(field_name);
        } else {
            array_iter = document[field_name].erase(array_iter);
            array_ele_erased = true;
        }

        return Option<uint32_t>(200);
    }

    // try to value coerce into an integer

    if(item.is_number_float()) {
        item = static_cast<int32_t>(item.get<float>());
    }

    else if(item.is_boolean()) {
        item = item == true ? 1 : 0;
    }

    else if(item.is_string() && StringUtils::is_int32_t(item)) {
        item = std::atol(item.get<std::string>().c_str());
    }

    else {
        if(dirty_values == DIRTY_VALUES::COERCE_OR_DROP) {
            if(!a_field.optional) {
                if(a_field.nested && item.is_array()) {
                    return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                          "Hint: field inside an array of objects must be an array type as well.");
                }
                return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int32.");
            }

            if(!is_array) {
                document.erase(field_name);
            } else {
                array_iter = document[field_name].erase(array_iter);
                array_ele_erased = true;
            }
        } else {
            // COERCE_OR_REJECT / non-optional + DROP
            if(a_field.nested && item.is_array()) {
                return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                      "Hint: field inside an array of objects must be an array type as well.");
            }
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int32.");
        }
    }

    if(document.contains(field_name) && document[field_name].get<int64_t>() > INT32_MAX) {
        if(a_field.optional && (dirty_values == DIRTY_VALUES::DROP || dirty_values == DIRTY_VALUES::COERCE_OR_REJECT)) {
            document.erase(field_name);
        } else {
            return Option<>(400, "Field `" + field_name  + "` exceeds maximum value of int32.");
        }
    }

    return Option<uint32_t>(200);
}

Option<uint32_t> validator_t::coerce_int64_t(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                       const std::string &field_name,
                                       nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased) {
    std::string suffix = is_array ? "an array of" : "an";
    auto& item = is_array ? array_iter.value() : document[field_name];

    if(dirty_values == DIRTY_VALUES::REJECT) {
        if(a_field.nested && item.is_array()) {
            return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                  "Hint: field inside an array of objects must be an array type as well.");
        }
        return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int64.");
    }

    if(dirty_values == DIRTY_VALUES::DROP) {
        if(!a_field.optional) {
            if(a_field.nested && item.is_array()) {
                return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                      "Hint: field inside an array of objects must be an array type as well.");
            }
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int64.");
        }

        if(!is_array) {
            document.erase(field_name);
        } else {
            array_iter = document[field_name].erase(array_iter);
            array_ele_erased = true;
        }
        return Option<uint32_t>(200);
    }

    // try to value coerce into an integer

    if(item.is_number_float()) {
        item = static_cast<int64_t>(item.get<float>());
    }

    else if(item.is_boolean()) {
        item = item == true ? 1 : 0;
    }

    else if(item.is_string() && StringUtils::is_int64_t(item)) {
        item = std::atoll(item.get<std::string>().c_str());
    }

    else {
        if(dirty_values == DIRTY_VALUES::COERCE_OR_DROP) {
            if(!a_field.optional) {
                if(a_field.nested && item.is_array()) {
                    return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                          "Hint: field inside an array of objects must be an array type as well.");
                }
                return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int64.");
            }

            if(!is_array) {
                document.erase(field_name);
            } else {
                array_iter = document[field_name].erase(array_iter);
                array_ele_erased = true;
            }
        } else {
            // COERCE_OR_REJECT / non-optional + DROP
            if(a_field.nested && item.is_array()) {
                return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                      "Hint: field inside an array of objects must be an array type as well.");
            }
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " int64.");
        }
    }

    return Option<uint32_t>(200);
}

Option<uint32_t> validator_t::coerce_bool(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                    const std::string &field_name,
                                    nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased) {
    std::string suffix = is_array ? "a array of" : "a";
    auto& item = is_array ? array_iter.value() : document[field_name];

    if(dirty_values == DIRTY_VALUES::REJECT) {
        if(a_field.nested && item.is_array()) {
            return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                  "Hint: field inside an array of objects must be an array type as well.");
        }
        return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " bool.");
    }

    if(dirty_values == DIRTY_VALUES::DROP) {
        if(!a_field.optional) {
            if(a_field.nested && item.is_array()) {
                return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                      "Hint: field inside an array of objects must be an array type as well.");
            }
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " bool.");
        }

        if(!is_array) {
            document.erase(field_name);
        } else {
            array_iter = document[field_name].erase(array_iter);
            array_ele_erased = true;
        }
        return Option<uint32_t>(200);
    }

    // try to value coerce into a bool
    if (item.is_number_integer() &&
        (item.get<int64_t>() == 1 || item.get<int64_t>() == 0)) {
        item = item.get<int64_t>() == 1;
    }

    else if(item.is_string()) {
        std::string str_val = item.get<std::string>();
        StringUtils::tolowercase(str_val);
        if(str_val == "true") {
            item = true;
            return Option<uint32_t>(200);
        } else if(str_val == "false") {
            item = false;
            return Option<uint32_t>(200);
        } else {
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " bool.");
        }
    }

    else {
        if(dirty_values == DIRTY_VALUES::COERCE_OR_DROP) {
            if(!a_field.optional) {
                if(a_field.nested && item.is_array()) {
                    return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                          "Hint: field inside an array of objects must be an array type as well.");
                }
                return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " bool.");
            }

            if(!is_array) {
                document.erase(field_name);
            } else {
                array_iter = document[field_name].erase(array_iter);
                array_ele_erased = true;
            }
        } else {
            // COERCE_OR_REJECT / non-optional + DROP
            if(a_field.nested && item.is_array()) {
                return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                      "Hint: field inside an array of objects must be an array type as well.");
            }
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " bool.");
        }
    }

    return Option<uint32_t>(200);
}

Option<uint32_t> validator_t::coerce_geopoint(const DIRTY_VALUES& dirty_values, const field& a_field,
                                              nlohmann::json &document, const std::string &field_name,
                                              nlohmann::json& lat, nlohmann::json& lng,
                                              nlohmann::json::iterator& array_iter,
                                              bool is_array, bool& array_ele_erased) {
    std::string suffix = is_array ? "an array of" : "a";

    if(dirty_values == DIRTY_VALUES::REJECT) {
        return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " geopoint.");
    }

    if(dirty_values == DIRTY_VALUES::DROP) {
        if(!a_field.optional) {
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " geopoint.");
        }

        if(!is_array) {
            document.erase(field_name);
        } else {
            array_iter = document[field_name].erase(array_iter);
            array_ele_erased = true;
        }
        return Option<uint32_t>(200);
    }

    // try to value coerce into a geopoint

    if(!lat.is_number() && lat.is_string()) {
        if(StringUtils::is_float(lat)) {
            lat = std::stof(lat.get<std::string>());
        }
    }

    if(!lng.is_number() && lng.is_string()) {
        if(StringUtils::is_float(lng)) {
            lng = std::stof(lng.get<std::string>());
        }
    }

    if(!lat.is_number() || !lng.is_number()) {
        if(dirty_values == DIRTY_VALUES::COERCE_OR_DROP) {
            if(!a_field.optional) {
                return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " geopoint.");
            }

            if(!is_array) {
                document.erase(field_name);
            } else {
                array_iter = document[field_name].erase(array_iter);
                array_ele_erased = true;
            }
        } else {
            // COERCE_OR_REJECT / non-optional + DROP
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " geopoint.");
        }
    }

    return Option<uint32_t>(200);
}

Option<uint32_t> validator_t::coerce_float(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                     const std::string &field_name,
                                     nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased) {
    std::string suffix = is_array ? "a array of" : "a";
    auto& item = is_array ? array_iter.value() : document[field_name];

    if(dirty_values == DIRTY_VALUES::REJECT) {
        if(a_field.nested && item.is_array()) {
            return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                  "Hint: field inside an array of objects must be an array type as well.");
        }
        return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " float.");
    }

    if(dirty_values == DIRTY_VALUES::DROP) {
        if(!a_field.optional) {
            if(a_field.nested && item.is_array()) {
                return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                      "Hint: field inside an array of objects must be an array type as well.");
            }
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " float.");
        }

        if(!is_array) {
            document.erase(field_name);
        } else {
            array_iter = document[field_name].erase(array_iter);
            array_ele_erased = true;
        }
        return Option<uint32_t>(200);
    }

    // try to value coerce into a float

    if(item.is_string() && StringUtils::is_float(item)) {
        item = std::atof(item.get<std::string>().c_str());
    }

    else if(item.is_boolean()) {
        item = item == true ? 1.0 : 0.0;
    }

    else {
        if(dirty_values == DIRTY_VALUES::COERCE_OR_DROP) {
            if(!a_field.optional) {
                if(a_field.nested && item.is_array()) {
                    return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                          "Hint: field inside an array of objects must be an array type as well.");
                }
                return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " float.");
            }

            if(!is_array) {
                document.erase(field_name);
            } else {
                array_iter = document[field_name].erase(array_iter);
                array_ele_erased = true;
            }
        } else {
            // COERCE_OR_REJECT / non-optional + DROP
            if(a_field.nested && item.is_array()) {
                return Option<>(400, "Field `" + field_name + "` has an incorrect type. "
                                      "Hint: field inside an array of objects must be an array type as well.");
            }
            return Option<>(400, "Field `" + field_name  + "` must be " + suffix + " float.");
        }
    }

    return Option<uint32_t>(200);
}

Option<uint32_t> validator_t::validate_index_in_memory(nlohmann::json& document, uint32_t seq_id,
                                                 const std::string & default_sorting_field,
                                                 const tsl::htrie_map<char, field> & search_schema,
                                                 const tsl::htrie_map<char, field> & embedding_fields,
                                                 const index_operation_t op,
                                                 const bool is_update,
                                                 const std::string& fallback_field_type,
                                                 const DIRTY_VALUES& dirty_values, const bool validate_embedding_fields) {

    bool missing_default_sort_field = (!default_sorting_field.empty() && document.count(default_sorting_field) == 0);

    if((op == CREATE || op == UPSERT) && missing_default_sort_field) {
        return Option<>(400, "Field `" + default_sorting_field  + "` has been declared as a default sorting field, "
                                                                  "but is not found in the document.");
    }

    for(const auto& a_field: search_schema) {
        const std::string& field_name = a_field.name;

        // ignore embedding fields, they will be validated later
        if(embedding_fields.count(field_name) > 0) {
            continue;
        }

        if(field_name == "id" || a_field.is_object()) {
            continue;
        }

        if((a_field.optional || op == UPDATE || op == EMPLACE) && document.count(field_name) == 0) {
            continue;
        }

        if(document.count(field_name) == 0) {
            return Option<>(400, "Field `" + field_name  + "` has been declared in the schema, "
                                                           "but is not found in the document.");
        }

        nlohmann::json& doc_ele = document[field_name];

        if(a_field.optional && doc_ele.is_null()) {
            // we will ignore `null` on an option field
            if(!is_update) {
                // for updates, the erasure is done later since we need to keep the key for overwrite
                document.erase(field_name);
            }
            continue;
        }

        auto coerce_op = coerce_element(a_field, document, doc_ele, fallback_field_type, dirty_values);
        if(!coerce_op.ok()) {
            return coerce_op;
        }
    }

    if(validate_embedding_fields) {
        // validate embedding fields
        auto validate_embed_op = validate_embed_fields(document, embedding_fields, search_schema, !is_update);
        if(!validate_embed_op.ok()) {
            return Option<>(validate_embed_op.code(), validate_embed_op.error());
        }
    }
    
    return Option<>(200);
}


Option<bool> validator_t::validate_embed_fields(const nlohmann::json& document, 
                                          const tsl::htrie_map<char, field>& embedding_fields, 
                                          const tsl::htrie_map<char, field> & search_schema,
                                          const bool& error_if_field_not_found) {
    for(const auto& field : embedding_fields) {
        const auto& embed_from = field.embed[fields::from].get<std::vector<std::string>>();
        // flag to check if all fields to embed from are optional and null
        bool all_optional_and_null = true;
        for(const auto& field_name : embed_from) {
            auto schema_field_it = search_schema.find(field_name);
            auto doc_field_it = document.find(field_name);
            if(schema_field_it == search_schema.end()) {
                return Option<bool>(400, "Field `" + field.name + "` has invalid fields to create embeddings from.");
            }
            if(doc_field_it == document.end()) {
                if(error_if_field_not_found && !schema_field_it->optional) {
                    return Option<bool>(400, "Field `" + field_name + "` is needed to create embedding.");
                } else {
                    continue;
                }
            }
            all_optional_and_null = false;
            if((schema_field_it.value().type == field_types::STRING && !doc_field_it.value().is_string()) || 
                (schema_field_it.value().type == field_types::STRING_ARRAY && !doc_field_it.value().is_array())) {
                return Option<bool>(400, "Field `" + field_name + "` has malformed data.");
            }
            if(doc_field_it.value().is_array()) {
                for(const auto& val : doc_field_it.value()) {
                    if(!val.is_string()) {
                        return Option<bool>(400, "Field `" + field_name + "` has malformed data.");
                    }
                }
            }
        }
        if(all_optional_and_null && !field.optional) {
            return Option<bool>(400, "No valid fields found to create embedding for `" + field.name + "`, please provide at least one valid field or make the embedding field optional.");
        }
    }

    return Option<bool>(true);
}