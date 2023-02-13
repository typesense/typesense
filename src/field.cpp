#include <store.h>
#include "field.h"
#include "magic_enum.hpp"
#include <stack>

Option<bool> filter::parse_geopoint_filter_value(std::string& raw_value,
                                                 const std::string& format_err_msg,
                                                 std::string& processed_filter_val,
                                                 NUM_COMPARATOR& num_comparator) {

    num_comparator = LESS_THAN_EQUALS;

    if(!(raw_value[0] == '(' && raw_value[raw_value.size() - 1] == ')')) {
        return Option<bool>(400, format_err_msg);
    }

    std::vector<std::string> filter_values;
    auto raw_val_without_paran = raw_value.substr(1, raw_value.size() - 2);
    StringUtils::split(raw_val_without_paran, filter_values, ",");

    // we will end up with: "10.45 34.56 2 km" or "10.45 34.56 2mi" or a geo polygon

    if(filter_values.size() < 3) {
        return Option<bool>(400, format_err_msg);
    }

    // do validation: format should match either a point + radius or polygon

    size_t num_floats = 0;
    for(const auto& fvalue: filter_values) {
        if(StringUtils::is_float(fvalue)) {
            num_floats++;
        }
    }

    bool is_polygon = (num_floats == filter_values.size());
    if(!is_polygon) {
        // we have to ensure that this is a point + radius match
        if(!StringUtils::is_float(filter_values[0]) || !StringUtils::is_float(filter_values[1])) {
            return Option<bool>(400, format_err_msg);
        }
    }

    if(is_polygon) {
        processed_filter_val = raw_val_without_paran;
    } else {
        // point + radius
        // filter_values[2] is distance, get the unit, validate it and split on that
        if(filter_values[2].size() < 2) {
            return Option<bool>(400, "Unit must be either `km` or `mi`.");
        }

        std::string unit = filter_values[2].substr(filter_values[2].size()-2, 2);

        if(unit != "km" && unit != "mi") {
            return Option<bool>(400, "Unit must be either `km` or `mi`.");
        }

        std::vector<std::string> dist_values;
        StringUtils::split(filter_values[2], dist_values, unit);

        if(dist_values.size() != 1) {
            return Option<bool>(400, format_err_msg);
        }

        if(!StringUtils::is_float(dist_values[0])) {
            return Option<bool>(400, format_err_msg);
        }

        processed_filter_val = filter_values[0] + ", " + filter_values[1] + ", " + // co-ords
                               dist_values[0] + ", " +  unit;           // X km
    }

    return Option<bool>(true);
}

bool isOperator(const std::string& expression) {
    return expression == "&&" || expression == "||";
}

// https://en.wikipedia.org/wiki/Shunting_yard_algorithm
Option<bool> toPostfix(std::queue<std::string>& tokens, std::queue<std::string>& postfix) {
    std::stack<std::string> operatorStack;

    while (!tokens.empty()) {
        auto expression = tokens.front();
        tokens.pop();

        if (isOperator(expression)) {
            // We only have two operators &&, || having the same precedence and both being left associative.
            while (!operatorStack.empty() && operatorStack.top() != "(") {
                postfix.push(operatorStack.top());
                operatorStack.pop();
            }

            operatorStack.push(expression);
        } else if (expression == "(") {
            operatorStack.push(expression);
        } else if (expression == ")") {
            while (!operatorStack.empty() && operatorStack.top() != "(") {
                postfix.push(operatorStack.top());
                operatorStack.pop();
            }

            if (operatorStack.empty() || operatorStack.top() != "(") {
                return Option<bool>(400, "Could not parse the filter query: unbalanced parentheses.");
            }
            operatorStack.pop();
        } else {
            postfix.push(expression);
        }
    }

    while (!operatorStack.empty()) {
        if (operatorStack.top() == "(") {
            return Option<bool>(400, "Could not parse the filter query: unbalanced parentheses.");
        }
        postfix.push(operatorStack.top());
        operatorStack.pop();
    }

    return Option<bool>(true);
}

Option<bool> toMultiValueNumericFilter(std::string& raw_value, filter& filter_exp, const field& _field) {
    std::vector<std::string> filter_values;
    StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, ",");
    filter_exp = {_field.name, {}, {}};
    for (std::string& filter_value: filter_values) {
        Option<NUM_COMPARATOR> op_comparator = filter::extract_num_comparator(filter_value);
        if (!op_comparator.ok()) {
            return Option<bool>(400, "Error with filter field `" + _field.name + "`: " + op_comparator.error());
        }
        if (op_comparator.get() == RANGE_INCLUSIVE) {
            // split the value around range operator to extract bounds
            std::vector<std::string> range_values;
            StringUtils::split(filter_value, range_values, filter::RANGE_OPERATOR());
            for (const std::string& range_value: range_values) {
                auto validate_op = filter::validate_numerical_filter_value(_field, range_value);
                if (!validate_op.ok()) {
                    return validate_op;
                }
                filter_exp.values.push_back(range_value);
                filter_exp.comparators.push_back(op_comparator.get());
            }
        } else {
            auto validate_op = filter::validate_numerical_filter_value(_field, filter_value);
            if (!validate_op.ok()) {
                return validate_op;
            }
            filter_exp.values.push_back(filter_value);
            filter_exp.comparators.push_back(op_comparator.get());
        }
    }

    return Option<bool>(true);
}

Option<bool> toFilter(const std::string expression,
                      filter& filter_exp,
                      const tsl::htrie_map<char, field>& search_schema,
                      const Store* store,
                      const std::string& doc_id_prefix) {
    // split into [field_name, value]
    size_t found_index = expression.find(':');
    if (found_index == std::string::npos) {
        return Option<bool>(400, "Could not parse the filter query.");
    }
    std::string&& field_name = expression.substr(0, found_index);
    StringUtils::trim(field_name);
    if (field_name == "id") {
        std::string&& raw_value = expression.substr(found_index + 1, std::string::npos);
        StringUtils::trim(raw_value);
        std::string empty_filter_err = "Error with filter field `id`: Filter value cannot be empty.";
        if (raw_value.empty()) {
            return Option<bool>(400, empty_filter_err);
        }
        filter_exp = {field_name, {}, {}};
        NUM_COMPARATOR id_comparator = EQUALS;
        size_t filter_value_index = 0;
        if (raw_value[0] == '=') {
            id_comparator = EQUALS;
            while (++filter_value_index < raw_value.size() && raw_value[filter_value_index] == ' ');
        } else if (raw_value.size() >= 2 && raw_value[0] == '!' && raw_value[1] == '=') {
            return Option<bool>(400, "Not equals filtering is not supported on the `id` field.");
        }
        if (filter_value_index != 0) {
            raw_value = raw_value.substr(filter_value_index);
        }
        if (raw_value.empty()) {
            return Option<bool>(400, empty_filter_err);
        }
        if (raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
            std::vector<std::string> doc_ids;
            StringUtils::split_to_values(raw_value.substr(1, raw_value.size() - 2), doc_ids);
            for (std::string& doc_id: doc_ids) {
                // we have to convert the doc_id to seq id
                std::string seq_id_str;
                StoreStatus seq_id_status = store->get(doc_id_prefix + doc_id, seq_id_str);
                if (seq_id_status != StoreStatus::FOUND) {
                    continue;
                }
                filter_exp.values.push_back(seq_id_str);
                filter_exp.comparators.push_back(id_comparator);
            }
        } else {
            std::vector<std::string> doc_ids;
            StringUtils::split_to_values(raw_value, doc_ids); // to handle backticks
            std::string seq_id_str;
            StoreStatus seq_id_status = store->get(doc_id_prefix + doc_ids[0], seq_id_str);
            if (seq_id_status == StoreStatus::FOUND) {
                filter_exp.values.push_back(seq_id_str);
                filter_exp.comparators.push_back(id_comparator);
            }
        }
        return Option<bool>(true);
    }

    auto field_it = search_schema.find(field_name);

    if (field_it == search_schema.end()) {
        return Option<bool>(404, "Could not find a filter field named `" + field_name + "` in the schema.");
    }

    if (field_it->num_dim > 0) {
        return Option<bool>(404, "Cannot filter on vector field `" + field_name + "`.");
    }

    const field& _field = field_it.value();
    std::string&& raw_value = expression.substr(found_index + 1, std::string::npos);
    StringUtils::trim(raw_value);
    // skip past optional `:=` operator, which has no meaning for non-string fields
    if (!_field.is_string() && raw_value[0] == '=') {
        size_t filter_value_index = 0;
        while (raw_value[++filter_value_index] == ' ');
        raw_value = raw_value.substr(filter_value_index);
    }
    if (_field.is_integer() || _field.is_float()) {
        // could be a single value or a list
        if (raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
            Option<bool> op = toMultiValueNumericFilter(raw_value, filter_exp, _field);
            if (!op.ok()) {
                return op;
            }
        } else {
            Option<NUM_COMPARATOR> op_comparator = filter::extract_num_comparator(raw_value);
            if (!op_comparator.ok()) {
                return Option<bool>(400, "Error with filter field `" + _field.name + "`: " + op_comparator.error());
            }
            if (op_comparator.get() == RANGE_INCLUSIVE) {
                // split the value around range operator to extract bounds
                std::vector<std::string> range_values;
                StringUtils::split(raw_value, range_values, filter::RANGE_OPERATOR());
                filter_exp.field_name = field_name;
                for (const std::string& range_value: range_values) {
                    auto validate_op = filter::validate_numerical_filter_value(_field, range_value);
                    if (!validate_op.ok()) {
                        return validate_op;
                    }
                    filter_exp.values.push_back(range_value);
                    filter_exp.comparators.push_back(op_comparator.get());
                }
            } else if (op_comparator.get() == NOT_EQUALS && raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
                Option<bool> op = toMultiValueNumericFilter(raw_value, filter_exp, _field);
                if (!op.ok()) {
                    return op;
                }
                filter_exp.apply_not_equals = true;
            } else {
                auto validate_op = filter::validate_numerical_filter_value(_field, raw_value);
                if (!validate_op.ok()) {
                    return validate_op;
                }
                filter_exp = {field_name, {raw_value}, {op_comparator.get()}};
            }
        }
    } else if (_field.is_bool()) {
        NUM_COMPARATOR bool_comparator = EQUALS;
        size_t filter_value_index = 0;
        if (raw_value[0] == '=') {
            bool_comparator = EQUALS;
            while (++filter_value_index < raw_value.size() && raw_value[filter_value_index] == ' ');
        } else if (raw_value.size() >= 2 && raw_value[0] == '!' && raw_value[1] == '=') {
            bool_comparator = NOT_EQUALS;
            filter_value_index++;
            while (++filter_value_index < raw_value.size() && raw_value[filter_value_index] == ' ');
        }
        if (filter_value_index != 0) {
            raw_value = raw_value.substr(filter_value_index);
        }
        if (filter_value_index == raw_value.size()) {
            return Option<bool>(400, "Error with filter field `" + _field.name +
                                     "`: Filter value cannot be empty.");
        }
        if (raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
            std::vector<std::string> filter_values;
            StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, ",");
            filter_exp = {field_name, {}, {}};
            for (std::string& filter_value: filter_values) {
                if (filter_value != "true" && filter_value != "false") {
                    return Option<bool>(400, "Values of filter field `" + _field.name +
                                             "`: must be `true` or `false`.");
                }
                filter_value = (filter_value == "true") ? "1" : "0";
                filter_exp.values.push_back(filter_value);
                filter_exp.comparators.push_back(bool_comparator);
            }
        } else {
            if (raw_value != "true" && raw_value != "false") {
                return Option<bool>(400, "Value of filter field `" + _field.name + "` must be `true` or `false`.");
            }
            std::string bool_value = (raw_value == "true") ? "1" : "0";
            filter_exp = {field_name, {bool_value}, {bool_comparator}};
        }
    } else if (_field.is_geopoint()) {
        filter_exp = {field_name, {}, {}};
        const std::string& format_err_msg = "Value of filter field `" + _field.name +
                                            "`: must be in the `(-44.50, 170.29, 0.75 km)` or "
                                            "(56.33, -65.97, 23.82, -127.82) format.";
        NUM_COMPARATOR num_comparator;
        // could be a single value or a list
        if (raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
            std::vector<std::string> filter_values;
            StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, "),");
            for (std::string& filter_value: filter_values) {
                filter_value += ")";
                std::string processed_filter_val;
                auto parse_op = filter::parse_geopoint_filter_value(filter_value, format_err_msg, processed_filter_val,
                                                                    num_comparator);
                if (!parse_op.ok()) {
                    return parse_op;
                }
                filter_exp.values.push_back(processed_filter_val);
                filter_exp.comparators.push_back(num_comparator);
            }
        } else {
            // single value, e.g. (10.45, 34.56, 2 km)
            std::string processed_filter_val;
            auto parse_op = filter::parse_geopoint_filter_value(raw_value, format_err_msg, processed_filter_val,
                                                                num_comparator);
            if (!parse_op.ok()) {
                return parse_op;
            }
            filter_exp.values.push_back(processed_filter_val);
            filter_exp.comparators.push_back(num_comparator);
        }
    } else if (_field.is_string()) {
        size_t filter_value_index = 0;
        NUM_COMPARATOR str_comparator = CONTAINS;
        if (raw_value[0] == '=') {
            // string filter should be evaluated in strict "equals" mode
            str_comparator = EQUALS;
            while (++filter_value_index < raw_value.size() && raw_value[filter_value_index] == ' ');
        } else if (raw_value.size() >= 2 && raw_value[0] == '!' && raw_value[1] == '=') {
            str_comparator = NOT_EQUALS;
            filter_value_index++;
            while (++filter_value_index < raw_value.size() && raw_value[filter_value_index] == ' ');
        }
        if (filter_value_index == raw_value.size()) {
            return Option<bool>(400, "Error with filter field `" + _field.name +
                                     "`: Filter value cannot be empty.");
        }
        if (raw_value[filter_value_index] == '[' && raw_value[raw_value.size() - 1] == ']') {
            std::vector<std::string> filter_values;
            StringUtils::split_to_values(
                    raw_value.substr(filter_value_index + 1, raw_value.size() - filter_value_index - 2), filter_values);
            filter_exp = {field_name, filter_values, {str_comparator}};
        } else {
            filter_exp = {field_name, {raw_value.substr(filter_value_index)}, {str_comparator}};
        }

        filter_exp.apply_not_equals = (str_comparator == NOT_EQUALS);
    } else {
        return Option<bool>(400, "Error with filter field `" + _field.name +
                                 "`: Unidentified field data type, see docs for supported data types.");
    }

    return Option<bool>(true);
}

// https://stackoverflow.com/a/423914/11218270
Option<bool> toParseTree(std::queue<std::string>& postfix, filter_node_t*& root,
                         const tsl::htrie_map<char, field>& search_schema,
                         const Store* store,
                         const std::string& doc_id_prefix) {
    std::stack<filter_node_t*> nodeStack;

    while (!postfix.empty()) {
        const std::string expression = postfix.front();
        postfix.pop();

        filter_node_t* filter_node;
        if (isOperator(expression)) {
            auto message = "Could not parse the filter query: unbalanced `" + expression + "` operands.";

            if (nodeStack.empty()) {
                return Option<bool>(400, message);
            }
            auto operandB = nodeStack.top();
            nodeStack.pop();

            if (nodeStack.empty()) {
                return Option<bool>(400, message);
            }
            auto operandA = nodeStack.top();
            nodeStack.pop();

            filter_node = new filter_node_t(expression == "&&" ? AND : OR, operandA, operandB);
        } else {
            filter filter_exp;
            Option<bool> toFilter_op = toFilter(expression, filter_exp, search_schema, store, doc_id_prefix);
            if (!toFilter_op.ok()) {
                return toFilter_op;
            }

            filter_node = new filter_node_t(filter_exp);
        }

        nodeStack.push(filter_node);
    }

    if (nodeStack.empty()) {
        return Option<bool>(400, "Filter query cannot be empty.");
    }

    root = nodeStack.top();
    return Option<bool>(true);
}

Option<bool> filter::parse_filter_query(const std::string& filter_query,
                                        const tsl::htrie_map<char, field>& search_schema,
                                        const Store* store,
                                        const std::string& doc_id_prefix,
                                        filter_node_t*& root) {
    auto _filter_query = filter_query;
    StringUtils::trim(_filter_query);
    if (_filter_query.empty()) {
        return Option<bool>(true);
    }

    std::queue<std::string> tokens;
    Option<bool> tokenize_op = StringUtils::tokenize_filter_query(filter_query, tokens);
    if (!tokenize_op.ok()) {
        return tokenize_op;
    }

    if (tokens.size() > 100) {
        return Option<bool>(400, "Filter expression is not valid.");
    }

    std::queue<std::string> postfix;
    Option<bool> toPostfix_op = toPostfix(tokens, postfix);
    if (!toPostfix_op.ok()) {
        return toPostfix_op;
    }

    Option<bool> toParseTree_op = toParseTree(postfix, root, search_schema, store, doc_id_prefix);
    if (!toParseTree_op.ok()) {
        return toParseTree_op;
    }

    return Option<bool>(true);
}

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

    if(field_json.count(fields::sort) == 0) {
        if(field_json["type"] == field_types::INT32 || field_json["type"] == field_types::INT64 ||
           field_json["type"] == field_types::FLOAT || field_json["type"] == field_types::BOOL ||
           field_json["type"] == field_types::GEOPOINT || field_json["type"] == field_types::GEOPOINT_ARRAY) {
            if(field_json.count(fields::num_dim) == 0) {
                field_json[fields::sort] = true;
            } else {
                field_json[fields::sort] = false;
            }
        } else {
            field_json[fields::sort] = false;
        }
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

    if(field_json.count(fields::optional) == 0) {
        // dynamic type fields are always optional
        bool is_dynamic = field::is_dynamic(field_json[fields::name], field_json[fields::type]);
        field_json[fields::optional] = is_dynamic;
    }

    bool is_obj = field_json[fields::type] == field_types::OBJECT || field_json[fields::type] == field_types::OBJECT_ARRAY;
    bool is_regexp_name = field_json[fields::name].get<std::string>().find(".*") != std::string::npos;

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

    the_fields.emplace_back(
            field(field_json[fields::name], field_json[fields::type], field_json[fields::facet],
                  field_json[fields::optional], field_json[fields::index], field_json[fields::locale],
                  field_json[fields::sort], field_json[fields::infix], field_json[fields::nested],
                  field_json[fields::nested_array], field_json[fields::num_dim], vec_dist)
    );

    return Option<bool>(true);
}

bool field::flatten_obj(nlohmann::json& doc, nlohmann::json& value, bool has_array, bool has_obj_array,
                        const field& the_field, const std::string& flat_name,
                        std::unordered_map<std::string, field>& flattened_fields) {
    if(value.is_object()) {
        has_obj_array = has_array;
        for(const auto& kv: value.items()) {
            flatten_obj(doc, kv.value(), has_array, has_obj_array, the_field, flat_name + "." + kv.key(), flattened_fields);
        }
    } else if(value.is_array()) {
        for(const auto& kv: value.items()) {
            flatten_obj(doc, kv.value(), true, has_obj_array, the_field, flat_name, flattened_fields);
        }
    } else { // must be a primitive
        if(doc.count(flat_name) != 0 && flattened_fields.find(flat_name) == flattened_fields.end()) {
            return true;
        }

        if(has_array) {
            doc[flat_name].push_back(value);
        } else {
            doc[flat_name] = value;
        }

        std::string detected_type;
        if(!field::get_type(value, detected_type)) {
            return false;
        }

        if(std::isalnum(detected_type.back()) && has_array) {
            // convert singular type to multi valued type
            detected_type += "[]";
        }

        field flattened_field = the_field;
        flattened_field.name = flat_name;
        flattened_field.type = detected_type;
        flattened_field.optional = true;
        flattened_field.nested = true;
        flattened_field.nested_array = has_obj_array;
        flattened_field.set_computed_defaults(-1, -1);
        flattened_fields[flat_name] = flattened_field;
    }

    return true;
}

Option<bool> field::flatten_field(nlohmann::json& doc, nlohmann::json& obj, const field& the_field,
                                  std::vector<std::string>& path_parts, size_t path_index,
                                  bool has_array, bool has_obj_array, std::unordered_map<std::string, field>& flattened_fields) {
    if(path_index == path_parts.size()) {
        // end of path: check if obj matches expected type
        std::string detected_type;
        if(!field::get_type(obj, detected_type)) {
            if(obj.is_null() && the_field.optional) {
                // null values are allowed only if field is optional
                return Option<bool>(false);
            }

            return Option<bool>(400, "Field `" + the_field.name + "` has an incorrect type.");
        }

        if(std::isalnum(detected_type.back()) && has_array) {
            // convert singular type to multi valued type
            detected_type += "[]";
        }

        has_obj_array = has_obj_array || ((detected_type == field_types::OBJECT) && has_array);

        // handle differences in detection of numerical types
        bool is_numericaly_valid = (detected_type != the_field.type) &&
            ( (detected_type == field_types::INT64 &&
                (the_field.type == field_types::INT32 || the_field.type == field_types::FLOAT)) ||

              (detected_type == field_types::INT64_ARRAY &&
                (the_field.type == field_types::INT32_ARRAY || the_field.type == field_types::FLOAT_ARRAY)) ||

              (detected_type == field_types::FLOAT_ARRAY && the_field.type == field_types::GEOPOINT_ARRAY)
           );

        if(detected_type == the_field.type || is_numericaly_valid) {
            if(the_field.is_object()) {
                flatten_obj(doc, obj, has_array, has_obj_array, the_field, the_field.name, flattened_fields);
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
                                                has_obj_array, flattened_fields);
                if(!op.ok()) {
                    return op;
                }
            }
            return Option<bool>(true);
        } else {
            return flatten_field(doc, it.value(), the_field, path_parts, path_index + 1, has_array, has_obj_array, flattened_fields);
        }
    } {
        return Option<bool>(404, "Field `" + the_field.name + "` not found.");
    }
}

Option<bool> field::flatten_doc(nlohmann::json& document,
                                const tsl::htrie_map<char, field>& nested_fields,
                                bool missing_is_ok, std::vector<field>& flattened_fields) {

    std::unordered_map<std::string, field> flattened_fields_map;

    for(auto& nested_field: nested_fields) {
        std::vector<std::string> field_parts;
        StringUtils::split(nested_field.name, field_parts, ".");

        if(field_parts.size() > 1 && document.count(nested_field.name) != 0) {
            // skip explicitly present nested fields
            continue;
        }

        auto op = flatten_field(document, document, nested_field, field_parts, 0, false, false, flattened_fields_map);
        if(op.ok()) {
            continue;
        }

        if(op.code() == 404 && (missing_is_ok || nested_field.optional)) {
            continue;
        } else {
            return op;
        }
    }

    document[".flat"] = nlohmann::json::array();
    for(auto& kv: flattened_fields_map) {
        document[".flat"].push_back(kv.second.name);
        flattened_fields.push_back(kv.second);
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
