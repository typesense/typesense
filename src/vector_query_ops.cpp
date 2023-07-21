#include "vector_query_ops.h"
#include "string_utils.h"
#include "collection.h"

Option<bool> VectorQueryOps::parse_vector_query_str(const std::string& vector_query_str,
                                                    vector_query_t& vector_query,
                                                    const bool is_wildcard_query,
                                                    const Collection* coll) {
    // FORMAT:
    // field_name([0.34, 0.66, 0.12, 0.68], exact: false, k: 10)
    size_t i = 0;
    while(i < vector_query_str.size()) {
        if(vector_query_str[i] != ':') {
            vector_query.field_name += vector_query_str[i];
            i++;
        } else {
            if(vector_query_str[i] != ':') {
                // missing ":"
                return Option<bool>(400, "Malformed vector query string: `:` is missing.");
            }

            // field name is done
            i++;

            StringUtils::trim(vector_query.field_name);

            while(i < vector_query_str.size() && vector_query_str[i] != '(') {
                i++;
            }

            if(vector_query_str[i] != '(') {
                // missing "("
                return Option<bool>(400, "Malformed vector query string.");
            }

            i++;

            while(i < vector_query_str.size() && vector_query_str[i] != '[') {
                i++;
            }

            if(vector_query_str[i] != '[') {
                // missing opening "["
                return Option<bool>(400, "Malformed vector query string.");
            }

            i++;

            std::string values_str;
            while(i < vector_query_str.size() && vector_query_str[i] != ']') {
                values_str += vector_query_str[i];
                i++;
            }

            if(vector_query_str[i] != ']') {
                // missing closing "]"
                return Option<bool>(400, "Malformed vector query string.");
            }

            i++;

            std::vector<std::string> svalues;
            StringUtils::split(values_str, svalues, ",");

            for(auto& svalue: svalues) {
                if(!StringUtils::is_float(svalue)) {
                    return Option<bool>(400, "Malformed vector query string: one of the vector values is not a float.");
                }

                vector_query.values.push_back(std::stof(svalue));
            }

            if(i == vector_query_str.size()-1) {
                // missing params
                if(vector_query.values.empty()) {
                    // when query values are missing, atleast the `id` parameter must be present
                    return Option<bool>(400, "When a vector query value is empty, an `id` parameter must be present.");
                }

                return Option<bool>(true);
            }

            std::string param_str = vector_query_str.substr(i, (vector_query_str.size() - i));
            std::vector<std::string> param_kvs;
            StringUtils::split(param_str, param_kvs, ",");

            for(auto& param_kv_str: param_kvs) {
                if(param_kv_str.back() == ')') {
                    param_kv_str.pop_back();
                }

                std::vector<std::string> param_kv;
                StringUtils::split(param_kv_str, param_kv, ":");
                if(param_kv.size() != 2) {
                    return Option<bool>(400, "Malformed vector query string.");
                }

                if(param_kv[0] == "id") {
                    if(!vector_query.values.empty()) {
                        // cannot pass both vector values and id
                        return Option<bool>(400, "Malformed vector query string: cannot pass both vector query "
                                                 "and `id` parameter.");
                    }

                    Option<uint32_t> id_op = coll->doc_id_to_seq_id(param_kv[1]);
                    if(!id_op.ok()) {
                        return Option<bool>(400, "Document id referenced in vector query is not found.");
                    }

                    nlohmann::json document;
                    auto doc_op  = coll->get_document_from_store(id_op.get(), document);
                    if(!doc_op.ok()) {
                        return Option<bool>(400, "Document id referenced in vector query is not found.");
                    }

                    if(!document.contains(vector_query.field_name) || !document[vector_query.field_name].is_array()) {
                        return Option<bool>(400, "Document referenced in vector query does not contain a valid "
                                                 "vector field.");
                    }

                    for(auto& fvalue: document[vector_query.field_name]) {
                        if(!fvalue.is_number_float()) {
                            return Option<bool>(400, "Document referenced in vector query does not contain a valid "
                                                     "vector field.");
                        }

                        vector_query.values.push_back(fvalue.get<float>());
                    }

                    vector_query.query_doc_given = true;
                    vector_query.seq_id = id_op.get();
                }

                if(param_kv[0] == "k") {
                    if(!StringUtils::is_uint32_t(param_kv[1])) {
                        return Option<bool>(400, "Malformed vector query string: `k` parameter must be an integer.");
                    }

                    vector_query.k = std::stoul(param_kv[1]);
                }

                if(param_kv[0] == "flat_search_cutoff") {
                    if(!StringUtils::is_uint32_t(param_kv[1])) {
                        return Option<bool>(400, "Malformed vector query string: "
                                                 "`flat_search_cutoff` parameter must be an integer.");
                    }

                    vector_query.flat_search_cutoff = std::stoi(param_kv[1]);
                }

                if(param_kv[0] == "distance_threshold") {
                    if(!StringUtils::is_float(param_kv[1]) || std::stof(param_kv[1]) < 0.0 || std::stof(param_kv[1]) > 2.0) {
                        return Option<bool>(400, "Malformed vector query string: "
                                                 "`distance_threshold` parameter must be a float between 0.0-2.0.");
                    }

                    vector_query.distance_threshold = std::stof(param_kv[1]);
                }
            }

            if(is_wildcard_query && !vector_query.query_doc_given && vector_query.values.empty()) {
                return Option<bool>(400, "When a vector query value is empty, an `id` parameter must be present.");
            }

            return Option<bool>(true);
        }
    }

    return Option<bool>(400, "Malformed vector query string.");
}
