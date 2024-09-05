#include "vector_query_ops.h"
#include "string_utils.h"
#include "collection.h"

Option<bool> VectorQueryOps::parse_vector_query_str(const std::string& vector_query_str,
                                                    vector_query_t& vector_query,
                                                    const bool is_wildcard_query,
                                                    const Collection* coll,
                                                    const bool allow_empty_query) {
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
                if(vector_query.values.empty() && !allow_empty_query) {
                    // when query values are missing, atleast the `id` parameter must be present
                    return Option<bool>(400, "When a vector query value is empty, an `id` parameter must be present.");
                }

                return Option<bool>(true);
            }

            std::string param_str = vector_query_str.substr(i, (vector_query_str.size() - i));
            std::vector<std::string> param_kvs;
            StringUtils::split(param_str, param_kvs, ",");

            for(size_t i = 0; i < param_kvs.size(); i++) {
                auto& param_kv_str = param_kvs[i];
                if(param_kv_str.back() == ')') {
                    param_kv_str.pop_back();
                }

                std::vector<std::string> param_kv;
                StringUtils::split(param_kv_str, param_kv, ":");
                if(param_kv.size() != 2) {
                    return Option<bool>(400, "Malformed vector query string.");
                }

                if(i < param_kvs.size() - 1 && param_kv[1].front() == '[' && param_kv[1].back() != ']') {
                    /*
                    Currently, we parse vector query parameters by splitting them with commas (e.g., alpha:0.7, k:100). 
                    However, this approach has challenges when dealing with array parameters, where values are also separated by commas. 
                    For instance, with a vector query like embedding:([], qs:[x, y]), our logic may incorrectly parse it as qs:[x and y]) due to the comma separator.
                    
                    To address this issue, we have implemented a workaround. 
                    If a comma-separated vector query parameter has '['  as its first character and does not have ']' as its last character, this means that the parameter is not yet complete.
                    In this case, we append the current parameter to the next parameter, and continue parsing the next parameter.
                    */
                    param_kvs[i+1] = param_kv_str + "," + param_kvs[i+1];
                    continue;
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
                        if(!fvalue.is_number()) {
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
                    auto search_schema = const_cast<Collection*>(coll)->get_schema();
                    auto vector_field_it = search_schema.find(vector_query.field_name);

                    if(!StringUtils::is_float(param_kv[1])) {
                        return Option<bool>(400, "Malformed vector query string: "
                                                 "`distance_threshold` parameter must be a float.");
                    }

                    auto distance_threshold = std::stof(param_kv[1]);
                    if(vector_field_it->vec_dist == cosine && (distance_threshold < 0.0 || distance_threshold > 2.0)) {
                        return Option<bool>(400, "Malformed vector query string: "
                                                 "`distance_threshold` parameter must be a float between 0.0-2.0.");
                    }

                    vector_query.distance_threshold = distance_threshold;
                }

                if(param_kv[0] == "alpha") {
                    if(!StringUtils::is_float(param_kv[1]) || std::stof(param_kv[1]) < 0.0 || std::stof(param_kv[1]) > 1.0) {
                        return Option<bool>(400, "Malformed vector query string: "
                                                 "`alpha` parameter must be a float between 0.0-1.0.");
                    }

                    vector_query.alpha = std::stof(param_kv[1]);
                }

                if(param_kv[0] == "ef") {
                    if(!StringUtils::is_uint32_t(param_kv[1]) || std::stoul(param_kv[1]) == 0) {
                        return Option<bool>(400, "Malformed vector query string: `ef` parameter must be a positive integer.");
                    }

                    vector_query.ef = std::stoul(param_kv[1]);
                }

                if(param_kv[0] == "queries") {
                    if(param_kv[1].front() != '[' || param_kv[1].back() != ']') {
                        return Option<bool>(400, "Malformed vector query string: "
                                                 "`queries` parameter must be a list of strings.");
                    }

                    param_kv[1].erase(0, 1);
                    param_kv[1].pop_back();

                    std::vector<std::string> qs;
                    StringUtils::split(param_kv[1], qs, ",");
                    for(auto& q: qs) {
                        StringUtils::trim(q);
                        vector_query.queries.push_back(q);
                    }
                }

                if(param_kv[0] == "query_weights") {
                    if(param_kv[1].front() != '[' || param_kv[1].back() != ']') {
                        return Option<bool>(400, "Malformed vector query string: "
                                                 "`query_weights` parameter must be a list of floats.");
                    }

                    param_kv[1].erase(0, 1);
                    param_kv[1].pop_back();

                    std::vector<std::string> ws;
                    StringUtils::split(param_kv[1], ws, ",");
                    for(auto& w: ws) {
                        StringUtils::trim(w);
                        if(!StringUtils::is_float(w)) {
                            return Option<bool>(400, "Malformed vector query string: "
                                                     "`query_weights` parameter must be a list of floats.");
                        }

                        vector_query.query_weights.push_back(std::stof(w));
                    }
                }
            }

            if(vector_query.queries.size() != vector_query.query_weights.size() && !vector_query.query_weights.empty()) {
                return Option<bool>(400, "Malformed vector query string: "
                                         "`queries` and `query_weights` must be of the same length.");
            }

            if(!vector_query.query_weights.empty()) {
                float sum = 0.0;
                for(auto& w: vector_query.query_weights) {
                    sum += w;
                }

                if(sum != 1.0) {
                    return Option<bool>(400, "Malformed vector query string: "
                                             "`query_weights` must sum to 1.0.");
                }
            }

            return Option<bool>(true);
        }
    }

    return Option<bool>(400, "Malformed vector query string.");
}
