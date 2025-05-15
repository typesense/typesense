#include "core_api_utils.h"
#include "auth_manager.h"

Option<bool> stateful_remove_docs(deletion_state_t* deletion_state, size_t batch_size, bool& done) {
    bool removed = true;
    size_t batch_count = 0;

    for(size_t i=0; i<deletion_state->index_ids.size(); i++) {
        std::pair<size_t, uint32_t*>& size_ids = deletion_state->index_ids[i];
        size_t ids_len = size_ids.first;
        uint32_t* ids = size_ids.second;

        size_t start_index = deletion_state->offsets[i];
        size_t batched_len = std::min(ids_len, (start_index+batch_size));

        for(size_t j=start_index; j<batched_len; j++) {
            uint32_t seq_id = ids[j];

            nlohmann::json doc;
            bool doc_found = false;

            if (deletion_state->return_doc || deletion_state->return_id) {
                Option<bool> get_op = deletion_state->collection->get_document_from_store(seq_id, doc);
                doc_found = get_op.ok();
            }

            Option<bool> remove_op = deletion_state->collection->remove_if_found(seq_id, true);
            if (!remove_op.ok()) {
                return remove_op;
            }

            removed = remove_op.get();
            if (removed) {
                deletion_state->num_removed++;
                if (doc_found) {
                    if (deletion_state->return_doc) {
                        Collection::remove_flat_fields(doc);
                        deletion_state->removed_docs.push_back(doc);
                    }

                    if (deletion_state->return_id) {
                        deletion_state->removed_ids.push_back(doc["id"]);
                    }
                }
            }

            deletion_state->offsets[i]++;
            batch_count++;

            if(batch_count == batch_size) {
                goto END;
            }
        }
    }

    END:

    done = true;
    for(size_t i=0; i<deletion_state->index_ids.size(); i++) {
        size_t current_offset = deletion_state->offsets[i];
        done = done && (current_offset == deletion_state->index_ids[i].first);
    }

    return Option<bool>(removed);
}

Option<bool> stateful_export_docs(export_state_t* export_state, size_t batch_size, bool& done) {
    size_t batch_count = 0;

    export_state->res_body->clear();

    auto const& filter_result = export_state->filter_result;
    size_t ids_len = filter_result.count;
    uint32_t* ids = filter_result.docs;

    size_t start_index = export_state->offset;
    size_t batched_len = std::min(ids_len, (start_index+batch_size));

    for(size_t j = start_index; j < batched_len; j++) {
        uint32_t seq_id = ids[j];
        nlohmann::json doc;

        auto const& coll = export_state->collection;
        Option<bool> get_op = coll->get_document_from_store(seq_id, doc);
        Collection::remove_flat_fields(doc);
        Collection::remove_reference_helper_fields(doc);

        if(get_op.ok()) {
            std::map<std::string, reference_filter_result_t> refs;
            coll->prune_doc_with_lock(doc, export_state->include_fields, export_state->exclude_fields,
                                      (filter_result.coll_to_references == nullptr ? refs :
                                       filter_result.coll_to_references[j]), seq_id,
                                      export_state->ref_include_exclude_fields_vec);
            export_state->res_body->append(doc.dump());

            export_state->res_body->append("\n");
        }

        export_state->offset++;
        batch_count++;

        if(batch_count == batch_size) {
            goto END;
        }
    }

    END:

    done = export_state->offset == export_state->filter_result.count;

    if(done && !export_state->res_body->empty()) {
        export_state->res_body->pop_back();
    }

    return Option<bool>(true);
}

Option<bool> multi_search_validate_and_add_params(std::map<std::string, std::string>& req_params,
                                                  nlohmann::json& search_params, const bool& is_conversation) {
    if(!search_params.is_object()) {
        return Option<bool>(400, "The value of `searches` must be an array of objects.");
    }

    for(auto const& search_item: search_params.items()) {
        if(search_item.key() == "cache_ttl") {
            // cache ttl can be applied only from an embedded key: cannot be a multi search param
            continue;
        }

        if(is_conversation && search_item.key() == "q") {
            // q is common for all searches
            return Option<bool>(400, "`q` parameter cannot be used in POST body if `conversation` is enabled."
                                     " Please set `q` as a query parameter in the request, instead of inside the POST body");
        }

        if(is_conversation && search_item.key() == "conversation_model_id") {
            // conversation_model_id is common for all searches
            return Option<bool>(400, "`conversation_model_id` cannot be used in POST body. Please set `conversation_model_id`"
                                     " as a query parameter in the request, instead of inside the POST body");
        }

        if(is_conversation && search_item.key() == "conversation_id") {
            // conversation_id is common for all searches
            return Option<bool>(400, "`conversation_id` cannot be used in POST body. Please set `conversation_id` as a"
                                     " query parameter in the request, instead of inside the POST body");
        }

        if(search_item.key() == "conversation") {
            return Option<bool>(400, "`conversation` cannot be used in POST body. Please set `conversation` as a query"
                                     " parameter in the request, instead of inside the POST body");
        }

        // overwrite = false since req params will contain embedded params and so has higher priority
        bool populated = AuthManager::add_item_to_params(req_params, search_item, false);
        if(!populated) {
            return Option<bool>(400, "One or more search parameters are malformed.");
        }
    }

    if(req_params.count("conversation") != 0) {
        req_params.erase("conversation");
    }

    if(req_params.count("conversation_id") != 0) {
        req_params.erase("conversation_id");
    }

    if(req_params.count("conversation_model_id") != 0) {
        req_params.erase("conversation_model_id");
    }

    return Option<bool>(true);
}