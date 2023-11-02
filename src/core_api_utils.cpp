#include "core_api_utils.h"

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
            Option<bool> remove_op = deletion_state->collection->remove_if_found(ids[j], true);

            if(!remove_op.ok()) {
                return remove_op;
            }

            removed = remove_op.get();
            if(removed) {
                deletion_state->num_removed++;
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

    for(size_t i = 0; i < export_state->index_ids.size(); i++) {
        std::pair<size_t, uint32_t*>& size_ids = export_state->index_ids[i];
        size_t ids_len = size_ids.first;
        uint32_t* ids = size_ids.second;

        size_t start_index = export_state->offsets[i];
        size_t batched_len = std::min(ids_len, (start_index+batch_size));

        for(size_t j = start_index; j < batched_len; j++) {
            auto seq_id = ids[j];
            nlohmann::json doc;
            Option<bool> get_op = export_state->collection->get_document_from_store(seq_id, doc);

            if(get_op.ok()) {
                if(export_state->include_fields.empty() && export_state->exclude_fields.empty()) {
                    export_state->res_body->append(doc.dump());
                } else {
                    Collection::remove_flat_fields(doc);
                    Collection::remove_reference_helper_fields(doc);
                    Collection::prune_doc(doc, export_state->include_fields, export_state->exclude_fields);
                    export_state->res_body->append(doc.dump());
                }

                export_state->res_body->append("\n");
            }

            export_state->offsets[i]++;
            batch_count++;

            if(batch_count == batch_size) {
                goto END;
            }
        }
    }

    END:

    done = true;
    for(size_t i=0; i<export_state->index_ids.size(); i++) {
        size_t current_offset = export_state->offsets[i];
        done = done && (current_offset == export_state->index_ids[i].first);
    }

    if(done && !export_state->res_body->empty()) {
        export_state->res_body->pop_back();
    }

    return Option<bool>(true);
}