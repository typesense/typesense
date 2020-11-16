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