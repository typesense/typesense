#include <collection_manager.h>
#include <posting.h>
#include <timsort.hpp>
#include "filter.h"

void filter_result_iterator_t::and_filter_iterators() {
    while (left_it->is_valid && right_it->is_valid) {
        while (left_it->seq_id < right_it->seq_id) {
            left_it->next();
            if (!left_it->is_valid) {
                is_valid = false;
                return;
            }
        }

        while (left_it->seq_id > right_it->seq_id) {
            right_it->next();
            if (!right_it->is_valid) {
                is_valid = false;
                return;
            }
        }

        if (left_it->seq_id == right_it->seq_id) {
            seq_id = left_it->seq_id;
            reference.clear();

            for (const auto& item: left_it->reference) {
                reference[item.first] = item.second;
            }
            for (const auto& item: right_it->reference) {
                reference[item.first] = item.second;
            }

            return;
        }
    }

    is_valid = false;
}

void filter_result_iterator_t::or_filter_iterators() {
    if (left_it->is_valid && right_it->is_valid) {
        if (left_it->seq_id < right_it->seq_id) {
            seq_id = left_it->seq_id;
            reference.clear();

            for (const auto& item: left_it->reference) {
                reference[item.first] = item.second;
            }

            return;
        }

        if (left_it->seq_id > right_it->seq_id) {
            seq_id = right_it->seq_id;
            reference.clear();

            for (const auto& item: right_it->reference) {
                reference[item.first] = item.second;
            }

            return;
        }

        seq_id = left_it->seq_id;
        reference.clear();

        for (const auto& item: left_it->reference) {
            reference[item.first] = item.second;
        }
        for (const auto& item: right_it->reference) {
            reference[item.first] = item.second;
        }

        return;
    }

    if (left_it->is_valid) {
        seq_id = left_it->seq_id;
        reference.clear();

        for (const auto& item: left_it->reference) {
            reference[item.first] = item.second;
        }

        return;
    }

    if (right_it->is_valid) {
        seq_id = right_it->seq_id;
        reference.clear();

        for (const auto& item: right_it->reference) {
            reference[item.first] = item.second;
        }

        return;
    }

    is_valid = false;
}

void filter_result_iterator_t::doc_matching_string_filter() {
    // If none of the filter value iterators are valid, mark this node as invalid.
    bool one_is_valid = false;

    // Since we do OR between filter values, the lowest seq_id id from all is selected.
    uint32_t lowest_id = UINT32_MAX;

    for (auto& filter_value_tokens : posting_list_iterators) {
        // Perform AND between tokens of a filter value.
        bool tokens_iter_is_valid;
        posting_list_t::intersect(filter_value_tokens, tokens_iter_is_valid);

        one_is_valid = tokens_iter_is_valid || one_is_valid;

        if (tokens_iter_is_valid && filter_value_tokens[0].id() < lowest_id) {
            lowest_id = filter_value_tokens[0].id();
        }
    }

    if (one_is_valid) {
        seq_id = lowest_id;
    }

    is_valid = one_is_valid;
}

void filter_result_iterator_t::next() {
    if (!is_valid) {
        return;
    }

    if (filter_node->isOperator) {
        // Advance the subtrees and then apply operators to arrive at the next valid doc.
        if (filter_node->filter_operator == AND) {
            left_it->next();
            right_it->next();
            and_filter_iterators();
        } else {
            if (left_it->seq_id == seq_id && right_it->seq_id == seq_id) {
                left_it->next();
                right_it->next();
            } else if (left_it->seq_id == seq_id) {
                left_it->next();
            } else {
                right_it->next();
            }

            or_filter_iterators();
        }

        return;
    }

    const filter a_filter = filter_node->filter_exp;

    bool is_referenced_filter = !a_filter.referenced_collection_name.empty();
    if (is_referenced_filter) {
        if (++result_index >= filter_result.count) {
            is_valid = false;
            return;
        }

        seq_id = filter_result.docs[result_index];
        reference.clear();
        for (auto const& item: filter_result.reference_filter_results) {
            reference[item.first] = item.second[result_index];
        }

        return;
    }

    if (a_filter.field_name == "id") {
        if (++result_index >= filter_result.count) {
            is_valid = false;
            return;
        }

        seq_id = filter_result.docs[result_index];
        return;
    }

    if (!index->field_is_indexed(a_filter.field_name)) {
        is_valid = false;
        return;
    }

    field f = index->search_schema.at(a_filter.field_name);

    if (f.is_string()) {
        // Advance all the filter values that are at doc. Then find the next doc.
        for (uint32_t i = 0; i < posting_list_iterators.size(); i++) {
            auto& filter_value_tokens = posting_list_iterators[i];

            if (filter_value_tokens[0].valid() && filter_value_tokens[0].id() == seq_id) {
                for (auto& iter: filter_value_tokens) {
                    iter.next();
                }
            }
        }

        doc_matching_string_filter();
        return;
    }
}

void filter_result_iterator_t::init() {
    if (filter_node->isOperator) {
        if (filter_node->filter_operator == AND) {
            and_filter_iterators();
        } else {
            or_filter_iterators();
        }

        return;
    }

    const filter a_filter = filter_node->filter_exp;

    bool is_referenced_filter = !a_filter.referenced_collection_name.empty();
    if (is_referenced_filter) {
        // Apply filter on referenced collection and get the sequence ids of current collection from the filtered documents.
        auto& cm = CollectionManager::get_instance();
        auto collection = cm.get_collection(a_filter.referenced_collection_name);
        if (collection == nullptr) {
            status = Option<bool>(400, "Referenced collection `" + a_filter.referenced_collection_name + "` not found.");
            is_valid = false;
            return;
        }

        auto reference_filter_op = collection->get_reference_filter_ids(a_filter.field_name,
                                                                        filter_result,
                                                                        collection_name);
        if (!reference_filter_op.ok()) {
            status = Option<bool>(400, "Failed to apply reference filter on `" + a_filter.referenced_collection_name
                                       + "` collection: " + reference_filter_op.error());
            is_valid = false;
            return;
        }

        is_valid = filter_result.count > 0;
        return;
    }

    if (a_filter.field_name == "id") {
        if (a_filter.values.empty()) {
            is_valid = false;
            return;
        }

        // we handle `ids` separately
        std::vector<uint32_t> result_ids;
        for (const auto& id_str : a_filter.values) {
            result_ids.push_back(std::stoul(id_str));
        }

        std::sort(result_ids.begin(), result_ids.end());

        filter_result.count = result_ids.size();
        filter_result.docs = new uint32_t[result_ids.size()];
        std::copy(result_ids.begin(), result_ids.end(), filter_result.docs);
    }

    if (!index->field_is_indexed(a_filter.field_name)) {
        is_valid = false;
        return;
    }

    field f = index->search_schema.at(a_filter.field_name);

    if (f.is_string()) {
        art_tree* t = index->search_index.at(a_filter.field_name);

        for (const std::string& filter_value : a_filter.values) {
            std::vector<void*> posting_lists;

            // there could be multiple tokens in a filter value, which we have to treat as ANDs
            // e.g. country: South Africa
            Tokenizer tokenizer(filter_value, true, false, f.locale, index->symbols_to_index, index->token_separators);

            std::string str_token;
            size_t token_index = 0;
            std::vector<std::string> str_tokens;

            while (tokenizer.next(str_token, token_index)) {
                str_tokens.push_back(str_token);

                art_leaf* leaf = (art_leaf *) art_search(t, (const unsigned char*) str_token.c_str(),
                                                         str_token.length()+1);
                if (leaf == nullptr) {
                    continue;
                }

                posting_lists.push_back(leaf->values);
            }

            if (posting_lists.size() != str_tokens.size()) {
                continue;
            }

            std::vector<posting_list_t*> plists;
            posting_t::to_expanded_plists(posting_lists, plists, expanded_plists);

            posting_list_iterators.emplace_back(std::vector<posting_list_t::iterator_t>());

            for (auto const& plist: plists) {
                posting_list_iterators.back().push_back(plist->new_iterator());
            }
        }

        doc_matching_string_filter();
        return;
    }
}

bool filter_result_iterator_t::valid() {
    if (!is_valid) {
        return false;
    }

    if (filter_node->isOperator) {
        if (filter_node->filter_operator == AND) {
            is_valid = left_it->valid() && right_it->valid();
            return is_valid;
        } else {
            is_valid = left_it->valid() || right_it->valid();
            return is_valid;
        }
    }

    const filter a_filter = filter_node->filter_exp;

    if (!a_filter.referenced_collection_name.empty() || a_filter.field_name == "id") {
        is_valid = result_index < filter_result.count;
        return is_valid;
    }

    if (!index->field_is_indexed(a_filter.field_name)) {
        is_valid = false;
        return is_valid;
    }

    field f = index->search_schema.at(a_filter.field_name);

    if (f.is_string()) {
        bool one_is_valid = false;
        for (auto& filter_value_tokens: posting_list_iterators) {
            posting_list_t::intersect(filter_value_tokens, one_is_valid);

            if (one_is_valid) {
                break;
            }
        }

        is_valid = one_is_valid;
        return is_valid;
    }

    return false;
}

void filter_result_iterator_t::skip_to(uint32_t id) {
    if (!is_valid) {
        return;
    }

    if (filter_node->isOperator) {
        // Skip the subtrees to id and then apply operators to arrive at the next valid doc.
        left_it->skip_to(id);
        right_it->skip_to(id);

        if (filter_node->filter_operator == AND) {
            and_filter_iterators();
        } else {
            or_filter_iterators();
        }

        return;
    }

    const filter a_filter = filter_node->filter_exp;

    bool is_referenced_filter = !a_filter.referenced_collection_name.empty();
    if (is_referenced_filter) {
        while (filter_result.docs[result_index] < id && ++result_index < filter_result.count);

        if (result_index >= filter_result.count) {
            is_valid = false;
            return;
        }

        seq_id = filter_result.docs[result_index];
        reference.clear();
        for (auto const& item: filter_result.reference_filter_results) {
            reference[item.first] = item.second[result_index];
        }

        return;
    }

    if (a_filter.field_name == "id") {
        while (filter_result.docs[result_index] < id && ++result_index < filter_result.count);

        if (result_index >= filter_result.count) {
            is_valid = false;
            return;
        }

        seq_id = filter_result.docs[result_index];
        return;
    }

    if (!index->field_is_indexed(a_filter.field_name)) {
        is_valid = false;
        return;
    }

    field f = index->search_schema.at(a_filter.field_name);

    if (f.is_string()) {
        // Skip all the token iterators and find a new match.
        for (auto& filter_value_tokens : posting_list_iterators) {
            for (auto& token: filter_value_tokens) {
                // We perform AND on tokens. Short-circuiting here.
                if (!token.valid()) {
                    break;
                }

                token.skip_to(id);
            }
        }

        doc_matching_string_filter();
        return;
    }
}

int filter_result_iterator_t::valid(uint32_t id) {
    if (!is_valid) {
        return -1;
    }

    if (filter_node->isOperator) {
        auto left_valid = left_it->valid(id), right_valid = right_it->valid(id);

        if (filter_node->filter_operator == AND) {
            is_valid = left_it->is_valid && right_it->is_valid;

            if (left_valid  < 1 || right_valid < 1) {
                if (left_valid == -1 || right_valid == -1) {
                    return -1;
                }

                return 0;
            }

            return 1;
        } else {
            is_valid = left_it->is_valid || right_it->is_valid;

            if (left_valid < 1 && right_valid < 1) {
                if (left_valid == -1 && right_valid == -1) {
                    return -1;
                }

                return 0;
            }

            return 1;
        }
    }

    if (filter_node->filter_exp.apply_not_equals) {
        // Even when iterator becomes invalid, we keep it marked as valid since we are evaluating not equals.
        if (!valid()) {
            is_valid = true;
            return 1;
        }

        skip_to(id);

        if (!is_valid) {
            is_valid = true;
            return 1;
        }

        return seq_id != id ? 1 : 0;
    }

    skip_to(id);
    return is_valid ? (seq_id == id ? 1 : 0) : -1;
}

Option<bool> filter_result_iterator_t::init_status() {
    if (filter_node->isOperator) {
        auto left_status = left_it->init_status();

        return !left_status.ok() ? left_status : right_it->init_status();
    }

    return status;
}

bool filter_result_iterator_t::contains_atleast_one(const void *obj) {
    if(IS_COMPACT_POSTING(obj)) {
        compact_posting_list_t* list = COMPACT_POSTING_PTR(obj);

        size_t i = 0;
        while(i < list->length && valid()) {
            size_t num_existing_offsets = list->id_offsets[i];
            size_t existing_id = list->id_offsets[i + num_existing_offsets + 1];

            if (existing_id == seq_id) {
                return true;
            }

            // advance smallest value
            if (existing_id < seq_id) {
                i += num_existing_offsets + 2;
            } else {
                skip_to(existing_id);
            }
        }
    } else {
        auto list = (posting_list_t*)(obj);
        posting_list_t::iterator_t it = list->new_iterator();

        while(it.valid() && valid()) {
            uint32_t id = it.id();

            if(id == seq_id) {
                return true;
            }

            if(id < seq_id) {
                it.skip_to(seq_id);
            } else {
                skip_to(id);
            }
        }
    }

    return false;
}