#include "or_iterator.h"


bool or_iterator_t::at_end(const std::vector<or_iterator_t>& its) {
    // if any iterator is empty, we stop
    for(const auto& it : its) {
        if(it.valid()) {
            return false;
        }
    }

    return true;
}

bool or_iterator_t::at_end2(const std::vector<or_iterator_t>& its) {
    // if any iterator is invalid, we stop
    return !its[0].valid() || !its[1].valid();
}

bool or_iterator_t::equals(std::vector<or_iterator_t>& its) {
    for(int i = 0; i < int(its.size()) - 1; i++) {
        if(its[i].id() != its[i+1].id()) {
            return false;
        }
    }

    return true;
}

bool or_iterator_t::equals2(std::vector<or_iterator_t>& its) {
    return its[0].id() == its[1].id();
}


void or_iterator_t::advance_all(std::vector<or_iterator_t>& its) {
    for(size_t i = 0; i < its.size(); i++) {
        auto& it = its[i];
        bool valid = it.next();
        if(!valid) {
            its.erase(its.begin() + i);
        }
    }
}

void or_iterator_t::advance_all2(std::vector<or_iterator_t>& its) {
    bool valid0 = its[0].next();
    bool valid1 = its[1].next();

    if(!valid0) {
        its.erase(its.begin() + 0);
        if(!valid1) {
            // 1st index will be now at 0th index
            its.erase(its.begin() + 0);
        }
    } else if(!valid1) {
        its.erase(its.begin() + 1);
    }
}

void or_iterator_t::advance_non_largest(std::vector<or_iterator_t>& its) {
    // we will find the iter with greatest value and then advance the rest until their value catches up
    uint32_t greatest_value = 0;

    for(size_t i = 0; i < its.size(); i++) {
        if(its[i].id() > greatest_value) {
            greatest_value = its[i].id();
        }
    }

    for(size_t i = 0; i < its.size(); i++) {
        if(its[i].id() != greatest_value) {
            bool valid = its[i].skip_to(greatest_value);
            if(!valid) {
                its.erase(its.begin() + i);
                i--;
            }
        }
    }
}

void or_iterator_t::advance_non_largest2(std::vector<or_iterator_t>& its) {
    if(its[0].id() > its[1].id()) {
        bool valid = its[1].skip_to(its[0].id());
        if(!valid) {
            its.erase(its.begin() + 1);
        }
    } else {
        bool valid = its[0].skip_to(its[1].id());
        if(!valid) {
            its.erase(its.begin() + 0);
        }
    }
}

bool or_iterator_t::valid() const {
    return !its.empty();
}

bool or_iterator_t::next() {
    size_t num_lists = its.size();

    switch (num_lists) {
        case 0:
            break;
        case 2:
            if(!posting_list_t::all_ended2(its)) {
                advance_smallest();
            }
            break;
        default:
            if(!posting_list_t::all_ended(its)) {
                advance_smallest();
            }
            break;
    }

    return !its.empty();
}

void or_iterator_t::advance_smallest() {
    // we will advance the smallest value and point current_index to next smallest value across the lists
    auto smallest_value = its[curr_index].id();
    curr_index = 0;

    for(int i = 0; i < int(its.size()); i++) {
        if(its[i].id() == smallest_value) {
            its[i].next();
        }

        if(!its[i].valid()) {
            its[i].destroy();
            its.erase(its.cbegin() + i);
            i--;
        }
    }

    uint32_t new_smallest_value = UINT32_MAX;
    for(int i = 0; i < int(its.size()); i++) {
        if(its[i].id() < new_smallest_value) {
            curr_index = i;
            new_smallest_value = its[i].id();
        }
    }
}

bool or_iterator_t::skip_to(uint32_t id) {
    auto current_value = UINT32_MAX;
    curr_index = 0;

    for(size_t i = 0; i < its.size(); i++) {
        auto& it = its[i];
        it.skip_to(id);

        if(!it.valid()) {
            its[i].destroy();
            its.erase(its.begin() + i);
            i--;
        } else {
            if(it.id() < current_value) {
                curr_index = i;
                current_value = it.id();
            }
        }
    }

    return !its.empty();
}

uint32_t or_iterator_t::id() const {
    return its[curr_index].id();
}

bool or_iterator_t::take_id(result_iter_state_t& istate, uint32_t id) {
    // decide if this result id should be excluded
    if(istate.excluded_result_ids_size != 0) {
        if (std::binary_search(istate.excluded_result_ids,
                               istate.excluded_result_ids + istate.excluded_result_ids_size, id)) {
            return false;
        }
    }

    // decide if this result be matched with filter results
    if(istate.filter_ids_length != 0) {
        return std::binary_search(istate.filter_ids, istate.filter_ids + istate.filter_ids_length, id);
    }

    return true;
}

or_iterator_t::or_iterator_t(std::vector<posting_list_t::iterator_t>& its): its(std::move(its)) {
    curr_index = 0;

    for(size_t i = 1; i < this->its.size(); i++) {
        if(this->its[i].id() < this->its[curr_index].id()) {
            curr_index = i;
        }
    }
}

or_iterator_t::or_iterator_t(or_iterator_t&& rhs) noexcept {
    its = std::move(rhs.its);
    curr_index = rhs.curr_index;
}

or_iterator_t& or_iterator_t::operator=(or_iterator_t&& rhs) noexcept {
    its = std::move(rhs.its);
    curr_index = rhs.curr_index;
    return *this;
}

const std::vector<posting_list_t::iterator_t>& or_iterator_t::get_its() const {
    return its;
}

or_iterator_t::~or_iterator_t() noexcept {
    for(auto& it: its) {
        it.destroy();
    }
}


