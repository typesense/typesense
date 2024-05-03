#pragma once

#include <vector>
#include <filter_result_iterator.h>
#include "posting_list.h"

/*
 *  Takes a list of posting list iterators and returns an unique OR sequence of elements lazily
 */
class or_iterator_t {
private:
    std::vector<posting_list_t::iterator_t> its;
    int curr_index = 0;

    void advance_smallest();

public:
    explicit or_iterator_t(std::vector<posting_list_t::iterator_t>& its);

    or_iterator_t(or_iterator_t&& rhs) noexcept;
    or_iterator_t& operator=(or_iterator_t&& rhs) noexcept;
    ~or_iterator_t() noexcept;

    // utility methods for manipulating groups of iterators

    static bool at_end(const std::vector<or_iterator_t>& its);
    static bool at_end2(const std::vector<or_iterator_t>& its);

    static bool equals(std::vector<or_iterator_t>& its);
    static bool equals2(std::vector<or_iterator_t>& its);

    static void advance_all(std::vector<or_iterator_t>& its);
    static void advance_all2(std::vector<or_iterator_t>& its);

    static void advance_non_largest(std::vector<or_iterator_t>& its);
    static void advance_non_largest2(std::vector<or_iterator_t>& its);

    // actual iterator operations

    [[nodiscard]] bool valid() const;

    bool next();

    bool skip_to(uint32_t id);

    [[nodiscard]] uint32_t id() const;

    [[nodiscard]] const std::vector<posting_list_t::iterator_t>& get_its() const;

    static bool take_id(result_iter_state_t& istate, uint32_t id, bool& is_excluded);

    static bool take_id(result_iter_state_t& istate, uint32_t id, bool& is_excluded, single_filter_result_t& filter_result);

    template<class T>
    static bool intersect(std::vector<or_iterator_t>& its, result_iter_state_t& istate, T func);

    static bool contains_atleast_one(std::vector<or_iterator_t>& its, result_iter_state_t&& istate);
};

template<class T>
bool or_iterator_t::intersect(std::vector<or_iterator_t>& its, result_iter_state_t& istate, T func) {
    size_t it_size = its.size();
    size_t num_processed = 0;
    bool is_excluded = false;

    switch (its.size()) {
        case 0:
            break;
        case 1:
            if(istate.is_filter_provided() && istate.is_filter_valid()) {
                its[0].skip_to(istate.get_filter_id());
            }

            while(its.size() == it_size && its[0].valid()) {
                num_processed++;
                if (num_processed % 65536 == 0 && (std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count() - search_begin_us) > search_stop_us) {
                    search_cutoff = true;
                    break;
                }

                auto id = its[0].id();
                single_filter_result_t filter_result;
                if(take_id(istate, id, is_excluded, filter_result)) {
                    func(filter_result, its);
                }

                if(istate.is_filter_provided() && !is_excluded) {
                    if(istate.is_filter_valid()) {
                        // skip iterator till next id available in filter
                        its[0].skip_to(istate.get_filter_id());
                    } else {
                        break;
                    }
                } else {
                    its[0].next();
                }
            }
            break;
        case 2:
            if(istate.is_filter_provided() && istate.is_filter_valid()) {
                its[0].skip_to(istate.get_filter_id());
                its[1].skip_to(istate.get_filter_id());
            }

            while(its.size() == it_size && !at_end2(its)) {
                num_processed++;
                if (num_processed % 65536 == 0 && (std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count() - search_begin_us) > search_stop_us) {
                    search_cutoff = true;
                    break;
                }

                if(equals2(its)) {
                    auto id = its[0].id();
                    single_filter_result_t filter_result;
                    if(take_id(istate, id, is_excluded, filter_result)) {
                        func(filter_result, its);
                    }

                    if(istate.is_filter_provided() != 0 && !is_excluded) {
                        if(istate.is_filter_valid()) {
                            // skip iterator till next id available in filter
                            its[0].skip_to(istate.get_filter_id());
                            its[1].skip_to(istate.get_filter_id());
                        } else {
                            break;
                        }
                    } else {
                        advance_all2(its);
                    }
                } else {
                    advance_non_largest2(its);
                }
            }
            break;
        default:
            if(istate.is_filter_provided() && istate.is_filter_valid()) {
                for(auto& it: its) {
                    it.skip_to(istate.get_filter_id());
                }
            }

            while(its.size() == it_size && !at_end(its)) {
                num_processed++;
                if (num_processed % 65536 == 0 && (std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count() - search_begin_us) > search_stop_us) {
                    search_cutoff = true;
                    break;
                }

                if(equals(its)) {
                    auto id = its[0].id();
                    single_filter_result_t filter_result;
                    if(take_id(istate, id, is_excluded, filter_result)) {
                        func(filter_result, its);
                    }

                    if(istate.is_filter_provided() && !is_excluded) {
                        if(istate.is_filter_valid()) {
                            // skip iterator till next id available in filter
                            for(auto& it: its) {
                                it.skip_to(istate.get_filter_id());
                            }
                        } else {
                            break;
                        }
                    } else {
                        advance_all(its);
                    }
                } else {
                    advance_non_largest(its);
                }
            }
    }

    return true;
}