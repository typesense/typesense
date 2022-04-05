#pragma once

#include <vector>
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

    static bool take_id(result_iter_state_t& istate, uint32_t id);

    template<class T>
    static bool intersect(std::vector<or_iterator_t>& its, result_iter_state_t& istate, T func);
};

template<class T>
bool or_iterator_t::intersect(std::vector<or_iterator_t>& its, result_iter_state_t& istate, T func) {
    size_t it_size = its.size();

    switch (its.size()) {
        case 0:
            break;
        case 1:
            while(its.size() == it_size && its[0].valid()) {
                auto id = its[0].id();
                if(take_id(istate, id)) {
                    func(id, its);
                }

                its[0].next();
            }
            break;
        case 2:
            while(its.size() == it_size && !at_end2(its)) {
                if(equals2(its)) {
                    auto id = its[0].id();
                    if(take_id(istate, id)) {
                        func(id, its);
                    }

                    advance_all2(its);
                } else {
                    advance_non_largest2(its);
                }
            }
            break;
        default:
            while(its.size() == it_size && !at_end(its)) {
                if(equals(its)) {
                    //LOG(INFO) << its[0].id();
                    auto id = its[0].id();
                    if(take_id(istate, its[0].id())) {
                        func(id, its);
                    }

                    advance_all(its);
                } else {
                    advance_non_largest(its);
                }
            }
    }

    return false;
}
