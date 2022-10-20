#pragma once

#include <cstdint>
#include <vector>
#include "id_list.h"
#include "threadpool.h"

#define IS_COMPACT_IDS(x) (((uintptr_t)(x) & 1))
#define SET_COMPACT_IDS(x) ((void*)((uintptr_t)(x) | 1))
#define RAW_IDS_PTR(x) ((void*)((uintptr_t)(x) & ~1))
#define COMPACT_IDS_PTR(x) ((compact_id_list_t*)((uintptr_t)(x) & ~1))

struct compact_id_list_t {
    // structured to get 4 byte alignment for `ids`
    uint8_t length = 0;
    uint16_t capacity = 0;

    // format: id1, id2,...
    uint32_t ids[];

    static compact_id_list_t* create(uint32_t num_ids, const std::vector<uint32_t>& ids);

    static compact_id_list_t* create(uint32_t num_ids, const uint32_t* ids);

    [[nodiscard]] id_list_t* to_full_ids_list() const;

    bool contains(uint32_t id);

    int64_t upsert(uint32_t id);

    void erase(uint32_t id);

    uint32_t first_id();
    uint32_t last_id();

    [[nodiscard]] uint32_t num_ids() const;

    bool contains_atleast_one(const uint32_t* target_ids, size_t target_ids_size);
};

class ids_t {
private:

    static void to_expanded_id_lists(const std::vector<void*>& raw_id_lists, std::vector<id_list_t*>& id_lists,
                                     std::vector<id_list_t*>& expanded_id_lists);

public:
    static constexpr size_t COMPACT_LIST_THRESHOLD_LENGTH = 64;
    static constexpr size_t MAX_BLOCK_ELEMENTS = 256;

    struct block_intersector_t {
        std::vector<id_list_t*> id_lists;
        std::vector<id_list_t*> expanded_id_lists;
        id_list_t::result_iter_state_t& iter_state;
        ThreadPool* thread_pool;
        size_t parallelize_min_ids;

        block_intersector_t(const std::vector<void*>& raw_id_lists,
                            id_list_t::result_iter_state_t& iter_state,
                            ThreadPool* thread_pool,
                            size_t parallelize_min_ids = 1):
                iter_state(iter_state), thread_pool(thread_pool),
                parallelize_min_ids(parallelize_min_ids) {

            to_expanded_id_lists(raw_id_lists, id_lists, expanded_id_lists);

            if(id_lists.size() > 1) {
                std::sort(this->id_lists.begin(), this->id_lists.end(), [](id_list_t* a, id_list_t* b) {
                    return a->num_blocks() < b->num_blocks();
                });
            }
        }

        ~block_intersector_t() {
            for(auto expanded_id_list: expanded_id_lists) {
                delete expanded_id_list;
            }
        }

        template<class T>
        bool intersect(T func, size_t concurrency=4);

        void split_lists(size_t concurrency, std::vector<std::vector<id_list_t::iterator_t>>& partial_its_vec);
    };

    static void upsert(void*& obj, uint32_t id);

    static void erase(void*& obj, uint32_t id);

    static void destroy_list(void*& obj);

    static uint32_t num_ids(const void* obj);

    static uint32_t first_id(const void* obj);

    static bool contains(const void* obj, uint32_t id);

    static bool contains_atleast_one(const void* obj, const uint32_t* target_ids, size_t target_ids_size);

    static void merge(const std::vector<void*>& id_lists, std::vector<uint32_t>& result_ids);

    static void intersect(const std::vector<void*>& id_lists, std::vector<uint32_t>& result_ids);

    static uint32_t* uncompress(void*& obj);

    static void uncompress(void*& obj, std::vector<uint32_t>& ids);
};

template<class T>
bool ids_t::block_intersector_t::intersect(T func, size_t concurrency) {
    // Split id lists into N chunks and intersect them in-parallel
    // 1. Sort id lists by number of blocks
    // 2. Iterate on the id list with least number of blocks on N-block windows
    // 3. On each window, pick the last block's last ID and identify blocks from other lists containing that ID
    // 4. Construct N groups of iterators this way (the last block must overlap on both sides of the window)

    if(id_lists.empty()) {
        return true;
    }

    if(id_lists[0]->num_ids() < parallelize_min_ids) {
        std::vector<id_list_t::iterator_t> its;
        its.reserve(id_lists.size());

        for(const auto& id_list: id_lists) {
            its.push_back(id_list->new_iterator());
        }

        id_list_t::block_intersect<T>(its, iter_state, func);
        return true;
    }

    std::vector<std::vector<id_list_t::iterator_t>> partial_its_vec(concurrency);
    split_lists(concurrency, partial_its_vec);

    size_t num_processed = 0;
    std::mutex m_process;
    std::condition_variable cv_process;
    size_t num_non_empty = 0;

    for(size_t i = 0; i < partial_its_vec.size(); i++) {
        auto& partial_its = partial_its_vec[i];

        if(partial_its.empty()) {
            continue;
        }

        num_non_empty++;

        thread_pool->enqueue([this, i, &func, &partial_its, &num_processed, &m_process, &cv_process]() {
            auto iter_state_copy = iter_state;
            iter_state_copy.index = i;
            id_list_t::block_intersect<T>(partial_its, iter_state_copy, func);
            std::unique_lock<std::mutex> lock(m_process);
            num_processed++;
            cv_process.notify_one();
        });
    }

    std::unique_lock<std::mutex> lock_process(m_process);
    cv_process.wait(lock_process, [&](){ return num_processed == num_non_empty; });

    return true;
}
