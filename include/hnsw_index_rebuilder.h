#pragma once

#include<index.h>
#include<mutex>
#include<condition_variable>
#include "collection_manager.h"

struct hnsw_index_rebuilder_thread {
    std::atomic<bool> quit = false;
    std::condition_variable cv;
    std::mutex mtx;

    void run() {
        while(!quit) {
            std::unique_lock lock(mtx);
            cv.wait_for(lock, std::chrono::seconds(30), [&] { return quit.load(); });

            if(quit) {
                return;
            }

            lock.unlock();
            rebuild_indexes();
        }
    }

    void rebuild_indexes() {
        auto& collection_manager = CollectionManager::get_instance();
        auto collections = collection_manager.get_collections();
        for(auto& collection: collections) {
            collection->rebuild_vector_indexes();
        }
    }

    void stop() {
        quit = true;
        cv.notify_all();
    }

};

class HNSWIndexRebuilder {
    private:
        hnsw_index_t* old_index;
        hnsw_index_t* new_index;
        std::mutex mtx;
    public:
        HNSWIndexRebuilder(hnsw_index_t* old_index) {
            this->old_index = old_index;
            this->new_index = new hnsw_index_t(old_index->num_dim, old_index->vecdex->getMaxElements(), old_index->distance_type, old_index->M, old_index->ef_construction, old_index->rebuild_index_interval);
        }

        hnsw_index_t* rebuild() {
            for (auto kv: old_index->vecdex->label_lookup_) {
                auto label = kv.first;
                auto values = old_index->vecdex->getDataByLabel<float>(label);
                std::unique_lock<std::mutex> lock(mtx);
                new_index->vecdex->addPoint(values.data(), label, true);
                if(old_index->vecdex->isMarkedDeleted(label)) {
                    new_index->vecdex->markDelete(label);
                }
            }

            return new_index;
        }

        void addPoint(const float* values, size_t label) {
            std::unique_lock<std::mutex> lock(mtx);
            new_index->vecdex->addPoint(values, label, true);
        }

        void markDelete(size_t label) {
            std::unique_lock<std::mutex> lock(mtx);
            new_index->vecdex->markDelete(label);
        }
};