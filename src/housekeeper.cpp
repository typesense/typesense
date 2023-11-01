#include <collection_manager.h>
#include "housekeeper.h"

void HouseKeeper::run() {
    uint64_t prev_persistence_s = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    while(!quit) {
        std::unique_lock lk(mutex);
        cv.wait_for(lk, std::chrono::seconds(60), [&] { return quit.load(); });

        if(quit) {
            lk.unlock();
            break;
        }

        auto now_ts_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        if(now_ts_seconds - prev_persistence_s < interval_seconds) {
            continue;
        }

        // iterate through all collections and repair all hnsw graphs
        auto coll_names = CollectionManager::get_instance().get_collection_names();

        for(auto& coll_name: coll_names) {
            auto coll = CollectionManager::get_instance().get_collection(coll_name);
            if(coll == nullptr) {
                continue;
            }

            coll->do_housekeeping();
            LOG(INFO) << "Ran housekeeping.";
        }

        prev_persistence_s = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        lk.unlock();
    }
}

void HouseKeeper::stop() {
    quit = true;
    cv.notify_all();
}

void HouseKeeper::init(uint32_t interval_seconds) {
    this->interval_seconds = interval_seconds;
}
