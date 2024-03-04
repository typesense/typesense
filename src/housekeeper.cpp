#include <collection_manager.h>
#include "housekeeper.h"

void HouseKeeper::run() {
    uint64_t prev_hnsw_repair_s = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    uint64_t prev_db_compaction_s = std::chrono::duration_cast<std::chrono::seconds>(
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

        // perform compaction on underlying store if enabled
        if(Config::get_instance().get_db_compaction_interval() > 0) {
            if(now_ts_seconds - prev_db_compaction_s >= Config::get_instance().get_db_compaction_interval()) {
                LOG(INFO) << "Starting DB compaction.";
                CollectionManager::get_instance().get_store()->compact_all();
                LOG(INFO) << "Finished DB compaction.";
                prev_db_compaction_s = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();
            }
        }

        /*if(now_ts_seconds - prev_hnsw_repair_s >= hnsw_repair_interval_s) {
            // iterate through all collections and repair all hnsw graphs (if any)
            auto coll_names = CollectionManager::get_instance().get_collection_names();

            for(auto& coll_name: coll_names) {
                auto coll = CollectionManager::get_instance().get_collection(coll_name);
                if(coll == nullptr) {
                    continue;
                }

                coll->do_housekeeping();
            }

            if(!coll_names.empty()) {
                LOG(INFO) << "Ran housekeeping for " << coll_names.size() << " collections.";
            }

            //do housekeeping for authmanager
            CollectionManager::get_instance().getAuthManager().do_housekeeping();

            prev_hnsw_repair_s = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
        }*/

        lk.unlock();
    }
}

void HouseKeeper::stop() {
    quit = true;
    cv.notify_all();
}

void HouseKeeper::init(uint32_t interval_seconds) {
    this->hnsw_repair_interval_s = interval_seconds;
}
