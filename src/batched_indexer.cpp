#include <collection_manager.h>
#include "batched_indexer.h"
#include "core_api.h"
#include "thread_local_vars.h"
#include "cached_resource_stat.h"

BatchedIndexer::BatchedIndexer(HttpServer* server, Store* store, Store* meta_store, const size_t num_threads,
                               const Config& config, const std::atomic<bool>& skip_writes):
                               server(server), store(store), meta_store(meta_store), num_threads(num_threads),
                               last_gc_run(std::chrono::high_resolution_clock::now()), quit(false),
                               config(config), skip_writes(skip_writes) {
    queues.resize(num_threads);
    qmutuxes = new await_t[num_threads];
    skip_index_iter_upper_bound = new rocksdb::Slice(skip_index_upper_bound_key);
}

void get_ref_coll_names(const std::string& body, std::unordered_set<std::string>& referenced_collections) {
    auto const& obj = nlohmann::json::parse(body, nullptr, false);

    if (!obj.is_discarded() && obj.is_object() && obj.contains("name") && obj["name"].is_string() &&
        obj.contains("fields")) {
        for (const auto &field: obj["fields"]) {
            if (!field.contains("reference")) {
                continue;
            }

            std::vector<std::string> split_result;
            StringUtils::split(field["reference"], split_result, ".");
            referenced_collections.insert(split_result.front());
        }
    }
}

void BatchedIndexer::enqueue(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    // Called by the raft write thread: goal is to quickly send the request to a queue and move on
    // NOTE: it's ok to access `req` and `res` in this function without synchronization
    // because the read thread for *this* request is paused now and resumes only messaged at the end

    //LOG(INFO) << "BatchedIndexer::enqueue";
    uint32_t chunk_sequence = 0;

    {
        uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        std::unique_lock lk(mutex);
        auto req_res_map_it = req_res_map.find(req->start_ts);

        if(req_res_map_it == req_res_map.end()) {
            // first chunk
            req_res_t req_res(req->start_ts, "", req, res, now, 1, 0, false);
            req_res_map.emplace(req->start_ts, req_res);
        } else {
            chunk_sequence = req_res_map_it->second.num_chunks;
            req_res_map_it->second.num_chunks += 1;
            req_res_map_it->second.last_updated = now;
        }
    }

    const std::string& req_key_prefix = get_req_prefix_key(req->start_ts);
    const std::string& request_chunk_key = req_key_prefix + StringUtils::serialize_uint32_t(chunk_sequence);

    //LOG(INFO) << "request_chunk_key: " << req->start_ts << "_" << chunk_sequence << ", req body: " << req->body;

    store->insert(request_chunk_key, req->to_json());

    bool is_old_serialized_request = (req->start_ts == 0);
    bool read_more_input = (req->_req != nullptr && req->_req->proceed_req);

    if(req->last_chunk_aggregate) {
        //LOG(INFO) << "Last chunk for req_id: " << req->start_ts;
        queued_writes += (chunk_sequence + 1);

        {
            auto const& coll_name = get_collection_name(req);
            uint64_t queue_id = StringUtils::hash_wy(coll_name.c_str(), coll_name.size()) % num_threads;
            req->body = "";

            {
                std::lock_guard lk(mutex);
                req_res_map[req->start_ts].is_complete = true;
            }

            {
                std::lock_guard lk(qmutuxes[queue_id].mcv);
                queues[queue_id].emplace_back(req->start_ts);
            }

            if (!res->is_alive && is_coll_create_route(req->route_hash)) {
                // Save reference mapping in case of replays to take care of ordering
                std::unordered_set<std::string> referenced_collections;
                get_ref_coll_names(req->body, referenced_collections);
                if (!referenced_collections.empty()) {
                    std::lock_guard lock(mutex);
                    coll_to_references[coll_name] = std::move(referenced_collections);
                }
            }

            qmutuxes[queue_id].cv.notify_one();
        }

        // IMPORTANT: must not read `req` variables (except _req) henceforth to prevent data races with indexing thread

        if(is_old_serialized_request) {
            // Indicates a serialized request from a version that did not support batching (v0.21 and below).
            // We can only do serial writes as we cannot reliably distinguish one streaming request from another.
            // So, wait for `req_res_map` to be empty before proceeding
            while(true) {
                {
                    std::lock_guard lk(mutex);
                    if(req_res_map.empty()) {
                        break;
                    }
                }

                std::this_thread::sleep_for(std::chrono::milliseconds (10));
            }
        }
    } else {
        req->body = "";
    }

    if(read_more_input) {
        // Tell the http library to read more input data
        deferred_req_res_t* req_res = new deferred_req_res_t(req, res, server, true);
        server->get_message_dispatcher()->send_message(HttpServer::REQUEST_PROCEED_MESSAGE, req_res);
    }
}

std::string BatchedIndexer::get_collection_name(const std::shared_ptr<http_req>& req) {
    std::string& coll_name = req->params["collection"];

    if(coll_name.empty()) {
        route_path* rpath = nullptr;
        bool route_found = server->get_route(req->route_hash, &rpath);

        // ensure that collection creation is sent to the same queue as writes to that collection
        if(route_found && rpath->handler == post_create_collection) {
            nlohmann::json obj = nlohmann::json::parse(req->body, nullptr, false);

            if(!obj.is_discarded() && obj.is_object() &&
               obj.count("name") != 0 && obj["name"].is_string()) {
                coll_name = obj["name"];
            }
        }
    }

    return coll_name;
}

void BatchedIndexer::populate_waiting_on_ids(const std::string& coll_name, const uint64_t& request_start_ts,
                                             std::set<uint64_t>& waiting_on_ids) {
    auto coll_to_references_iter = coll_to_references.find(coll_name);
    if(coll_to_references_iter == coll_to_references.end()) {
        return ;
    }

    for (const auto& ref_coll_name: coll_to_references_iter->second) {
        auto ref_coll_queue_id = StringUtils::hash_wy(ref_coll_name.c_str(), ref_coll_name.size()) % num_threads;
        std::lock_guard ref_queue_lock(qmutuxes[ref_coll_queue_id].mcv);
        auto const& ref_queue = queues[ref_coll_queue_id];

        // Checking every request of the ref queue that was enqueued earlier than this
        // request since requests of collections other than the referenced collection might be present.
        for (const auto& ref_req_id: ref_queue) {
            auto ref_req_iter = req_res_map.find(ref_req_id);
            if (ref_req_iter == req_res_map.end()) {
                continue;
            }

            const auto& ref_req_res = ref_req_iter->second;
            if (ref_req_res.start_ts > request_start_ts) {
                break;
            }

            const auto& ref_req = ref_req_res.req;
            // Only wait for import docs request of the referenced collection.
            if (is_doc_import_route(ref_req->route_hash) &&
                get_collection_name(ref_req) == ref_coll_name) {
                waiting_on_ids.insert(ref_req_id);
                break;
            }
        }
    }
}

void BatchedIndexer::run() {
    LOG(INFO) << "Starting batch indexer with " << num_threads << " threads.";
    ThreadPool* thread_pool = new ThreadPool(num_threads);
    skip_index_iter = meta_store->scan(SKIP_INDICES_PREFIX, skip_index_iter_upper_bound);
    populate_skip_index();

    LOG(INFO) << "BatchedIndexer skip_index: " << skip_index;

    for(size_t i = 0; i < num_threads; i++) {
        std::deque<uint64_t>& queue = queues[i];
        await_t& queue_mutex = qmutuxes[i];

        thread_pool->enqueue([&queue, &queue_mutex, this, i]() {
            while(!quit) {
                std::unique_lock<std::mutex> qlk(queue_mutex.mcv);
                queue_mutex.cv.wait(qlk, [&] { return quit || !queue.empty(); });

                if(quit) {
                    break;
                }

                uint64_t req_id = queue.front();
                qlk.unlock();

                std::unique_lock mlk(mutex);
                auto req_res_map_it = req_res_map.find(req_id);
                if(req_res_map_it == req_res_map.end()) {
                    LOG(ERROR) << "Req ID " << req_id << " not found in req_res_map.";

                    qlk.lock();
                    queue.pop_front();
                    continue;
                }

                req_res_t& orig_req_res = req_res_map_it->second;
                mlk.unlock();

                // scan db for all logs associated with request
                const std::string& req_key_prefix = get_req_prefix_key(req_id);

                /*  Format of the key: $RL_reqId_chunkId
                    NOTE: we use an explicit `next_chunk_index` so that the reads can resume from a partially request.
                */
                const std::string& req_key_start_prefix = req_key_prefix + StringUtils::serialize_uint32_t(
                                                                  orig_req_res.next_chunk_index);

                const std::string& req_key_upper_bound = get_req_suffix_key(req_id);  // cannot inline this
                rocksdb::Slice upper_bound(req_key_upper_bound);
                rocksdb::Iterator* iter = store->scan(req_key_start_prefix, &upper_bound);

                // used to handle partial JSON documents caused by chunking
                std::string& prev_body = orig_req_res.prev_req_body;

                const std::shared_ptr<http_req>& orig_req = orig_req_res.req;
                const std::shared_ptr<http_res>& orig_res = orig_req_res.res;
                bool is_live_req = orig_res->is_alive;

                route_path* found_rpath = nullptr;
                bool route_found = server->get_route(orig_req->route_hash, &found_rpath);
                bool async_res = false;

                while(iter->Valid() && iter->key().starts_with(req_key_prefix)) {
                    std::shared_lock slk(pause_mutex); // used for snapshot
                    orig_req->body = prev_body;
                    orig_req->load_from_json(iter->value().ToString());

                    // update thread local for reference during a crash
                    write_log_index = orig_req->log_index;

                    if(write_log_index == skip_index) {
                        LOG(ERROR) << "Skipping write log index " << write_log_index
                                   << " which seems to have triggered a crash previously.";
                        populate_skip_index();
                    }

                    else {
                        //LOG(INFO) << "index req " << req_id << ", chunk index: " << orig_req_res.next_chunk_index;
                        auto resource_check = cached_resource_stat_t::get_instance()
                                              .has_enough_resources(config.get_data_dir(),
                                                                    config.get_disk_used_max_percentage(),
                                                                    config.get_memory_used_max_percentage());

                        if (resource_check != cached_resource_stat_t::OK &&
                            orig_req->http_method != "DELETE"  && found_rpath->handler != post_health) {
                            const std::string& err_msg = "Rejecting write: running out of resource type: " +
                                                          std::string(magic_enum::enum_name(resource_check));
                            LOG(ERROR) << err_msg;
                            orig_res->set_422(err_msg);
                            orig_res->final = true;
                            async_req_res_t* async_req_res = new async_req_res_t(orig_req, orig_res, true);
                            server->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, async_req_res);
                            goto end;
                        }

                        else if(route_found) {
                            if(skip_writes && found_rpath->handler != post_config) {
                                orig_res->set(422, "Skipping write.");
                                orig_res->final = true;
                                async_req_res_t* async_req_res = new async_req_res_t(orig_req, orig_res, true);
                                server->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, async_req_res);
                                goto end;
                            }

                            // When raft logs are replayed on restart, import requests of related collections
                            // might end up being processed in parallel causing indexing of document having a reference
                            // to fail. So we wait until all the referenced collections complete the import.
                            if (!is_live_req && is_doc_import_route(orig_req->route_hash)) {
                                std::set<uint64_t> waiting_on_ids;
                                const auto& coll_name = get_collection_name(orig_req);

                                std::unique_lock mlock(mutex);
                                populate_waiting_on_ids(coll_name, orig_req_res.start_ts, waiting_on_ids);

                                while(!waiting_on_ids.empty()) {
                                    mlock.unlock();
                                    std::this_thread::sleep_for(std::chrono::milliseconds(100));

                                    if(quit) {
                                        goto end;
                                    }

                                    mlock.lock();
                                    for (auto it = waiting_on_ids.begin(); it != waiting_on_ids.end();) {
                                        if (req_res_map.count(*it) == 0) {
                                            it = waiting_on_ids.erase(it);
                                        } else {
                                            it++;
                                        }
                                    }
                                }

                                coll_to_references.erase(coll_name);
                            }

                            async_res = found_rpath->async_res;
                            try {
                                found_rpath->handler(orig_req, orig_res);
                            } catch(const std::exception& e) {
                                LOG(ERROR) << "Exception while calling handler " << found_rpath->_get_action();
                                LOG(ERROR) << "Raw error: " << e.what();
                                // bad request gets a response immediately
                                orig_res->set_400("Bad request.");
                                orig_res->final = true;
                                async_res = false;
                            }
                            prev_body = orig_req->body;
                        } else {
                            orig_res->set_404();
                        }

                        if(is_live_req && (!route_found ||!async_res)) {
                            // sync request gets a response immediately
                            async_req_res_t* async_req_res = new async_req_res_t(orig_req, orig_res, true);
                            server->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, async_req_res);
                        }

                        if(!route_found) {
                            goto end;
                        }
                    }

                    end:

                    queued_writes--;
                    orig_req_res.next_chunk_index++;
                    iter->Next();

                    if(quit) {
                        break;
                    }
                }

                delete iter;

                //LOG(INFO) << "Erasing request data from disk and memory for request " << req_id;

                // we can delete the buffered request content
                store->delete_range(req_key_prefix, req_key_prefix + StringUtils::serialize_uint32_t(UINT32_MAX));

                {
                    std::lock_guard lk(mutex);
                    req_res_map.erase(req_id);
                }

                qlk.lock();
                queue.pop_front();
            }
        });
    }

    uint64_t stuck_counter = 0;
    uint64_t prev_count = 0;

    while(!quit) {
        std::this_thread::sleep_for(std::chrono::milliseconds (1000));

        // do gc, if we are due for one
        uint64_t seconds_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::high_resolution_clock::now() - last_gc_run).count();

        if(seconds_elapsed > GC_INTERVAL_SECONDS) {

            std::unique_lock lk(mutex);
            LOG(INFO) << "Running GC for aborted requests, req map size: " << req_res_map.size();

            if(req_res_map.size() > 0 && prev_count == req_res_map.size()) {
                stuck_counter++;
                if(stuck_counter > 3) {
                    size_t max_loop = 0;
                    for(const auto& it : req_res_map) {
                        max_loop++;
                        LOG(INFO) << "Stuck req_key: " << it.first;
                        if(max_loop == 5) {
                            break;
                        }
                    }

                    stuck_counter = 0;
                }

            } else {
                stuck_counter = 0;
            }

            prev_count = req_res_map.size();

            // iterate through all map entries and delete ones which are not complete but > GC_PRUNE_MAX_SECONDS
            for (auto it = req_res_map.cbegin(); it != req_res_map.cend();) {
                uint64_t seconds_since_batch_update = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count() - it->second.last_updated;

                //LOG(INFO) << "GC checking on req id: " << it->first;
                //LOG(INFO) << "Seconds since last batch update: " << seconds_since_batch_update;

                if(!it->second.is_complete && seconds_since_batch_update > GC_PRUNE_MAX_SECONDS) {
                    LOG(INFO) << "Deleting partial upload for req id " << it->second.start_ts;

                    const std::string& req_key_prefix = get_req_prefix_key(it->second.start_ts);
                    store->delete_range(req_key_prefix, req_key_prefix + StringUtils::serialize_uint32_t(UINT32_MAX));

                    if(it->second.res->is_alive) {
                        it->second.res->final = true;
                        async_req_res_t* async_req_res = new async_req_res_t(it->second.req, it->second.res, true);
                        server->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, async_req_res);
                    }

                    it = req_res_map.erase(it);
                } else {
                    it++;
                }
            }

            last_gc_run = std::chrono::high_resolution_clock::now();
        }
    }

    LOG(INFO) << "Notifying batch indexer threads about shutdown...";
    for(size_t i = 0; i < num_threads; i++) {
        await_t& queue_mutex = qmutuxes[i];
        std::lock_guard<std::mutex> lk(queue_mutex.mcv);
        queue_mutex.cv.notify_one();
    }


    LOG(INFO) << "Batched indexer threadpool shutdown...";
    thread_pool->shutdown();
    delete thread_pool;
}

std::string BatchedIndexer::get_req_prefix_key(uint64_t req_id) {
    const std::string& req_key_prefix = RAFT_REQ_LOG_PREFIX + StringUtils::serialize_uint64_t(req_id) + "_";
    return req_key_prefix;
}

std::string BatchedIndexer::get_req_suffix_key(uint64_t req_id) {
    const std::string& req_key_prefix = RAFT_REQ_LOG_PREFIX + StringUtils::serialize_uint64_t(req_id) + "`";
    return req_key_prefix;
}

BatchedIndexer::~BatchedIndexer() {
    delete [] qmutuxes;
    delete skip_index_iter_upper_bound;
    delete skip_index_iter;
}

void BatchedIndexer::stop() {
    quit = true;
}

int64_t BatchedIndexer::get_queued_writes() {
    return queued_writes;
}

void BatchedIndexer::populate_skip_index() {
    if(skip_index_iter->Valid() && skip_index_iter->key().starts_with(SKIP_INDICES_PREFIX)) {
        const std::string& index_value = skip_index_iter->value().ToString();
        if(StringUtils::is_int64_t(index_value)) {
            skip_index = std::stoll(index_value);
        }

        skip_index_iter->Next();
    } else {
        skip_index = UNSET_SKIP_INDEX;
    }
}

void BatchedIndexer::persist_applying_index() {
    LOG(INFO) << "Saving currently applying index: " << write_log_index;
    std::string key = SKIP_INDICES_PREFIX + std::to_string(write_log_index);
    meta_store->insert(key, std::to_string(write_log_index));
}

void BatchedIndexer::serialize_state(nlohmann::json& state) {
    // requires external synchronization!
    state["queued_writes"] = queued_writes.load();
    state["req_res_map"] = nlohmann::json();

    size_t num_reqs_stored = 0;
    std::unique_lock lk(mutex);

    for(auto& kv: req_res_map) {
        std::string req_key = std::to_string(kv.first);
        state["req_res_map"].emplace(req_key, nlohmann::json());
        nlohmann::json& req_res = state["req_res_map"][req_key];
        req_res["start_ts"] = kv.second.start_ts;
        req_res["last_updated"] = kv.second.last_updated;
        req_res["num_chunks"] = kv.second.num_chunks;
        req_res["next_chunk_index"] = kv.second.next_chunk_index;
        req_res["is_complete"] = kv.second.is_complete;
        req_res["req"] = kv.second.req->to_json();
        req_res["prev_req_body"] = kv.second.prev_req_body;
        num_reqs_stored++;

        //LOG(INFO) << "req_key: " << req_key << ", next_chunk_index: " << kv.second.next_chunk_index;
    }

    LOG(INFO) << "Serialized " << num_reqs_stored << " in-flight requests for snapshot.";
}

void BatchedIndexer::load_state(const nlohmann::json& state) {
    queued_writes = state["queued_writes"].get<int64_t>();

    size_t num_reqs_restored = 0;
    std::set<uint64_t> queue_ids;

    for(auto& kv: state["req_res_map"].items()) {
        std::shared_ptr<http_req> req = std::make_shared<http_req>();
        req->load_from_json(kv.value()["req"].get<std::string>());

        std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
        req_res_t req_res(kv.value()["start_ts"].get<uint64_t>(),
                          kv.value()["prev_req_body"].get<std::string>(), req, res,
                          kv.value()["last_updated"].get<uint64_t>(),
                          kv.value()["num_chunks"].get<uint32_t>(),
                          kv.value()["next_chunk_index"].get<uint32_t>(),
                          kv.value()["is_complete"].get<bool>());

        {
            std::unique_lock mlk(mutex);
            req_res_map.emplace(std::stoull(kv.key()), req_res);
        }

        // add only completed requests to their respective collection-based queues
        // the rest will be added by enqueue() when raft log is completely read

        if(req_res.is_complete) {
            LOG(INFO) << "req_res.start_ts: " <<  req_res.start_ts
                      << ", req_res.next_chunk_index: " << req_res.next_chunk_index;

            const std::string& coll_name = get_collection_name(req);
            uint64_t queue_id = StringUtils::hash_wy(coll_name.c_str(), coll_name.size()) % num_threads;
            queue_ids.insert(queue_id);
            std::unique_lock qlk(qmutuxes[queue_id].mcv);
            queues[queue_id].emplace_back(req->start_ts);
        }

        num_reqs_restored++;
    }

    // need to sort on `start_ts` to preserve original order before notifying queues
    for(auto queue_id: queue_ids) {
        std::unique_lock lk(qmutuxes[queue_id].mcv);
        std::sort(queues[queue_id].begin(), queues[queue_id].end());
        qmutuxes[queue_id].cv.notify_one();
    }

    LOG(INFO) << "Restored " << num_reqs_restored << " in-flight requests from snapshot.";
}

std::shared_mutex& BatchedIndexer::get_pause_mutex() {
    return pause_mutex;
}

void BatchedIndexer::clear_skip_indices() {
    delete skip_index_iter;
    skip_index_iter = meta_store->scan(SKIP_INDICES_PREFIX, skip_index_iter_upper_bound);

    while(skip_index_iter->Valid() && skip_index_iter->key().starts_with(SKIP_INDICES_PREFIX)) {
        meta_store->remove(skip_index_iter->key().ToString());
        skip_index_iter->Next();
    }

    meta_store->flush();
}
