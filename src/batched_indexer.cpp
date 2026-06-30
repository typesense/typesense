#include "batched_indexer.h"
#include "core_api.h"
#include "thread_local_vars.h"
#include "cached_resource_stat.h"
#include "collection_manager.h"
#include <queue>

BatchedIndexer::BatchedIndexer(HttpServer* server, Store* store, Store* meta_store, const size_t num_threads,
                               const Config& config, const std::atomic<bool>& skip_writes):
                               server(server), store(store), meta_store(meta_store), num_threads(num_threads),
                               last_gc_run(std::chrono::high_resolution_clock::now()), quit(false),
                               config(config), skip_writes(skip_writes) {
    queues.resize(num_threads);
    qmutuxes = new await_t[num_threads];
    skip_index_iter_upper_bound = new rocksdb::Slice(skip_index_upper_bound_key);
}

std::string get_ref_coll_names(const std::string& body, std::unordered_set<std::string>& referenced_collections,
                               std::unordered_set<std::string>& dropped_referenced_collections) {
    std::string collection_name;
    auto const& obj = nlohmann::json::parse(body, nullptr, false);

    if (!obj.is_discarded() && obj.is_object()) {
        if (obj.contains("name") && obj["name"].is_string()) {
            collection_name = obj["name"];
        }

        if (obj.contains("fields") && obj["fields"].is_array()) {
            for (const auto &field: obj["fields"]) {
                if (!field.contains("reference") || !field["reference"].is_string()) {
                    continue;
                }

                std::vector<std::string> split_result;
                StringUtils::split(field["reference"], split_result, ".");

                auto& ref_coll_name = split_result[0];
                auto symlink_op = CollectionManager::get_instance().resolve_symlink(ref_coll_name);
                if (symlink_op.ok()) {
                    ref_coll_name = symlink_op.get();
                }
                if (field.contains("drop") && field["drop"]) {
                    dropped_referenced_collections.insert(ref_coll_name);
                } else {
                    referenced_collections.insert(ref_coll_name);
                }
            }
        }
    }

    return collection_name;
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
            req_res_t req_res(req->start_ts, "", req, res, now, 1, 0, false, static_cast<uint64_t>(req->log_index));
            req_res_map.emplace(req->start_ts, req_res);
        } else {
            auto& req_res = req_res_map_it->second;
            chunk_sequence = req_res.num_chunks;
            req_res.num_chunks += 1;
            req_res.last_updated = now;
            req_res.latest_chunk_log_index = std::max(req_res.latest_chunk_log_index, static_cast<uint64_t>(req->log_index));
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
            const std::string& coll_name = get_collection_name(req);
            uint64_t queue_id = StringUtils::hash_wy(coll_name.c_str(), coll_name.size()) % num_threads;
            req->params["collection"] = coll_name;
            update_coll_to_references(req, coll_name);

            {
                std::unique_lock lk2(mutex);
                req_res_map[req->start_ts].is_complete = true;
            }

            auto wait_on_request_ids = get_requests_to_wait_on_with_lock(req->start_ts, coll_name);
            if(wait_on_request_ids.empty()) {
                std::unique_lock qlk(qmutuxes[queue_id].mcv);
                queues[queue_id].emplace_back(req->start_ts);
                qlk.unlock();
                qmutuxes[queue_id].cv.notify_one();
            } else {
                refq_entry ref(queue_id, req->start_ts);
                ref.waiting_on_requests = std::move(wait_on_request_ids);

                std::unique_lock lk(refq_wait.mcv);
                reference_q.emplace_back(std::move(ref));
                lk.unlock();
                refq_wait.cv.notify_one();
            }
        }

        // IMPORTANT: must not read `req` variables (except _req) henceforth to prevent data races with indexing thread

        if(is_old_serialized_request) {
            // Indicates a serialized request from a version that did not support batching (v0.21 and below).
            // We can only do serial writes as we cannot reliably distinguish one streaming request from another.
            // So, wait for `req_res_map` to be empty before proceeding
            while(true) {
                {
                    std::unique_lock lk(mutex);
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
        } else if(route_found && rpath->handler == post_conversation_model) {
            nlohmann::json obj = nlohmann::json::parse(req->body, nullptr, false);

            if(!obj.is_discarded() && obj.is_object() &&
               obj.count("history_collection") != 0 && obj["history_collection"].is_string()) {
                coll_name = obj["history_collection"];
            }
        }
    } else {
        // `CollectionManager::get_collection` accounts for collection alias being used and provides pointer to the
        // original collection.
        auto& cm = CollectionManager::get_instance();
        auto coll = cm.get_collection(coll_name);
        if (coll != nullptr) {
            coll_name = coll->get_name();
        }
    }

    return coll_name;
}

void BatchedIndexer::run() {
    LOG(INFO) << "Starting batch indexer with " << num_threads << " threads.";
    ThreadPool* thread_pool = new ThreadPool(num_threads);
    skip_index_iter = meta_store->scan(SKIP_INDICES_PREFIX, skip_index_iter_upper_bound);
    populate_skip_index();

    LOG(INFO) << "BatchedIndexer skip_index: " << skip_index;

    for(size_t i = 0; i < num_threads; i++) {
        thread_pool->enqueue([this, i]() {
            std::deque<uint64_t>& queue = queues[i];
            await_t& queue_mutex = qmutuxes[i];

            while(!quit) {
                std::unique_lock<std::mutex> qlk(queue_mutex.mcv);
                queue_mutex.cv.wait(qlk, [&] { return quit || !queue.empty(); });

                if(quit) {
                    break;
                }

                uint64_t req_id = queue.front();
                queue.pop_front();
                qlk.unlock();

                std::unique_lock mlk(mutex);
                auto req_res_map_it = req_res_map.find(req_id);
                if(req_res_map_it == req_res_map.end()) {
                    LOG(ERROR) << "Req ID " << req_id << " not found in req_res_map.";
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

                std::string ub_str = get_req_suffix_key(req_id);
                rocksdb::Slice ub_slice(ub_str);
                std::unique_ptr<rocksdb::Iterator> iter {
                    store->scan(req_key_start_prefix, &ub_slice)
                };

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

                        if (resource_check != cached_resource_stat_t::OK && orig_req->do_resource_check()) {
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

                            async_res = found_rpath->async_res;
                            try {
                                found_rpath->handler(orig_req, orig_res);
                            } catch(const std::exception& e) {
                                const std::string& api_action = found_rpath->_get_action();
                                LOG(ERROR) << "Exception while calling handler " << api_action;
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

                //LOG(INFO) << "Erasing request data from disk and memory for request " << req_id;

                // we can delete the buffered request content
                store->delete_range(req_key_prefix, req_key_prefix + StringUtils::serialize_uint32_t(UINT32_MAX));

                std::unique_lock lk(mutex);

                update_coll_to_references_after_request(orig_req, get_collection_name(orig_req));

                req_res_map.erase(req_id);
                lk.unlock();
                refq_wait.cv.notify_one();
            }
        });
    }

    std::thread ref_sequence_thread([&]() {
        // Waits for dependent requests that are ahead to finish before pushing a request onto main indexing queue.
        LOG(INFO) << "Starting reference sequence thread.";

        while(!quit) {
            std::unique_lock ref_qlk(refq_wait.mcv);
            refq_wait.cv.wait(ref_qlk, [&] {
                return quit || !reference_q.empty();
            });

            if(quit) {
                break;
            }

            std::lock_guard lock(mutex);

            // We will iterate on the reference queue and check if there are any ongoing requests that have been
            // sent prior to this request.
            auto reference_q_it = reference_q.begin();
            while(reference_q_it != reference_q.end()) {
                std::unordered_set<uint64_t> waiting_on_requests_updated;
                for (const auto& waiting_on_req_id : reference_q_it->waiting_on_requests) {
                    if (req_res_map.count(waiting_on_req_id) != 0) {
                        waiting_on_requests_updated.insert(waiting_on_req_id);
                    }
                }
                if (waiting_on_requests_updated.empty()) {
                    // All the dependent requests have been completed. Push this request onto main processing queue and
                    // remove node from reference_q.
                    std::unique_lock qlk(qmutuxes[reference_q_it->queue_id].mcv);
                    queues[reference_q_it->queue_id].emplace_back(reference_q_it->start_ts);
                    qlk.unlock();
                    qmutuxes[reference_q_it->queue_id].cv.notify_one();
                    reference_q_it = reference_q.erase(reference_q_it);
                } else {
                    reference_q_it->waiting_on_requests = std::move(waiting_on_requests_updated);
                    reference_q_it++;
                }
            }
        }
    });

    uint64_t stuck_counter = 0;
    uint64_t prev_count = 0;

    while(!quit) {
        std::this_thread::sleep_for(std::chrono::milliseconds (1000));

        // do gc, if we are due for one
        uint64_t seconds_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::high_resolution_clock::now() - last_gc_run).count();

        if(seconds_elapsed > GC_INTERVAL_SECONDS) {

            std::unique_lock lk(mutex);
            LOG(INFO) << "Running GC for aborted requests, req map size: " << req_res_map.size()
                      << ", reference_q.size: " << reference_q.size();

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
        queue_mutex.cv.notify_one();
    }

    LOG(INFO) << "Notifying reference sequence thread about shutdown...";
    refq_wait.cv.notify_one();
    ref_sequence_thread.join();

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
        req_res["latest_chunk_log_index"] = kv.second.latest_chunk_log_index;
        req_res["req"] = kv.second.req->to_json();
        req_res["prev_req_body"] = kv.second.prev_req_body;
        num_reqs_stored++;

        //LOG(INFO) << "req_key: " << req_key << ", next_chunk_index: " << kv.second.next_chunk_index;
    }

    state["reference_q"] = nlohmann::json::array();
    for(auto& ref_req: reference_q) {
        nlohmann::json ref_req_obj;
        ref_req_obj["queue_id"] = ref_req.queue_id;
        ref_req_obj["start_ts"] = ref_req.start_ts;
        ref_req_obj["waiting_on_requests"] = nlohmann::json::array();
        for (const auto& waiting_on_req_id : ref_req.waiting_on_requests) {
            ref_req_obj["waiting_on_requests"].push_back(waiting_on_req_id);
        }
        state["reference_q"].push_back(ref_req_obj);
    }

    LOG(INFO) << "Serialized " << num_reqs_stored << " in-flight requests for snapshot.";
}

void BatchedIndexer::load_state(const nlohmann::json& state) {
    // `queued_writes` is a denormalized counter that must always equal the sum of the unprocessed chunks
    // across the *complete* requests in `req_res_map`. Restoring it verbatim from the snapshot let a drifted
    // value survive forever: e.g. a value that counted writes whose request entries were already gone would
    // never be decremented (there is nothing left to process), and it would even be re-persisted and
    // propagated to followers via InstallSnapshot. Instead we recompute it from the restored request map so
    // that every load self-heals any inconsistency. Incomplete requests are intentionally excluded: they are
    // not counted at enqueue time either, and will be counted by enqueue() when the raft log is replayed.
    const int64_t persisted_queued_writes = state.contains("queued_writes") ?
                                                state["queued_writes"].get<int64_t>() : 0;
    queued_writes = 0;

    // Tracked alongside `queued_writes` purely for the post-load sanity check below: by the time we log, the
    // restored queues have been notified and a worker may have already decremented the live counter, so we
    // must compare against this deterministic local total rather than re-reading `queued_writes`.
    int64_t recomputed_queued_writes = 0;

    size_t num_reqs_restored = 0;
    std::set<uint64_t> queue_ids;
    std::unordered_set<uint64_t> reference_q_start_ts;

    if(state.contains("reference_q")) {
        for(const auto& item: state["reference_q"].items()) {
            const nlohmann::json& ref_entry = item.value();
            reference_q_start_ts.insert(ref_entry["start_ts"].get<uint64_t>());
        }
    }

    for(auto& kv: state["req_res_map"].items()) {
        std::shared_ptr<http_req> req = std::make_shared<http_req>();
        req->load_from_json(kv.value()["req"].get<std::string>());

        std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
        const uint64_t latest_chunk_log_index = kv.value().contains("latest_chunk_log_index") ?
                                                    kv.value()["latest_chunk_log_index"].get<uint64_t>() :
                                                    static_cast<uint64_t>(req->log_index);
        req_res_t req_res(kv.value()["start_ts"].get<uint64_t>(),
                          kv.value()["prev_req_body"].get<std::string>(), req, res,
                          kv.value()["last_updated"].get<uint64_t>(),
                          kv.value()["num_chunks"].get<uint32_t>(),
                          kv.value()["next_chunk_index"].get<uint32_t>(),
                          kv.value()["is_complete"].get<bool>(),
                          latest_chunk_log_index);

        // Recompute queued_writes from the request map (see note at the top of this method). Must happen
        // before the request is queued below, so the counter is fully set before a worker can drain it.
        if(req_res.is_complete && req_res.num_chunks > req_res.next_chunk_index) {
            const int64_t remaining_chunks = req_res.num_chunks - req_res.next_chunk_index;
            queued_writes += remaining_chunks;
            recomputed_queued_writes += remaining_chunks;
        }

        {
            std::unique_lock mlk(mutex);
            req_res_map.emplace(std::stoull(kv.key()), req_res);
        }

        update_coll_to_references(req, get_collection_name(req));

        // add only completed requests to their respective collection-based queues
        // the rest will be added by enqueue() when raft log is completely read

        if(req_res.is_complete && reference_q_start_ts.count(req->start_ts) == 0) {
            const std::string& coll_name = get_collection_name(req);
            uint64_t queue_id = StringUtils::hash_wy(coll_name.c_str(), coll_name.size()) % num_threads;
            queue_ids.insert(queue_id);
            std::unique_lock qlk(qmutuxes[queue_id].mcv);
            queues[queue_id].emplace_back(req->start_ts);
        }

        num_reqs_restored++;
    }

    if(state.contains("reference_q")) {
        std::unique_lock lk(mutex);
        for(const auto& item: state["reference_q"].items()) {
            const nlohmann::json& ref_entry = item.value();
            refq_entry ref(ref_entry["queue_id"], ref_entry["start_ts"]);
            if (ref_entry.contains("waiting_on_requests")) {
                for (const auto& waiting_on_req_id : ref_entry["waiting_on_requests"]) {
                    ref.waiting_on_requests.insert(waiting_on_req_id.get<uint64_t>());
                }
            } else {
                // For backwards compatibility since `ref_entry["waiting_on_requests"]` will not be present in previous
                // versions.
                auto req_res_it = req_res_map.find(ref.start_ts);
                if (req_res_it != req_res_map.end()) {
                    const auto& req = req_res_it->second.req;
                    const std::string& coll_name = get_collection_name(req);
                    ref.waiting_on_requests = get_requests_to_wait_on(ref.start_ts, coll_name);
                }
            }
            reference_q.emplace_back(std::move(ref));
        }

        lk.unlock();
        refq_wait.cv.notify_one();
    }

    std::unordered_map<uint64_t, std::pair<uint64_t, uint64_t>> restored_request_order;
    {
        std::unique_lock lk(mutex);
        for (const auto& [req_id, req_res] : req_res_map) {
            const uint64_t log_index = req_res.latest_chunk_log_index;
            restored_request_order.emplace(req_id, std::make_pair(log_index, req_id));
        }
    }

    // Replay the restored per-collection queues in the same order as live writes: prefer raft log order when
    // present, and fall back to start_ts for older serialized requests that do not have log indices.
    for(auto queue_id: queue_ids) {
        std::unique_lock lk(qmutuxes[queue_id].mcv);
        std::sort(queues[queue_id].begin(), queues[queue_id].end(),
                  [&restored_request_order](const uint64_t lhs, const uint64_t rhs) {
                      const auto lhs_it = restored_request_order.find(lhs);
                      const auto rhs_it = restored_request_order.find(rhs);

                      const auto [lhs_log_order, lhs_req_id] = (lhs_it != restored_request_order.end()) ?
                                             lhs_it->second : std::make_pair(UINT64_MAX, lhs);
                      const auto [rhs_log_order, rhs_req_id] = (rhs_it != restored_request_order.end()) ?
                                             rhs_it->second : std::make_pair(UINT64_MAX, rhs);

                      const bool has_log_order = lhs_log_order != UINT64_MAX && rhs_log_order != UINT64_MAX;
                      if (has_log_order && lhs_log_order != rhs_log_order) {
                          return lhs_log_order < rhs_log_order;
                      }

                      return lhs_req_id < rhs_req_id;
                  });
        qmutuxes[queue_id].cv.notify_one();
    }

    LOG(INFO) << "Restored " << num_reqs_restored << " in-flight requests from snapshot.";

    if(persisted_queued_writes != recomputed_queued_writes) {
        LOG(WARNING) << "Snapshot queued_writes (" << persisted_queued_writes << ") did not match the value "
                     << "recomputed from " << num_reqs_restored << " restored requests ("
                     << recomputed_queued_writes << "). Using the recomputed value.";
    }
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

void BatchedIndexer::update_coll_to_references(const std::shared_ptr<http_req>& req, const std::string& coll_name) {
    route_path* found_rpath = nullptr;
    const bool route_found = server->get_route(req->route_hash, &found_rpath);
    if (!route_found || (found_rpath->handler != post_create_collection &&
                         found_rpath->handler != patch_update_collection &&
                         found_rpath->handler != post_import_documents)) {
        std::unique_lock lk(mutex);
        if (!coll_name.empty() && coll_to_references.count(coll_name) == 0) {
            coll_to_references[coll_name] = CollectionManager::get_instance().get_collection_references(coll_name);
        }
        return;
    }

    auto& cm = CollectionManager::get_instance();
    std::unordered_set<std::string> referenced_collections;
    std::unordered_set<std::string> dropped_referenced_collections;
    std::string parsed_coll_name = coll_name;
    if (found_rpath->handler == post_import_documents) {
        std::unique_lock lk(mutex);
        auto it = coll_to_references.find(parsed_coll_name);
        if (it != coll_to_references.end()) {
            return;
        }
        referenced_collections = std::move(cm.get_collection_references(parsed_coll_name));
    } else {
        parsed_coll_name = get_ref_coll_names(req->body, referenced_collections, dropped_referenced_collections);
    }

    auto symlink_op = cm.resolve_symlink(parsed_coll_name);
    if (symlink_op.ok()) {
        parsed_coll_name = symlink_op.get();
    }
    if (parsed_coll_name.empty()) {
        return;
    }

    if (found_rpath->handler == patch_update_collection) {
        std::unique_lock lk(mutex);
        auto it = coll_to_references.find(parsed_coll_name);
        if (it != coll_to_references.end()) {
            auto& existing_references = it->second;
            for (const auto& ref_coll_name: dropped_referenced_collections) {
                existing_references.erase(ref_coll_name);
            }
            existing_references.insert(referenced_collections.begin(), referenced_collections.end());
        } else {
            coll_to_references[parsed_coll_name] = std::move(referenced_collections);
        }
        return;
    }

    std::unique_lock lk(mutex);
    coll_to_references[parsed_coll_name] = std::move(referenced_collections);
}

void BatchedIndexer::update_coll_to_references_after_request(const std::shared_ptr<http_req>& req,
                                                             const std::string& coll_name) {
    if (coll_name.empty()) {
        return;
    }

    route_path* found_rpath = nullptr;
    const bool route_found = server->get_route(req->route_hash, &found_rpath);
    if (!route_found || (found_rpath->handler != post_create_collection &&
                         found_rpath->handler != patch_update_collection &&
                         found_rpath->handler != del_drop_collection)) {
        return;
    }

    auto it = coll_to_references.find(coll_name);
    if (it == coll_to_references.end()) {
        return;
    }

    it->second = CollectionManager::get_instance().get_collection_references(coll_name);
}

std::unordered_set<uint64_t> BatchedIndexer::get_requests_to_wait_on_with_lock(const uint64_t req_id,
                                                                               const std::string& coll_name) {
    std::unique_lock lk(mutex);
    return get_requests_to_wait_on(req_id, coll_name);
}

std::unordered_set<uint64_t> BatchedIndexer::get_requests_to_wait_on(const uint64_t req_id,
                                                                     const std::string& coll_name) {
    std::unordered_set<std::string> processed_collections;
    std::queue<std::string> pending_collections;
    std::unordered_set<std::string> wait_for_collections;

    // Wait for all the referenced collections.
    auto coll_to_ref_it = coll_to_references.find(coll_name);
    if (coll_to_ref_it != coll_to_references.end()) {
        wait_for_collections.insert(coll_to_ref_it->second.begin(), coll_to_ref_it->second.end());
        for (const auto& item: wait_for_collections) {
            pending_collections.push(item);
        }
    }

    // Also wait for the all the referencing collections.
    for (const auto& [ref_coll_name, references] : coll_to_references) {
        if (references.count(coll_name) != 0 && processed_collections.insert(ref_coll_name).second) {
            pending_collections.push(ref_coll_name);
        }
    }

    // Handle nested references.
    while (!pending_collections.empty()) {
        const auto referenced_coll_name = pending_collections.front();
        pending_collections.pop();

        coll_to_ref_it = coll_to_references.find(referenced_coll_name);
        if (coll_to_ref_it == coll_to_references.end()) {
            continue;
        }

        for (const auto& nested_ref_coll_name: coll_to_ref_it->second) {
            if (processed_collections.insert(nested_ref_coll_name).second) {
                pending_collections.push(nested_ref_coll_name);
            }
        }
    }

    wait_for_collections.insert(processed_collections.begin(), processed_collections.end());
    if (wait_for_collections.empty()) {
        return {};
    }

    // Requests waiting in `reference_q` temporarily leave the collection's main queue, so later writes to the
    // same collection must wait on them as well to preserve per-collection ordering.
    wait_for_collections.insert(coll_name);

    const auto current_req_it = req_res_map.find(req_id);
    if (current_req_it == req_res_map.end()) {
        return {};
    }

    const auto current_req_last_log_index = current_req_it->second.latest_chunk_log_index;
    std::unordered_set<uint64_t> wait_on_request_ids;
    for (const auto& [other_req_id, other_req_res] : req_res_map) {
        // We won't wait on requests whose last chunk has still not been received.
        if (!other_req_res.is_complete) {
            continue;
        }
        const auto& other_req_last_log_index = other_req_res.latest_chunk_log_index;
        const bool has_log_order = current_req_last_log_index != 0 && other_req_last_log_index != 0;
        const auto& other_req_last_updated = other_req_res.last_updated;
        const auto& current_req_last_updated = current_req_it->second.last_updated;
        const bool is_earlier_request = has_log_order ? (other_req_last_log_index < current_req_last_log_index)
                                                      : (other_req_last_updated < current_req_last_updated ||
                                                         (other_req_last_updated == current_req_last_updated &&
                                                            other_req_id < req_id));
        if (!is_earlier_request) {
            continue;
        }

        const auto& ref_coll_name = get_collection_name(other_req_res.req);
        if (wait_for_collections.count(ref_coll_name) == 0) {
            continue;
        }
        wait_on_request_ids.insert(other_req_id);
    }

    return wait_on_request_ids;
}
