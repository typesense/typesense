#include "raft_server.h"
#include "raft_config.h"
#include "store.h"
#include <butil/files/file_enumerator.h>
#include <butil/files/file_path.h>
#include <file_utils.h>
#include <collection_manager.h>
#include <conversation_model_manager.h>
#include "rocksdb/utilities/checkpoint.h"
#include "core_api.h"
#include "personalization_model_manager.h"
#include <logger.h>

// Raft Lifecycle and Snapshot Management Module
// This file implements the braft::StateMachine interface methods for ReplicationState

namespace braft {
    DECLARE_int32(raft_do_snapshot_min_index_gap);
    DECLARE_int32(raft_max_parallel_append_entries_rpc_num);
    DECLARE_bool(raft_enable_append_entries_cache);
    DECLARE_int32(raft_max_append_entries_cache_size);
    DECLARE_int32(raft_max_byte_count_per_rpc);
    DECLARE_int32(raft_rpc_channel_connect_timeout_ms);
}

// Initialize database after node is ready
int ReplicationState::init_db() {
    LOG(INFO) << "Loading collections from disk...";

    Option<bool> init_op = CollectionManager::get_instance().load(
        num_collections_parallel_load, num_documents_parallel_load
    );

    if(!init_op.ok()) {
        LOG(ERROR) << "Failed to load collections: " << init_op.error();
        return 1;
    }

    LOG(INFO) << "Finished loading collections from disk";

    // Initialize conversation models
    auto conversation_models_init = ConversationModelManager::init(store);
    if(!conversation_models_init.ok()) {
        LOG(INFO) << "Failed to initialize conversation model manager: " << conversation_models_init.error();
    } else {
        LOG(INFO) << "Loaded " << conversation_models_init.get() << " conversation model(s)";
    }

    // Initialize batched indexer state
    if(batched_indexer) {
        LOG(INFO) << "Initializing batched indexer from snapshot state...";
        std::string batched_indexer_state_str;
        StoreStatus s = store->get(BATCHED_INDEXER_STATE_KEY, batched_indexer_state_str);
        if(s == FOUND) {
            nlohmann::json batch_indexer_state = nlohmann::json::parse(batched_indexer_state_str);
            batched_indexer->load_state(batch_indexer_state);
        }
    }

    // Initialize personalization models
    auto personalization_models_init = PersonalizationModelManager::init(store);
    if(!personalization_models_init.ok()) {
        LOG(INFO) << "Failed to initialize personalization model manager: " << personalization_models_init.error();
    } else {
        LOG(INFO) << "Loaded " << personalization_models_init.get() << " personalization model(s)";
    }

    return 0;
}

void* ReplicationState::save_snapshot(void* arg) {
    LOG(INFO) << "save_snapshot called";

    SnapshotArg* sa = static_cast<SnapshotArg*>(arg);
    std::unique_ptr<SnapshotArg> arg_guard(sa);

    // add the db snapshot files to writer state
    butil::FileEnumerator dir_enum(butil::FilePath(sa->db_snapshot_path), false, butil::FileEnumerator::FILES);

    for (butil::FilePath file = dir_enum.Next(); !file.empty(); file = dir_enum.Next()) {
        std::string file_name = std::string(db_snapshot_name) + "/" + file.BaseName().value();
        if (sa->writer->add_file(file_name) != 0) {
            sa->done->status().set_error(EIO, "Fail to add file to writer.");
            sa->replication_state->snapshot_in_progress = false;
            return nullptr;
        }
    }

    if(!sa->analytics_db_snapshot_path.empty()) {
        //add analytics db snapshot files to writer state
        butil::FileEnumerator analytics_dir_enum(butil::FilePath(sa->analytics_db_snapshot_path), false,
                                                 butil::FileEnumerator::FILES);
        for (butil::FilePath file = analytics_dir_enum.Next(); !file.empty(); file = analytics_dir_enum.Next()) {
            auto file_name = std::string(analytics_db_snapshot_name) + "/" + file.BaseName().value();
            if (sa->writer->add_file(file_name) != 0) {
                sa->done->status().set_error(EIO, "Fail to add analytics file to writer.");
                sa->replication_state->snapshot_in_progress = false;
                return nullptr;
            }
        }
    }

    sa->done->Run();

    // NOTE: *must* do a dummy write here since snapshots cannot be triggered if no write has happened since the
    // last snapshot. By doing a dummy write right after a snapshot, we ensure that this can never be the case.
    sa->replication_state->do_dummy_write();

    LOG(INFO) << "save_snapshot done";

    return nullptr;
}

// this method is serial to on_apply so guarantees a snapshot view of the state machine
void ReplicationState::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    LOG(INFO) << "on_snapshot_save";

    snapshot_in_progress = true;
    std::string db_snapshot_path = writer->get_path() + "/" + db_snapshot_name;
    std::string analytics_db_snapshot_path = writer->get_path() + "/" + analytics_db_snapshot_name;

    {
        // grab batch indexer lock so that we can take a clean snapshot
        std::shared_mutex& pause_mutex = batched_indexer->get_pause_mutex();
        std::unique_lock lk(pause_mutex);

        nlohmann::json batch_index_state;
        batched_indexer->serialize_state(batch_index_state);
        store->insert(BATCHED_INDEXER_STATE_KEY, batch_index_state.dump());

        // we will delete all the skip indices in meta store and flush that DB
        // this will block writes, but should be pretty fast
        batched_indexer->clear_skip_indices();

        rocksdb::Checkpoint* checkpoint = nullptr;
        rocksdb::Status status = store->create_check_point(&checkpoint, db_snapshot_path);
        std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);

        if(!status.ok()) {
            LOG(ERROR) << "Failure during checkpoint creation, msg:" << status.ToString();
            done->status().set_error(EIO, "Checkpoint creation failure.");
        }

        if(analytics_store) {
            // to ensure that in-memory table is sent to disk (we don't use WAL)
            analytics_store->flush();

            rocksdb::Checkpoint* checkpoint2 = nullptr;
            status = analytics_store->create_check_point(&checkpoint2, analytics_db_snapshot_path);
            std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint2);

            if(!status.ok()) {
                LOG(ERROR) << "AnalyticsStore : Failure during checkpoint creation, msg:" << status.ToString();
                done->status().set_error(EIO, "AnalyticsStore : Checkpoint creation failure.");
            }
        }
    }

    SnapshotArg* arg = new SnapshotArg;
    arg->replication_state = this;
    arg->writer = writer;
    arg->state_dir_path = raft_dir_path;
    arg->db_snapshot_path = db_snapshot_path;
    arg->done = done;

    if(analytics_store) {
        arg->analytics_db_snapshot_path = analytics_db_snapshot_path;
    }

    if(!ext_snapshot_path.empty()) {
        arg->ext_snapshot_path = ext_snapshot_path;
    }

    // Start a new bthread to avoid blocking StateMachine for slower operations that don't need a blocking view
    bthread_t tid;
    bthread_start_urgent(&tid, NULL, save_snapshot, arg);
}

// Apply committed entries to the state machine
void ReplicationState::on_apply(braft::Iterator& iter) {
    // NOTE: this is executed on a different thread and runs concurrent to http thread
    for(; iter.valid(); iter.next()) {
        // Guard invokes done->Run() asynchronously to avoid blocking
        braft::AsyncClosureGuard closure_guard(iter.done());

        const std::shared_ptr<http_req>& request_generated = iter.done() ?
            dynamic_cast<ReplicationClosure*>(iter.done())->get_request() :
            std::make_shared<http_req>();

        const std::shared_ptr<http_res>& response_generated = iter.done() ?
            dynamic_cast<ReplicationClosure*>(iter.done())->get_response() :
            std::make_shared<http_res>(nullptr);

        if(!iter.done()) {
            // Log entry - deserialize request
            request_generated->load_from_json(iter.data().to_string());
        }

        request_generated->log_index = iter.index();

        // Queue for batch processing to avoid blocking Raft thread
        batched_indexer->enqueue(request_generated, response_generated);

        if(iter.done()) {
            pending_writes--;
        }
    }
}

// Load a snapshot to restore state machine
int ReplicationState::on_snapshot_load(braft::SnapshotReader* reader) {
    // Critical safety check - leader should NEVER load a snapshot
    CHECK(!node_manager || !node_manager->is_leader())
        << "Leader is not supposed to load snapshot";

    LOG(INFO) << "on_snapshot_load";

    // Ensure reads/writes are rejected during reload, as `store->reload()` unique locks the DB handle
    if(node_manager) {
        // This will set read_caught_up and write_caught_up to false internally
        node_manager->refresh_catchup_status(false);
    }

    // Load analytics snapshot from leader, replacing the running StateMachine
    std::string analytics_snapshot_path = reader->get_path();
    analytics_snapshot_path.append(std::string("/") + analytics_db_snapshot_name);

    if(analytics_store && directory_exists(analytics_snapshot_path)) {
        // analytics db snapshot could be missing (older version or disabled earlier)
        int reload_store = analytics_store->reload(true, analytics_snapshot_path,
                                                   config->get_analytics_db_ttl());
        if(reload_store != 0) {
            LOG(ERROR) << "Failed to reload analytics db snapshot";
            return reload_store;
        }
    }

    // Load main DB snapshot
    std::string db_snapshot_path = reader->get_path();
    db_snapshot_path.append(std::string("/") + db_snapshot_name);

    int reload_store = store->reload(true, db_snapshot_path);
    if(reload_store != 0) {
        return reload_store;
    }

    // Reinitialize database from loaded snapshot
    return init_db();
}

// Snapshot closure implementations

OnDemandSnapshotClosure::OnDemandSnapshotClosure(ReplicationState* replication_state,
                                                 const std::shared_ptr<http_req>& req,
                                                 const std::shared_ptr<http_res>& res,
                                                 const std::string& ext_snapshot_path,
                                                 const std::string& state_dir_path)
    : replication_state(replication_state), req(req), res(res),
      ext_snapshot_path(ext_snapshot_path), state_dir_path(state_dir_path) {}

void OnDemandSnapshotClosure::Run() {
    // Auto delete this after Done()
    std::unique_ptr<OnDemandSnapshotClosure> self_guard(this);

    bool ext_snapshot_succeeded = false;

    // Copy snapshot to external path if requested
    if(!ext_snapshot_path.empty()) {
        const butil::FilePath& dest_state_dir = butil::FilePath(ext_snapshot_path + "/state");

        if(!butil::DirectoryExists(dest_state_dir)) {
            butil::CreateDirectory(dest_state_dir, true);
        }

        const butil::FilePath& src_snapshot_dir = butil::FilePath(state_dir_path + "/snapshot");
        const butil::FilePath& src_meta_dir = butil::FilePath(state_dir_path + "/meta");

        bool snapshot_copied = butil::CopyDirectory(src_snapshot_dir, dest_state_dir, true);
        bool meta_copied = butil::CopyDirectory(src_meta_dir, dest_state_dir, true);

        ext_snapshot_succeeded = snapshot_copied && meta_copied;
    }

    // Clear external snapshot path and mark complete
    // Order is important, because the atomic boolean guards write to the path
    replication_state->set_ext_snapshot_path("");
    replication_state->set_snapshot_in_progress(false);

    req->last_chunk_aggregate = true;
    res->final = true;

    nlohmann::json response;
    uint32_t status_code;

    if(!status().ok()) {
        // in case of internal raft error
        LOG(ERROR) << "On demand snapshot failed, error: " << status().error_str() << ", code: " << status().error_code();
        status_code = 500;
        response["success"] = false;
        response["error"] = status().error_str();
    } else if(!ext_snapshot_succeeded && !ext_snapshot_path.empty()) {
        LOG(ERROR) << "On demand snapshot failed, error: copy failed.";
        status_code = 500;
        response["success"] = false;
        response["error"] = "Copy failed.";
    } else {
        LOG(INFO) << "On demand snapshot succeeded!";
        status_code = 201;
        response["success"] = true;
    }

    res->status_code = status_code;
    res->body = response.dump();

    auto req_res = new async_req_res_t(req, res, true);
    replication_state->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);

    // Wait for response to be sent
    res->wait();
}

TimedSnapshotClosure::TimedSnapshotClosure(ReplicationState* replication_state)
    : replication_state(replication_state) {}

void TimedSnapshotClosure::Run() {
    std::unique_ptr<TimedSnapshotClosure> self_guard(this);

    if(status().ok()) {
        LOG(INFO) << "Timed snapshot succeeded";
    } else {
        LOG(ERROR) << "Timed snapshot failed: " << status().error_str();
    }

    replication_state->set_snapshot_in_progress(false);
}

void ReplicationState::shutdown() {
    LOG(INFO) << "Set shutting_down = true";
    shutting_down = true;

    // wait for pending writes to drop to zero
    LOG(INFO) << "Waiting for in-flight writes to finish...";
    while(pending_writes.load() != 0) {
        LOG(INFO) << "pending_writes: " << pending_writes;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    LOG(INFO) << "Replication state shutdown, store sequence: " << store->get_latest_seq_number();

    // Note: RaftNodeManager shutdown is now handled by RaftCoordinator
    // to maintain proper shutdown order and avoid circular dependencies
    if (node_manager) {
        node_manager->shutdown();
    }
}

void ReplicationState::do_dummy_write() {
    if(!node_manager) {
        LOG(ERROR) << "Could not do a dummy write, as node manager is not initialized";
        return;
    }

    braft::PeerId leader_id = node_manager->leader_id();
    if(leader_id.is_empty()) {
        LOG(ERROR) << "Could not do a dummy write, as node does not have a leader";
        return;
    }

    const std::string protocol = api_uses_ssl ? "https" : "http";
    std::string url = raft_config::get_node_url_path(leader_id, "/health", protocol);

    std::string api_res;
    std::map<std::string, std::string> res_headers;
    long status_code = HttpClient::post_response(url, "", api_res, res_headers, {}, 4000, true);

    LOG(INFO) << "Dummy write to " << url << ", status = " << status_code << ", response = " << api_res;
}

