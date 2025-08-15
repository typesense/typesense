#include "store.h"
#include "raft_server.h"
#include <butil/files/file_enumerator.h>
#include <butil/endpoint.h>
#include <braft/raft.h>
#include <thread>
#include <chrono>
#include <file_utils.h>
#include <collection_manager.h>
#include <conversation_model_manager.h>
#include "rocksdb/utilities/checkpoint.h"
#include "thread_local_vars.h"
#include "core_api.h"
#include "personalization_model_manager.h"

// Raft Lifecycle and Snapshot Management Module
// Extracted from raft_server.cpp for better organization

namespace braft {
    DECLARE_int32(raft_do_snapshot_min_index_gap);
    DECLARE_int32(raft_max_parallel_append_entries_rpc_num);
    DECLARE_bool(raft_enable_append_entries_cache);
    DECLARE_int32(raft_max_append_entries_cache_size);
    DECLARE_int32(raft_max_byte_count_per_rpc);
    DECLARE_int32(raft_rpc_channel_connect_timeout_ms);
}

// Raft Node Startup and Initialization
int ReplicationState::start_raft_node(const butil::EndPoint & peering_endpoint, const int api_port,
                                      int election_timeout_ms, int snapshot_max_byte_count_per_rpc,
                                      const std::string & raft_dir, const std::string & nodes,
                                      const std::atomic<bool>& quit_abruptly) {
    
    // Full Raft node initialization
    butil::ip_t ip;
    if (butil::str2ip(butil::endpoint2str(peering_endpoint).c_str(), &ip) < 0) {
        LOG(ERROR) << "Invalid peering endpoint: " << butil::endpoint2str(peering_endpoint).c_str();
        return -1;
    }

    braft::NodeOptions node_options;
    if (node_options.initial_conf.parse_from(to_nodes_config(peering_endpoint, api_port, nodes)) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << nodes << "'";
        return -1;
    }

    node_options.election_timeout_ms = election_timeout_ms;
    node_options.fsm = this;
    node_options.node_owns_fsm = false;
    node_options.snapshot_interval_s = 0; // we will trigger snapshots manually
    node_options.log_uri = raft_dir + "/log";
    node_options.raft_meta_uri = raft_dir + "/raft_meta";
    node_options.snapshot_uri = raft_dir + "/snapshot";
    node_options.disable_cli = false;

    std::unique_lock lock(node_mutex);
    node = new braft::Node(braft::GroupId("ReplicationState"), braft::PeerId(peering_endpoint, 0));

    if (node->init(node_options) != 0) {
        LOG(ERROR) << "Fail to init raft node";
        delete node;
        node = nullptr;
        return -1;
    }
    lock.unlock();

    // wait for node to come online
    const int WAIT_FOR_RAFT_TIMEOUT_MS = 60 * 1000;
    auto begin_ts = std::chrono::high_resolution_clock::now();

    while(true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        auto current_ts = std::chrono::high_resolution_clock::now();
        auto time_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(current_ts - begin_ts).count();

        if(time_elapsed > WAIT_FOR_RAFT_TIMEOUT_MS) {
            LOG(ERROR) << "Raft state not ready even after " << time_elapsed << " ms. Stopping.";
            return -1;
        }

        if(quit_abruptly.load()) {
            LOG(ERROR) << "Server is quitting abruptly.";
            return -1;
        }

        bool is_single_node = node_options.initial_conf.size() == 1;
        ready = is_single_node || node->is_leader();
        bool leader_or_follower;

        {
            std::shared_lock node_guard(node_mutex);
            leader_or_follower = ready || (!node->leader_id().is_empty());
        }

        if(leader_or_follower) {
            LOG(INFO) << "Raft node is now ready. Proceeding with DB init. ready=" << ready
                      << ", single_node=" << is_single_node;
            break;
        } else {
            LOG(INFO) << "Waiting for raft node to come online, time_elapsed=" << time_elapsed << " ms";
        }
    }

    // do init only on node ready (i.e. elections are done)
    if(init_db() != 0) {
        return -1;
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

int ReplicationState::init_db() {
    LOG(INFO) << "Loading collections from disk...";

    Option<bool> init_op = CollectionManager::get_instance().load(
        num_collections_parallel_load, num_documents_parallel_load
    );

    if(init_op.ok()) {
        LOG(INFO) << "Finished loading collections from disk.";
    } else {
        LOG(ERROR)<< "Typesense failed to start. " << "Could not load collections from disk: " << init_op.error();
        return 1;
    }

    // important to init conversation models only after all collections have been loaded
    auto conversation_models_init = ConversationModelManager::init(store);
    if(!conversation_models_init.ok()) {
        LOG(INFO) << "Failed to initialize conversation model manager: " << conversation_models_init.error();
    } else {
        LOG(INFO) << "Loaded " << conversation_models_init.get() << " conversation model(s).";
    }

    if(batched_indexer != nullptr) {
        LOG(INFO) << "Initializing batched indexer from snapshot state...";
        std::string batched_indexer_state_str;
        StoreStatus s = store->get(BATCHED_INDEXER_STATE_KEY, batched_indexer_state_str);
        if(s == FOUND) {
            nlohmann::json batch_indexer_state = nlohmann::json::parse(batched_indexer_state_str);
            batched_indexer->load_state(batch_indexer_state);
        }
    }

    auto personalization_models_init = PersonalizationModelManager::init(store);
    if(!personalization_models_init.ok()) {
        LOG(INFO) << "Failed to initialize personalization model manager: " << personalization_models_init.error();
    } else {
        LOG(INFO) << "Loaded " << personalization_models_init.get() << " personalization model(s).";
    }

    return 0;
}

int ReplicationState::on_snapshot_load(braft::SnapshotReader* reader) {
    std::shared_lock lock(node_mutex);
    CHECK(!node || !node->is_leader()) << "Leader is not supposed to load snapshot";
    lock.unlock();

    LOG(INFO) << "on_snapshot_load";

    // ensures that reads and writes are rejected, as `store->reload()` unique locks the DB handle
    read_caught_up = false;
    write_caught_up = false;

    // Load snapshot from leader, replacing the running StateMachine
    std::string analytics_snapshot_path = reader->get_path();
    analytics_snapshot_path.append(std::string("/") + analytics_db_snapshot_name);

    if(analytics_store && directory_exists(analytics_snapshot_path)) {
        // analytics db snapshot could be missing (older version or disabled earlier)
        int reload_store = analytics_store->reload(true, analytics_snapshot_path,
                                                   Config::get_instance().get_analytics_db_ttl());
        if (reload_store != 0) {
            LOG(ERROR) << "Failed to reload analytics db snapshot.";
            return reload_store;
        }
    }

    std::string db_snapshot_path = reader->get_path();
    db_snapshot_path.append(std::string("/") + db_snapshot_name);

    int reload_store = store->reload(true, db_snapshot_path);
    if(reload_store != 0) {
        return reload_store;
    }

    bool init_db_status = init_db();

    return init_db_status;
}

void ReplicationState::on_apply(braft::Iterator& iter) {
    // NOTE: this is executed on a different thread and runs concurrent to http thread
    // A batch of tasks are committed, which must be processed through
    // |iter|
    for (; iter.valid(); iter.next()) {
        // Guard invokes replication_arg->done->Run() asynchronously to avoid the callback blocking the main thread
        braft::AsyncClosureGuard closure_guard(iter.done());

        const std::shared_ptr<http_req>& request_generated = iter.done() ?
                         dynamic_cast<ReplicationClosure*>(iter.done())->get_request() : std::make_shared<http_req>();

        const std::shared_ptr<http_res>& response_generated = iter.done() ?
                dynamic_cast<ReplicationClosure*>(iter.done())->get_response() : std::make_shared<http_res>(nullptr);

        if(!iter.done()) {
            // indicates log serialized request
            request_generated->load_from_json(iter.data().to_string());
        }

        request_generated->log_index = iter.index();

        // To avoid blocking the serial Raft write thread persist the log entry in local storage.
        // Actual operations will be done in collection-sharded batch indexing threads.

        batched_indexer->enqueue(request_generated, response_generated);

        if(iter.done()) {
            pending_writes--;
        }
    }
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
    std::unique_lock lock(node_mutex);

    if (node) {
        LOG(INFO) << "node->shutdown";
        node->shutdown(nullptr);

        // Blocking this thread until the node is eventually down.
        LOG(INFO) << "node->join";
        node->join();
        delete node;
        node = nullptr;
    }
}

void ReplicationState::do_dummy_write() {
    std::shared_lock lock(node_mutex);

    if(!node || node->leader_id().is_empty()) {
        LOG(ERROR) << "Could not do a dummy write, as node does not have a leader";
        return ;
    }

    const std::string & leader_addr = node->leader_id().to_string();
    lock.unlock();

    const std::string protocol = api_uses_ssl ? "https" : "http";
    std::string url = get_node_url_path(leader_addr, "/health", protocol);

    std::string api_res;
    std::map<std::string, std::string> res_headers;
    long status_code = HttpClient::post_response(url, "", api_res, res_headers, {}, 4000, true);

    LOG(INFO) << "Dummy write to " << url << ", status = " << status_code << ", response = " << api_res;
}
