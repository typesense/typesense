#pragma once

#include <brpc/controller.h>             // brpc::Controller
#include <brpc/server.h>                 // brpc::Server
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include <rocksdb/db.h>
#include <future>
#include <memory>

#include "http_data.h"
#include "threadpool.h"
#include "http_server.h"
#include "batched_indexer.h"
#include "cached_resource_stat.h"
#include "option.h"
#include "raft_node_manager.h"

class Store;

// Callback for write operations
class ReplicationClosure : public braft::Closure {
private:
    const std::shared_ptr<http_req> request;
    const std::shared_ptr<http_res> response;

public:
    ReplicationClosure(const std::shared_ptr<http_req>& request,
                      const std::shared_ptr<http_res>& response)
        : request(request), response(response) {}

    ~ReplicationClosure() {}

    const std::shared_ptr<http_req>& get_request() const { return request; }
    const std::shared_ptr<http_res>& get_response() const { return response; }

    void Run();
};


// Snapshot closures
class OnDemandSnapshotClosure : public braft::Closure {
private:
    class RaftServer* replication_state;
    const std::shared_ptr<http_req> req;
    const std::shared_ptr<http_res> res;
    const std::string ext_snapshot_path;
    const std::string state_dir_path;

public:
    OnDemandSnapshotClosure(RaftServer* replication_state,
                           const std::shared_ptr<http_req>& req,
                           const std::shared_ptr<http_res>& res,
                           const std::string& ext_snapshot_path,
                           const std::string& state_dir_path);
    void Run();
};

class TimedSnapshotClosure : public braft::Closure {
private:
    class RaftServer* replication_state;

public:
    explicit TimedSnapshotClosure(RaftServer* replication_state);
    void Run();
};

/**
 * RaftServer implements the complete Raft state machine.
 * It handles both application business logic and braft::StateMachine interface.
 * This combines HTTP processing, database operations, and Raft lifecycle management.
 */
class RaftServer : public braft::StateMachine {
private:
    static constexpr const char* db_snapshot_name = "db_snapshot";
    static constexpr const char* analytics_db_snapshot_name = "analytics_db_snapshot";
    static constexpr const char* BATCHED_INDEXER_STATE_KEY = "$BI";

    // Node management (owned)
    std::unique_ptr<RaftNodeManager> node_manager;

    // Core components
    HttpServer* server;
    BatchedIndexer* batched_indexer;

    Store* store;
    Store* analytics_store;

    ThreadPool* thread_pool;
    http_message_dispatcher* message_dispatcher;

    const bool api_uses_ssl;

    const Config* config;

    const size_t num_collections_parallel_load;
    const size_t num_documents_parallel_load;

    // State management
    std::string raft_dir_path;

    std::string ext_snapshot_path;
    butil::EndPoint peering_endpoint;
    int election_timeout_interval_ms;

    // Synchronization
    std::mutex mcv;
    std::condition_variable cv;
    bool ready;

    // Operation tracking
    std::atomic<bool> shutting_down;
    std::atomic<size_t> pending_writes;
    std::atomic<bool> snapshot_in_progress;

    // Snapshot timing
    const uint64_t snapshot_interval_s;
    uint64_t last_snapshot_ts;

public:

    static constexpr const char* log_dir_name = "log";
    static constexpr const char* meta_dir_name = "meta";
    static constexpr const char* snapshot_dir_name = "snapshot";

    RaftServer(HttpServer* server,
                    BatchedIndexer* batched_indexer,
                    Store* store,
                    Store* analytics_store,
                    ThreadPool* thread_pool,
                    http_message_dispatcher* message_dispatcher,
                    bool api_uses_ssl,
                    const Config* config,
                    size_t num_collections_parallel_load,
                    size_t num_documents_parallel_load);

    /**
     * Start the entire Raft system
     */
    int start(const butil::EndPoint& peering_endpoint,
             int api_port,
             int election_timeout_ms,
             int snapshot_max_byte_count_per_rpc,
             const std::string& raft_dir,
             const std::string& nodes,
             const std::atomic<bool>& quit_abruptly);

    /**
     * Process write requests through Raft
     */
    void write(const std::shared_ptr<http_req>& request,
              const std::shared_ptr<http_res>& response);

    /**
     * Process read requests (not currently used)
     */
    void read(const std::shared_ptr<http_res>& response);

    /**
     * Shutdown the entire Raft system
     */
    void shutdown();

    /**
     * Integration method for HttpServer - handles server lifecycle
     */
    int run_http_server(HttpServer* server) {
        return server->run(this);
    }

    /**
     * Initialize database after node is ready
     */
    int init_db();

    // Accessors
    Store* get_store() { return store; }
    const Config* get_config() const { return config; }
    BatchedIndexer* get_batched_indexer() { return batched_indexer; }
    http_message_dispatcher* get_message_dispatcher() const { return message_dispatcher; }

    // Node operations (delegated to node_manager)
    bool is_leader() const { return node_manager && node_manager->is_leader(); }
    bool is_alive() const { return node_manager && node_manager->is_read_ready(); }
    nlohmann::json get_status() const { return node_manager ? node_manager->get_status() : nlohmann::json{}; }
    std::string get_leader_url() const { return node_manager ? node_manager->get_leader_url() : ""; }

    // State checks
    bool is_read_caught_up() const { return node_manager && node_manager->is_read_ready(); }
    bool is_write_caught_up() const { return node_manager && node_manager->is_write_ready(); }
    bool has_leader_term() const { return node_manager && node_manager->get_leader_term() > 0; }

    // Node management
    void refresh_nodes(const std::string& nodes, size_t raft_counter,
                      const std::atomic<bool>& reset_peers_on_error);
    void refresh_catchup_status(bool log_msg);
    bool trigger_vote();
    bool reset_peers();
    void persist_applying_index();

    // Snapshot management
    void do_snapshot(const std::string& snapshot_path,
                    const std::shared_ptr<http_req>& req,
                    const std::shared_ptr<http_res>& res);
    void do_snapshot(const std::string& nodes);
    void set_ext_snapshot_path(const std::string& path) { ext_snapshot_path = path; }
    void set_snapshot_in_progress(bool in_progress) { snapshot_in_progress = in_progress; }

    // Write tracking
    void decr_pending_writes() { pending_writes--; }
    int64_t get_num_queued_writes() { return batched_indexer->get_queued_writes(); }

    // Synchronization helpers
    void wait() {
        auto lk = std::unique_lock<std::mutex>(mcv);
        cv.wait(lk, [&] { return ready; });
        ready = false;
    }

    void notify() {
        std::lock_guard<std::mutex> lk(mcv);
        ready = true;
        cv.notify_all();
    }

    // State machine status
    uint64_t node_state() const;

private:
    friend class ReplicationClosure;
    friend class OnDemandSnapshotClosure;
    friend class TimedSnapshotClosure;

    // braft::StateMachine interface implementation
    void on_apply(braft::Iterator& iter) override;
    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) override;
    int on_snapshot_load(braft::SnapshotReader* reader) override;

    void on_leader_start(int64_t term) override {
        if(node_manager) node_manager->set_leader_term(term);
        LOG(INFO) << "Node becomes leader, term: " << term;
    }

    void on_leader_stop(const butil::Status& status) override {
        if(node_manager) node_manager->set_leader_term(-1);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void on_shutdown() override {
        LOG(INFO) << "This node is down";
    }

    void on_error(const ::braft::Error& e) override {
        LOG(ERROR) << "Met raft error " << e;
    }

    void on_configuration_committed(const ::braft::Configuration& conf) override {
        LOG(INFO) << "Configuration of this group is " << conf;
    }

    void on_start_following(const ::braft::LeaderChangeContext& ctx) override {
        refresh_catchup_status(true);
        LOG(INFO) << "Node starts following " << ctx;
    }

    void on_stop_following(const ::braft::LeaderChangeContext& ctx) override {
        LOG(INFO) << "Node stops following " << ctx;
    }

    // Internal methods
    void write_to_leader(const std::shared_ptr<http_req>& request,
                        const std::shared_ptr<http_res>& response);

    void do_dummy_write();

    // Snapshot helper
    struct SnapshotArg {
        RaftServer* replication_state;
        braft::SnapshotWriter* writer;
        std::string state_dir_path;
        std::string db_snapshot_path;
        std::string analytics_db_snapshot_path;
        std::string ext_snapshot_path;
        braft::Closure* done;
    };

    static void* save_snapshot(void* arg);
};
