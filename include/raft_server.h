#pragma once

#include <brpc/controller.h>             // brpc::Controller
#include <brpc/server.h>                 // brpc::Server
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include <rocksdb/db.h>
#include <future>

#include "http_data.h"
#include "threadpool.h"
#include "http_server.h"
#include "batched_indexer.h"
#include "cached_resource_stat.h"

class Store;
class ReplicationState;

// Implements the callback for the state machine
class ReplicationClosure : public braft::Closure {
private:
    const std::shared_ptr<http_req> request;
    const std::shared_ptr<http_res> response;

public:
    ReplicationClosure(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response): request(request), response(response) {

    }

    ~ReplicationClosure() {
        //LOG(INFO) << "~ReplicationClosure req use count " << request.use_count();
    }

    const std::shared_ptr<http_req>& get_request() const {
        return request;
    }

    const std::shared_ptr<http_res>& get_response() const {
        return response;
    }

    void Run();
};

// Closure that fires when refresh nodes operation finishes
class RefreshNodesClosure : public braft::Closure {
public:

    RefreshNodesClosure() {}

    ~RefreshNodesClosure() {}

    void Run() {
        // Auto delete this after Run()
        std::unique_ptr<RefreshNodesClosure> self_guard(this);

        if(status().ok()) {
            LOG(INFO) << "Peer refresh succeeded!";
        } else {
            LOG(ERROR) << "Peer refresh failed, error: " << status().error_str();
        }
    }
};

// Closure that fires when requested
class OnDemandSnapshotClosure : public braft::Closure {
private:
    ReplicationState* replication_state;
    const std::shared_ptr<http_req> req;
    const std::shared_ptr<http_res> res;

public:

    OnDemandSnapshotClosure(ReplicationState *replication_state, const std::shared_ptr<http_req>& req,
                            const std::shared_ptr<http_res>& res) :
        replication_state(replication_state), req(req), res(res) {}

    ~OnDemandSnapshotClosure() {}

    void Run();
};

class TimedSnapshotClosure : public braft::Closure {
private:
    ReplicationState* replication_state;

public:

    TimedSnapshotClosure(ReplicationState *replication_state) : replication_state(replication_state){}

    ~TimedSnapshotClosure() {}

    void Run();
};

// Implements braft::StateMachine.
class ReplicationState : public braft::StateMachine {
private:
    static constexpr const char* db_snapshot_name = "db_snapshot";

    mutable std::shared_mutex node_mutex;

    braft::Node* volatile node;
    butil::atomic<int64_t> leader_term;

    HttpServer* server;
    BatchedIndexer* batched_indexer;

    Store* store;

    ThreadPool* thread_pool;
    http_message_dispatcher* message_dispatcher;

    const bool api_uses_ssl;

    const Config* config;

    const size_t num_collections_parallel_load;
    const size_t num_documents_parallel_load;

    std::atomic<bool> read_caught_up;
    std::atomic<bool> write_caught_up;

    std::string raft_dir_path;

    std::string ext_snapshot_path;

    int election_timeout_interval_ms;

    std::mutex mcv;
    std::condition_variable cv;
    bool ready;

    std::atomic<bool> shutting_down;
    std::atomic<size_t> pending_writes;

    const uint64_t snapshot_interval_s;     // frequency of actual snapshotting
    uint64_t last_snapshot_ts;              // when last snapshot ran

public:

    static constexpr const char* log_dir_name = "log";
    static constexpr const char* meta_dir_name = "meta";
    static constexpr const char* snapshot_dir_name = "snapshot";

    ReplicationState(HttpServer* server, BatchedIndexer* batched_indexer, Store* store,
                     ThreadPool* thread_pool, http_message_dispatcher* message_dispatcher,
                     bool api_uses_ssl, const Config* config,
                     size_t num_collections_parallel_load, size_t num_documents_parallel_load);

    // Starts this node
    int start(const butil::EndPoint & peering_endpoint, int api_port,
              int election_timeout_ms, int snapshot_max_byte_count_per_rpc,
              const std::string & raft_dir, const std::string & nodes,
              const std::atomic<bool>& quit_abruptly);

    // Generic write method for synchronizing all writes
    void write(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response);

    // Generic read method for consistent reads, not used for now
    void read(const std::shared_ptr<http_res>& response);

    // updates cluster membership
    void refresh_nodes(const std::string & nodes);

    void refresh_catchup_status(bool log_msg);

    bool trigger_vote();

    bool has_leader_term() const {
        return leader_term.load(butil::memory_order_acquire) > 0;
    }

    bool is_read_caught_up() const {
        return read_caught_up;
    }

    bool is_write_caught_up() const {
        return write_caught_up;
    }

    bool is_alive() const;

    uint64_t node_state() const;

    // Shut this node down.
    void shutdown();

    int init_db();

    Store* get_store();

    // for manual / external snapshots
    void do_snapshot(const std::string& snapshot_path, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);

    static std::string to_nodes_config(const butil::EndPoint &peering_endpoint, const int api_port,
                                       const std::string &nodes_config);

    void set_ext_snapshot_path(const std::string &snapshot_path);

    const std::string& get_ext_snapshot_path() const;

    // for timed snapshots
    void do_snapshot(const std::string& nodes);

    void persist_applying_index();

    http_message_dispatcher* get_message_dispatcher() const;

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

    static std::string resolve_node_hosts(const std::string& nodes_config);

    int64_t get_num_queued_writes();

    bool is_leader();

    nlohmann::json get_status();

private:

    friend class ReplicationClosure;

    // actual application of writes onto the WAL
    void on_apply(braft::Iterator& iter);

    struct SnapshotArg {
        ReplicationState* replication_state;
        braft::SnapshotWriter* writer;
        std::string state_dir_path;
        std::string db_snapshot_path;
        std::string ext_snapshot_path;
        braft::Closure* done;
    };

    static void *save_snapshot(void* arg);

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done);

    int on_snapshot_load(braft::SnapshotReader* reader);

    void on_leader_start(int64_t term) {
        leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader, term: " << term;
    }

    void on_leader_stop(const butil::Status& status) {
        leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void on_shutdown() {
        LOG(INFO) << "This node is down";
    }

    void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Met peering error " << e;
    }

    void on_configuration_committed(const ::braft::Configuration& conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }

    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        refresh_catchup_status(true);
        LOG(INFO) << "Node starts following " << ctx;
    }

    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }

    void write_to_leader(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response);

    void do_dummy_write();

    std::string get_node_url_path(const std::string& node_addr, const std::string& path,
                                  const std::string& protocol) const;
};
