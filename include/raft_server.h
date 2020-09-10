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

class Store;
class ReplicationState;

// Implements the callback for the state machine
class ReplicationClosure : public braft::Closure {
private:
    http_req* request;
    http_res* response;

public:
    ReplicationClosure(http_req* request, http_res* response): request(request), response(response) {

    }

    ~ReplicationClosure() {}

    http_req* get_request() const {
        return request;
    }

    http_res* get_response() const {
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

// Closure that fires when initial snapshot operation finishes
class InitSnapshotClosure : public braft::Closure {
private:
    ReplicationState* replication_state;
public:

    InitSnapshotClosure(ReplicationState* replication_state): replication_state(replication_state) {}

    ~InitSnapshotClosure() {}

    void Run();
};


// Implements braft::StateMachine.
class ReplicationState : public braft::StateMachine {
private:
    static constexpr const char* db_snapshot_name = "db_snapshot";

    braft::Node* volatile node;
    butil::atomic<int64_t> leader_term;
    std::set<braft::PeerId> peers;

    Store *store;
    ThreadPool* thread_pool;
    http_message_dispatcher* message_dispatcher;

    std::atomic<size_t> init_readiness_count;

    bool create_init_db_snapshot;

    std::atomic<bool>& shut_down;

public:

    static constexpr const char* log_dir_name = "log";
    static constexpr const char* meta_dir_name = "meta";
    static constexpr const char* snapshot_dir_name = "snapshot";

    ReplicationState(Store* store, ThreadPool* thread_pool, http_message_dispatcher* message_dispatcher,
                     bool create_init_db_snapshot, std::atomic<bool>& quit_service);

    // Starts this node
    int start(const butil::EndPoint & peering_endpoint, int api_port,
              int election_timeout_ms, int snapshot_interval_s,
              const std::string & raft_dir, const std::string & nodes);

    // Generic write method for synchronizing all writes
    void write(http_req* request, http_res* response);

    // Generic read method for consistent reads, not used for now
    void read(http_res* response);

    // updates cluster membership
    void refresh_nodes(const std::string & nodes);

    bool has_leader_term() const {
        return leader_term.load(butil::memory_order_acquire) > 0;
    }

    bool is_ready() const {
        return init_readiness_count >= 2;
    }

    bool is_alive() const;

    uint64_t node_state() const;

    // Shut this node down.
    void shutdown() {
        LOG(INFO) << "Replication state shutdown.";
        shut_down = true;
        if (node) {
            node->shutdown(nullptr);
        }
    }

    // Blocking this thread until the node is eventually down.
    void join() {
        if (node) {
            node->join();
            delete node;
            node = nullptr;
        }
    }

    int init_db();

    void reset_db();

    size_t get_init_readiness_count() const;

    static std::string to_nodes_config(const butil::EndPoint &peering_endpoint, const int api_port,
                                       const std::string &nodes_config);

    static constexpr const char* REPLICATION_MSG = "raft_replication";

private:

    friend class ReplicationClosure;

    // actual application of writes onto the WAL
    void on_apply(braft::Iterator& iter);

    struct SnapshotArg {
        rocksdb::DB* db;
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };

    static void *save_snapshot(void* arg);

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done);

    int on_snapshot_load(braft::SnapshotReader* reader);

    void on_leader_start(int64_t term) {
        leader_term.store(term, butil::memory_order_release);

        // have to do a dummy write, otherwise snapshot will not trigger
        if(create_init_db_snapshot) {
            http_req* request = new http_req(nullptr, "POST", 0, {}, "INIT_SNAPSHOT");
            http_res* response = new http_res();
            write(request, response);
        }

        init_readiness_count++;

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
        conf.list_peers(&peers);
    }

    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node starts following " << ctx;
        init_readiness_count++;
    }

    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }

    void follower_write(http_req *request, http_res *response) const;
};
