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

// Closure that fires when refresh peers operation finishes
class RefreshPeersClosure : public braft::Closure {
public:

    RefreshPeersClosure() {}

    ~RefreshPeersClosure() {}

    void Run() {
        // Auto delete this after Run()
        std::unique_ptr<RefreshPeersClosure> self_guard(this);

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

    rocksdb::DB* db;
    std::string db_path;
    rocksdb::Options db_options;

    http_message_dispatcher* message_dispatcher;

    butil::atomic<bool> has_initialized;
    std::promise<bool>* ready;
    bool create_init_db_snapshot;

public:

    static constexpr const char* log_dir_name = "log";
    static constexpr const char* meta_dir_name = "meta";
    static constexpr const char* snapshot_dir_name = "snapshot";

    ReplicationState(Store* store, http_message_dispatcher* message_dispatcher,
                     std::promise<bool>* ready, bool create_init_db_snapshot);

    ~ReplicationState() {
        delete node;
    }

    // Starts this node
    int start(int port, int election_timeout_ms, int snapshot_interval_s,
              const std::string & raft_dir, const std::string & peers);

    // Generic write method for synchronizing all writes
    void write(http_req* request, http_res* response);

    // Generic read method for consistent reads
    void read(http_res* response);

    // updates cluster membership
    void refresh_peers(const std::string & peers);

    bool is_leader() const {
        return leader_term.load(butil::memory_order_acquire) > 0;
    }

    // Shut this node down.
    void shutdown() {
        if (node) {
            node->shutdown(nullptr);
        }
    }

    // Blocking this thread until the node is eventually down.
    void join() {
        if (node) {
            node->join();
        }
    }

    int init_db();

    rocksdb::DB *get_db() const;

    static constexpr const char* REPLICATION_MSG = "raft_replication";

private:

    friend class ReplicationClosure;

    // redirecting request to leader
    void redirect(http_res* response);

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
            http_req* request = new http_req(nullptr, "PRIVATE", 0, {}, "INIT_SNAPSHOT");
            http_res* response = new http_res();
            write(request, response);
        }

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
        LOG(ERROR) << "Met raft error " << e;
    }

    void on_configuration_committed(const ::braft::Configuration& conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }

    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node start following " << ctx;
    }

    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }
};
