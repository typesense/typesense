#pragma once

#include <brpc/controller.h>             // brpc::Controller
#include <brpc/server.h>                 // brpc::Server
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include <rocksdb/db.h>

#include "http_data.h"
#include "http_server.h"

class ReplicationState;

// Implements the callback for the state machine
class ReplicationClosure : public braft::Closure {
private:
    ReplicationState *searchStore;
    HttpServer* httpServer;
    const http_req* request;
    http_res* response;
    google::protobuf::Closure* done;

public:
    ReplicationClosure(ReplicationState *searchStore, HttpServer* httpServer, const http_req* request, http_res* response,
                       google::protobuf::Closure* done):
                       searchStore(searchStore), httpServer(httpServer),
                       request(request), response(response), done(done) {

    }

    ~ReplicationClosure() {}

    const http_req* get_request() const {
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

// Implements braft::StateMachine.
class ReplicationState : public braft::StateMachine {
private:
    braft::Node* volatile _node;
    butil::atomic<int64_t> _value;
    butil::atomic<int64_t> _leader_term;
    rocksdb::DB* _db;
    HttpServer* _http_server;

public:
    ReplicationState(): _node(NULL), _value(0), _leader_term(-1) {

    }

    ~ReplicationState() {
        delete _node;
    }

    // Starts this node
    int start(int port, int election_timeout_ms, int snapshot_interval_s,
              const std::string & data_path, const std::string & peers);

    // Generic write method for synchronizing all writes
    void write(const http_req* request, http_res* response, google::protobuf::Closure* done);

    // Generic read method for consistent reads
    void read(http_res* response);

    // updates cluster membership
    void refresh_peers(const std::string & peers);

    bool is_leader() const {
        return _leader_term.load(butil::memory_order_acquire) > 0;
    }

    // Shut this node down.
    void shutdown() {
        if (_node) {
            _node->shutdown(NULL);
        }
    }

    // Blocking this thread until the node is eventually down.
    void join() {
        if (_node) {
            _node->join();
        }
    }

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

    static bool copy_snapshot(const std::string& from_path, const std::string& to_path);

    int on_snapshot_load(braft::SnapshotReader* reader);

    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader, term: " << term;
    }

    void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
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

    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }

    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node start following " << ctx;
    }
};
