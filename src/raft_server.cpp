#include "raft_server.h"
#include <butil/files/file_enumerator.h>
#include <thread>
#include "rocksdb/utilities/checkpoint.h"


void ReplicationClosure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<ReplicationClosure> self_guard(this);

    // Respond to this RPC.
    brpc::ClosureGuard done_guard(done);
    if (status().ok()) {
        return;
    }

    // Try redirect if this request failed.
    // TODO: searchStore->redirect(response);
}

// State machine implementation

int ReplicationState::start(int port, int election_timeout_ms, int snapshot_interval_s,
                            const std::string & data_path, const std::string & peers) {
    butil::EndPoint addr(butil::my_ip(), port);
    braft::NodeOptions node_options;

    if(node_options.initial_conf.parse_from(peers) != 0) {
        LOG(ERROR) << "Fail to parse peer configuration `" << peers << "`";
        return -1;
    }

    node_options.election_timeout_ms = election_timeout_ms;
    node_options.fsm = this;
    node_options.node_owns_fsm = false;
    node_options.snapshot_interval_s = snapshot_interval_s;
    std::string prefix = "local://" + data_path;
    node_options.log_uri = prefix + "/log";
    node_options.raft_meta_uri = prefix + "/raft_meta";
    node_options.snapshot_uri = prefix + "/snapshot";
    node_options.disable_cli = true;

    braft::Node* node = new braft::Node("default_group", braft::PeerId(addr));
    if (node->init(node_options) != 0) {
        LOG(ERROR) << "Fail to init raft node";
        delete node;
        return -1;
    }

    _node = node;
    return 0;
}

void ReplicationState::write(const http_req* request, http_res* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    if (!is_leader()) {
        // TODO: return redirect(response);
    }

    // Serialize request to replicated WAL so that all the peers in the group receive it as well.
    // NOTE: actual write must be done only on the `on_apply` method to maintain consistency.

    butil::IOBufBuilder bufBuilder;
    bufBuilder << request->serialize();
    butil::IOBuf log = bufBuilder.buf();

    // Apply this log as a braft::Task
    braft::Task task;
    task.data = &log;
    // This callback would be invoked when the task actually executes or fails
    task.done = new ReplicationClosure(this, _http_server, request, response, done_guard.release());

    // To avoid ABA problem
    task.expected_term = _leader_term.load(butil::memory_order_relaxed);

    // Now the task is applied to the group, waiting for the result.
    return _node->apply(task);
}

void ReplicationState::read(http_res* response) {
    // For consistency, reads to follower must be rejected.
    if (!is_leader()) {
        // This node is a follower or it's not up-to-date. Redirect to
        // the leader if possible.
        return redirect(response);
    }

    // This is the leader and is up-to-date. It's safe to respond client
    // TODO:
    // response->set_value(_value.load(butil::memory_order_relaxed));
}

void ReplicationState::redirect(http_res* response) {
    if (_node) {
        braft::PeerId leader = _node->leader_id();
        if (!leader.is_empty()) {
            // TODO: response->set_redirect(leader.to_string());
        }
    }
}

void ReplicationState::on_apply(braft::Iterator& iter) {
    // A batch of tasks are committed, which must be processed through
    // |iter|
    for (; iter.valid(); iter.next()) {
        int64_t detal_value = 0;
        http_res* response = NULL;
        const http_req* request;
        std::unique_ptr<http_req> remote_request(new http_req);

        // Guard helps to invoke iter.done()->Run() asynchronously to avoid the callback blocking the StateMachine
        braft::AsyncClosureGuard closure_guard(iter.done());

        if (iter.done()) {
            // This task is applied by this node, get value from the closure to avoid additional parsing.
            ReplicationClosure* c = dynamic_cast<ReplicationClosure*>(iter.done());
            response = c->get_response();
            request = c->get_request();
        } else {
            // Parse request from the log
            remote_request->deserialize(iter.data().to_string());
            request = remote_request.get();
        }

        // Now that the log has been parsed, perform the actual operation
        // TODO: call http server thread for write

        if (response) {
            // TODO: leader, so send response back to the client
        }
    }
}

void* ReplicationState::save_snapshot(void* arg) {
    ReplicationState::SnapshotArg* sa = (ReplicationState::SnapshotArg*) arg;
    std::unique_ptr<ReplicationState::SnapshotArg> arg_guard(sa);

    brpc::ClosureGuard done_guard(sa->done);

    std::string snapshot_path = sa->writer->get_path() + "/rocksdb_snapshot";

    rocksdb::Checkpoint* checkpoint = NULL;
    rocksdb::Status status = rocksdb::Checkpoint::Create(sa->db, &checkpoint);

    if(!status.ok()) {
        LOG(WARNING) << "Checkpoint Create failed, msg:" << status.ToString();
        return NULL;
    }

    std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);
    status = checkpoint->CreateCheckpoint(snapshot_path);
    if(!status.ok()) {
        LOG(WARNING) << "Checkpoint CreateCheckpoint failed, msg:" << status.ToString();
        return NULL;
    }

    butil::FileEnumerator dir_enum(butil::FilePath(snapshot_path),false, butil::FileEnumerator::FILES);
    for (butil::FilePath name = dir_enum.Next(); !name.empty(); name = dir_enum.Next()) {
        std::string file_name = "rocksdb_snapshot/" + name.BaseName().value();
        sa->writer->add_file(file_name);

        if (sa->writer->add_file(file_name) != 0) {
            sa->done->status().set_error(EIO, "Fail to add file to writer.");
            return NULL;
        }
    }

    return NULL;
}

void ReplicationState::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    // Start a new bthread to avoid blocking StateMachine since it could be slow to write data to disk
    SnapshotArg* arg = new SnapshotArg;
    arg->db = _db;
    arg->writer = writer;
    arg->done = done;
    bthread_t tid;
    bthread_start_urgent(&tid, NULL, save_snapshot, arg);
}

bool ReplicationState::copy_snapshot(const std::string& from_path, const std::string& to_path) {
    struct stat from_stat;

    if (stat(from_path.c_str(), &from_stat) < 0 || !S_ISDIR(from_stat.st_mode)) {
        LOG(WARNING) << "stat " << from_path << " failed";
        return false;
    }

    if (!butil::CreateDirectory(butil::FilePath(to_path))) {
        LOG(WARNING) << "CreateDirectory " << to_path << " failed";
        return false;
    }

    butil::FileEnumerator dir_enum(butil::FilePath(from_path),false, butil::FileEnumerator::FILES);
    for (butil::FilePath name = dir_enum.Next(); !name.empty(); name = dir_enum.Next()) {
        std::string src_file(from_path);
        std::string dst_file(to_path);
        butil::string_appendf(&src_file, "/%s", name.BaseName().value().c_str());
        butil::string_appendf(&dst_file, "/%s", name.BaseName().value().c_str());

        if (0 != link(src_file.c_str(), dst_file.c_str())) {
            if (!butil::CopyFile(butil::FilePath(src_file), butil::FilePath(dst_file))) {
                LOG(WARNING) << "copy " << src_file << " to " << dst_file << " failed";
                return false;
            }
        }
    }

    return true;
}

int ReplicationState::on_snapshot_load(braft::SnapshotReader* reader) {
    // reset rocksdb handle
    if (_db != NULL) {
        delete _db;
        _db = NULL;
    }

    CHECK(!is_leader()) << "Leader is not supposed to load snapshot";

    // Load snapshot from reader, replacing the running StateMachine

    std::string snapshot_path = reader->get_path();
    snapshot_path.append("/rocksdb_snapshot");

    std::string db_path = "./data/rocksdb_data";  // TODO
    if (!butil::DeleteFile(butil::FilePath(db_path), true)) {
        LOG(WARNING) << "rm " << db_path << " failed";
        return -1;
    }

    LOG(TRACE) << "rm " << db_path << " success";
    //TODO: try use link instead of copy

    if (!copy_snapshot(snapshot_path, db_path)) {
        LOG(WARNING) << "copy snapshot " << snapshot_path << " to " << db_path << " failed";
        return -1;
    }

    LOG(TRACE) << "copy snapshot " << snapshot_path << " to " << db_path << " success";

    return 0;
}

void ReplicationState::refresh_peers(const std::string & peers) {
    if(_node && is_leader()) {
        LOG(INFO) << "Refreshing peer config";
        braft::Configuration conf;
        conf.parse_from(peers);
        RefreshPeersClosure* refresh_peers_done = new RefreshPeersClosure;
        _node->change_peers(conf, refresh_peers_done);
    }
}
