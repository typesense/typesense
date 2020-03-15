#include "store.h"
#include "raft_server.h"
#include <butil/files/file_enumerator.h>
#include <thread>
#include <string_utils.h>
#include <file_utils.h>
#include <collection_manager.h>
#include "rocksdb/utilities/checkpoint.h"


void ReplicationClosure::Run() {
    // Auto delete `this` after Run()
    std::unique_ptr<ReplicationClosure> self_guard(this);

    // Respond to this RPC.
    if (status().ok()) {
        return;
    }

    // Try redirect if this request failed.
    // TODO: searchStore->redirect(response);
}

// State machine implementation

int ReplicationState::start(int port, int election_timeout_ms, int snapshot_interval_s,
                            const std::string & raft_dir, const std::string & peers) {

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
    std::string prefix = "local://" + raft_dir;
    node_options.log_uri = prefix + "/" + log_dir_name;
    node_options.raft_meta_uri = prefix + "/" + meta_dir_name;
    node_options.snapshot_uri = prefix + "/" + snapshot_dir_name;
    node_options.disable_cli = true;

    braft::Node* node = new braft::Node("default_group", braft::PeerId(addr));

    std::string snapshot_dir = raft_dir + "/" + snapshot_dir_name;
    bool snapshot_exists = dir_enum_count(snapshot_dir) > 0;

    if(snapshot_exists) {
        // we will be assured of on_snapshot() firing and we will wait for that to return the promise
    } else if(!create_init_db_snapshot) {
        // `create_init_db_snapshot` will be handled only after leader starts, otherwise:

        LOG(INFO) << "Snapshot does not exist. We will remove db dir and init db fresh.";

        reset_db();
        if (!butil::DeleteFile(butil::FilePath(db_path), true)) {
            LOG(WARNING) << "rm " << db_path << " failed";
            return -1;
        }

        init_db(); // TODO: handle error
    }

    if (node->init(node_options) != 0) {
        LOG(ERROR) << "Fail to init raft node";
        delete node;
        return -1;
    }

    std::vector<std::string> peer_vec;
    StringUtils::split(peers, peer_vec, ",");

    if(peer_vec.size() == 1) {
        // NOTE: `reset_peers` is NOT safe to run on a cluster of nodes, but okay for standalone
        braft::Configuration conf;
        conf.parse_from(peers);
        auto status = node->reset_peers(conf);
        if(!status.ok()) {
            LOG(ERROR) << status.error_str();
        }
    }

    this->node = node;
    return 0;
}

void ReplicationState::write(http_req* request, http_res* response) {
    if (!is_leader()) {
        LOG(INFO) << "Write called on a follower.";
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
    task.done = new ReplicationClosure(request, response);

    // To avoid ABA problem
    task.expected_term = leader_term.load(butil::memory_order_relaxed);

    // Now the task is applied to the group, waiting for the result.
    return node->apply(task);
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
    if (node) {
        braft::PeerId leader = node->leader_id();
        if (!leader.is_empty()) {
            // TODO: response->set_redirect(leader.to_string());
        }
    }
}

void ReplicationState::on_apply(braft::Iterator& iter) {
    // A batch of tasks are committed, which must be processed through
    // |iter|
    for (; iter.valid(); iter.next()) {
        http_res* response;
        http_req* request;

        // Guard invokes replication_arg->done->Run() asynchronously to avoid the callback blocking the main thread
        braft::AsyncClosureGuard closure_guard(iter.done());

        if (iter.done()) {
            // This task is applied by this node, get value from the closure to avoid additional parsing.
            ReplicationClosure* c = dynamic_cast<ReplicationClosure*>(iter.done());
            response = c->get_response();
            request = c->get_request();
        } else {
            // Parse request from the log
            response = new http_res;
            http_req* remote_request = new http_req;
            remote_request->deserialize(iter.data().to_string());
            remote_request->_req = nullptr;  // indicates remote request

            request = remote_request;
        }

        if(request->http_method == "PRIVATE" && request->body == "INIT_SNAPSHOT") {
            // We attempt to trigger a cold snapshot against an existing stand-alone DB for backward compatibility
            InitSnapshotClosure* init_snapshot_closure = new InitSnapshotClosure(this);
            node->snapshot(init_snapshot_closure);
            delete request;
            delete response;
            continue ;
        }

        // Now that the log has been parsed, perform the actual operation
        // Call http server thread for write and response back to client (if `response` is NOT null)
        // We use a future to block current thread until the async flow finishes

        std::promise<bool> promise;
        std::future<bool> future = promise.get_future();
        auto replication_arg = new AsyncIndexArg{request, response, &promise};
        message_dispatcher->send_message(REPLICATION_MSG, replication_arg);
        future.get();
    }
}

void* ReplicationState::save_snapshot(void* arg) {
    SnapshotArg* sa = static_cast<SnapshotArg*>(arg);
    std::unique_ptr<SnapshotArg> arg_guard(sa);
    brpc::ClosureGuard done_guard(sa->done);

    std::string snapshot_path = sa->writer->get_path() + "/" + db_snapshot_name;

    rocksdb::Checkpoint* checkpoint = nullptr;
    rocksdb::Status status = rocksdb::Checkpoint::Create(sa->db, &checkpoint);

    if(!status.ok()) {
        LOG(WARNING) << "Checkpoint Create failed, msg:" << status.ToString();
        return nullptr;
    }

    std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);
    status = checkpoint->CreateCheckpoint(snapshot_path);
    if(!status.ok()) {
        LOG(WARNING) << "Checkpoint CreateCheckpoint failed, msg:" << status.ToString();
        return nullptr;
    }

    butil::FileEnumerator dir_enum(butil::FilePath(snapshot_path),false, butil::FileEnumerator::FILES);
    for (butil::FilePath name = dir_enum.Next(); !name.empty(); name = dir_enum.Next()) {
        std::string file_name = std::string(db_snapshot_name) + "/" + name.BaseName().value();
        if (sa->writer->add_file(file_name) != 0) {
            sa->done->status().set_error(EIO, "Fail to add file to writer.");
            return nullptr;
        }
    }

    return nullptr;
}

void ReplicationState::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    // Start a new bthread to avoid blocking StateMachine since it could be slow to write data to disk
    SnapshotArg* arg = new SnapshotArg;
    arg->db = db;
    arg->writer = writer;
    arg->done = done;
    bthread_t tid;
    bthread_start_urgent(&tid, NULL, save_snapshot, arg);
}

int ReplicationState::init_db() {
    if (!butil::CreateDirectory(butil::FilePath(db_path))) {
        LOG(WARNING) << "CreateDirectory " << db_path << " failed";
        return -1;
    }

    db_options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(db_options, db_path, &db);
    if (!status.ok()) {
        LOG(WARNING) << "Open DB " << db_path << " failed, msg: " << status.ToString();
        return -1;
    }

    LOG(NOTICE) << "DB open success!";
    LOG(INFO) << "Loading collections from disk...";

    Option<bool> init_op = CollectionManager::get_instance().load();

    if(init_op.ok()) {
        LOG(INFO) << "Finished loading collections from disk.";
    } else {
        LOG(ERROR)<< "Typesense failed to start. " << "Could not load collections from disk: " << init_op.error();
        return 1;
    }

    if(!has_initialized.load()) {
        has_initialized.store(true, butil::memory_order_release);
        ready->set_value(true);
    }

    return 0;
}

int ReplicationState::on_snapshot_load(braft::SnapshotReader* reader) {
    CHECK(!is_leader()) << "Leader is not supposed to load snapshot";

    LOG(INFO) << "on_snapshot_load";

    // Load snapshot from reader, replacing the running StateMachine

    reset_db();
    if (!butil::DeleteFile(butil::FilePath(db_path), true)) {
        LOG(WARNING) << "rm " << db_path << " failed";
        return -1;
    }

    LOG(TRACE) << "rm " << db_path << " success";

    std::string snapshot_path = reader->get_path();
    snapshot_path.append(std::string("/") + db_snapshot_name);

    // tries to use link if possible, or else copies
    if (!copy_dir(snapshot_path, db_path)) {
        LOG(WARNING) << "copy snapshot " << snapshot_path << " to " << db_path << " failed";
        return -1;
    }

    LOG(TRACE) << "copy snapshot " << snapshot_path << " to " << db_path << " success";

    return init_db();
}

void ReplicationState::refresh_peers(const std::string & peers) {
    if(node && is_leader()) {
        LOG(INFO) << "Refreshing peer config";

        braft::Configuration conf;
        conf.parse_from(peers);

        RefreshPeersClosure* refresh_peers_done = new RefreshPeersClosure;
        node->change_peers(conf, refresh_peers_done);
    }
}

rocksdb::DB *ReplicationState::get_db() const {
    return db;
}

ReplicationState::ReplicationState(Store *store, http_message_dispatcher *message_dispatcher,
                                   std::promise<bool> *ready, bool create_init_db_snapshot):
        node(nullptr), leader_term(-1), message_dispatcher(message_dispatcher),
        has_initialized(false), ready(ready), create_init_db_snapshot(create_init_db_snapshot) {
    db = store->_get_db_unsafe();
    db_path = store->get_state_dir_path();
    db_options = store->get_db_options();
}

void ReplicationState::reset_db() {
    delete db;
    db = nullptr;
}

void InitSnapshotClosure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<InitSnapshotClosure> self_guard(this);

    if(status().ok()) {
        LOG(INFO) << "Init snapshot succeeded!";
        replication_state->reset_db();
        replication_state->init_db();
    } else {
        LOG(ERROR) << "Init snapshot failed, error: " << status().error_str();
    }
}
