#include "store.h"
#include "raft_server.h"
#include <butil/files/file_enumerator.h>
#include <thread>
#include <string_utils.h>
#include <file_utils.h>
#include <collection_manager.h>
#include <http_client.h>
#include "rocksdb/utilities/checkpoint.h"


void ReplicationClosure::Run() {
    // nothing much to do here since responding to client is handled upstream
    // Auto delete `this` after Run()
    std::unique_ptr<ReplicationClosure> self_guard(this);
}

// State machine implementation

int ReplicationState::start(const butil::EndPoint & peering_endpoint, const int api_port,
                            int election_timeout_ms, int snapshot_interval_s,
                            const std::string & raft_dir, const std::string & nodes) {

    braft::NodeOptions node_options;

    std::string actual_nodes = nodes;

    if(actual_nodes.empty()) {
        std::string ip_str = butil::ip2str(peering_endpoint.ip).c_str();
        actual_nodes = ip_str + ":" + std::to_string(peering_endpoint.port) + ":" + std::to_string(api_port);
    }

    if(node_options.initial_conf.parse_from(actual_nodes) != 0) {
        LOG(ERROR) << "Failed to parse peer configuration `" << nodes << "`";
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

    // api_port is used as the node identifier
    braft::Node* node = new braft::Node("default_group", braft::PeerId(peering_endpoint, api_port));

    std::string snapshot_dir = raft_dir + "/" + snapshot_dir_name;
    bool snapshot_exists = dir_enum_count(snapshot_dir) > 0;

    if(snapshot_exists) {
        // we will be assured of on_snapshot_load() firing and we will wait for that to init_db()
    } else if(!create_init_db_snapshot) {
        // `create_init_db_snapshot` can be handled separately only after leader starts
        LOG(INFO) << "Snapshot does not exist. We will remove db dir and init db fresh.";

        reset_db();
        if (!butil::DeleteFile(butil::FilePath(store->get_state_dir_path()), true)) {
            LOG(WARNING) << "rm " << store->get_state_dir_path() << " failed";
            return -1;
        }

        int init_db_status = init_db();
        if(init_db_status != 0) {
            LOG(ERROR) << "Failed to initialize DB.";
            return init_db_status;
        }
    }

    if (node->init(node_options) != 0) {
        LOG(ERROR) << "Fail to init peering node";
        delete node;
        return -1;
    }

    std::vector<std::string> peer_vec;
    StringUtils::split(actual_nodes, peer_vec, ",");

    if(peer_vec.size() == 1) {
        // NOTE: `reset_peers` is NOT safe to run on a cluster of nodes, but okay for standalone
        // This is handy to reset local state if the instance is started on a different IP
        node->reset_peers(node_options.initial_conf);
    }

    this->node = node;
    return 0;
}

void ReplicationState::write(http_req* request, http_res* response) {
    if (!is_leader()) {
        if(node->leader_id().is_empty()) {
            // Handle no leader scenario
            LOG(ERROR) << "Rejecting write: could not find a leader.";
            response->set_500("Could not find a leader.");
            auto replication_arg = new AsyncIndexArg{request, response, nullptr};
            replication_arg->req->route_hash = static_cast<uint64_t>(ROUTE_CODES::ALREADY_HANDLED);
            return message_dispatcher->send_message(REPLICATION_MSG, replication_arg);
        }

        const std::string & leader_addr = node->leader_id().to_string();
        //LOG(INFO) << "Redirecting write to leader at: " << leader_addr;

        thread_pool->enqueue([leader_addr, request, response, this]() {
            auto raw_req = request->_req;
            std::string scheme = std::string(raw_req->scheme->name.base, raw_req->scheme->name.len);
            std::vector<std::string> addr_parts;
            StringUtils::split(leader_addr, addr_parts, ":");
            std::string leader_host_port = addr_parts[0] + ":" + addr_parts[2];
            const std::string & path = std::string(raw_req->path.base, raw_req->path.len);
            std::string url = scheme + "://" + leader_host_port + path;

            if(request->http_method == "POST") {
                std::string api_res;
                long status = HttpClient::post_response(url, request->body, api_res);
                response->set_body(status, api_res);
            } else if(request->http_method == "PUT") {
                std::string api_res;
                long status = HttpClient::put_response(url, request->body, api_res);
                response->set_body(status, api_res);
            } else if(request->http_method == "DELETE") {
                std::string api_res;
                long status = HttpClient::delete_response(url, api_res);
                response->set_body(status, api_res);
            } else {
                const std::string& err = "Forwarding for http method not implemented: " + request->http_method;
                LOG(ERROR) << err;
                response->set_500(err);
            }

            auto replication_arg = new AsyncIndexArg{request, response, nullptr};
            replication_arg->req->route_hash = static_cast<uint64_t>(ROUTE_CODES::ALREADY_HANDLED);
            message_dispatcher->send_message(REPLICATION_MSG, replication_arg);
        });

        return ;
    }

    // Serialize request to replicated WAL so that all the nodes in the group receive it as well.
    // NOTE: actual write must be done only on the `on_apply` method to maintain consistency.

    butil::IOBufBuilder bufBuilder;
    bufBuilder << request->serialize();

    // Apply this log as a braft::Task

    braft::Task task;
    task.data = &bufBuilder.buf();
    // This callback would be invoked when the task actually executes or fails
    task.done = new ReplicationClosure(request, response);

    // To avoid ABA problem
    task.expected_term = leader_term.load(butil::memory_order_relaxed);

    // Now the task is applied to the group, waiting for the result.
    return node->apply(task);
}

void ReplicationState::read(http_res* response) {
    // NOT USED:
    // For consistency, reads to followers could be rejected.
    // Currently, we don't do implement reads via raft.
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

        if(request->_req == nullptr && request->body == "INIT_SNAPSHOT") {
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
    arg->db = store->_get_db_unsafe();
    arg->writer = writer;
    arg->done = done;
    bthread_t tid;
    bthread_start_urgent(&tid, NULL, save_snapshot, arg);
}

int ReplicationState::init_db() {
    if (!butil::CreateDirectory(butil::FilePath(store->get_state_dir_path()))) {
        LOG(WARNING) << "CreateDirectory " << store->get_state_dir_path() << " failed";
        return -1;
    }

    const rocksdb::Status& status = store->init_db();
    if (!status.ok()) {
        LOG(WARNING) << "Open DB " << store->get_state_dir_path() << " failed, msg: " << status.ToString();
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

    init_readiness_count++;

    return 0;
}

int ReplicationState::on_snapshot_load(braft::SnapshotReader* reader) {
    CHECK(!is_leader()) << "Leader is not supposed to load snapshot";

    LOG(INFO) << "on_snapshot_load";

    // Load snapshot from reader, replacing the running StateMachine

    reset_db();
    if (!butil::DeleteFile(butil::FilePath(store->get_state_dir_path()), true)) {
        LOG(WARNING) << "rm " << store->get_state_dir_path() << " failed";
        return -1;
    }

    LOG(TRACE) << "rm " << store->get_state_dir_path() << " success";

    std::string snapshot_path = reader->get_path();
    snapshot_path.append(std::string("/") + db_snapshot_name);

    // tries to use link if possible, or else copies
    if (!copy_dir(snapshot_path, store->get_state_dir_path())) {
        LOG(WARNING) << "copy snapshot " << snapshot_path << " to " << store->get_state_dir_path() << " failed";
        return -1;
    }

    LOG(TRACE) << "copy snapshot " << snapshot_path << " to " << store->get_state_dir_path() << " success";

    return init_db();
}

void ReplicationState::refresh_nodes(const std::string & nodes) {
    if(!node) {
        return ;
    }

    braft::Configuration conf;
    conf.parse_from(nodes);

    if(is_leader()) {
        RefreshNodesClosure* refresh_nodes_done = new RefreshNodesClosure;
        node->change_peers(conf, refresh_nodes_done);
    } else {
        // if the node is not a leader and is also not able to find a leader, we have to forcefully reset the peers
        if(node->leader_id().is_empty()) {
            LOG(WARNING) << "No leader: resetting peers.";
            node->reset_peers(conf);
        }
    }
}

ReplicationState::ReplicationState(Store *store, ThreadPool* thread_pool, http_message_dispatcher *message_dispatcher,
                                   bool create_init_db_snapshot):
        node(nullptr), leader_term(-1), store(store), thread_pool(thread_pool),
        message_dispatcher(message_dispatcher), init_readiness_count(0),
        create_init_db_snapshot(create_init_db_snapshot) {

}

void ReplicationState::reset_db() {
    store->close();
}

size_t ReplicationState::get_init_readiness_count() const {
    return init_readiness_count.load();
}

bool ReplicationState::is_alive() const {
    if(node == nullptr) {
        return false;
    }

    if(!is_ready()) {
        return false;
    }

    braft::NodeStatus node_status;
    node->get_status(&node_status);

    LOG(INFO) << "Status is: " << node_status.state;

    return (node_status.state == braft::State::STATE_LEADER ||
            node_status.state == braft::State::STATE_TRANSFERRING ||
            node_status.state == braft::State::STATE_CANDIDATE ||
            node_status.state == braft::State::STATE_FOLLOWER);
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
