#include "store.h"
#include "raft_server.h"
#include <butil/files/file_enumerator.h>
#include <thread>
#include <algorithm>
#include <string_utils.h>
#include <file_utils.h>
#include <collection_manager.h>
#include <http_client.h>
#include "rocksdb/utilities/checkpoint.h"

namespace braft {
    DECLARE_int32(raft_do_snapshot_min_index_gap);
    DECLARE_int32(raft_max_parallel_append_entries_rpc_num);
    DECLARE_bool(raft_enable_append_entries_cache);
    DECLARE_int32(raft_max_append_entries_cache_size);

    DECLARE_int32(raft_max_byte_count_per_rpc);
}

void ReplicationClosure::Run() {
    // nothing much to do here since responding to client is handled upstream
    // Auto delete `this` after Run()
    std::unique_ptr<ReplicationClosure> self_guard(this);
}

// State machine implementation

int ReplicationState::start(const butil::EndPoint & peering_endpoint, const int api_port,
                            int election_timeout_ms, int snapshot_interval_s,
                            const std::string & raft_dir, const std::string & nodes) {

    this->election_timeout_interval_ms = election_timeout_ms;
    this->raft_dir_path = raft_dir;

    braft::NodeOptions node_options;
    std::string actual_nodes_config = to_nodes_config(peering_endpoint, api_port, nodes);

    if(node_options.initial_conf.parse_from(actual_nodes_config) != 0) {
        LOG(ERROR) << "Failed to parse nodes configuration `" << nodes << "`";
        return -1;
    }

    this->read_caught_up = false;
    this->write_caught_up = false;

    // do snapshot only when the gap between applied index and last snapshot index is >= this number
    braft::FLAGS_raft_do_snapshot_min_index_gap = 1;

    // flags for controlling parallelism of append entries
    braft::FLAGS_raft_max_parallel_append_entries_rpc_num = 1;
    braft::FLAGS_raft_enable_append_entries_cache = false;
    braft::FLAGS_raft_max_append_entries_cache_size = 8;

    // flag controls snapshot download size of each RPC
    braft::FLAGS_raft_max_byte_count_per_rpc = 4 * 1024 * 1024; // 4 MB

    node_options.catchup_margin = read_max_lag;
    node_options.election_timeout_ms = election_timeout_ms;
    node_options.fsm = this;
    node_options.node_owns_fsm = false;
    node_options.snapshot_interval_s = snapshot_interval_s;
    node_options.filter_before_copy_remote = true;
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
    } else {
        LOG(INFO) << "Snapshot does not exist. We will remove db dir and init db fresh.";

        int reload_store = store->reload(true, "");
        if(reload_store != 0) {
            return reload_store;
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

    braft::NodeStatus node_status;
    node->get_status(&node_status);

    skip_index_iter = meta_store->scan(SKIP_INDICES_PREFIX);
    populate_skip_index();

    LOG(INFO) << "Node last_index: " << node_status.last_index << ", skip_index: " << skip_index;

    this->node = node;
    return 0;
}

void ReplicationState::populate_skip_index() {
    if(skip_index_iter->Valid() && skip_index_iter->key().starts_with(SKIP_INDICES_PREFIX)) {
        const std::string& index_value = skip_index_iter->value().ToString();
        if(StringUtils::is_int64_t(index_value)) {
            skip_index = std::stoll(index_value);
        }

        skip_index_iter->Next();
    } else {
        skip_index = UNSET_SKIP_INDEX;
    }
}

std::string ReplicationState::to_nodes_config(const butil::EndPoint& peering_endpoint, const int api_port,
                                              const std::string& nodes_config) {
    std::string actual_nodes_config = nodes_config;

    if(nodes_config.empty()) {
        std::string ip_str = butil::ip2str(peering_endpoint.ip).c_str();
        actual_nodes_config = ip_str + ":" + std::to_string(peering_endpoint.port) + ":" + std::to_string(api_port);
    }

    return actual_nodes_config;
}

void ReplicationState::write(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response) {
    if(shutting_down) {
        //LOG(INFO) << "write(), force shutdown";
        response->set_503("Shutting down.");
        response->final = true;
        request->_req = nullptr;
        request->notify();
        return ;
    }

    std::shared_lock lock(node_mutex);

    if(!node) {
        return ;
    }

    if (!node->is_leader()) {
        return write_to_leader(request, response);
    }

    // Serialize request to replicated WAL so that all the nodes in the group receive it as well.
    // NOTE: actual write must be done only on the `on_apply` method to maintain consistency.

    butil::IOBufBuilder bufBuilder;
    bufBuilder << request->serialize();

    //LOG(INFO) << "write() pre request ref count " << request.use_count();

    // Apply this log as a braft::Task

    braft::Task task;
    task.data = &bufBuilder.buf();
    // This callback would be invoked when the task actually executes or fails
    task.done = new ReplicationClosure(request, response);

    //LOG(INFO) << "write() post request ref count " << request.use_count();

    // To avoid ABA problem
    task.expected_term = leader_term.load(butil::memory_order_relaxed);

    //LOG(INFO) << ":::" << "body size before apply: " << request->body.size();

    // Now the task is applied to the group
    node->apply(task);

    pending_writes++;
}

void ReplicationState::write_to_leader(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response) {
    // no lock on `node` needed as caller uses the lock
    if(!node || node->leader_id().is_empty()) {
        // Handle no leader scenario
        LOG(ERROR) << "Rejecting write: could not find a leader.";

        if(request->_req->proceed_req && response->proxied_stream) {
            // streaming in progress: ensure graceful termination (cannot start response again)
            LOG(ERROR) << "Terminating streaming request gracefully.";
            request->_req = nullptr;
            request->notify();
            return ;
        }

        response->set_500("Could not find a leader.");
        auto req_res = new deferred_req_res_t(request, response, server, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }

    if (request->_req->proceed_req && response->proxied_stream) {
        // indicates async request body of in-flight request
        //LOG(INFO) << "Inflight proxied request, returning control to caller, body_size=" << request->body.size();
        request->notify();
        return ;
    }

    const std::string & leader_addr = node->leader_id().to_string();
    //LOG(INFO) << "Redirecting write to leader at: " << leader_addr;

    h2o_custom_generator_t* custom_generator = reinterpret_cast<h2o_custom_generator_t *>(response->generator);
    HttpServer* server = custom_generator->h2o_handler->http_server;

    auto raw_req = request->_req;
    const std::string& path = std::string(raw_req->path.base, raw_req->path.len);
    const std::string& scheme = std::string(raw_req->scheme->name.base, raw_req->scheme->name.len);
    const std::string url = get_leader_url_path(leader_addr, path, scheme);

    thread_pool->enqueue([request, response, server, path, url, this]() {
        pending_writes++;

        std::map<std::string, std::string> res_headers;

        if(request->http_method == "POST") {
            std::vector<std::string> path_parts;
            StringUtils::split(path, path_parts, "/");

            if(path_parts.back().rfind("import", 0) == 0) {
                // imports are handled asynchronously
                response->proxied_stream = true;
                long status = HttpClient::post_response_async(url, request, response, server);

                if(status == 500) {
                    response->content_type_header = res_headers["content-type"];
                    response->set_500("");
                } else {
                    pending_writes--;
                    return ;
                }
            } else {
                std::string api_res;
                long status = HttpClient::post_response(url, request->body, api_res, res_headers);
                response->content_type_header = res_headers["content-type"];
                response->set_body(status, api_res);
            }
        } else if(request->http_method == "PUT") {
            std::string api_res;
            long status = HttpClient::put_response(url, request->body, api_res, res_headers);
            response->content_type_header = res_headers["content-type"];
            response->set_body(status, api_res);
        } else if(request->http_method == "DELETE") {
            std::string api_res;
            long status = HttpClient::delete_response(url, api_res, res_headers);
            response->content_type_header = res_headers["content-type"];
            response->set_body(status, api_res);
        } else if(request->http_method == "PATCH") {
            std::string api_res;
            long status = HttpClient::patch_response(url, request->body, api_res, res_headers);
            response->content_type_header = res_headers["content-type"];
            response->set_body(status, api_res);
        } else {
            const std::string& err = "Forwarding for http method not implemented: " + request->http_method;
            LOG(ERROR) << err;
            response->set_500(err);
        }

        auto req_res = new deferred_req_res_t(request, response, server, true);
        message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        pending_writes--;
    });
}

std::string ReplicationState::get_leader_url_path(const std::string& leader_addr, const std::string& path,
                                                  const std::string& protocol) const {
    std::vector<std::string> addr_parts;
    StringUtils::split(leader_addr, addr_parts, ":");
    std::string leader_host_port = addr_parts[0] + ":" + addr_parts[2];
    std::string url = protocol + "://" + leader_host_port + path;
    return url;
}

void ReplicationState::on_apply(braft::Iterator& iter) {
    //LOG(INFO) << "ReplicationState::on_apply";
    // NOTE: this is executed on a different thread and runs concurrent to http thread
    // A batch of tasks are committed, which must be processed through
    // |iter|
    for (; iter.valid(); iter.next()) {
        // Guard invokes replication_arg->done->Run() asynchronously to avoid the callback blocking the main thread
        braft::AsyncClosureGuard closure_guard(iter.done());

        if(iter.index() == skip_index) {
            LOG(ERROR) << "Skipping write log index " << iter.index()
                       << " which seems to have triggered a crash previously.";
            populate_skip_index();
            continue;
        }

        //LOG(INFO) << "Init use count: " << dynamic_cast<ReplicationClosure*>(iter.done())->get_request().use_count();

        const std::shared_ptr<http_req>& request_generated = iter.done() ?
                         dynamic_cast<ReplicationClosure*>(iter.done())->get_request() : std::make_shared<http_req>();

        //LOG(INFO) << "Post assignment " << request_generated.get() << ", use count: " << request_generated.use_count();

        const std::shared_ptr<http_res>& response_generated = iter.done() ?
                dynamic_cast<ReplicationClosure*>(iter.done())->get_response() : std::make_shared<http_res>();

        if(!iter.done()) {
            // indicates log serialized request
            request_generated->deserialize(iter.data().to_string());
        }

        // Now that the log has been parsed, perform the actual operation

        bool async_res = false;

        route_path* found_rpath = nullptr;
        bool route_found = server->get_route(request_generated->route_hash, &found_rpath);

        //LOG(INFO) << "Pre handler " << request_generated.get() << ", use count: " << request_generated.use_count();

        if(route_found) {
            async_res = found_rpath->async_res;
            found_rpath->handler(request_generated, response_generated);
        } else {
            response_generated->set_404();
        }

        //LOG(INFO) << "Pre dispatch " << request_generated.get() << ", use count: " << request_generated.use_count();

        if(!async_res) {
            deferred_req_res_t* req_res = new deferred_req_res_t(request_generated, response_generated, server, true);
            message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        }

        //LOG(INFO) << "Raft write pre wait " << request_generated.get() << ", use count: " << request_generated.use_count();

        response_generated->wait();

        //LOG(INFO) << "Raft write post wait " << request_generated.get() << ", use count: " << request_generated.use_count();

        if(iter.done()) {
            pending_writes--;
            //LOG(INFO) << "pending_writes: " << pending_writes;
        }
    }
}

void ReplicationState::read(const std::shared_ptr<http_res>& response) {
    // NOT USED:
    // For consistency, reads to followers could be rejected.
    // Currently, we don't do implement reads via raft.
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
            return nullptr;
        }
    }

    sa->done->Run();

    // if an external snapshot is requested, copy latest snapshot directory into that
    if(!sa->ext_snapshot_path.empty()) {
        LOG(INFO) << "Copying system snapshot to external snapshot directory at " << sa->ext_snapshot_path;

        const butil::FilePath& dest_state_dir = butil::FilePath(sa->ext_snapshot_path + "/state");

        if(!butil::DirectoryExists(dest_state_dir)) {
            butil::CreateDirectory(dest_state_dir, true);
        }

        const butil::FilePath& src_snapshot_dir = butil::FilePath(sa->state_dir_path + "/snapshot");
        const butil::FilePath& src_meta_dir = butil::FilePath(sa->state_dir_path + "/meta");

        butil::CopyDirectory(src_snapshot_dir, dest_state_dir, true);
        butil::CopyDirectory(src_meta_dir, dest_state_dir, true);

        // notify on demand closure that external snapshotting is done
        sa->replication_state->notify();
    }

    // NOTE: *must* do a dummy write here since snapshots cannot be triggered if no write has happened since the
    // last snapshot. By doing a dummy write right after a snapshot, we ensure that this can never be the case.
    sa->replication_state->do_dummy_write();

    LOG(INFO) << "save_snapshot done";

    return nullptr;
}

// this method is serial to on_apply so guarantees a snapshot view of the state machine
void ReplicationState::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    LOG(INFO) << "on_snapshot_save";

    std::string db_snapshot_path = writer->get_path() + "/" + db_snapshot_name;
    rocksdb::Checkpoint* checkpoint = nullptr;
    rocksdb::Status status = store->create_check_point(&checkpoint, db_snapshot_path);
    std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);

    if(!status.ok()) {
        LOG(ERROR) << "Failure during checkpoint creation, msg:" << status.ToString();
        done->status().set_error(EIO, "Checkpoint creation failure.");
    }

    SnapshotArg* arg = new SnapshotArg;
    arg->replication_state = this;
    arg->writer = writer;
    arg->state_dir_path = raft_dir_path;
    arg->db_snapshot_path = db_snapshot_path;
    arg->done = done;

    if(!ext_snapshot_path.empty()) {
        arg->ext_snapshot_path = ext_snapshot_path;
        ext_snapshot_path = "";
    }

    // we will also delete all the skip indices in meta store and flush that DB
    // this will block raft writes, but should be pretty fast
    delete skip_index_iter;
    skip_index_iter = meta_store->scan(SKIP_INDICES_PREFIX);

    while(skip_index_iter->Valid() && skip_index_iter->key().starts_with(SKIP_INDICES_PREFIX)) {
        meta_store->remove(skip_index_iter->key().ToString());
        skip_index_iter->Next();
    }

    meta_store->flush();

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
    std::string snapshot_path = reader->get_path();
    snapshot_path.append(std::string("/") + db_snapshot_name);

    int reload_store = store->reload(true, snapshot_path);
    if(reload_store != 0) {
        return reload_store;
    }

    bool init_db_status = init_db();

    read_caught_up = write_caught_up = (init_db_status == 0);
    return init_db_status;
}

void ReplicationState::refresh_nodes(const std::string & nodes) {
    std::shared_lock lock(node_mutex);

    if(!node) {
        LOG(WARNING) << "Node state is not initialized: unable to refresh nodes.";
        return ;
    }

    braft::Configuration new_conf;
    new_conf.parse_from(nodes);

    braft::NodeStatus nodeStatus;
    node->get_status(&nodeStatus);

    LOG(INFO) << "Term: " << nodeStatus.term
             << ", last_index index: " << nodeStatus.last_index
             << ", committed_index: " << nodeStatus.committed_index
             << ", known_applied_index: " << nodeStatus.known_applied_index
             << ", applying_index: " << nodeStatus.applying_index
             << ", pending_index: " << nodeStatus.pending_index
             << ", disk_index: " << nodeStatus.disk_index
             << ", pending_queue_size: " << nodeStatus.pending_queue_size
             << ", local_sequence: " << store->get_latest_seq_number();

    if(node->is_leader()) {
        RefreshNodesClosure* refresh_nodes_done = new RefreshNodesClosure;
        node->change_peers(new_conf, refresh_nodes_done);
    } else {
        if(node->leader_id().is_empty()) {
            // When node is not a leader, does not have a leader and is also a single-node cluster,
            // we forcefully reset its peers.
            // NOTE: `reset_peers()` is not a safe call to make as we give up on consistency and consensus guarantees.
            // We are doing this solely to handle single node cluster whose IP changes.
            // Examples: Docker container IP change, local DHCP leased IP change etc.

            std::vector<braft::PeerId> latest_nodes;
            new_conf.list_peers(&latest_nodes);

            if(latest_nodes.size() == 1) {
                LOG(WARNING) << "Single-node with no leader. Resetting peers.";
                node->reset_peers(new_conf);
            } else {
                LOG(WARNING) << "Multi-node with no leader: refusing to reset peers.";
            }

            return ;
        }
    }
}

void ReplicationState::refresh_catchup_status(bool log_msg) {
    std::shared_lock lock(node_mutex);

    if (!node) {
        LOG_IF(WARNING, log_msg) << "Node state is not initialized: unable to refresh nodes.";
        return;
    }

    if(!node->is_leader() && node->leader_id().is_empty()) {
        // follower does not have a leader!
        this->read_caught_up = false;
        this->write_caught_up = false;
        return ;
    }

    braft::NodeStatus n_status;
    node->get_status(&n_status);
    lock.unlock();

    if (n_status.applying_index == 0) {
        this->read_caught_up = true;
        this->write_caught_up = true;
        return ;
    }

    size_t apply_lag = size_t(n_status.last_index - n_status.known_applied_index);

    if (apply_lag > read_max_lag) {
        LOG(ERROR) << apply_lag << " lagging entries > read max lag of " + std::to_string(read_max_lag);
        this->read_caught_up = false;
    }

    if (apply_lag > write_max_lag) {
        LOG(ERROR) << apply_lag << " lagging entries > write max lag of " + std::to_string(write_max_lag);
        this->write_caught_up = false;
    }
}

ReplicationState::ReplicationState(HttpServer* server, Store *store, Store* meta_store, ThreadPool* thread_pool,
                                   http_message_dispatcher *message_dispatcher,
                                   bool api_uses_ssl,
                                   size_t read_max_lag, size_t write_max_lag,
                                   size_t num_collections_parallel_load, size_t num_documents_parallel_load):
        node(nullptr), leader_term(-1), server(server), store(store), meta_store(meta_store),
        thread_pool(thread_pool), message_dispatcher(message_dispatcher), api_uses_ssl(api_uses_ssl),
        read_max_lag(read_max_lag), write_max_lag(write_max_lag),
        num_collections_parallel_load(num_collections_parallel_load),
        num_documents_parallel_load(num_documents_parallel_load),
        ready(false), shutting_down(false), pending_writes(0) {

}

bool ReplicationState::is_alive() const {
    std::shared_lock lock(node_mutex);

    if(node == nullptr ) {
        return false;
    }

    bool leader_or_follower = (node->is_leader() || !node->leader_id().is_empty());
    if(!leader_or_follower) {
        return false;
    }

    // for general health check we will only care about the `read_caught_up` threshold
    return read_caught_up;
}

uint64_t ReplicationState::node_state() const {
    std::shared_lock lock(node_mutex);

    if(node == nullptr) {
        return 0;
    }

    braft::NodeStatus node_status;
    node->get_status(&node_status);

    return node_status.state;
}

void ReplicationState::do_snapshot(const std::string& snapshot_path, const std::shared_ptr<http_req>& req,
                                   const std::shared_ptr<http_res>& res) {
    LOG(INFO) << "Triggerring an on demand snapshot...";

    thread_pool->enqueue([&snapshot_path, req, res, this]() {
        OnDemandSnapshotClosure* snapshot_closure = new OnDemandSnapshotClosure(this, req, res);
        ext_snapshot_path = snapshot_path;
        std::shared_lock lock(this->node_mutex);
        node->snapshot(snapshot_closure);
    });
}

void ReplicationState::set_ext_snapshot_path(const std::string& snapshot_path) {
    this->ext_snapshot_path = snapshot_path;
}

const std::string &ReplicationState::get_ext_snapshot_path() const {
    return ext_snapshot_path;
}

void ReplicationState::do_dummy_write() {
    std::shared_lock lock(node_mutex);

    if(node->leader_id().is_empty()) {
        LOG(ERROR) << "Could not do a dummy write, as node does not have a leader";
        return ;
    }

    const std::string & leader_addr = node->leader_id().to_string();
    lock.unlock();

    const std::string protocol = api_uses_ssl ? "https" : "http";
    std::string url = get_leader_url_path(leader_addr, "/health", protocol);

    std::string api_res;
    std::map<std::string, std::string> res_headers;
    long status_code = HttpClient::post_response(url, "", api_res, res_headers);

    LOG(INFO) << "Dummy write to " << url << ", status = " << status_code << ", response = " << api_res;
}

bool ReplicationState::trigger_vote() {
    std::shared_lock lock(node_mutex);

    if(node) {
        auto status = node->vote(election_timeout_interval_ms);
        LOG(INFO) << "Triggered vote. Ok? " << status.ok() << ", status: " << status;
        return status.ok();
    }

    return false;
}

http_message_dispatcher* ReplicationState::get_message_dispatcher() const {
    return message_dispatcher;
}

Store* ReplicationState::get_store() {
    return store;
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

    delete skip_index_iter;
}

void ReplicationState::persist_applying_index() {
    std::shared_lock lock(node_mutex);

    if(node == nullptr) {
        return ;
    }

    braft::NodeStatus node_status;
    node->get_status(&node_status);

    lock.unlock();

    LOG(INFO) << "Saving currently applying index: " << node_status.applying_index;

    std::string key = SKIP_INDICES_PREFIX + std::to_string(node_status.applying_index);
    meta_store->insert(key, std::to_string(node_status.applying_index));
}

void OnDemandSnapshotClosure::Run() {
    // Auto delete this after Done()
    std::unique_ptr<OnDemandSnapshotClosure> self_guard(this);

    replication_state->wait(); // until on demand snapshotting completes
    replication_state->set_ext_snapshot_path("");

    req->last_chunk_aggregate = true;
    res->final = true;

    nlohmann::json response;
    uint32_t status_code;

    if(status().ok()) {
        LOG(INFO) << "On demand snapshot succeeded!";
        status_code = 201;
        response["success"] = true;
    } else {
        LOG(ERROR) << "On demand snapshot failed, error: " << status().error_str() << ", code: " << status().error_code();
        status_code = 500;
        response["success"] = false;
        response["error"] = status().error_str();
    }

    res->status_code = status_code;
    res->body = response.dump();

    auto req_res = new deferred_req_res_t(req, res, nullptr, true);
    replication_state->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);

    // wait for response to be sent
    res->wait();
}
