#include <curl/curl.h>
#include <sys/stat.h>

#include <gflags/gflags.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <braft/raft.h>
#include <raft_server.h>
#include <fstream>
#include <execinfo.h>
#include <http_client.h>

#include "core_api.h"
#include "typesense_server_utils.h"
#include "file_utils.h"
#include "threadpool.h"

HttpServer* server;
std::atomic<bool> quit_raft_service;

void catch_interrupt(int sig) {
    LOG(INFO) << "Stopping Typesense server...";
    signal(sig, SIG_IGN);  // ignore for now as we want to shut down elegantly
    quit_raft_service = true;
    server->stop();
}

void catch_crash(int sig) {
    void *bt[1024];
    char **bt_syms;
    size_t bt_size;

    bt_size = backtrace(bt, 1024);
    bt_syms = backtrace_symbols(bt, bt_size);

    LOG(ERROR) << "Typesense crashed...";
    for (size_t i = 0; i < bt_size; i++) {
        LOG(ERROR) << bt_syms[i];
    }

    exit(-1);
}

Option<std::string> fetch_file_contents(const std::string & file_path) {
    if(!file_exists(file_path)) {
        return Option<std::string>(404, "Error reading file containing peers.");
    }

    std::ifstream infile(file_path);
    std::string content((std::istreambuf_iterator<char>(infile)), (std::istreambuf_iterator<char>()));
    infile.close();

    return Option<std::string>(content);
}

void response_proceed(h2o_generator_t *generator, h2o_req_t *req) {
    h2o_custom_generator_t* custom_generator = reinterpret_cast<h2o_custom_generator_t*>(generator);
    custom_generator->handler(custom_generator->req, custom_generator->res, custom_generator->data);

    h2o_iovec_t body = h2o_strdup(&req->pool, custom_generator->res->body.c_str(), SIZE_MAX);
    const h2o_send_state_t state = custom_generator->res->final ?
                                   H2O_SEND_STATE_FINAL : H2O_SEND_STATE_IN_PROGRESS;
    h2o_send(req, &body, 1, state);

    if(custom_generator->res->final) {
        h2o_dispose_request(req);
        delete custom_generator->req;
        delete custom_generator->res;
        delete custom_generator;
    }
}

void response_stop(h2o_generator_t *generator, h2o_req_t *req) {
    h2o_custom_generator_t* custom_generator = reinterpret_cast<h2o_custom_generator_t*>(generator);

    h2o_dispose_request(req);
    delete custom_generator->req;
    delete custom_generator->res;
    delete custom_generator;
}

void stream_response(bool (*handler)(http_req* req, http_res* res, void* data),
                     http_req & request, http_res & response, void* data) {
    h2o_req_t* req = request._req;
    h2o_custom_generator_t* custom_generator = new h2o_custom_generator_t {
            h2o_generator_t {response_proceed, response_stop}, handler, &request, &response, data
    };

    req->res.status = response.status_code;
    req->res.reason = http_res::get_status_reason(response.status_code);
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, NULL, response.content_type_header.c_str(),
                   response.content_type_header.size());
    h2o_start_response(req, &custom_generator->super);

    h2o_iovec_t body = h2o_strdup(&req->pool, "", SIZE_MAX);
    h2o_send(req, &body, 1, H2O_SEND_STATE_IN_PROGRESS);
}

void init_cmdline_options(cmdline::parser & options, int argc, char **argv) {
    options.set_program_name("./typesense-server");

    options.add<std::string>("data-dir", 'd', "Directory where data will be stored.", true);
    options.add<std::string>("api-key", 'a', "API key that allows all operations.", true);
    options.add<std::string>("search-only-api-key", 's', "API key that allows only searches.", false);

    options.add<std::string>("api-address", '\0', "Address to which Typesense API service binds.", false, "0.0.0.0");
    options.add<uint32_t>("api-port", '\0', "Port on which Typesense API service listens.", false, 8108);

    options.add<std::string>("peering-address", '\0', "Internal IP address to which Typesense peering service binds.", false, "");
    options.add<uint32_t>("peering-port", '\0', "Port on which Typesense peering service listens.", false, 8107);
    options.add<std::string>("peers", '\0', "Path to file with comma separated string of peer node IPs.", false);

    options.add<std::string>("ssl-certificate", 'c', "Path to the SSL certificate file.", false, "");
    options.add<std::string>("ssl-certificate-key", 'k', "Path to the SSL certificate key file.", false, "");

    options.add("enable-cors", '\0', "Enable CORS requests.");

    options.add<std::string>("log-dir", '\0', "Path to the log directory.", false, "");

    options.add<std::string>("config", '\0', "Path to the configuration file.", false, "");

    // DEPRECATED
    options.add<std::string>("listen-address", 'h', "[DEPRECATED: use `api-address`] Address to which Typesense API service binds.", false, "0.0.0.0");
    options.add<uint32_t>("listen-port", 'p', "[DEPRECATED: use `api-port`] Port on which Typesense API service listens.", false, 8108);
    options.add<std::string>("master", 'm', "[DEPRECATED: use clustering via --peers] Master's address in http(s)://<master_address>:<master_port> format "
                                            "to start as read-only replica.", false, "");
}

int init_logger(Config & config, const std::string & server_version) {
    // remove SIGTERM since we handle it on our own
    signal(SIGABRT, catch_crash);
    signal(SIGFPE, catch_crash);
    signal(SIGILL, catch_crash);
    signal(SIGSEGV, catch_crash);

    logging::LoggingSettings log_settings;

    // we can install new signal handlers only after overriding above
    signal(SIGINT, catch_interrupt);
    signal(SIGTERM, catch_interrupt);

    std::string log_dir = config.get_log_dir();
    std::string log_path;

    if(log_dir.empty()) {
        // use console logger if log dir is not specified
        log_settings.logging_dest = logging::LOG_TO_SYSTEM_DEBUG_LOG;
    } else {
        if(!directory_exists(log_dir)) {
            std::cerr << "Typesense failed to start. " << "Log directory " << log_dir << " does not exist.";
            return 1;
        }

        log_settings.logging_dest = logging::LOG_TO_FILE;
        log_path = log_dir + "/" + "typesense.log";
        log_settings.log_file = log_path.c_str();

        LOG(INFO) << "Starting Typesense " << server_version << ". Log directory is configured as: "
                  << log_dir << std::endl;
    }

    logging::InitLogging(log_settings);
    logging::SetMinLogLevel(0);

    return 0;
}

bool on_send_response(void *data) {
    request_response* req_res = static_cast<request_response*>(data);
    server->send_response(req_res->req, req_res->res);
    delete req_res;

    return true;
}

int start_raft_server(ReplicationState& replication_state, const std::string& state_dir, const std::string& path_to_peers,
                      const std::string& peering_address, uint32_t peering_port, uint32_t api_port) {

    std::string peer_ips;

    if(path_to_peers.empty()) {
        LOG(INFO) << "Since no --peers argument is provided, starting a single node Typesense cluster.";
    } else {
        const Option<std::string> & peers_op = fetch_file_contents(path_to_peers);

        if(!peers_op.ok()) {
            LOG(ERROR) << peers_op.error();
            exit(-1);
        } else {
            peer_ips = peers_op.get();
            if(peer_ips.empty()) {
                LOG(ERROR) << "File containing peers is empty.";
                exit(-1);
            } else {
                peer_ips = peers_op.get();
            }
        }
    }

    // start peering server
    brpc::Server raft_server;

    if (braft::add_service(&raft_server, peering_port) != 0) {
        LOG(ERROR) << "Failed to add peering service";
        exit(-1);
    }

    if (raft_server.Start(peering_port, nullptr) != 0) {
        LOG(ERROR) << "Failed to start peering service";
        exit(-1);
    }

    if (replication_state.start(peering_address, peering_port, api_port, 1000, 600, state_dir, peer_ips) != 0) {
        LOG(ERROR) << "Failed to start peering state";
        exit(-1);
    }

    LOG(INFO) << "Typesense peering service is running on " << raft_server.listen_address();

    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    size_t raft_counter = 0;
    while (!brpc::IsAskedToQuit() && !quit_raft_service.load()) {
        if(++raft_counter % 10 == 0 && !path_to_peers.empty()) {
            // reset peer configuration periodically to identify change in cluster membership
            const Option<std::string> & refreshed_peers_op = fetch_file_contents(path_to_peers);
            if(!refreshed_peers_op.ok()) {
                LOG(ERROR) << "Error while refreshing peer configuration: " << refreshed_peers_op.error();
                continue;
            }

            const std::string & refreshed_peer_ips_string = refreshed_peers_op.get();
            replication_state.refresh_peers(refreshed_peer_ips_string);
        }

        sleep(1);
    }

    LOG(INFO) << "Typesense peering service is going to quit.";

    // Stop counter before server
    replication_state.shutdown();
    raft_server.Stop(0);

    // Wait until all the processing tasks are over.
    replication_state.join();
    raft_server.Join();

    LOG(INFO) << "Typesense peering service has quit.";
    return 0;
}

int run_server(const Config & config, const std::string & version, void (*master_server_routes)()) {

    LOG(INFO) << "Starting Typesense " << version << std::flush;
    quit_raft_service = false;

    if(!directory_exists(config.get_data_dir())) {
        LOG(ERROR) << "Typesense failed to start. " << "Data directory " << config.get_data_dir()
                 << " does not exist.";
        return 1;
    }

    if(!config.get_master().empty()) {
        LOG(ERROR) << "The --master option has been deprecated. Please use clustering for high availability. "
                   << "Look for the --peers configuration in the documentation.";
        return 1;
    }

    std::string data_dir = config.get_data_dir();
    std::string db_dir = config.get_data_dir() + "/db";
    std::string state_dir = config.get_data_dir() + "/state";

    bool create_init_db_snapshot = false;  // for importing raw DB from earlier versions

    if(!directory_exists(db_dir) && file_exists(data_dir+"/CURRENT") && file_exists(data_dir+"/IDENTITY")) {
        if(!config.get_peers().empty()) {
            LOG(ERROR) << "Your data directory needs to be migrated to the new format.";
            LOG(ERROR) << "To do that, please start the Typesense server without the --peers argument.";
            return 1;
        }

        LOG(INFO) << "Migrating contents of data directory in a `db` sub-directory, as per the new data layout.";
        bool moved = mv_dir(data_dir, db_dir);
        if(!moved) {
            LOG(ERROR) << "CRITICAL ERROR! Failed to migrate all files in the data directory into a `db` sub-directory.";
            LOG(ERROR) << "NOTE: Please move remaining files manually. Failure to do so **WILL** lead to **DATA LOSS**.";
            return 1;
        }

        create_init_db_snapshot = true;
    }

    Store store(db_dir);
    CollectionManager & collectionManager = CollectionManager::get_instance();
    collectionManager.init(&store, config.get_indices_per_collection(),
                           config.get_api_key(), config.get_search_only_api_key());

    curl_global_init(CURL_GLOBAL_SSL);
    HttpClient & httpClient = HttpClient::get_instance();
    httpClient.init(config.get_api_key());

    server = new HttpServer(
        version,
        config.get_api_address(),
        config.get_api_port(),
        config.get_ssl_cert(),
        config.get_ssl_cert_key(),
        config.get_enable_cors()
    );

    server->set_auth_handler(handle_authentication);

    server->on(SEND_RESPONSE_MSG, on_send_response);
    server->on(ReplicationState::REPLICATION_MSG, async_write_request);

    // first we start the peering service

    ThreadPool thread_pool(4);
    ReplicationState replication_state(&store, &thread_pool, server->get_message_dispatcher(), create_init_db_snapshot);

    std::thread raft_thread([&replication_state, &config, &state_dir]() {
        std::string path_to_peers = config.get_peers();
        start_raft_server(replication_state, state_dir, path_to_peers,
                          config.get_peering_address(), config.get_peering_port(), config.get_api_port());
    });

    LOG(INFO) << "Starting API service...";

    master_server_routes();
    int ret_code = server->run(&replication_state);

    // we are out of the event loop here

    LOG(INFO) << "Typesense API service has quit. Stopping peering service...";
    quit_raft_service = true;
    raft_thread.join();

    curl_global_cleanup();

    delete server;
    CollectionManager::get_instance().dispose();

    return ret_code;
}
