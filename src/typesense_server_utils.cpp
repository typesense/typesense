#include <curl/curl.h>
#include <sys/stat.h>

#include <gflags/gflags.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <braft/raft.h>
#include <braft/storage.h>
#include <braft/util.h>
#include <braft/protobuf_file.h>
#include <raft_server.h>
#include <fstream>

#include "core_api.h"
#include "typesense_server_utils.h"

HttpServer* server;

void catch_interrupt(int sig) {
    LOG(INFO) << "Stopping Typesense server...";
    signal(sig, SIG_IGN);  // ignore for now as we want to shut down elegantly
    server->stop();
}

bool directory_exists(const std::string & dir_path) {
    struct stat info;
    return stat(dir_path.c_str(), &info) == 0 && (info.st_mode & S_IFDIR);
}

bool file_exists(const std::string & file_path) {
    struct stat info;
    return stat(file_path.c_str(), &info) == 0;
}

void init_cmdline_options(cmdline::parser & options, int argc, char **argv) {
    options.set_program_name("./typesense-server");

    options.add<std::string>("data-dir", 'd', "Directory where data will be stored.", true);
    options.add<std::string>("api-key", 'a', "API key that allows all operations.", true);
    options.add<std::string>("search-only-api-key", 's', "API key that allows only searches.", false);

    options.add<std::string>("listen-address", 'h', "Address to which Typesense server binds.", false, "0.0.0.0");
    options.add<uint32_t>("listen-port", 'p', "Port on which Typesense server listens.", false, 8108);

    options.add<std::string>("raft-dir", '\0', "Directory where raft data will be stored.", false);
    options.add<uint32_t>("raft-port", '\0', "Port on which Typesense raft service listens.", false, 8107);
    options.add<std::string>("raft-peers", '\0', "Path to file having comma separated string of Raft node IPs.", false);

    options.add<std::string>("master", 'm', "To start the server as read-only replica, "
                             "provide the master's address in http(s)://<master_address>:<master_port> format.",
                             false, "");

    options.add<std::string>("ssl-certificate", 'c', "Path to the SSL certificate file.", false, "");
    options.add<std::string>("ssl-certificate-key", 'k', "Path to the SSL certificate key file.", false, "");

    options.add("enable-cors", '\0', "Enable CORS requests.");

    options.add<std::string>("log-dir", '\0', "Path to the log directory.", false, "");

    options.add<std::string>("config", '\0', "Path to the configuration file.", false, "");
}

int init_logger(Config & config, const std::string & server_version, std::unique_ptr<g3::LogWorker> & log_worker) {
    // remove SIGTERM since we handle it on our own
    g3::overrideSetupSignals({{SIGABRT, "SIGABRT"}, {SIGFPE, "SIGFPE"},{SIGILL, "SIGILL"}, {SIGSEGV, "SIGSEGV"},});

    // we can install new signal handlers only after overriding above
    signal(SIGINT, catch_interrupt);
    signal(SIGTERM, catch_interrupt);

    std::string log_dir = config.get_log_dir();

    if(log_dir.empty()) {
        // use console logger if log dir is not specified
        log_worker->addSink(std2::make_unique<ConsoleLoggingSink>(),
                            &ConsoleLoggingSink::ReceiveLogMessage);
    } else {
        if(!directory_exists(log_dir)) {
            std::cerr << "Typesense failed to start. " << "Log directory " << log_dir << " does not exist.";
            return 1;
        }

        log_worker->addDefaultLogger("typesense", log_dir, "");

        std::cout << "Starting Typesense " << server_version << ". Log directory is configured as: "
                  << log_dir << std::endl;
    }

    g3::initializeLogging(log_worker.get());

    return 0;
}

int run_server(const Config & config, const std::string & version,
               void (*master_server_routes)(), void (*replica_server_routes)()) {

    LOG(INFO) << "Starting Typesense " << version << std::flush;

    if(!directory_exists(config.get_data_dir())) {
        LOG(ERR) << "Typesense failed to start. " << "Data directory " << config.get_data_dir()
                 << " does not exist.";
        return 1;
    }

    Store store(config.get_data_dir());

    LOG(INFO) << "Loading collections from disk...";

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Option<bool> init_op = collectionManager.init(&store,
                                                  config.get_indices_per_collection(),
                                                  config.get_api_key(),
                                                  config.get_search_only_api_key());

    if(init_op.ok()) {
        LOG(INFO) << "Finished loading collections from disk.";
    } else {
        LOG(ERR)<< "Typesense failed to start. " << "Could not load collections from disk: " << init_op.error();
        return 1;
    }

    curl_global_init(CURL_GLOBAL_SSL);

    server = new HttpServer(
        version,
        config.get_listen_address(),
        config.get_listen_port(),
        config.get_ssl_cert(),
        config.get_ssl_cert_key(),
        config.get_enable_cors()
    );

    server->set_auth_handler(handle_authentication);

    server->on(SEND_RESPONSE_MSG, on_send_response);
    server->on(REPLICATION_EVENT_MSG, Replicator::on_replication_event);

    if(config.get_master().empty()) {
        master_server_routes();
    } else {
        replica_server_routes();

        const std::string & master_host_port = config.get_master();
        std::vector<std::string> parts;
        StringUtils::split(master_host_port, parts, ":");
        if(parts.size() != 3) {
            LOG(ERR) << "Invalid value for --master option. Usage: http(s)://<master_address>:<master_port>";
            return 1;
        }

        LOG(INFO) << "Typesense is starting as a read-only replica... Master URL is: " << master_host_port;
        LOG(INFO) << "Spawning replication thread...";

        std::thread replication_thread([&store, &config]() {
            Replicator::start(::server, config.get_master(), config.get_api_key(), store);
        });

        replication_thread.detach();
    }

    std::thread raft_thread([&store, &config]() {
        std::string path_to_peers = config.get_raft_peers();

        if(path_to_peers.empty()) {
            return 0;
        }

        if(!file_exists(path_to_peers)) {
            LOG(ERR) << "Error reading file containing raft peers.";
            return -1;
        }

        std::string peer_ips_string;
        std::ifstream infile(path_to_peers);
        std::getline(infile, peer_ips_string);
        infile.close();

        if(peer_ips_string.empty()) {
            LOG(ERR) << "File containing raft peers is empty.";
            return -1;
        }

        // start raft server
        brpc::Server raft_server;
        ReplicationState replication_state;

        uint32_t raft_port = config.get_raft_port();

        if (braft::add_service(&raft_server, raft_port) != 0) {
            LOG(ERR) << "Failed to add raft service";
            return -1;  // TODO: must quit parent process as well
        }

        if (raft_server.Start(raft_port, NULL) != 0) {
            LOG(ERR) << "Failed to start raft server";
            return -1;
        }

        std::vector<std::string> peer_ips;
        StringUtils::split(peer_ips_string, peer_ips, ",");
        std::string peers = StringUtils::join(peer_ips, ":0,");

        if (replication_state.start(raft_port, 1000, 3600, config.get_raft_dir(), peers) != 0) {
            LOG(ERR) << "Failed to start raft state";
            return -1;
        }

        LOG(INFO) << "Typesense raft service is running on " << raft_server.listen_address();

        // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
        size_t raft_counter = 0;
        while (!brpc::IsAskedToQuit()) {
            if(++raft_counter % 10 == 0) {
                // reset peer configuration periodically to identify change in cluster membership
                std::string new_peer_ips_string;
                std::ifstream peers_infile(path_to_peers);
                std::getline(peers_infile, new_peer_ips_string);
                infile.close();
                replication_state.refresh_peers(new_peer_ips_string);
            }
            sleep(1);
        }

        LOG(INFO) << "Typesense raft service is going to quit";

        // Stop counter before server
        replication_state.shutdown();
        raft_server.Stop(0);

        // Wait until all the processing tasks are over.
        replication_state.join();
        raft_server.Join();
        return 0;
    });

    raft_thread.detach();

    int ret_code = server->run();

    // we are out of the event loop here

    curl_global_cleanup();

    delete server;
    CollectionManager::get_instance().dispose();

    return ret_code;
}