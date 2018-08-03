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

void init_cmdline_options(cmdline::parser & options, int argc, char **argv) {
    options.set_program_name("./typesense-server");

    options.add<std::string>("data-dir", 'd', "Directory where data will be stored.", true);
    options.add<std::string>("api-key", 'a', "API key that allows all operations.", true);
    options.add<std::string>("search-only-api-key", 's', "API key that allows only searches.", false);

    options.add<std::string>("listen-address", 'h', "Address to which Typesense server binds.", false, "0.0.0.0");
    options.add<uint32_t>("listen-port", 'p', "Port on which Typesense server listens.", false, 8108);
    options.add<std::string>("master", 'm', "Provide the master's address in http(s)://<master_address>:<master_port> "
            "format to start the server as a read-only replica.", false, "");

    options.add<std::string>("ssl-certificate", 'c', "Path to the SSL certificate file.", false, "");
    options.add<std::string>("ssl-certificate-key", 'k', "Path to the SSL certificate key file.", false, "");

    options.add("enable-cors", '\0', "Enable CORS requests.");
    options.add<std::string>("log-dir", '\0', "Path to the log file.", false, "");
}

int init_logger(cmdline::parser & options, std::unique_ptr<g3::LogWorker> & log_worker) {
    // remove SIGTERM since we handle it on our own
    g3::overrideSetupSignals({{SIGABRT, "SIGABRT"}, {SIGFPE, "SIGFPE"},{SIGILL, "SIGILL"}, {SIGSEGV, "SIGSEGV"},});

    // we can install new signal handlers only after overriding above
    signal(SIGINT, catch_interrupt);
    signal(SIGTERM, catch_interrupt);

    std::string log_dir = options.get<std::string>("log-dir");

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

        std::cout << "Starting Typesense " << TYPESENSE_VERSION << ". Log directory is configured as: "
                  << log_dir << std::endl;
    }

    g3::initializeLogging(log_worker.get());

    return 0;
}

int run_server(cmdline::parser & options, void (*master_server_routes)(), void (*replica_server_routes)()) {
    LOG(INFO) << "Starting Typesense " << TYPESENSE_VERSION << std::flush;

    if(!directory_exists(options.get<std::string>("data-dir"))) {
        LOG(ERR) << "Typesense failed to start. " << "Data directory " << options.get<std::string>("data-dir")
                 << " does not exist.";
        return 1;
    }

    Store store(options.get<std::string>("data-dir"));

    LOG(INFO) << "Loading collections from disk...";

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Option<bool> init_op = collectionManager.init(&store, options.get<std::string>("api-key"),
                                                  options.get<std::string>("search-only-api-key"));

    if(init_op.ok()) {
        LOG(INFO) << "Finished loading collections from disk.";
    } else {
        LOG(ERR)<< "Typesense failed to start. " << "Could not load collections from disk: " << init_op.error();
        return 1;
    }

    server = new HttpServer(
            options.get<std::string>("listen-address"),
            options.get<uint32_t>("listen-port"),
            options.get<std::string>("ssl-certificate"),
            options.get<std::string>("ssl-certificate-key"),
            options.exist("enable-cors")
    );

    server->set_auth_handler(handle_authentication);

    server->on(SEND_RESPONSE_MSG, on_send_response);
    server->on(REPLICATION_EVENT_MSG, Replicator::on_replication_event);

    if(options.get<std::string>("master").empty()) {
        master_server_routes();
    } else {
        replica_server_routes();

        const std::string & master_host_port = options.get<std::string>("master");
        std::vector<std::string> parts;
        StringUtils::split(master_host_port, parts, ":");
        if(parts.size() != 3) {
            LOG(ERR) << "Invalid value for --master option. Usage: http(s)://<master_address>:<master_port>";
            return 1;
        }

        LOG(INFO) << "Typesense is starting as a read-only replica... Spawning replication thread...";
        std::thread replication_thread([&master_host_port, &store, &options]() {
            Replicator::start(::server, master_host_port, options.get<std::string>("api-key"), store);
        });

        replication_thread.detach();
    }

    int ret_code = server->run();

    // we are out of the event loop here
    delete server;
    CollectionManager::get_instance().dispose();

    return ret_code;
}