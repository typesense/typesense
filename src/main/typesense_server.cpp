#include <signal.h>
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <sys/types.h>
#include <sys/stat.h>
#include <cmdline.h>
#include "http_server.h"
#include "api.h"
#include "string_utils.h"
#include "replicator.h"
#include "typesense_version.h"

HttpServer* server;

void catch_interrupt(int sig) {
    std::cout << "Stopping Typesense server..." << std::endl;
    signal(sig, SIG_IGN);  // ignore for now as we want to shut down elegantly
    server->stop();
}

bool directory_exists(const std::string & dir_path) {
    struct stat info;
    return stat(dir_path.c_str(), &info) == 0 && (info.st_mode & S_IFDIR);
}

void master_server_routes() {
    // collection management
    server->post("/collections", post_create_collection);
    server->get("/collections", get_collections);
    server->del("/collections/:collection", del_drop_collection);
    server->get("/collections/:collection", get_collection_summary);

    // document management
    server->post("/collections/:collection/documents", post_add_document);
    server->get("/collections/:collection/documents/search", get_search);
    server->get("/collections/:collection/documents/:id", get_fetch_document);
    server->get("/collections/:collection/documents/export", get_collection_export, true);
    server->del("/collections/:collection/documents/:id", del_remove_document);

    // replication
    server->get("/replication/updates", get_replication_updates, true);
}

void replica_server_routes() {
    // collection management
    server->get("/collections", get_collections);
    server->get("/collections/:collection", get_collection_summary);

    // document management
    server->get("/collections/:collection/documents/search", get_search);
    server->get("/collections/:collection/documents/:id", get_fetch_document);
    server->get("/collections/:collection/export", get_collection_export, true);

    // replication
    server->get("/replication/updates", get_replication_updates, true);
}

int main(int argc, char **argv) {
    cmdline::parser options;
    options.add<std::string>("data-dir", 'd', "Directory where data will be stored.", true);
    options.add<std::string>("api-key", 'k', "API key that allows all operations.", true);
    options.add<std::string>("search-only-api-key", 's', "API key that allows only searches.", false);

    options.add<std::string>("listen-address", 'h', "Address to which Typesense server binds.", false, "0.0.0.0");
    options.add<uint32_t>("listen-port", 'p', "Port on which Typesense server listens.", false, 8108);
    options.add<std::string>("master", 'm', "Provide the master's address in http(s)://<master_address>:<master_port> "
                                            "format to start the server as a read-only replica.", false, "");

    options.add<std::string>("ssl-certificate", 'c', "Path to the SSL certificate file.", false, "");
    options.add<std::string>("ssl-certificate-key", 'e', "Path to the SSL certificate key file.", false, "");

    options.add("enable-cors", '\0', "Enable CORS requests.");

    options.parse_check(argc, argv);

    signal(SIGINT, catch_interrupt);

    std::cout << "Typesense version " << TYPESENSE_VERSION << std::endl;

    if(!directory_exists(options.get<std::string>("data-dir"))) {
        std::cerr << "Typesense failed to start. " << "Data directory " << options.get<std::string>("data-dir")
                  << " does not exist." << std::endl;
        return 1;
    }

    Store store(options.get<std::string>("data-dir"));
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Option<bool> init_op = collectionManager.init(&store, options.get<std::string>("api-key"),
                                                  options.get<std::string>("search-only-api-key"));

    if(init_op.ok()) {
        std::cout << "Finished loading collections from disk." << std::endl;
    } else {
        std::cerr << "Typesense failed to start. " << "Could not load collections from disk: "
                  << init_op.error() << std::endl;
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
            std::cerr << "Invalid value for --master option. Usage: http(s)://<master_address>:<master_port>" << std::endl;
            return 1;
        }

        std::cout << "Typesense server started as a read-only replica... Spawning replication thread..." << std::endl;
        std::thread replication_thread([&master_host_port, &store, &options]() {
            Replicator::start(::server, master_host_port, options.get<std::string>("api-key"), store);
        });

        replication_thread.detach();
    }

    server->run();

    // we are out of the event loop here
    delete server;
    CollectionManager::get_instance().dispose();
    return 0;
}