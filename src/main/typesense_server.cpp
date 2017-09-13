#include <signal.h>
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <cmdline.h>
#include "http_server.h"
#include "api.h"
#include "string_utils.h"
#include "replicator.h"

void catch_interrupt(int sig) {
    std::cout << "Quitting Typesense..." << std::endl;
}

int main(int argc, char **argv) {
    cmdline::parser options;
    options.add<std::string>("data-dir", 'd', "Directory where data will be stored.", true);
    options.add<std::string>("api-auth-key", 'k', "Key for authenticating the API endpoints.", true);
    options.add<std::string>("listen-address", 'a', "Address to which Typesense server binds.", false, "0.0.0.0");
    options.add<uint32_t>("listen-port", 'p', "Port on which Typesense server listens.", false, 8080);
    options.add<std::string>("master", 'm', "<master_address>:<master_port> combination to start the server as a "
                                            "read-only replica.", false, "");
    options.parse_check(argc, argv);

    signal(SIGINT, catch_interrupt);

    Store store(options.get<std::string>("data-dir"));
    CollectionManager & collectionManager = CollectionManager::get_instance();
    collectionManager.init(&store, options.get<std::string>("api-auth-key"));

    HttpServer server(
        options.get<std::string>("listen-address"),
        options.get<uint32_t>("listen-port")
    );

    // collection management
    server.post("/collections", post_create_collection, true);
    server.get("/collections", get_collections, true);
    server.del("/collections/:collection", del_drop_collection, true);

    // document management
    server.post("/collections/:collection", post_add_document, true);
    server.get("/collections/:collection", get_collection_summary, true);
    server.get("/collections/:collection/search", get_search, false);
    server.get("/collections/:collection/:id", get_fetch_document, true);
    server.del("/collections/:collection/:id", del_remove_document, true);

    // replication
    server.get("/replication/updates", get_replication_updates, true, true);

    server.on(SEND_RESPONSE_MSG, on_send_response);
    server.on(REPLICATION_EVENT_MSG, Replicator::on_replication_event);

    // start a background replication thread if the server is started as a read-only replica
    if(!options.get<std::string>("master").empty()) {
        const std::string & master_host_port = options.get<std::string>("master");
        std::vector<std::string> parts;
        StringUtils::split(master_host_port, parts, ":");
        if(parts.size() != 2) {
            std::cerr << "Invalid value for --master option. Usage: <master_address>:<master_port>" << std::endl;
            return 1;
        }

        std::cout << "Typesense server started as a read-only replica... Spawning replication thread..." << std::endl;
        std::thread replication_thread([&server, &master_host_port, &store]() {
            Replicator::start(&server, master_host_port, store);
        });

        replication_thread.detach();
    }

    server.run();
    return 0;
}