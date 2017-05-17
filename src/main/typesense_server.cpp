#include <cmdline.h>
#include "http_server.h"
#include "api.h"

int main(int argc, char **argv) {
    cmdline::parser options;
    options.add<std::string>("data-dir", 'd', "Directory where data will be stored.", true);
    options.add<std::string>("listen-address", 'a', "Address to which Typesense server binds.", false, "0.0.0.0");
    options.add<uint32_t>("listen-port", 'p', "Port on which Typesense server listens.", false, 8080);
    options.parse_check(argc, argv);

    Store store(options.get<std::string>("data-dir"));
    CollectionManager & collectionManager = CollectionManager::get_instance();
    collectionManager.init(&store);

    HttpServer server;

    server.post("/collection", post_create_collection);
    server.post("/collection/:collection", post_add_document);
    server.get("/collection/:collection/search", get_search);
    server.del("/collection/:collection/:id", del_remove_document);

    server.run();
    return 0;
}