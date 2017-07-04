#include <cmdline.h>
#include "http_server.h"
#include "api.h"

int main(int argc, char **argv) {
    cmdline::parser options;
    options.add<std::string>("data-dir", 'd', "Directory where data will be stored.", true);
    options.add<std::string>("api-auth-key", 'k', "Key for authenticating the API endpoints.", true);
    options.add<std::string>("listen-address", 'a', "Address to which Typesense server binds.", false, "0.0.0.0");
    options.add<uint32_t>("listen-port", 'p', "Port on which Typesense server listens.", false, 8080);
    options.parse_check(argc, argv);

    Store store(options.get<std::string>("data-dir"));
    CollectionManager & collectionManager = CollectionManager::get_instance();
    collectionManager.init(&store, options.get<std::string>("api-auth-key"));

    HttpServer server(
        options.get<std::string>("listen-address"),
        options.get<uint32_t>("listen-port")
    );

    server.post("/collection", post_create_collection, true);
    server.post("/collection/:collection", post_add_document, true);
    server.get("/collection/:collection/search", get_search, false);
    server.del("/collection/:collection/:id", del_remove_document, true);

    server.run();
    return 0;
}