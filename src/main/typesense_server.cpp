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

    server.get("/search/:collection", get_search);
    server.post("/search/:collection", post_add_document);

    /*server.get("/search/:collection", [](http_req & req, http_res & res) -> int {
        res.status_code = 200;
        res.body = "{\"collection\": \"" + req.params["collection"] + "\"}";
        return 0;
    });*/

    server.run();
    return 0;
}