#include "core_api.h"
#include "typesense_server_utils.h"

void master_server_routes() {
    // collection management
    server->post("/collections", post_create_collection);
    server->get("/collections", get_collections);
    server->del("/collections/:collection", del_drop_collection);
    server->get("/collections/:collection", get_collection_summary);

    // document management - `/documents/:id` end-points must be placed last in the list
    server->post("/collections/:collection/documents", post_add_document);
    server->get("/collections/:collection/documents/search", get_search);
    server->get("/collections/:collection/documents/export", get_collection_export, true);
    server->get("/collections/:collection/documents/:id", get_fetch_document);
    server->del("/collections/:collection/documents/:id", del_remove_document);

    // meta
    server->get("/debug", get_debug);
    server->get("/health", get_health);

    // replication
    server->get("/replication/updates", get_replication_updates, true);
}

void replica_server_routes() {
    // collection management
    server->get("/collections", get_collections);
    server->get("/collections/:collection", get_collection_summary);

    // document management - `/documents/:id` end-points must be placed last in the list
    server->get("/collections/:collection/documents/search", get_search);
    server->get("/collections/:collection/documents/export", get_collection_export, true);
    server->get("/collections/:collection/documents/:id", get_fetch_document);

    // meta
    server->get("/debug", get_debug);

    // replication
    server->get("/replication/updates", get_replication_updates, true);
}

int main(int argc, char **argv) {
    cmdline::parser options;
    init_cmdline_options(options, argc, argv);
    options.parse_check(argc, argv);

    std::unique_ptr<g3::LogWorker> log_worker = g3::LogWorker::createLogWorker();
    int ret_code = init_logger(options, log_worker);
    if(ret_code != 0) {
        return ret_code;
    }

    return run_server(options, &master_server_routes, &replica_server_routes);
}