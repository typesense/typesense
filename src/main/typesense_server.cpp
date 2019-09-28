#include "core_api.h"
#include "config.h"
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

    server->post("/collections/:collection/documents/import", post_import_documents);
    server->get("/collections/:collection/documents/export", get_export_documents, true);

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
    server->get("/collections/:collection/documents/export", get_export_documents, true);
    server->get("/collections/:collection/documents/:id", get_fetch_document);

    // meta
    server->get("/debug", get_debug);

    // replication
    server->get("/replication/updates", get_replication_updates, true);
}

int main(int argc, char **argv) {
    Config config;

    cmdline::parser options;
    init_cmdline_options(options, argc, argv);
    options.parse(argc, argv);

    // Command line args override env vars
    config.load_config_env();
    config.load_config_file(options);
    config.load_config_cmd_args(options);

    Option<bool> config_validitation = config.is_valid();

    if(!config_validitation.ok()) {
        std::cerr << "Invalid configuration: " << config_validitation.error() << std::endl;
        std::cerr << "Command line " << options.usage() << std::endl;
        std::cerr << "You can also pass these arguments as environment variables such as "
                  << "DATA_DIR, API_KEY, etc." << std::endl;
        exit(1);
    }

    std::unique_ptr<g3::LogWorker> log_worker = g3::LogWorker::createLogWorker();
    int ret_code = init_logger(config, TYPESENSE_VERSION, log_worker);
    if(ret_code != 0) {
        return ret_code;
    }

    return run_server(config, TYPESENSE_VERSION, &master_server_routes, &replica_server_routes);
}