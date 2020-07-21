#include "typesense_server_utils.h"
#include "core_api.h"
#include "config.h"

extern "C" {
#include "jemalloc.h"
}

#ifdef __APPLE__
extern "C" {
    extern void je_zone_register();
}
#endif

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

    server->get("/collections/:collection/overrides", get_overrides);
    server->get("/collections/:collection/overrides/:id", get_override);
    server->put("/collections/:collection/overrides/:id", put_override);
    server->del("/collections/:collection/overrides/:id", del_override);

    server->get("/aliases", get_aliases);
    server->get("/aliases/:alias", get_alias);
    server->put("/aliases/:alias", put_upsert_alias);
    server->del("/aliases/:alias", del_alias);

    server->get("/keys", get_keys);
    server->get("/keys/:id", get_key);
    server->post("/keys", post_create_key);
    server->del("/keys/:id", del_key);

    // meta
    server->get("/metrics.json", get_metrics_json);
    server->get("/debug", get_debug);
    server->get("/health", get_health);
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
    server->get("/health", get_health);
}

int main(int argc, char **argv) {
    #ifdef __APPLE__
    // On OS X, je_zone_register registers jemalloc with the system allocator.
    // We have to force the presence of these symbols on macOS by explicitly calling this method.
    // See these issues:
    // - https://github.com/jemalloc/jemalloc/issues/708
    // - https://github.com/ClickHouse/ClickHouse/pull/11897
    je_zone_register();
    #endif

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
                  << "TYPESENSE_DATA_DIR, TYPESENSE_API_KEY, etc." << std::endl;
        exit(1);
    }

    int ret_code = init_logger(config, TYPESENSE_VERSION);
    if(ret_code != 0) {
        return ret_code;
    }

    return run_server(config, TYPESENSE_VERSION, &master_server_routes);
}