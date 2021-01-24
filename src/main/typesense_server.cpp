#include "typesense_server_utils.h"
#include "core_api.h"
#include "config.h"
#include "stackprinter.h"
#include "backward.hpp"

extern "C" {
#include "jemalloc.h"
}

#ifdef __APPLE__
extern "C" {
    extern void je_zone_register();
}
#endif

void master_server_routes() {
    // collection operations
    // NOTE: placing this first to score an immediate hit on O(N) route search
    server->get("/collections/:collection/documents/search", get_search);
    server->post("/multi_search", post_multi_search);

    // document management
    // NOTE:`/documents/:id` end-points must be placed last in the list
    server->post("/collections/:collection/documents", post_add_document);
    server->del("/collections/:collection/documents", del_remove_documents, false, true);

    server->post("/collections/:collection/documents/import", post_import_documents, true, true);
    server->get("/collections/:collection/documents/export", get_export_documents, false, true);

    server->get("/collections/:collection/documents/:id", get_fetch_document);
    server->patch("/collections/:collection/documents/:id", patch_update_document);
    server->del("/collections/:collection/documents/:id", del_remove_document);

    server->get("/collections/:collection/overrides", get_overrides);
    server->get("/collections/:collection/overrides/:id", get_override);
    server->put("/collections/:collection/overrides/:id", put_override);
    server->del("/collections/:collection/overrides/:id", del_override);

    server->get("/collections/:collection/synonyms", get_synonyms);
    server->get("/collections/:collection/synonyms/:id", get_synonym);
    server->put("/collections/:collection/synonyms/:id", put_synonym);
    server->del("/collections/:collection/synonyms/:id", del_synonym);

    // collection management
    server->post("/collections", post_create_collection);
    server->get("/collections", get_collections);
    server->del("/collections/:collection", del_drop_collection);
    server->get("/collections/:collection", get_collection_summary);

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
    server->get("/stats.json", get_stats_json);
    server->get("/debug", get_debug);
    server->get("/health", get_health);
    server->post("/health", post_health);
    server->get("/sequence", get_log_sequence);

    server->post("/operations/snapshot", post_snapshot, false, true);
    server->post("/operations/vote", post_vote, false, false);

    server->post("/config", post_config, false, false);
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

    Config& config = Config::get_instance();

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

#ifdef __APPLE__
    signal(SIGABRT, StackPrinter::bt_sighandler);
    signal(SIGFPE, StackPrinter::bt_sighandler);
    signal(SIGILL, StackPrinter::bt_sighandler);
    signal(SIGSEGV, StackPrinter::bt_sighandler);
#elif __linux__
    backward::SignalHandling sh;
#endif

    // we can install new signal handlers only after overriding above
    signal(SIGINT, catch_interrupt);
    signal(SIGTERM, catch_interrupt);

    return run_server(config, TYPESENSE_VERSION, &master_server_routes);
}