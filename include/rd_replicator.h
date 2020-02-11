#pragma once

#include <string>
#include "http_server.h"

class RDReplicator {
private:
    int64_t last_upserted_id;
    int64_t last_deleted_id;
    std::string last_updated_at;

    void replicate();

    void upsert_delete(const std::vector<std::string> & columns,
                              const std::string & upsert_query,
                              const std::string & delete_query);
public:

    void start(HttpServer* server);

    void on_replication_event(void *data);
};