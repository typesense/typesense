#pragma once

#include <string>
#include "http_server.h"

class RDReplicator {
private:
    static void replicate();
public:
    static void start(HttpServer* server);

    static void on_replication_event(void *data);
};