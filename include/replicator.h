#pragma once

#include <string>
#include "http_server.h"
#include "store.h"

static constexpr const char* REPLICATION_EVENT_MSG = "replication_event";

struct ReplicationEvent {
    std::string type;
    std::string key;
    std::string value;

    ReplicationEvent(const std::string& type, const uint32_t collection_id,
                     const std::string& key, const std::string& value): type(type), key(key), value(value) {

    }
};

class IterateBatchHandler: public rocksdb::WriteBatch::Handler {
private:
    HttpServer* server;
public:
    IterateBatchHandler(HttpServer* server): server(server) {

    }

    void Put(const rocksdb::Slice& key, const rocksdb::Slice& value);

    void Delete(const rocksdb::Slice& key);

    void Merge(const rocksdb::Slice& key, const rocksdb::Slice& value);
};

class Replicator {
public:
    static void start(HttpServer* server, const std::string & master_host_port, const std::string & api_key, Store& store);

    static void on_replication_event(void *data);
};
