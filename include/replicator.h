#pragma once

#include <string>
#include "http_data.h"
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
    http_message_dispatcher* message_dispatcher;
public:
    IterateBatchHandler(http_message_dispatcher* message_dispatcher): message_dispatcher(message_dispatcher) {

    }

    void Put(const rocksdb::Slice& key, const rocksdb::Slice& value);

    void Delete(const rocksdb::Slice& key);

    void Merge(const rocksdb::Slice& key, const rocksdb::Slice& value);
};

class Replicator {
public:
    static void start(http_message_dispatcher* message_dispatcher, const std::string & master_host_port, const std::string & api_key, Store& store);

    static bool on_replication_event(void *data);
};
