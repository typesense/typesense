#include "replicator.h"
#include <iostream>
#include "http_client.h"
#include "collection_manager.h"
#include "collection.h"
#include "string_utils.h"
#include <json.hpp>
#include <thread>
#include "logger.h"


void IterateBatchHandler::Put(const rocksdb::Slice& key, const rocksdb::Slice& value) {
    std::vector<std::string> parts;
    StringUtils::split(key.ToString(), parts, "_");

    std::string val = value.ToString() + "\n";

    if(parts.size() >= 2 && parts[0] == Collection::COLLECTION_NEXT_SEQ_PREFIX) {
        // nothing to do here as this is called only when a new collection is created and it's always "0"
    }

    if(parts.size() == 1 && parts[0] == CollectionManager::NEXT_COLLECTION_ID_KEY) {
        ReplicationEvent* replication_event = new ReplicationEvent("UPDATE_NEXT_COLLECTION_ID", 0,
                                                                   key.ToString(), value.ToString());
        server->send_message(REPLICATION_EVENT_MSG, replication_event);
    }

    if(parts.size() >= 2 && parts[0] == Collection::COLLECTION_META_PREFIX) {
        ReplicationEvent* replication_event = new ReplicationEvent("ADD_COLLECTION_META", 0,
                                                                   key.ToString(), value.ToString());
        server->send_message(REPLICATION_EVENT_MSG, replication_event);
    }

    if(parts.size() == 3 && parts[1] == Collection::SEQ_ID_PREFIX) {
        ReplicationEvent* replication_event = new ReplicationEvent("ADD_DOCUMENT", std::stoi(parts[0]),
                                                                   key.ToString(), value.ToString());
        server->send_message(REPLICATION_EVENT_MSG, replication_event);
    }

    if(parts.size() >= 2 && parts[0] == CollectionManager::SYMLINK_PREFIX) {
        ReplicationEvent* replication_event = new ReplicationEvent("ADD_SYMLINK", 0,
                                                                   key.ToString(), value.ToString());
        server->send_message(REPLICATION_EVENT_MSG, replication_event);
    }
}

void IterateBatchHandler::Delete(const rocksdb::Slice& key) {
    std::vector<std::string> parts;
    StringUtils::split(key.ToString(), parts, "_");

    if(parts.size() == 3 && parts[1] == Collection::DOC_ID_PREFIX) {
        ReplicationEvent* replication_event = new ReplicationEvent("REMOVE_DOCUMENT", 0, key.ToString(), "");
        server->send_message(REPLICATION_EVENT_MSG, replication_event);
    }

    if(parts.size() >= 2 && parts[0] == Collection::COLLECTION_META_PREFIX) {
        ReplicationEvent* replication_event = new ReplicationEvent("DROP_COLLECTION", 0, key.ToString(), "");
        server->send_message(REPLICATION_EVENT_MSG, replication_event);
    }

    if(parts.size() >= 2 && parts[0] == CollectionManager::SYMLINK_PREFIX) {
        ReplicationEvent* replication_event = new ReplicationEvent("REMOVE_SYMLINK", 0,
                                                                   key.ToString(), "");
        server->send_message(REPLICATION_EVENT_MSG, replication_event);
    }

}

void IterateBatchHandler::Merge(const rocksdb::Slice& key, const rocksdb::Slice& value) {
    std::vector<std::string> parts;
    StringUtils::split(key.ToString(), parts, "_");

    if(parts.size() >= 2 && parts[0] == Collection::COLLECTION_NEXT_SEQ_PREFIX) {
        ReplicationEvent* replication_event = new ReplicationEvent("INCR_COLLECTION_NEXT_SEQ", 0,
                                                                   key.ToString(), value.ToString());
        server->send_message(REPLICATION_EVENT_MSG, replication_event);
    }
}

void Replicator::start(HttpServer* server, const std::string & master_host_port,
                       const std::string & api_key, Store& store) {
    size_t total_runs = 0;

    while(true) {
        IterateBatchHandler handler(server);
        uint64_t latest_seq_num = store.get_latest_seq_number();

        if(total_runs++ % 20 == 0) {
            // roughly every 60 seconds
            LOG(INFO) << "Replica's latest sequence number: " << latest_seq_num;
        }

        std::string url = master_host_port+"/replication/updates?seq_number="+std::to_string(latest_seq_num+1);
        HttpClient client(url, api_key);

        std::string json_response;
        long status_code = client.get_reponse(json_response);

        if(status_code == 200) {
            nlohmann::json json_content = nlohmann::json::parse(json_response);
            nlohmann::json updates = json_content["updates"];

            // first write to memory
            for (nlohmann::json::iterator update = updates.begin(); update != updates.end(); ++update) {
                const std::string update_decoded = StringUtils::base64_decode(*update);
                rocksdb::WriteBatch write_batch(update_decoded);
                write_batch.Iterate(&handler);
            }

            // now write to store
            for (nlohmann::json::iterator update = updates.begin(); update != updates.end(); ++update) {
                const std::string update_decoded = StringUtils::base64_decode(*update);
                rocksdb::WriteBatch write_batch(update_decoded);
                store._get_db_unsafe()->Write(rocksdb::WriteOptions(), &write_batch);
            }

            if(updates.size() > 0) {
                LOG(INFO) << "Replica has consumed " << store.get_latest_seq_number() << "/"
                          << json_content["latest_seq_num"] << " updates from master.";
            }

        } else {
            LOG(ERR) << "Replication error while fetching records from master, status_code=" << status_code
                     << ", replica's latest sequence number: " << latest_seq_num;

            if(status_code != 0) {
                LOG(ERR) << json_response;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    }
}

void Replicator::on_replication_event(void *data) {
    ReplicationEvent* replication_event = static_cast<ReplicationEvent*>(data);

    if(replication_event->type == "UPDATE_NEXT_COLLECTION_ID") {
        CollectionManager & collection_manager = CollectionManager::get_instance();
        collection_manager.set_next_collection_id(std::stoi(replication_event->value));
    }

    if(replication_event->type == "ADD_COLLECTION_META") {
        nlohmann::json collection_meta;
        try {
            collection_meta = nlohmann::json::parse(replication_event->value);
        } catch(...) {
            LOG(ERR) << "Failed to parse collection meta JSON.";
            LOG(ERR) << "Replication event value: " << replication_event->value;
            delete replication_event;
            exit(1);
        }

        CollectionManager & collection_manager = CollectionManager::get_instance();
        Collection* collection = collection_manager.init_collection(collection_meta, 0);
        collection_manager.add_to_collections(collection);
    }

    if(replication_event->type == "ADD_DOCUMENT") {
        CollectionManager & collection_manager = CollectionManager::get_instance();
        std::vector<std::string> parts;
        StringUtils::split(replication_event->key, parts, "_"); // collection_id, seq_id_prefix, seq_id
        Collection* collection = collection_manager.get_collection_with_id(std::stoi(parts[0]));
        nlohmann::json document = nlohmann::json::parse(replication_event->value);

        uint32_t seq_id = Collection::get_seq_id_from_key(replication_event->key);
        collection->index_in_memory(document, seq_id);
    }

    if(replication_event->type == "ADD_SYMLINK") {
        CollectionManager & collection_manager = CollectionManager::get_instance();
        std::vector<std::string> parts;
        std::string symlink_prefix_key = std::string(CollectionManager::SYMLINK_PREFIX) + "_";
        StringUtils::split(replication_event->key, parts, symlink_prefix_key); // symlink_prefix, symlink_name
        std::string & symlink_name = parts[0];
        spp::sparse_hash_map<std::string, std::string> & simlinks = collection_manager.get_symlinks();
        simlinks[symlink_name] = replication_event->value;
    }

    if(replication_event->type == "INCR_COLLECTION_NEXT_SEQ") {
        CollectionManager & collection_manager = CollectionManager::get_instance();
        const std::string & collection_name = replication_event->key.substr(strlen(Collection::COLLECTION_NEXT_SEQ_PREFIX)+1);
        Collection* collection = collection_manager.get_collection(collection_name);
        collection->increment_next_seq_id_field();
    }

    if(replication_event->type == "REMOVE_DOCUMENT") {
        std::vector<std::string> parts;
        StringUtils::split(replication_event->key, parts, "_"); // collection_id, doc_id_prefix, doc_id
        CollectionManager & collection_manager = CollectionManager::get_instance();
        Collection* collection = collection_manager.get_collection_with_id(std::stoi(parts[0]));
        collection->remove(parts[2], false);
    }

    if(replication_event->type == "REMOVE_SYMLINK") {
        CollectionManager & collection_manager = CollectionManager::get_instance();
        std::vector<std::string> parts;
        std::string symlink_prefix_key = std::string(CollectionManager::SYMLINK_PREFIX) + "_";
        StringUtils::split(replication_event->key, parts, symlink_prefix_key); // symlink_prefix, symlink_name
        std::string & symlink_name = parts[0];
        collection_manager.delete_symlink(symlink_name);
    }

    if(replication_event->type == "DROP_COLLECTION") {
        CollectionManager & collection_manager = CollectionManager::get_instance();
        // <collection_meta_prefix>_<collection_name>
        const std::string & collection_name = replication_event->key.substr(strlen(Collection::COLLECTION_META_PREFIX)+1);
        collection_manager.drop_collection(collection_name, false);
    }

    delete replication_event;
}
