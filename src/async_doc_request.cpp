#include "async_doc_request.h"
#include "collection_manager.h"

void AsyncDocRequestHandler::init(Store* store, int batch_interval, uint32_t db_size, uint32_t db_interval) {
    async_req_store = store;
    async_batch_interval = batch_interval;
    async_db_size = db_size;
    async_db_size_check_interval = db_interval;
    last_batch_flush_secs = std::chrono::steady_clock::now();
    last_db_size_check_secs = std::chrono::steady_clock::now();
}

void AsyncDocRequestHandler::check_handle_async_doc_request() {
    auto now = std::chrono::steady_clock::now();
    if (!async_request_batch.empty()) {
        auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_batch_flush_secs).count();
        if (time_elapsed >= async_batch_interval) {
            for (const auto& kv : async_request_batch) {
                auto coll_name = kv.first;
                auto collection = CollectionManager::get_instance().get_collection(coll_name);
                auto async_reqs = kv.second;
                nlohmann::json document;

                std::vector <std::string> json_lines;
                for (const auto& async_req: async_reqs) {
                    json_lines.push_back(async_req.first);
                }

                nlohmann::json json_res = collection->add_many(json_lines, document, index_operation_t::CREATE, "",
                                                               DIRTY_VALUES::COERCE_OR_REJECT, false, false, 200,
                                                               60000,
                                                               2, true);


                if (json_res["success"] == false) {
                    for (const auto& doc: json_res["async_docs_status"]) {
                        auto hash = async_reqs[doc["id"].get<size_t>()].second;

                        auto key = ASYNC_DOC_REQ_PREFIX + hash;
                        nlohmann::json value;
                        value["message"] = doc["error"];
                        value["req_id"] = hash;

                        bool inserted = async_req_store->insert(key, value.dump());
                        if (!inserted) {
                            LOG(ERROR) << "Error while dumping async doc request message.";
                        }
                    }
                }
            }

            async_request_batch.clear();
            last_batch_flush_secs = std::chrono::steady_clock::now();
        }
    }
}

void AsyncDocRequestHandler::check_handle_db_size() {
    auto now = std::chrono::steady_clock::now();
    auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_db_size_check_secs).count();
    if(time_elapsed >= async_db_size_check_interval) {
        std::string iter_upper_bound_key = std::string(ASYNC_DOC_REQ_PREFIX) + "`";
        auto iter_upper_bound = new rocksdb::Slice(iter_upper_bound_key);
        const std::string req_id_prefix = ASYNC_DOC_REQ_PREFIX;
        rocksdb::Iterator* it = async_req_store->scan(req_id_prefix, iter_upper_bound);
        //first count the values
        int values_count = 0;
        while(it->Valid()) {
            values_count++;
            it->Next();
        }

        if(values_count > async_db_size) {
            //values exceed the limit
            //need to remove excess values from db, oldest first
            it = async_req_store->scan(req_id_prefix, iter_upper_bound);
            auto begin_key = it->key().ToString();

            auto delete_count = values_count - async_db_size;
            while(it->Valid() && delete_count > 0) {
                it->Next();
                delete_count--;
            }

            auto end_key = it->key().ToString();

            auto status = async_req_store->delete_range(begin_key, end_key);
            if(!status.ok()) {
                LOG(ERROR) << "Failed to remove messages from async_doc_store with code : " + std::to_string(status.code());
            }
        }

        last_batch_flush_secs = std::chrono::steady_clock::now();
        delete iter_upper_bound;
        delete it;
    }
}

nlohmann::json AsyncDocRequestHandler::enqueue(const std::shared_ptr<http_req>& req, const std::string& reqid) {
    async_request_batch[req->params["collection"]].push_back(std::make_pair(req->body, reqid));
    nlohmann::json resp;
    resp["req_id"] = reqid;
    resp["message"] = "Request Queued.";

    return resp;
}

Option<std::string> AsyncDocRequestHandler::get_req_status(const std::string& req) {
    std::string json_doc_str;
    auto key = ASYNC_DOC_REQ_PREFIX + req;
    StoreStatus json_doc_status = async_req_store->get(key, json_doc_str);

    if(json_doc_status != StoreStatus::FOUND) {
        return Option<std::string>(404, "req_id not found.");
    }

    return Option<std::string>(json_doc_str);
}

void AsyncDocRequestHandler::get_last_n_req_status(int n, nlohmann::json& res) {
    const std::string req_id_prefix = ASYNC_DOC_REQ_PREFIX;
    std::vector<std::string> db_values;
    async_req_store->get_last_N_values(ASYNC_DOC_REQ_PREFIX, n, db_values);

    res = nlohmann::json::array();
    nlohmann::json json_val;
    for(const auto& val : db_values) {
        json_val = nlohmann::json::parse(val);
        res.push_back(json_val);
    }
}

bool AsyncDocRequestHandler::is_enabled() {
    return async_batch_interval > 0;
}

uint32_t AsyncDocRequestHandler::get_async_batch_size() {
    size_t req_count = 0;
    for(const auto& group : async_request_batch) {
        req_count += group.second.size();
    }

    return req_count;
}