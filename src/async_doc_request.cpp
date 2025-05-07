#include "async_doc_request.h"

void AsyncDocRequestHandler::init(Store* store, int batch_interval) {
    async_req_store = store;
    async_batch_interval = batch_interval;
    last_batch_flush_secs = std::chrono::steady_clock::now();
}

void AsyncDocRequestHandler::check_handle_async_doc_request() {
    if(!async_request_batch.empty()) {
        auto now = std::chrono::steady_clock::now();
        auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_batch_flush_secs).count();
        if (time_elapsed > async_batch_interval) {
            nlohmann::json res_doc;
            std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

            for (int i = 0; i < async_request_batch.size(); ++i) {
                auto req = async_request_batch[i].first;
                auto hash = async_request_batch[i].second;
                post_add_document(req, res);

                try {
                    res_doc = nlohmann::json::parse(res->body);
                } catch (const std::exception& e) {
                    LOG(ERROR) << "JSON error: " << e.what() << "while parsing req : " << hash;
                    continue;
                }

                if (res_doc.contains("message")) {
                    //error occurred while adding
                    auto key = ASYNC_DOC_REQ_PREFIX + hash;
                    nlohmann::json value;
                    value["message"] = res_doc["message"];
                    value["req_id"] = hash;

                    bool inserted = async_req_store->insert(key, value.dump());
                    if(!inserted) {
                        LOG(ERROR) << "Error while dumping async doc request message.";
                    }
                }
            }

            async_request_batch.clear();
            last_batch_flush_secs = std::chrono::steady_clock::now();
        }
    }
}

void AsyncDocRequestHandler::enqueue(const std::shared_ptr<http_req>& req, const std::string& req_hash) {
    async_request_batch.push_back(std::make_pair(req, req_hash));
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

bool AsyncDocRequestHandler::is_enabled() {
    return async_batch_interval > 0;
}