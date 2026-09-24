#include "async_write_handler.h"
#include "collection_manager.h"

namespace {
    constexpr size_t DEFAULT_REMOTE_EMBEDDING_TIMEOUT_MS = 60000;
    constexpr size_t DEFAULT_REMOTE_EMBEDDING_NUM_TRIES = 2;

    bool is_valid_action(const std::string& action) {
        return action == "create" || action == "update" || action == "upsert" || action == "emplace";
    }

    bool is_valid_dirty_values(int dirty_values) {
        return dirty_values >= static_cast<int>(DIRTY_VALUES::REJECT) &&
               dirty_values <= static_cast<int>(DIRTY_VALUES::COERCE_OR_DROP);
    }

    std::string prefix_upper_bound(const std::string& prefix) {
        auto upper_bound = prefix;
        ++upper_bound.back();
        return upper_bound;
    }
}

std::string AsyncWriteHandler::get_pending_key(const std::string& req_id) {
    return ASYNC_DOC_PENDING_PREFIX + req_id;
}

std::string AsyncWriteHandler::get_failure_key(const std::string& req_id) {
    return ASYNC_DOC_REQ_PREFIX + req_id;
}

nlohmann::json AsyncWriteHandler::serialize_pending_request(const async_req_coll_action_t& group,
                                                            const async_req_t& request) {
    return {
        {"collection", group.coll},
        {"action", group.action},
        {"dirty_values", group.dirty_values},
        {"remote_embedding_timeout_ms", group.remote_embedding_timeout_ms},
        {"remote_embedding_num_tries", group.remote_embedding_num_tries},
        {"body", request.body},
    };
}

Option<std::pair<AsyncWriteHandler::async_req_coll_action_t, AsyncWriteHandler::async_req_t>>
AsyncWriteHandler::deserialize_pending_request(const std::string& req_id, const std::string& value) {
    try {
        const auto pending = nlohmann::json::parse(value);
        async_req_coll_action_t group{
            pending.at("collection").get<std::string>(),
            pending.at("action").get<std::string>(),
            pending.at("dirty_values").get<int>(),
            pending.at("remote_embedding_timeout_ms").get<size_t>(),
            pending.at("remote_embedding_num_tries").get<size_t>(),
        };
        if(group.coll.empty() || !is_valid_action(group.action) || !is_valid_dirty_values(group.dirty_values)) {
            return Option<std::pair<async_req_coll_action_t, async_req_t>>(
                500, "Pending async write contains invalid request metadata.");
        }
        async_req_t request{pending.at("body").get<std::string>(), req_id};
        return Option<std::pair<async_req_coll_action_t, async_req_t>>(
            std::make_pair(std::move(group), std::move(request)));
    } catch(const std::exception& e) {
        return Option<std::pair<async_req_coll_action_t, async_req_t>>(
            500, "Failed to deserialize pending async write: " + std::string(e.what()));
    }
}

void AsyncWriteHandler::load_pending_requests() {
    const std::string upper_bound_key = prefix_upper_bound(ASYNC_DOC_PENDING_PREFIX);
    rocksdb::Slice upper_bound(upper_bound_key);
    std::unique_ptr<rocksdb::Iterator> it(async_req_store->scan(ASYNC_DOC_PENDING_PREFIX, &upper_bound));

    while(it->Valid() && it->key().starts_with(ASYNC_DOC_PENDING_PREFIX)) {
        const auto key = it->key().ToString();
        const auto req_id = key.substr(std::string(ASYNC_DOC_PENDING_PREFIX).size());
        auto pending_op = deserialize_pending_request(req_id, it->value().ToString());
        if(!pending_op.ok()) {
            LOG(ERROR) << pending_op.error() << " Request id: " << req_id;
            it->Next();
            continue;
        }

        auto pending = pending_op.get();
        async_request_batch[pending.first].push_back(std::move(pending.second));
        it->Next();
    }

    if(!async_request_batch.empty()) {
        batch_started_at = std::chrono::steady_clock::now();
    }
}

void AsyncWriteHandler::load_failure_count() {
    const std::string upper_bound_key = prefix_upper_bound(ASYNC_DOC_REQ_PREFIX);
    rocksdb::Slice upper_bound(upper_bound_key);
    std::unique_ptr<rocksdb::Iterator> it(async_req_store->scan(ASYNC_DOC_REQ_PREFIX, &upper_bound));

    async_req_count = 0;
    while(it->Valid() && it->key().starts_with(ASYNC_DOC_REQ_PREFIX)) {
        ++async_req_count;
        it->Next();
    }
}

void AsyncWriteHandler::init(Store* store, int batch_interval, uint32_t db_size, uint32_t db_interval) {
    std::lock_guard lock(mutex);
    async_req_store = store;
    async_batch_interval = batch_interval;
    async_db_size = db_size;
    async_db_size_check_interval = db_interval;
    async_request_batch.clear();
    batch_started_at.reset();
    last_db_size_check_secs = std::chrono::steady_clock::now();
    load_pending_requests();
    load_failure_count();
}

bool AsyncWriteHandler::finalize_requests(const std::vector<async_req_t>& requests,
                                          const std::map<size_t, std::string>& failures) {
    rocksdb::WriteBatch batch;
    for(size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        auto failure_it = failures.find(i);
        if(failure_it != failures.end()) {
            nlohmann::json value;
            value["message"] = failure_it->second;
            value["req_id"] = request.req_id;
            batch.Put(get_failure_key(request.req_id), value.dump());
        }
        batch.Delete(get_pending_key(request.req_id));
    }

    if(!async_req_store->durable_batch_write(batch)) {
        return false;
    }

    if(!failures.empty()) {
        std::lock_guard lock(mutex);
        async_req_count += failures.size();
    }
    return true;
}

void AsyncWriteHandler::requeue_requests(const async_req_coll_action_t& group, std::vector<async_req_t>&& requests) {
    std::lock_guard lock(mutex);
    if(async_request_batch.empty()) {
        batch_started_at = std::chrono::steady_clock::now();
    }
    auto& queued_requests = async_request_batch[group];
    queued_requests.insert(queued_requests.end(),
                           std::make_move_iterator(requests.begin()), std::make_move_iterator(requests.end()));
}

void AsyncWriteHandler::process_async_writes(bool force) {
    async_request_batch_t ready_batch;
    {
        std::lock_guard lock(mutex);
        if(async_request_batch.empty() || !batch_started_at.has_value()) {
            return;
        }

        const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - batch_started_at.value()).count();
        if(!force && elapsed < async_batch_interval) {
            return;
        }

        ready_batch.swap(async_request_batch);
        batch_started_at.reset();
    }

    for(auto& kv : ready_batch) {
        const auto& group = kv.first;
        auto& async_reqs = kv.second;
        std::map<size_t, std::string> failures;
        auto collection = CollectionManager::get_instance().get_collection(group.coll);

        if(collection == nullptr) {
            for(size_t i = 0; i < async_reqs.size(); ++i) {
                failures.emplace(i, "Collection not found");
            }
        } else {
            std::vector<std::string> json_lines;
            json_lines.reserve(async_reqs.size());
            for(const auto& async_req : async_reqs) {
                json_lines.push_back(async_req.body);
            }

            try {
                nlohmann::json document;
                const auto json_res = collection->add_many(
                    json_lines, document, get_index_operation(group.action), "",
                    static_cast<DIRTY_VALUES>(group.dirty_values),
                    false, false, 200, group.remote_embedding_timeout_ms,
                    group.remote_embedding_num_tries, true);
                for(const auto& doc : json_res["async_docs_status"]) {
                    failures.emplace(doc["id"].get<size_t>(), doc["error"].get<std::string>());
                }
            } catch(const std::exception& e) {
                for(size_t i = 0; i < async_reqs.size(); ++i) {
                    failures.emplace(i, "Async write failed: " + std::string(e.what()));
                }
            }
        }

        if(!finalize_requests(async_reqs, failures)) {
            LOG(ERROR) << "Failed to finalize durable async write batch for collection " << group.coll;
            requeue_requests(group, std::move(async_reqs));
        }
    }
}

void AsyncWriteHandler::check_and_truncate() {
    std::lock_guard lock(mutex);
    auto now = std::chrono::steady_clock::now();
    auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_db_size_check_secs).count();
    if(time_elapsed >= async_db_size_check_interval) {
        std::string iter_upper_bound_key = prefix_upper_bound(ASYNC_DOC_REQ_PREFIX);
        rocksdb::Slice iter_upper_bound(iter_upper_bound_key);
        const std::string req_id_prefix = ASYNC_DOC_REQ_PREFIX;

        if(async_req_count > async_db_size) {
            //values exceed the limit
            //need to remove excess values from db, oldest first
            std::unique_ptr<rocksdb::Iterator> it(async_req_store->scan(req_id_prefix, &iter_upper_bound));
            if(!it->Valid() || !it->key().starts_with(req_id_prefix)) {
                async_req_count = 0;
                last_db_size_check_secs = now;
                return;
            }

            const auto begin_key = it->key().ToString();

            const auto target_delete_count = async_req_count - async_db_size;
            auto delete_count = target_delete_count;
            while(it->Valid() && delete_count > 0) {
                it->Next();
                delete_count--;
            }

            const auto end_key = it->Valid() ? it->key().ToString() : iter_upper_bound_key;

            auto status = async_req_store->delete_range(begin_key, end_key);
            if(!status.ok()) {
                LOG(ERROR) << "Failed to remove messages from async_doc_store with code : " + std::to_string(status.code());
            } else {
                async_req_count -= (target_delete_count - delete_count);
            }
        }

        last_db_size_check_secs = now;
    }
}

Option<nlohmann::json> AsyncWriteHandler::enqueue(const std::shared_ptr<http_req>& req, const std::string& reqid) {
    auto collection = CollectionManager::get_instance().get_collection(req->params["collection"]);
    if(collection == nullptr) {
        return Option<nlohmann::json>(404, "Collection not found");
    }

    auto action = req->params["action"];
    if(action.empty()) {
        action = "create";
    }
    if(!is_valid_action(action)) {
        return Option<nlohmann::json>(400, "Parameter `action` must be a create|update|upsert|emplace.");
    }

    auto dirty_values_param = req->params["dirty_values"];
    const auto dirty_values = collection->parse_dirty_values_option(dirty_values_param);
    size_t remote_embedding_timeout_ms = DEFAULT_REMOTE_EMBEDDING_TIMEOUT_MS;
    size_t remote_embedding_num_tries = DEFAULT_REMOTE_EMBEDDING_NUM_TRIES;
    const auto& timeout_param = req->params["remote_embedding_timeout_ms"];
    const auto& tries_param = req->params["remote_embedding_num_tries"];
    try {
        if(!timeout_param.empty()) {
            if(!StringUtils::is_uint32_t(timeout_param)) {
                throw std::invalid_argument("timeout");
            }
            remote_embedding_timeout_ms = std::stoull(timeout_param);
        }
        if(!tries_param.empty()) {
            if(!StringUtils::is_uint32_t(tries_param)) {
                throw std::invalid_argument("tries");
            }
            remote_embedding_num_tries = std::stoull(tries_param);
        }
    } catch(const std::exception&) {
        return Option<nlohmann::json>(400, "Remote embedding timeout and retry parameters must be non-negative integers.");
    }

    async_req_coll_action_t group{collection->get_name(), action, static_cast<int>(dirty_values),
                                  remote_embedding_timeout_ms, remote_embedding_num_tries};
    async_req_t request{req->body, reqid};
    if(!async_req_store->durable_insert(get_pending_key(reqid), serialize_pending_request(group, request).dump())) {
        return Option<nlohmann::json>(500, "Failed to persist async write request.");
    }

    {
        std::lock_guard lock(mutex);
        if(async_request_batch.empty()) {
            batch_started_at = std::chrono::steady_clock::now();
        }
        async_request_batch[group].push_back(std::move(request));
    }
    nlohmann::json resp;
    resp["req_id"] = reqid;
    resp["message"] = "Request Queued.";

    return Option<nlohmann::json>(resp);
}

Option<std::string> AsyncWriteHandler::get_req_status(const std::string& req) {
    std::string json_doc_str;
    auto key = ASYNC_DOC_REQ_PREFIX + req;
    StoreStatus json_doc_status = async_req_store->get(key, json_doc_str);

    if(json_doc_status != StoreStatus::FOUND) {
        return Option<std::string>(404, "req_id not found.");
    }

    return Option<std::string>(json_doc_str);
}

void AsyncWriteHandler::get_last_n_req_status(int n, nlohmann::json& res) {
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

bool AsyncWriteHandler::is_enabled() {
    return async_batch_interval > 0;
}

uint32_t AsyncWriteHandler::get_async_batch_size() {
    std::lock_guard lock(mutex);
    size_t req_count = 0;
    for(const auto& group : async_request_batch) {
        req_count += group.second.size();
    }

    return req_count;
}
