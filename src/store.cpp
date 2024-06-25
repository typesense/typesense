#include "include/store.h"

Store::Store(const std::string & state_dir_path,
      const size_t wal_ttl_secs,
      const size_t wal_size_mb, bool disable_wal, int32_t ttl):
      state_dir_path(state_dir_path){
    // Optimize RocksDB
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;
    options.write_buffer_size = 4*1048576;
    options.max_write_buffer_number = 2;
    options.merge_operator.reset(new UInt64AddOperator);
    options.compression = rocksdb::CompressionType::kSnappyCompression;

    options.max_log_file_size = 4*1048576;
    options.keep_log_file_num = 5;

    /*options.table_properties_collector_factories.emplace_back(
            rocksdb::NewCompactOnDeletionCollectorFactory(10000, 7500, 0.5));*/

    // these need to be high for replication scenarios
    options.WAL_ttl_seconds = wal_ttl_secs;
    options.WAL_size_limit_MB = wal_size_mb;

    // Disable WAL for master writes (Raft's WAL is used)
    // The replica uses native WAL, though.
    write_options.disableWAL = disable_wal;

    // open DB
    init_db(ttl);
}

Store::~Store() {
    close();
}

rocksdb::Status Store::init_db(int32_t ttl) {
    LOG(INFO) << "Initializing DB by opening state dir: " << state_dir_path;

    rocksdb::Status s;

    if(ttl > 0) {
        rocksdb::DBWithTTL* dbWithTtl;
        s = rocksdb::DBWithTTL::Open(options, state_dir_path,
                                     &dbWithTtl, ttl, false);
        db = dbWithTtl;
    } else {
        s = rocksdb::DB::Open(options, state_dir_path, &db);
    }

    if(!s.ok()) {
        LOG(ERROR) << "Error while initializing store: " << s.ToString();
        if(s.code() == rocksdb::Status::Code::kIOError) {
            LOG(ERROR) << "It seems like the data directory " << state_dir_path << " is already being used by "
                       << "another Typesense server. ";
            LOG(ERROR) << "If you are SURE that this is not the case, delete the LOCK file "
                       << "in the data db directory and try again.";
        }
    }

    assert(s.ok());
    return s;
}

bool Store::insert(const std::string& key, const std::string& value) {
    std::shared_lock lock(mutex);
    rocksdb::Status status = db->Put(write_options, key, value);
    return status.ok();
}

bool Store::batch_write(rocksdb::WriteBatch& batch) {
    std::shared_lock lock(mutex);
    rocksdb::Status status = db->Write(write_options, &batch);
    return status.ok();
}

bool Store::contains(const std::string& key) const {
    std::shared_lock lock(mutex);

    std::string value;
    bool value_found;
    bool key_may_exist = db->KeyMayExist(rocksdb::ReadOptions(), key, &value, &value_found);

    // returns false when key definitely does not exist
    if(!key_may_exist) {
        return false;
    }

    if(value_found) {
        return true;
    }

    // otherwise, we have try getting the value
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &value);
    return status.ok() && !status.IsNotFound();
}

StoreStatus Store::get(const std::string& key, std::string& value) const {
    std::shared_lock lock(mutex);
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &value);

    if(status.ok()) {
        return StoreStatus::FOUND;
    }

    if(status.IsNotFound()) {
        return StoreStatus::NOT_FOUND;
    }

    LOG(ERROR) << "Error while fetching the key: " << key << " - status is: " << status.ToString();
    return StoreStatus::ERROR;
}

bool Store::remove(const std::string& key) {
    std::shared_lock lock(mutex);
    rocksdb::Status status = db->Delete(write_options, key);
    return status.ok();
}

rocksdb::Iterator* Store::scan(const std::string & prefix, const rocksdb::Slice* iterate_upper_bound) {
    std::shared_lock lock(mutex);
    rocksdb::ReadOptions read_opts;
    if(iterate_upper_bound) {
        read_opts.iterate_upper_bound = iterate_upper_bound;
    }
    rocksdb::Iterator *iter = db->NewIterator(read_opts);
    iter->Seek(prefix);
    return iter;
}

rocksdb::Iterator* Store::get_iterator() {
    std::shared_lock lock(mutex);
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    return it;
}

void Store::scan_fill(const std::string& prefix_start, const std::string& prefix_end, std::vector<std::string> & values) {
    rocksdb::ReadOptions read_opts;
    rocksdb::Slice upper_bound(prefix_end);
    read_opts.iterate_upper_bound = &upper_bound;

    std::shared_lock lock(mutex);
    rocksdb::Iterator *iter = db->NewIterator(read_opts);
    for (iter->Seek(prefix_start); iter->Valid() && iter->key().starts_with(prefix_start); iter->Next()) {
        values.push_back(iter->value().ToString());
    }

    delete iter;
}

void Store::increment(const std::string & key, uint32_t value) {
    std::shared_lock lock(mutex);
    db->Merge(write_options, key, StringUtils::serialize_uint32_t(value));
}

uint64_t Store::get_latest_seq_number() const {
    std::shared_lock lock(mutex);
    return db->GetLatestSequenceNumber();
}

Option<std::vector<std::string>*> Store::get_updates_since(const uint64_t seq_number_org, const uint64_t max_updates) const {
    std::shared_lock lock(mutex);
    const uint64_t local_latest_seq_num = db->GetLatestSequenceNumber();

    // Since GetUpdatesSince(0) == GetUpdatesSince(1)
    const uint64_t seq_number = (seq_number_org == 0) ? 1 : seq_number_org;

    if(seq_number == local_latest_seq_num+1) {
        // replica has caught up, send an empty list as result
        std::vector<std::string>* updates = new std::vector<std::string>();
        return Option<std::vector<std::string>*>(updates);
    }

    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    rocksdb::Status status = db->GetUpdatesSince(seq_number, &iter);

    if(!status.ok()) {
        LOG(ERROR) << "Error while fetching updates for replication: " << status.ToString();

        std::ostringstream error;
        error << "Unable to fetch updates. " << "Master's latest sequence number is " << local_latest_seq_num
              << " but requested sequence number is " << seq_number;
        LOG(ERROR) << error.str();

        return Option<std::vector<std::string>*>(400, error.str());
    }

    if(!iter->Valid()) {
        std::ostringstream error;
        error << "Invalid iterator. Master's latest sequence number is " << local_latest_seq_num << " but "
              << "updates are requested from sequence number " << seq_number << ". "
              << "The master's WAL entries might have expired (they are kept only for 24 hours).";
        LOG(ERROR) << error.str();
        return Option<std::vector<std::string>*>(400, error.str());
    }

    uint64_t num_updates = 0;
    std::vector<std::string>* updates = new std::vector<std::string>();

    bool first_iteration = true;

    while(iter->Valid() && num_updates < max_updates) {
        const rocksdb::BatchResult & batch = iter->GetBatch();
        if(first_iteration) {
            first_iteration = false;
            if(batch.sequence != seq_number) {
                std::ostringstream error;
                error << "Invalid iterator. Requested sequence number is " << seq_number << " but "
                      << "updates are available only from sequence number " << batch.sequence << ". "
                      << "The master's WAL entries might have expired (they are kept only for 24 hours).";
                LOG(ERROR) << error.str();
                return Option<std::vector<std::string>*>(400, error.str());
            }
        }

        const std::string & write_batch_serialized = batch.writeBatchPtr->Data();
        updates->push_back(write_batch_serialized);
        num_updates += 1;
        iter->Next();
    }

    return Option<std::vector<std::string>*>(updates);
}

void Store::close() {
    std::unique_lock lock(mutex);
    delete db;
    db = nullptr;
}

int Store::reload(bool clear_state_dir, const std::string& snapshot_path, int32_t ttl) {
    std::unique_lock lock(mutex);

    // we don't use close() to avoid nested lock and because lock is required until db is re-initialized
    delete db;
    db = nullptr;

    if(clear_state_dir) {
        if (!delete_path(state_dir_path, true)) {
            LOG(WARNING) << "rm " << state_dir_path << " failed";
            return -1;
        }

        LOG(INFO) << "rm " << state_dir_path << " success";
    }

    if(!snapshot_path.empty()) {
        // tries to use link if possible, or else copies
        if (!copy_dir(snapshot_path, state_dir_path)) {
            LOG(WARNING) << "copy snapshot " << snapshot_path << " to " << state_dir_path << " failed";
            return -1;
        }

        LOG(INFO) << "copy snapshot " << snapshot_path << " to " << state_dir_path << " success";
    }

    if (!create_directory(state_dir_path)) {
        LOG(WARNING) << "CreateDirectory " << state_dir_path << " failed";
        return -1;
    }

    const rocksdb::Status& status = init_db(ttl);
    if (!status.ok()) {
        LOG(WARNING) << "Open DB " << state_dir_path << " failed, msg: " << status.ToString();
        return -1;
    }

    LOG(INFO) << "DB open success!";

    return 0;
}

void Store::flush() {
    std::shared_lock lock(mutex);
    rocksdb::FlushOptions options;
    db->Flush(options);
}

rocksdb::Status Store::compact_all() {
    std::shared_lock lock(mutex);
    return db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
}

rocksdb::Status Store::create_check_point(rocksdb::Checkpoint** checkpoint_ptr, const std::string& db_snapshot_path) {
    std::shared_lock lock(mutex);
    rocksdb::Status status = rocksdb::Checkpoint::Create(db, checkpoint_ptr);
    if(!status.ok()) {
        LOG(ERROR) << "Checkpoint Create failed, msg:" << status.ToString();
        return status;
    }

    status = (*checkpoint_ptr)->CreateCheckpoint(db_snapshot_path);

    if(!status.ok()) {
        LOG(WARNING) << "Checkpoint CreateCheckpoint failed at snapshot path: "
                     << db_snapshot_path << ", msg:" << status.ToString();
    }

    return status;
}

rocksdb::Status Store::delete_range(const std::string& begin_key, const std::string& end_key) {
    std::shared_lock lock(mutex);
    return db->DeleteRange(rocksdb::WriteOptions(), db->DefaultColumnFamily(), begin_key, end_key);
}

rocksdb::Status Store::compact_range(const rocksdb::Slice& begin_key, const rocksdb::Slice& end_key) {
    std::shared_lock lock(mutex);
    return db->CompactRange(rocksdb::CompactRangeOptions(), &begin_key, &end_key);
}

rocksdb::DB* Store::_get_db_unsafe() const {
    return db;
}

const std::string& Store::get_state_dir_path() const {
    return state_dir_path;
}

const rocksdb::Options& Store::get_db_options() const {
    return options;
}

void Store::print_memory_usage() {
    std::string index_usage;
    db->GetProperty("rocksdb.estimate-table-readers-mem", &index_usage);
    LOG(INFO) << "rocksdb.estimate-table-readers-mem: " << index_usage;

    std::string memtable_usage;
    db->GetProperty("rocksdb.cur-size-all-mem-tables", &memtable_usage);
    LOG(INFO) << "rocksdb.cur-size-all-mem-tables: " << memtable_usage;
}

void Store::get_last_N_values(const std::string& userid_prefix, uint32_t N, std::vector<std::string>& values) {
    std::shared_lock lock(mutex);

    rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());
    auto prefix_key = userid_prefix + "~";
    iter->SeekForPrev(prefix_key);

    while(iter->Valid() && N) {
        auto key = iter->key().ToString();
        if(!StringUtils::begins_with(key, userid_prefix)) {
            break;
        }

        values.push_back(iter->value().ToString());
        N--;
        iter->Prev();
    }

    delete iter;
}