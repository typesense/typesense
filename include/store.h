#pragma once

#include <stdint.h>
#include <cstdlib>
#include <string>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/merge_operator.h>

class UInt64AddOperator : public rocksdb::AssociativeMergeOperator {
public:
    virtual bool Merge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value, const rocksdb::Slice& value,
                       std::string* new_value, rocksdb::Logger* logger) const override {
        uint64_t existing = 0;
        if (existing_value) {
            existing = (uint64_t) std::stoi(existing_value->ToString());
        }
        *new_value = std::to_string(existing + std::stoi(value.ToString()));
        return true;
    }

    virtual const char* Name() const override {
        return "UInt64AddOperator";
    }
};

/*
 *  Abstraction for underlying KV store (RocksDB)
 */
class Store {
private:

    std::string state_dir_path;

    rocksdb::DB *db;
    rocksdb::Options options;

public:

    Store() = delete;

    Store(std::string state_dir_path): state_dir_path(state_dir_path) {
        // Optimize RocksDB
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        // create the DB if it's not already present
        options.create_if_missing = true;
        options.write_buffer_size = 4*1048576;
        options.max_write_buffer_number = 2;
        options.merge_operator.reset(new UInt64AddOperator);

        // open DB
        rocksdb::Status s = rocksdb::DB::Open(options, state_dir_path, &db);
        assert(s.ok());
    }

    ~Store() {
        delete db;
    }

    bool insert(const std::string& key, const std::string& value) {
        rocksdb::Status status = db->Put(rocksdb::WriteOptions(), key, value);
        return status.ok();
    }

    bool contains(const std::string& key) {
        std::string value;
        rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &value);
        return status.ok() && !status.IsNotFound();
    }

    bool get(const std::string& key, std::string& value) {
        rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &value);
        return status.ok();
    }

    bool remove(const std::string& key) {
        rocksdb::Status status = db->Delete(rocksdb::WriteOptions(), key);
        return status.ok();
    }

    rocksdb::Iterator* scan(const std::string & prefix) {
        rocksdb::Iterator *iter = db->NewIterator(rocksdb::ReadOptions());
        iter->Seek(prefix);
        return iter;
    }

    rocksdb::Iterator* get_iterator() {
        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
        return it;
    };

    void scan_fill(const std::string & prefix, std::vector<std::string> & values) {
        rocksdb::Iterator *iter = db->NewIterator(rocksdb::ReadOptions());
        for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
            values.push_back(iter->value().ToString());
        }

        delete iter;
    }

    void increment(const std::string & key, uint32_t value) {
        db->Merge(rocksdb::WriteOptions(), key, std::to_string(value));
    }

    void print_memory_usage() {
        std::string index_usage;
        db->GetProperty("rocksdb.estimate-table-readers-mem", &index_usage);
        std::cout << "rocksdb.estimate-table-readers-mem: " << index_usage << std::endl;

        std::string memtable_usage;
        db->GetProperty("rocksdb.cur-size-all-mem-tables", &memtable_usage);
        std::cout << "rocksdb.cur-size-all-mem-tables: " << memtable_usage << std::endl;
    }
};