#pragma once

#include <stdint.h>
#include <cstdlib>
#include <string>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

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

    void scan_fill(const std::string & prefix, std::vector<std::string> & values) {
        rocksdb::Iterator *iter = db->NewIterator(rocksdb::ReadOptions());
        for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
            values.push_back(iter->value().ToString());
        }

        delete iter;
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