#pragma once

#include <stdint.h>
#include <string>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

/*
 *  Stores all information about a collection.
 *  Uses RocksDB for persistence.
 */
class Store {

private:

    std::string state_dir_path;

    rocksdb::DB *db;
    rocksdb::Options options;

public:

    Store() = delete;

    Store(std::string state_dir_path): state_dir_path(state_dir_path) {
        // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        // create the DB if it's not already present
        options.create_if_missing = true;

        // open DB
        rocksdb::Status s = rocksdb::DB::Open(options, state_dir_path, &db);
        assert(s.ok());
    }

    ~Store() {
        delete db;
    }

    bool insert(std::string&& key, std::string&& value) {
        rocksdb::Status status = db->Put(rocksdb::WriteOptions(), key, value);
        return status.ok();
    }

    std::string get(std::string&& key) {
        std::string value;
        rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &value);
        return value;
    }
};