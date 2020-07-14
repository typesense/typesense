#include <gtest/gtest.h>
#include <vector>
#include <store.h>
#include <string_utils.h>

TEST(StoreTest, GetUpdatesSince) {
    std::string primary_store_path = "/tmp/typesense_test/primary_store_test";
    LOG(INFO) << "Truncating and creating: " << primary_store_path;
    system(("rm -rf "+primary_store_path+" && mkdir -p "+primary_store_path).c_str());

    // add some records, get the updates and restore them in a new store

    Store primary_store(primary_store_path, 24*60*60, 1024, false);

    // on a fresh store, sequence number is 0
    Option<std::vector<std::string>*> updates_op = primary_store.get_updates_since(0, 10);
    ASSERT_TRUE(updates_op.ok());
    ASSERT_EQ(0, updates_op.get()->size());
    ASSERT_EQ(0, primary_store.get_latest_seq_number());
    delete updates_op.get();

    // get_updates_since(1) == get_updates_since(0)
    updates_op = primary_store.get_updates_since(1, 10);
    ASSERT_TRUE(updates_op.ok());
    ASSERT_EQ(0, updates_op.get()->size());
    ASSERT_EQ(0, primary_store.get_latest_seq_number());
    delete updates_op.get();

    // querying for a seq_num > 1 on a fresh store
    updates_op = primary_store.get_updates_since(2, 10);
    ASSERT_FALSE(updates_op.ok());
    ASSERT_EQ("Unable to fetch updates. Master's latest sequence number is 0 but "
              "requested sequence number is 2", updates_op.error());

    // get_updates_since(1) == get_updates_since(0) even after inserting a record
    primary_store.insert("foo1", "bar1");
    ASSERT_EQ(1, primary_store.get_latest_seq_number());
    updates_op = primary_store.get_updates_since(1, 10);
    std::cout << updates_op.error() << std::endl;
    ASSERT_TRUE(updates_op.ok());
    ASSERT_EQ(1, updates_op.get()->size());
    delete updates_op.get();

    updates_op = primary_store.get_updates_since(0, 10);
    ASSERT_TRUE(updates_op.ok());
    ASSERT_EQ(1, updates_op.get()->size());
    delete updates_op.get();

    // add more records
    primary_store.insert("foo2", "bar2");
    primary_store.insert("foo3", "bar3");
    ASSERT_EQ(3, primary_store.get_latest_seq_number());

    updates_op = primary_store.get_updates_since(0, 10);
    ASSERT_EQ(3, updates_op.get()->size());
    delete updates_op.get();

    updates_op = primary_store.get_updates_since(1, 10);
    ASSERT_EQ(3, updates_op.get()->size());
    delete updates_op.get();

    updates_op = primary_store.get_updates_since(3, 10);
    ASSERT_EQ(1, updates_op.get()->size());
    delete updates_op.get();

    std::string replica_store_path = "/tmp/typesense_test/replica_store_test";
    LOG(INFO) << "Truncating and creating: " << replica_store_path;
    system(("rm -rf "+replica_store_path+" && mkdir -p "+replica_store_path).c_str());

    Store replica_store(replica_store_path, 24*60*60, 1024, false);
    rocksdb::DB* replica_db = replica_store._get_db_unsafe();

    updates_op = primary_store.get_updates_since(0, 10);

    for(const std::string & update: *updates_op.get()) {
        // Do Base64 encoding and decoding as we would in the API layer
        const std::string update_encoded = StringUtils::base64_encode(update);
        const std::string update_decoded = StringUtils::base64_decode(update_encoded);
        rocksdb::WriteBatch write_batch(update_decoded);
        replica_db->Write(rocksdb::WriteOptions(), &write_batch);
    }

    delete updates_op.get();

    std::string value;
    for(auto i=1; i<=3; i++) {
        replica_store.get(std::string("foo")+std::to_string(i), value);
        ASSERT_EQ(std::string("bar")+std::to_string(i), value);
    }

    // Ensure that updates are limited to max_updates argument
    updates_op = primary_store.get_updates_since(0, 10);
    ASSERT_EQ(3, updates_op.get()->size());
    delete updates_op.get();

    // sequence numbers 0 and 1 are the same
    updates_op = primary_store.get_updates_since(0, 10);
    ASSERT_EQ(3, updates_op.get()->size());
    delete updates_op.get();

    updates_op = primary_store.get_updates_since(1, 10);
    ASSERT_EQ(3, updates_op.get()->size());
    delete updates_op.get();

    updates_op = primary_store.get_updates_since(3, 100);
    ASSERT_TRUE(updates_op.ok());
    ASSERT_EQ(1, updates_op.get()->size());
    delete updates_op.get();

    updates_op = primary_store.get_updates_since(4, 100);
    ASSERT_TRUE(updates_op.ok());
    ASSERT_EQ(0, updates_op.get()->size());
    delete updates_op.get();

    updates_op = primary_store.get_updates_since(50, 100);
    ASSERT_FALSE(updates_op.ok());
    ASSERT_EQ("Unable to fetch updates. Master's latest sequence number is 3 but "
              "requested sequence number is 50", updates_op.error());
}

TEST(StoreTest, GetUpdateSinceInvalidIterator) {
    std::string primary_store_path = "/tmp/typesense_test/primary_store_test";
    LOG(INFO) << "Truncating and creating: " << primary_store_path;
    system(("rm -rf "+primary_store_path+" && mkdir -p "+primary_store_path).c_str());

    // add some records, get the updates and restore them in a new store

    Store primary_store(primary_store_path, 0, 0, true);  // disable WAL
    primary_store.insert("foo1", "bar1");
    primary_store.insert("foo2", "bar2");
    primary_store.insert("foo3", "bar3");
    primary_store.insert("foo4", "bar4");

    primary_store.flush();

    Option<std::vector<std::string>*> updates_op = primary_store.get_updates_since(2, 10);
    ASSERT_FALSE(updates_op.ok());
    ASSERT_EQ("Invalid iterator. Master's latest sequence number is 4 but updates are requested from sequence number 2. "
                      "The master's WAL entries might have expired (they are kept only for 24 hours).", updates_op.error());
}

TEST(StoreTest, Contains) {
    std::string primary_store_path = "/tmp/typesense_test/primary_store_test";
    LOG(INFO) << "Truncating and creating: " << primary_store_path;
    system(("rm -rf "+primary_store_path+" && mkdir -p "+primary_store_path).c_str());

    // add some records, flush and try to query
    Store primary_store(primary_store_path, 0, 0, true);  // disable WAL
    primary_store.insert("foo1", "bar1");
    primary_store.insert("foo2", "bar2");
    primary_store.flush();

    ASSERT_EQ(true, primary_store.contains("foo1"));
    ASSERT_EQ(true, primary_store.contains("foo2"));
    ASSERT_EQ(false, primary_store.contains("foo"));
    ASSERT_EQ(false, primary_store.contains("foo3"));

    // add more records without flushing and query again
    primary_store.insert("foo3", "bar1");
    primary_store.insert("foo4", "bar2");
    primary_store.flush();

    ASSERT_EQ(true, primary_store.contains("foo1"));
    ASSERT_EQ(true, primary_store.contains("foo3"));
    ASSERT_EQ(true, primary_store.contains("foo4"));
    ASSERT_EQ(false, primary_store.contains("foo"));
    ASSERT_EQ(false, primary_store.contains("foo5"));
}