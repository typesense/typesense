#include <gtest/gtest.h>
#include <vector>
#include <store.h>
#include <string_utils.h>

TEST(StoreTest, GetUpdatesSince) {
    std::string primary_store_path = "/tmp/typesense_test/primary_store_test";
    std::cout << "Truncating and creating: " << primary_store_path << std::endl;
    system(("rm -rf "+primary_store_path+" && mkdir -p "+primary_store_path).c_str());

    // add some records, get the updates and restore them in a new store
    Store primary_store(primary_store_path);
    primary_store.insert("foo1", "bar1");
    primary_store.insert("foo2", "bar2");
    primary_store.insert("foo3", "bar3");

    Option<std::vector<std::string>*> updates_op = primary_store.get_updates_since(0, 10);
    ASSERT_EQ(3, updates_op.get()->size());

    std::string replica_store_path = "/tmp/typesense_test/replica_store_test";
    std::cout << "Truncating and creating: " << replica_store_path << std::endl;
    system(("rm -rf "+replica_store_path+" && mkdir -p "+replica_store_path).c_str());

    Store replica_store(replica_store_path);
    rocksdb::DB* replica_db = replica_store._get_db_unsafe();

    for(const std::string & update: *updates_op.get()) {
        // Do Base64 encoding and decoding as we would in the API layer
        const std::string update_encoded = StringUtils::base64_encode(update);
        const std::string update_decoded = StringUtils::base64_decode(update_encoded);
        rocksdb::WriteBatch write_batch(update_decoded);
        replica_db->Write(rocksdb::WriteOptions(), &write_batch);
    }

    std::string value;
    for(auto i=1; i<=3; i++) {
        replica_store.get(std::string("foo")+std::to_string(i), value);
        ASSERT_EQ(std::string("bar")+std::to_string(i), value);
    }

    delete updates_op.get();

    // Ensure that updates are limited to max_updates argument
    updates_op = primary_store.get_updates_since(0, 2);
    ASSERT_EQ(2, updates_op.get()->size());
    delete updates_op.get();
}