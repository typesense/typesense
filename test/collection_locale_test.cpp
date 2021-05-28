#include <gtest/gtest.h>
#include <collection.h>
#include <collection_manager.h>

class CollectionLocaleTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_locale";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key");
        collectionManager.load(8, 1000);
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.dispose();
        delete store;
    }
};

TEST_F(CollectionLocaleTest, SearchAgainstThaiText) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, DEFAULT_GEO_RESOLUTION, "th"),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"ลงที่นั่นโดยรถไฟ", "Dustin Kensrue"},
        {"พกติดตัวเสมอ", "Gord Downie"},
        {"พกไฟ\nเสมอ", "Dustin Kensrue"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("ลงรถไฟ",
                                 {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("<mark>ลง</mark>ที่นั่นโดย<mark>รถไฟ</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    results = coll1->search("ลงรถไฟ downie",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("<mark>ลง</mark>ที่นั่นโดย<mark>รถไฟ</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("Gord <mark>Downie</mark>", results["hits"][1]["highlights"][0]["snippet"].get<std::string>());

    results = coll1->search("พกไฟ", {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("<mark>พกไฟ</mark>\nเสมอ", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
}

TEST_F(CollectionLocaleTest, SearchAgainstThaiTextExactMatch) {
    Collection* coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, DEFAULT_GEO_RESOLUTION, "th"),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"ติดกับดักรายได้ปานกลาง", "Expected Result"},
        {"ข้อมูลรายคนหรือรายบริษัทในการเชื่อมโยงส่วนได้ส่วนเสีย", "Another Result"},
    };

    for (size_t i = 0; i < records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = { sort_by(sort_field_const::text_match, "DESC"), sort_by("points", "DESC") };
    auto results = coll1->search("รายได้",
                                 {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_EQ("ติดกับดัก<mark>ราย</mark><mark>ได้</mark>ปานกลาง",
              results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    ASSERT_EQ("ข้อมูลรายคนหรือ<mark>ราย</mark>บริษัทในการเชื่อมโยงส่วน<mark>ได้</mark>ส่วนเสีย",
              results["hits"][1]["highlights"][0]["snippet"].get<std::string>());

}

TEST_F(CollectionLocaleTest, SearchAgainstKoreanText) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, DEFAULT_GEO_RESOLUTION, "th"),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"경승지·산악·협곡", "Dustin Kensrue"},
        {"안녕은하철도999극장판", "Gord Downie"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("극장판",
                                 {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("안녕은하철도999<mark>극장판</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    results = coll1->search("산악",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("경승지·<mark>산악</mark>·협곡", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
}

TEST_F(CollectionLocaleTest, SearchAgainstKoreanTextContainingEnglishChars) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, DEFAULT_GEO_RESOLUTION, "th"),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"개혁 등의 영향으로 11%나 위축됐다", "Dustin Kensrue"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("위축됐다",
                                 {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("개혁 등의 영향으로 11%나 <mark>위축됐다</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    results = coll1->search("11%",
                            {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("개혁 등의 영향으로 <mark>11</mark>%나 위축됐다", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
}
