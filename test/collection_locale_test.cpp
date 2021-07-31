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

TEST_F(CollectionLocaleTest, SearchAgainstJapaneseText) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "ja"),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"今ぶり拍治ルツ", "Dustin Kensrue"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("拍治",
                                 {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();

    //LOG(INFO) << results;
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    //ASSERT_EQ("今ぶり<mark>拍</mark><mark>治</mark>ルツ", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
}

TEST_F(CollectionLocaleTest, SearchAgainstChineseText) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "zh"),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"爱并不会因时间而", "Dustin Kensrue"},
        {"很久以前，傳說在臺中北屯的一個地方", "Gord Downie"},
        {"獻給我思念的每一朵雲──海", "Dustin Kensrue"},
        {"看誰先跑到小山丘上。媽媽總是第", "Jamie Phua"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("并",
                                 {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("爱<mark>并不</mark>会因时间而", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    // partial token should not match as prefix when prefix is set to false

    results = coll1->search("并",
                            {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    results = coll1->search("上媽",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("看誰先跑到小山丘<mark>上</mark>。<mark>媽媽</mark>總是第", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    // search using simplified chinese

    results = coll1->search("妈",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("看誰先跑到小山丘上。<mark>媽媽</mark>總是第", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
}

TEST_F(CollectionLocaleTest, SearchAgainstThaiText) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "th"),
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

TEST_F(CollectionLocaleTest, SearchThaiTextPreSegmentedQuery) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "th"),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"ความเหลื่อมล้ำ", "Compound Word"},  // ความ, เหลื่อม, ล้ำ
        {"การกระจายรายได้", "Doc A"},
        {"จารีย์", "Doc B"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("เหลื่",
                                 {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1}, 1000, true, true).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionLocaleTest, SearchAgainstThaiTextExactMatch) {
    Collection* coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "th"),
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

    ASSERT_EQ("ข้อมูล<mark>ราย</mark>คนหรือ<mark>ราย</mark>บริษัทในการเชื่อมโยงส่วน<mark>ได้</mark>ส่วนเสีย",
              results["hits"][1]["highlights"][0]["snippet"].get<std::string>());

}

TEST_F(CollectionLocaleTest, SearchAgainstKoreanText) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "ko"),
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
    ASSERT_EQ("안녕은하철도999<mark>극장판</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    results = coll1->search("산악",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("경승지·<mark>산악</mark>·협곡", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
}

TEST_F(CollectionLocaleTest, KoreanTextPrefixConsonant) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "ko"),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"서울특별시 성북구", "Wrong Result"},
        {"서울특별시 중구 초동", "Wrong Result"},
        {"서울특별시 관악구", "Expected Result"},
        {"서울특별시 용산구 용산동", "Wrong Result"},
        {"서울특별시 동대문구 이문동", "Wrong Result"},
        {"서울특별시 서대문구 현저동", "Wrong Result"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = { sort_by(sort_field_const::text_match, "DESC"), sort_by("points", "DESC") };

    // To ensure that NFKD works, we will test for both &#4352; (Hangul Choseong Kiyeok)
    auto results = coll1->search("서울특별시 ᄀ",
                                 {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(6, results["found"].get<size_t>());
    ASSERT_EQ(6, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    // and &#12593; (Hangul Letter Kiyeok)
    results = coll1->search("서울특별시 ㄱ",
                             {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(6, results["found"].get<size_t>());
    ASSERT_EQ(6, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    // search for full word
    results = coll1->search("서울특별시 관",
                             {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(6, results["found"].get<size_t>());
    ASSERT_EQ(6, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionLocaleTest, KoreanTextPrefixVowel) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "ko"),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"서울특별시 강서구 공항동", "Wrong Result"},
        {"서울특별시 관악구", "Wrong Result"},
        {"서울특별시 강동구 고덕동", "Expected Result"},
        {"서울특별시 관악구 관악산나들길", "Wrong Result"},
        {"서울특별시 관악구 관악로", "Wrong Result"},
        {"서울특별시 관악구 과천대로", "Wrong Result"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = { sort_by(sort_field_const::text_match, "DESC"), sort_by("points", "DESC") };

    auto results = coll1->search("서울특별시 고",
                                 {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(6, results["found"].get<size_t>());
    ASSERT_EQ(6, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionLocaleTest, SearchAgainstKoreanTextContainingEnglishChars) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "th"),
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
