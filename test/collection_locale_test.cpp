#include <gtest/gtest.h>
#include <collection.h>
#include <collection_manager.h>

class CollectionLocaleTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_locale";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
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
    ASSERT_EQ("爱<mark>并</mark>不会因时间而", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    // partial token should not match as prefix when prefix is set to false

    results = coll1->search("并",
                            {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    results = coll1->search("上媽",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("看誰先跑到小山丘<mark>上</mark>。<mark>媽</mark>媽總是第", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    // search using simplified chinese

    results = coll1->search("妈",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("看誰先跑到小山丘上。<mark>媽</mark>媽總是第", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
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
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 10).get();

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

TEST_F(CollectionLocaleTest, ThaiTextShouldBeNormalizedToNFKC) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "th"),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"น้ำมัน", "Dustin Kensrue"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("น้ํามัน",{"title"}, "", {}, {},
                                 {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
}

TEST_F(CollectionLocaleTest, ThaiTextShouldRespectSeparators) {
    nlohmann::json coll_json = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string", "locale": "th"}
        ]
    })"_json;

    auto coll1 = collectionManager.create_collection(coll_json).get();

    nlohmann::json doc;
    doc["title"] = "alpha-beta-gamma";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("*",{}, "title:=alpha-beta-gamma", {}, {},
                                 {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());

    // now with `symbols_to_index`
    coll_json = R"({
        "name": "coll2",
        "symbols_to_index": ["-"],
        "fields": [
            {"name": "title", "type": "string", "locale": "th"}
        ]
    })"_json;

    auto coll2 = collectionManager.create_collection(coll_json).get();
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    results = coll2->search("*",{}, "title:=alpha-beta-gamma", {}, {},
                            {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll2->search("*",{}, "title:=alphabetagamma", {}, {},
                            {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(0, results["found"].get<size_t>());
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
        {"Meiji", "Doc C"},
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

    results = coll1->search("meji",
                            {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true},
                            10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1}, 1000, true, true).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("ควม",
                            {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true},
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

    std::string word_9bytes = "น้ำ";
    std::string word_12bytes = "น้ํา";

    std::vector<std::vector<std::string>> records = {
        {"ติดกับดักรายได้ปานกลาง", "Expected Result"},
        {"ข้อมูลรายคนหรือรายบริษัทในการเชื่อมโยงส่วนได้ส่วนเสีย", "Another Result"},
        {word_9bytes, "Another Result"},  // NKC normalization
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

    // check text index overflow regression with NFC normalization + highlighting

    results = coll1->search(word_12bytes, {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("<mark>น้ำ</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
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
                                 {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();

    ASSERT_EQ(6, results["found"].get<size_t>());
    ASSERT_EQ(6, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    // and &#12593; (Hangul Letter Kiyeok)
    results = coll1->search("서울특별시 ㄱ",
                             {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}, 10,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10).get();

    ASSERT_EQ(6, results["found"].get<size_t>());
    ASSERT_EQ(6, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    // search for full word
    results = coll1->search("서울특별시 관",
                             {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}, 10,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10).get();

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
                                 {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();

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


TEST_F(CollectionLocaleTest, SearchCyrillicText) {
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "sr"),};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;
    doc["title"] = "Test Тест";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "TEST ТЕСТ";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("тест", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    ASSERT_EQ("<mark>TEST</mark> <mark>ТЕСТ</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
    ASSERT_EQ("<mark>Test</mark> <mark>Тест</mark>", results["hits"][1]["highlights"][0]["snippet"].get<std::string>());

    // with typo

    results = coll1->search("тетст", {"title"}, "", {}, {}, {1}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    ASSERT_EQ("<mark>TEST</mark> <mark>ТЕСТ</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
    ASSERT_EQ("<mark>Test</mark> <mark>Тест</mark>", results["hits"][1]["highlights"][0]["snippet"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionLocaleTest, SearchCyrillicTextWithDefaultLocale) {
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, ""),};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;
    doc["title"] = "Test Тест";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "TEST ТЕСТ";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("тетст", {"title"}, "", {}, {}, {1}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(0, results["hits"].size());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionLocaleTest, SearchCyrillicTextWithDropTokens) {
    // this test ensures that even when tokens are dropped, the eventual text is highlighted on all query tokens
    std::vector<field> fields = {field("description", field_types::STRING, false, false, true, "sr"),
                                 field("points", field_types::INT32, false, false, true, "sr"),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["description"] = "HPE Aruba AP575 802.11ax Wireless Access Point - TAA Compliant - 2.40 GHz, "
                          "5 GHz - MIMO Technology - 1 x Network (RJ-45) - Gigabit Ethernet - Bluetooth 5";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("HPE Aruba AP575 Technology Gigabit Bluetooth 5", {"description"}, "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "description", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>").get();

    ASSERT_EQ(1, results["hits"][0]["highlights"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    ASSERT_EQ("<mark>HPE</mark> <mark>Aruba</mark> <mark>AP575</mark> 802.11ax Wireless Access Point - "
              "TAA Compliant - 2.40 GHz, <mark>5</mark> GHz - MIMO <mark>Technology</mark> - 1 x Network (RJ-45) - "
              "<mark>Gigabit</mark> Ethernet - <mark>Bluetooth</mark> <mark>5</mark>",
              results["hits"][0]["highlights"][0]["value"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionLocaleTest, SearchAndFacetSearchForGreekText) {
    std::vector<field> fields = {field("title", field_types::STRING, true, false, true, "el"),};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;
    doc["title"] = "Εμφάνιση κάθε μέρα.";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("Εμφάν", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title").get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("<mark>Εμφάν</mark>ιση κάθε μέρα.", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
    ASSERT_EQ("<mark>Εμφάν</mark>ιση κάθε μέρα.", results["hits"][0]["highlights"][0]["value"].get<std::string>());

    // with typo
    results = coll1->search("Εμφάιση", {"title"}, "", {}, {}, {1}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("<mark>Εμφάνιση</mark> κάθε μέρα.", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    // facet search with prefix

    results = coll1->search("*", {"title"}, "", {"title"}, {}, {1}, 10, 1, FREQUENCY, {false},
                            Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "title: Εμφάν").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ("<mark>Εμφάν</mark>ιση κάθε μέρα.", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>());

    // facet search with prefix typo

    results = coll1->search("*", {"title"}, "", {"title"}, {}, {1}, 10, 1, FREQUENCY, {false},
                            Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "title: Εμφάνση").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ("<mark>Εμφάνισ</mark>η κάθε μέρα.", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionLocaleTest, SearchOnCyrillicTextWithSpecialCharacters) {
    std::vector<field> fields = {field("title", field_types::STRING, true, false, true, "ru"),};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;
    doc["title"] = "«Сирый», «несчастный», «никчёмный» — принятое "
                   "особ, сейчас, впрочем, оттенок скромности. Посыл, "
                   "среди которых отсутствие мобильного страшное.";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("отсутствие", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>(),
                                 10, "", 10, 4, "title").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("скромности. Посыл, среди которых <mark>отсутствие</mark> мобильного страшное.",
              results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
    ASSERT_EQ("«Сирый», «несчастный», «никчёмный» — принятое особ, сейчас, впрочем, оттенок скромности. "
              "Посыл, среди которых <mark>отсутствие</mark> мобильного страшное.",
              results["hits"][0]["highlights"][0]["value"].get<std::string>());

    results = coll1->search("принятое", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("«Сирый», «несчастный», «никчёмный» — <mark>принятое</mark> особ, сейчас, впрочем, оттенок скромности. Посыл, среди которых отсутствие мобильного страшное.",
              results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    results = coll1->search("*", {}, "", {"title"}, {}, {0}, 0, 1, FREQUENCY, {true}, 10,
                            spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>(),
                            10, "title: отсутствие").get();

    ASSERT_STREQ("«Сирый», «несчастный», «никчёмный» — принятое особ, сейчас, впрочем, оттенок скромности. "
                 "Посыл, среди которых <mark>отсутствие</mark> мобильного страшное.",
                 results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    results = coll1->search("*", {}, "", {"title"}, {}, {0}, 0, 1, FREQUENCY, {true}, 10,
                            spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>(),
                            10, "title: отсутст").get();

    ASSERT_STREQ("«Сирый», «несчастный», «никчёмный» — принятое особ, сейчас, впрочем, оттенок скромности. "
                 "Посыл, среди которых <mark>отсутст</mark>вие мобильного страшное.",
                 results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionLocaleTest, SearchOnCyrillicLargeText) {
    std::vector<field> fields = {field("title", field_types::STRING, true, false, true, "ru"),};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;
    doc["title"] = "Петр Великий, царь России, в начале 18 века санкционировал использование западных буквенных форм "
                   "(ru). Со временем они были в значительной степени приняты на других языках, использующих этот "
                   "сценарий. Таким образом, в отличие от большинства современных греческих шрифтов, которые сохранили "
                   "свой собственный набор принципов дизайна для строчных букв (таких как размещение засечек, форма "
                   "концов штриха и правила толщины штриха, хотя греческие заглавные буквы действительно используют "
                   "латинский дизайн принципы) современные кириллические шрифты во многом такие же, как современные "
                   "латинские шрифты того же семейства. Развитие некоторых кириллических компьютерных шрифтов из "
                   "латинских также способствовало визуальной латинизации кириллического шрифта.";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("Великий", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_STREQ("Петр <mark>Великий</mark>, царь России, в начале",
                 results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());
}

TEST_F(CollectionLocaleTest, SearchOnJapaneseLargeText) {
    std::vector<field> fields = {field("title", field_types::STRING, true, false, true, "ja"),};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;
    doc["title"] = "王獣を倒すと入手した折れた角。追放された後、この世に存在すべきではないもの。\n獣域ウルブズの中で帝王と呼ばれていても、"
                   "魔獣たちの系譜では、その兄たちの万分の一にも満たないだろう。\n「黄"
                   "金」が無数の獣域ウルブズを捨て紙のように圧縮して偶然にできた異形の魔獣。その角には、黒いウルブズを命じて自分のため"
                   "に空間を溶かす権威が秘めている。";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("王獣を", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_STREQ("<mark>王</mark><mark>獣</mark><mark>を</mark><mark>倒す</mark>と入手した折",
                 results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    results = coll1->search("業果材", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_STREQ("に空間を溶かす<mark>権威</mark><mark>が</mark><mark>秘</mark>めている。",
                 results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());
}

TEST_F(CollectionLocaleTest, SearchOnArabicText) {
    std::vector<field> fields = {field("title", field_types::STRING, true, false, true, ""),};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    std::string data = "جهينة";
    std::string q = "جوهينة";

    auto dchars = data.c_str();
    auto qchars = q.c_str();

    nlohmann::json doc;
    doc["title"] = "جهينة";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("جوهينة", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_STREQ("<mark>جهينة</mark>",
                 results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());
}

TEST_F(CollectionLocaleTest, SearchOnArabicTextWithTypo) {
    std::vector<field> fields = {field("title", field_types::STRING, true, false, true, ""),};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();
    std::string q = "دوني";
    std::string title1 = "سوني";
    std::string title2 = "داوني";

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "ينوس";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["title"] = "ينواد";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("ينود", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {false}, 1,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "", 10).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());
}

TEST_F(CollectionLocaleTest, SearchOnBulgarianText) {
    std::vector<field> fields = {field("title", field_types::STRING, true, false, true, "bg"),};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();
    std::string title1 = "Сърце от любов";
    std::string title2 = "Съблезъб тигър";
    std::string title3 = "Сърна";

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = title1;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["title"] = title2;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "2";
    doc["title"] = title3;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("Сърце", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 1,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "", 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionLocaleTest, HighlightOfAllQueryTokensShouldConsiderUnicodePoints) {
    // For perfomance reasons, we highlight all query tokens in a text only on smaller text
    // Here, "small" threshold must be defined using unicode points and not raw string size.
    std::vector<field> fields = {field("title", field_types::STRING, true, false, true, ""),};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "رجلا منهم اجتهد اربعين ليله ثم دعا فلم يستجب له فاتي عيسي ابن مريم عليه السلام يشكو اليه ما هو فيه ويساله الدعاء له فتطهر عيسي وصلي ثم";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("لة ثم دعا فلم يستجب له فأتى عيسى ابن مريم عليه السلام يشكو إل", {"title"}, "", {}, {},
                                 {2}, 10, 1, FREQUENCY, {true}, 1).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(17, results["hits"][0]["highlights"][0]["matched_tokens"].size());
}

TEST_F(CollectionLocaleTest, SearchInGermanLocaleShouldBeTypoTolerant) {
    nlohmann::json coll_json = R"({
            "name": "coll1",
            "fields": [
                {"name": "title_de", "type": "string", "locale": "de"}
            ]
        })"_json;

    auto coll1 = collectionManager.create_collection(coll_json).get();

    nlohmann::json doc;
    doc["title_de"] = "mülltonne";
    doc["title_en"] = "trash bin";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("mulltonne", {"title_de"}, "", {}, {},
                                 {2}, 10, 1, FREQUENCY, {true}, 1).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
}

TEST_F(CollectionLocaleTest, HandleSpecialCharsInThai) {
    nlohmann::json coll_json = R"({
            "name": "coll1",
            "fields": [
                {"name": "title_th", "type": "string", "locale": "th"},
                {"name": "sku", "type": "string"}
            ]
        })"_json;

    auto coll1 = collectionManager.create_collection(coll_json).get();

    nlohmann::json doc;
    doc["title_th"] = "สวัสดี";
    doc["sku"] = "12345_";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // query string is parsed using the locale of the first field in the query_by list
    auto results = coll1->search("12345_", {"title_th", "sku"}, "", {}, {},
                                 {2, 0}, 10, 1, FREQUENCY, {true, false}, 1).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
}

/*
TEST_F(CollectionLocaleTest, TranslitPad) {
    UErrorCode translit_status = U_ZERO_ERROR;
    auto transliterator = icu::Transliterator::createInstance("Any-Latin; Latin-ASCII",
                                                         UTRANS_FORWARD, translit_status);

    icu::UnicodeString unicode_input = icu::UnicodeString::fromUTF8("எண்ணெய்");
    transliterator->transliterate(unicode_input);
    std::string output;
    unicode_input.toUTF8String(output);
    LOG(INFO) << output;

    unicode_input = icu::UnicodeString::fromUTF8("எண்");
    transliterator->transliterate(unicode_input);
    unicode_input.toUTF8String(output);
    LOG(INFO) << output;

    unicode_input = icu::UnicodeString::fromUTF8("என்னை");
    transliterator->transliterate(unicode_input);
    unicode_input.toUTF8String(output);
    LOG(INFO) << output;

    delete transliterator;
}*/
