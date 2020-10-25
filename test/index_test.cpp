#include <gtest/gtest.h>
#include "index.h"
#include <vector>

TEST(IndexTest, ScrubReindexDoc) {
    std::unordered_map<std::string, field> search_schema;
    search_schema.emplace("title", field("title", field_types::STRING, false));
    search_schema.emplace("points", field("title", field_types::INT32, false));
    search_schema.emplace("cast", field("cast", field_types::STRING_ARRAY, false));
    search_schema.emplace("movie", field("movie", field_types::BOOL, false));

    Index index("index", search_schema, {}, {});
    nlohmann::json old_doc;
    old_doc["id"] = "1";
    old_doc["title"] = "One more thing.";
    old_doc["points"] = 100;
    old_doc["cast"] = {"John Wick", "Jeremy Renner"};
    old_doc["movie"] = true;

    // all fields remain same

    nlohmann::json update_doc1, del_doc1;
    update_doc1 = old_doc;
    del_doc1 = old_doc;

    index.scrub_reindex_doc(update_doc1, del_doc1, old_doc);
    ASSERT_EQ(1, del_doc1.size());
    ASSERT_STREQ("1", del_doc1["id"].get<std::string>().c_str());

    // when only some fields are updated

    nlohmann::json update_doc2, del_doc2;
    update_doc2["id"] = "1";
    update_doc2["points"] = 100;
    update_doc2["cast"] = {"Jack"};

    del_doc2 = update_doc2;

    index.scrub_reindex_doc(update_doc2, del_doc2, old_doc);
    ASSERT_EQ(2, del_doc2.size());
    ASSERT_STREQ("1", del_doc2["id"].get<std::string>().c_str());
    std::vector<std::string> cast = del_doc2["cast"].get<std::vector<std::string>>();
    ASSERT_EQ(1, cast.size());
    ASSERT_STREQ("Jack", cast[0].c_str());

    // containing fields not part of search schema

    nlohmann::json update_doc3, del_doc3;
    update_doc3["id"] = "1";
    update_doc3["title"] = "The Lawyer";
    update_doc3["foo"] = "Bar";

    del_doc3 = update_doc3;
    index.scrub_reindex_doc(update_doc3, del_doc3, old_doc);
    ASSERT_EQ(3, del_doc3.size());
    ASSERT_STREQ("1", del_doc3["id"].get<std::string>().c_str());
    ASSERT_STREQ("The Lawyer", del_doc3["title"].get<std::string>().c_str());
    ASSERT_STREQ("Bar", del_doc3["foo"].get<std::string>().c_str());
}