#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <collection_manager.h>
#include "string_utils.h"
#include "collection.h"
#include "core_api.h"


class CollectionMultiSearchAggTest : public ::testing::Test {
 protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_multi_search_agg_test";
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



TEST_F(CollectionMultiSearchAggTest, BasicMergeTest) {

    Collection* coll_people = collectionManager.get_collection("coll_people").get();
    if(!coll_people) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("age", field_types::INT32, false),
            field("country", field_types::STRING, true)
        };
        
        coll_people =collectionManager.create_collection("coll_people", 4, fields, "age").get();
    }

    auto add_res = coll_people->add("{\"name\": \"John Doe\", \"age\": 25, \"country\": \"USA\"}");

    if(add_res.ok()) {
        LOG(INFO) << "Added document with id: " << add_res.get();
    } else {
        LOG(ERROR) << "Failed to add document: " << add_res.error();
    }

    coll_people->add("{\"name\": \"Jane Doe\", \"age\": 30, \"country\": \"USA\"}");
    coll_people->add("{\"name\": \"Adam Smith\", \"age\": 35, \"country\": \"UK\"}");
    coll_people->add("{\"name\": \"John Smith\", \"age\": 40, \"country\": \"UK\"}");

    Collection* coll_cars = collectionManager.get_collection("coll_cars").get();
    if(!coll_cars) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("price", field_types::INT32, false),
            field("country", field_types::STRING, true)
        };
        
        coll_cars =collectionManager.create_collection("coll_cars", 4, fields, "price").get();
    }

    coll_cars->add("{\"name\": \"Ford\", \"price\": 10000, \"country\": \"USA\"}");
    coll_cars->add("{\"name\": \"BMW\", \"price\": 20000, \"country\": \"Germany\"}");
    coll_cars->add("{\"name\": \"Ferrari\", \"price\": 30000, \"country\": \"Italy\"}");
    coll_cars->add("{\"name\": \"Audi\", \"price\": 40000, \"country\": \"Germany\"}");


    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    nlohmann::json body;

    body["searches"] = nlohmann::json::array();

    nlohmann::json search1, search2;

    search1["q"] = "Adam";
    search1["collection"] = "coll_people";
    search1["query_by"] = "name";

    search2["q"] = "Ford";
    search2["collection"] = "coll_cars";
    search2["query_by"] = "name";

    body["searches"].push_back(search1);
    body["searches"].push_back(search2);
    body["merge_hits"] = true;

    req->body = body.dump();

    req->embedded_params_vec.push_back(nlohmann::json{});
    req->embedded_params_vec.push_back(nlohmann::json{});

    post_multi_search(req, res);

    nlohmann::json response = nlohmann::json::parse(res->body);


    ASSERT_EQ(response["hits"].size(), 2);
    ASSERT_EQ(response["hits"][0]["document"]["name"], "Ford");
    ASSERT_EQ(response["hits"][0]["document"]["country"], "USA");
    ASSERT_EQ(response["hits"][0]["document"]["price"], 10000);

    ASSERT_EQ(response["hits"][1]["document"]["name"], "Adam Smith");
    ASSERT_EQ(response["hits"][1]["document"]["country"], "UK");
    ASSERT_EQ(response["hits"][1]["document"]["age"], 35);


    collectionManager.drop_collection("coll_people");
    collectionManager.drop_collection("coll_cars");
}

TEST_F(CollectionMultiSearchAggTest, MergeWithCommonSortTest) {
    Collection* coll_people = collectionManager.get_collection("coll_people").get();
    if(!coll_people) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("age", field_types::INT32, false),
            field("country", field_types::STRING, true),
            field("points", field_types::INT32, true)
        };
        
        coll_people =collectionManager.create_collection("coll_people", 4, fields, "points").get();
    }

    coll_people->add("{\"name\": \"John Doe\", \"age\": 25, \"country\": \"USA\", \"points\": 100}");
    coll_people->add("{\"name\": \"Jane Doe\", \"age\": 30, \"country\": \"USA\", \"points\": 200}");
    coll_people->add("{\"name\": \"Adam Smith\", \"age\": 35, \"country\": \"UK\", \"points\": 300}");
    coll_people->add("{\"name\": \"John Smith\", \"age\": 40, \"country\": \"UK\", \"points\": 400}");

    Collection* coll_cars = collectionManager.get_collection("coll_cars").get();
    if(!coll_cars) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("price", field_types::INT32, false),
            field("country", field_types::STRING, true),
            field("points", field_types::INT32, true)
        };
        
        coll_cars =collectionManager.create_collection("coll_cars", 4, fields, "points").get();
    }

    coll_cars->add("{\"name\": \"Ford\", \"price\": 10000, \"country\": \"USA\", \"points\": 100}");
    coll_cars->add("{\"name\": \"BMW\", \"price\": 20000, \"country\": \"Germany\", \"points\": 200}");
    coll_cars->add("{\"name\": \"Ferrari\", \"price\": 30000, \"country\": \"Italy\", \"points\": 300}");
    coll_cars->add("{\"name\": \"Audi\", \"price\": 40000, \"country\": \"Germany\", \"points\": 400}");

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    req->params["sort_by"] = "points:desc";
    nlohmann::json body;

    body["searches"] = nlohmann::json::array();

    nlohmann::json search1, search2;

    search1["q"] = "Jane";
    search1["collection"] = "coll_people";
    search1["query_by"] = "name";

    search2["q"] = "Audi";
    search2["collection"] = "coll_cars";
    search2["query_by"] = "name";

    body["searches"].push_back(search1);
    body["searches"].push_back(search2);
    body["merge_hits"] = true;

    req->body = body.dump();

    req->embedded_params_vec.push_back(nlohmann::json{});
    req->embedded_params_vec.push_back(nlohmann::json{});

    post_multi_search(req, res);

    nlohmann::json response = nlohmann::json::parse(res->body);

    ASSERT_EQ(response["hits"].size(), 2);
    ASSERT_EQ(response["hits"][0]["document"]["name"], "Audi");
    ASSERT_EQ(response["hits"][0]["document"]["country"], "Germany");
    ASSERT_EQ(response["hits"][0]["document"]["price"], 40000);
    ASSERT_EQ(response["hits"][0]["document"]["points"], 400);


    ASSERT_EQ(response["hits"][1]["document"]["name"], "Jane Doe");
    ASSERT_EQ(response["hits"][1]["document"]["country"], "USA");
    ASSERT_EQ(response["hits"][1]["document"]["age"], 30);
    ASSERT_EQ(response["hits"][1]["document"]["points"], 200);


    collectionManager.drop_collection("coll_people");
    collectionManager.drop_collection("coll_cars");
}



TEST_F(CollectionMultiSearchAggTest, FacetTest) {

    Collection* coll_people = collectionManager.get_collection("coll_people").get();
    if(!coll_people) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("age", field_types::INT32, false),
            field("country", field_types::STRING, true),
            field("points", field_types::INT32, true)
        };
        
        coll_people =collectionManager.create_collection("coll_people", 4, fields, "points").get();
    }

    coll_people->add("{\"name\": \"John Doe\", \"age\": 25, \"country\": \"USA\", \"points\": 100}");
    coll_people->add("{\"name\": \"Jane Doe\", \"age\": 30, \"country\": \"USA\", \"points\": 200}");
    coll_people->add("{\"name\": \"Adam Smith\", \"age\": 35, \"country\": \"UK\", \"points\": 300}");
    coll_people->add("{\"name\": \"John Smith\", \"age\": 40, \"country\": \"UK\", \"points\": 400}");

    Collection* coll_cars = collectionManager.get_collection("coll_cars").get();
    if(!coll_cars) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("price", field_types::INT32, false),
            field("country", field_types::STRING, true),
            field("points", field_types::INT32, true)
        };
        
        coll_cars =collectionManager.create_collection("coll_cars", 4, fields, "points").get();
    }

    coll_cars->add("{\"name\": \"Ford\", \"price\": 10000, \"country\": \"USA\", \"points\": 100}");
    coll_cars->add("{\"name\": \"BMW\", \"price\": 20000, \"country\": \"Germany\", \"points\": 200}");
    coll_cars->add("{\"name\": \"Ferrari\", \"price\": 30000, \"country\": \"Italy\", \"points\": 300}");
    coll_cars->add("{\"name\": \"Audi\", \"price\": 40000, \"country\": \"Germany\", \"points\": 400}");

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);


    req->params["sort_by"] = "points:desc";
    nlohmann::json body;
    
    body["searches"] = nlohmann::json::array();

    nlohmann::json search1, search2;

    search1["q"] = "*";
    search1["collection"] = "coll_people";
    search1["query_by"] = "name";
    search1["facet_by"] = "country";

    search2["q"] = "*";
    search2["collection"] = "coll_cars";
    search2["query_by"] = "name";
    search2["facet_by"] = "country";

    body["searches"].push_back(search1);
    body["searches"].push_back(search2);
    body["merge_hits"] = true;

    req->body = body.dump();

    req->embedded_params_vec.push_back(nlohmann::json{});
    req->embedded_params_vec.push_back(nlohmann::json{});

    post_multi_search(req, res);

    nlohmann::json response = nlohmann::json::parse(res->body);

    LOG(INFO) << response.dump();

    ASSERT_EQ(response["hits"].size(), 8);

    ASSERT_EQ(response["facet_counts"].size(), 1);
    ASSERT_EQ(response["facet_counts"][0]["counts"].size(), 4);
    ASSERT_EQ(response["facet_counts"][0]["counts"][0]["count"], 3);
    ASSERT_EQ(response["facet_counts"][0]["counts"][0]["value"], "USA");
    ASSERT_EQ(response["facet_counts"][0]["counts"][1]["count"], 2);
    ASSERT_EQ(response["facet_counts"][0]["counts"][1]["value"], "UK");
    ASSERT_EQ(response["facet_counts"][0]["counts"][2]["count"], 2);
    ASSERT_EQ(response["facet_counts"][0]["counts"][2]["value"], "Germany");
    ASSERT_EQ(response["facet_counts"][0]["counts"][3]["count"], 1);
    ASSERT_EQ(response["facet_counts"][0]["counts"][3]["value"], "Italy");

    ASSERT_EQ(response["facet_counts"][0]["field_name"], "country");

    collectionManager.drop_collection("coll_people");
    collectionManager.drop_collection("coll_cars");
}


TEST_F(CollectionMultiSearchAggTest, NoStringSortingTest) {
    
    Collection* coll_people = collectionManager.get_collection("coll_people").get();
    if(!coll_people) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("age", field_types::INT32, false),
            field("country", field_types::STRING, true, false, true, "", 1),
            field("points", field_types::INT32, true)
        };
        
        coll_people =collectionManager.create_collection("coll_people", 4, fields, "points").get();
    }

    coll_people->add("{\"name\": \"John Doe\", \"age\": 25, \"country\": \"USA\", \"points\": 100}");
    coll_people->add("{\"name\": \"Jane Doe\", \"age\": 30, \"country\": \"USA\", \"points\": 200}");
    coll_people->add("{\"name\": \"Adam Smith\", \"age\": 35, \"country\": \"UK\", \"points\": 300}");
    coll_people->add("{\"name\": \"John Smith\", \"age\": 40, \"country\": \"UK\", \"points\": 400}");

    Collection* coll_cars = collectionManager.get_collection("coll_cars").get();
    if(!coll_cars) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("price", field_types::INT32, false),
            field("country", field_types::STRING, true, false, true, "", 1),
            field("points", field_types::INT32, true)
        };
        
        coll_cars =collectionManager.create_collection("coll_cars", 4, fields, "points").get();
    }

    coll_cars->add("{\"name\": \"Ford\", \"price\": 10000, \"country\": \"USA\", \"points\": 100}");
    coll_cars->add("{\"name\": \"BMW\", \"price\": 20000, \"country\": \"Germany\", \"points\": 200}");
    coll_cars->add("{\"name\": \"Ferrari\", \"price\": 30000, \"country\": \"Italy\", \"points\": 300}");
    coll_cars->add("{\"name\": \"Audi\", \"price\": 40000, \"country\": \"Germany\", \"points\": 400}");

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    req->params["sort_by"] = "country:desc";
    nlohmann::json body;

    body["searches"] = nlohmann::json::array();

    nlohmann::json search1, search2;

    search1["q"] = "*";
    search1["collection"] = "coll_people";
    search1["query_by"] = "name";
    
    search2["q"] = "*";
    search2["collection"] = "coll_cars";
    search2["query_by"] = "name";

    body["searches"].push_back(search1);
    body["searches"].push_back(search2);
    body["merge_hits"] = true;

    req->body = body.dump();

    req->embedded_params_vec.push_back(nlohmann::json{});
    req->embedded_params_vec.push_back(nlohmann::json{});

    post_multi_search(req, res);

    nlohmann::json response = nlohmann::json::parse(res->body);

    
    LOG(INFO) << response.dump();

    ASSERT_EQ(response["message"], "Sorting on string fields is not supported while merging multi search results.");


    collectionManager.drop_collection("coll_people");
    collectionManager.drop_collection("coll_cars");
}


TEST_F(CollectionMultiSearchAggTest, FilterTest) {
        
    Collection* coll_people = collectionManager.get_collection("coll_people").get();
    if(!coll_people) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("age", field_types::INT32, false),
            field("country", field_types::STRING, true),
            field("points", field_types::INT32, true)
        };
        
        coll_people =collectionManager.create_collection("coll_people", 4, fields, "points").get();
    }
    
    coll_people->add("{\"name\": \"John Doe\", \"age\": 25, \"country\": \"USA\", \"points\": 100}");
    coll_people->add("{\"name\": \"Jane Doe\", \"age\": 30, \"country\": \"USA\", \"points\": 200}");
    coll_people->add("{\"name\": \"Adam Smith\", \"age\": 35, \"country\": \"UK\", \"points\": 300}");
    coll_people->add("{\"name\": \"John Smith\", \"age\": 40, \"country\": \"UK\", \"points\": 400}");
    
    Collection* coll_cars = collectionManager.get_collection("coll_cars").get();
    if(!coll_cars) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("price", field_types::INT32, false),
            field("country", field_types::STRING, true),
            field("points", field_types::INT32, true)
        };
        
        coll_cars =collectionManager.create_collection("coll_cars", 4, fields, "points").get();
    }
    
    coll_cars->add("{\"name\": \"Ford\", \"price\": 10000, \"country\": \"USA\", \"points\": 100}");
    coll_cars->add("{\"name\": \"BMW\", \"price\": 20000, \"country\": \"Germany\", \"points\": 200}");
    coll_cars->add("{\"name\": \"Ferrari\", \"price\": 30000, \"country\": \"Italy\", \"points\": 300}");
    coll_cars->add("{\"name\": \"Audi\", \"price\": 40000, \"country\": \"Germany\", \"points\": 400}");
    
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    req->params["sort_by"] = "points:desc";
    nlohmann::json body;

    body["searches"] = nlohmann::json::array();

    nlohmann::json search1, search2;

    search1["q"] = "*";
    search1["collection"] = "coll_people";
    search1["query_by"] = "name";
    search1["filter_by"] = "points:>=200";

    search2["q"] = "*";
    search2["collection"] = "coll_cars";
    search2["query_by"] = "name";
    search2["filter_by"] = "points:>=300";

    body["searches"].push_back(search1);
    body["searches"].push_back(search2);
    body["merge_hits"] = true;

    req->body = body.dump();

    req->embedded_params_vec.push_back(nlohmann::json{});
    req->embedded_params_vec.push_back(nlohmann::json{});

    post_multi_search(req, res);
    

    nlohmann::json response = nlohmann::json::parse(res->body);

    LOG(INFO) << response.dump();

    ASSERT_EQ(response["hits"].size(), 5);

    ASSERT_EQ(response["hits"][0]["document"]["name"], "John Smith");
    ASSERT_EQ(response["hits"][0]["document"]["points"], 400);
    ASSERT_EQ(response["hits"][1]["document"]["name"], "Audi");
    ASSERT_EQ(response["hits"][1]["document"]["points"], 400);
    ASSERT_EQ(response["hits"][2]["document"]["name"], "Adam Smith");
    ASSERT_EQ(response["hits"][2]["document"]["points"], 300);
    ASSERT_EQ(response["hits"][3]["document"]["name"], "Ferrari");
    ASSERT_EQ(response["hits"][3]["document"]["points"], 300);
    ASSERT_EQ(response["hits"][4]["document"]["name"], "Jane Doe");
    ASSERT_EQ(response["hits"][4]["document"]["points"], 200);

    collectionManager.drop_collection("coll_people");
    collectionManager.drop_collection("coll_cars");
}


TEST_F(CollectionMultiSearchAggTest, SearchDetailsTest) {

    Collection* coll_people = collectionManager.get_collection("coll_people").get();
    if(!coll_people) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("age", field_types::INT32, false),
            field("country", field_types::STRING, true),
            field("points", field_types::INT32, true)
        };
        
        coll_people =collectionManager.create_collection("coll_people", 4, fields, "points").get();
    }
    
    coll_people->add("{\"name\": \"John Doe\", \"age\": 25, \"country\": \"USA\", \"points\": 100}");
    coll_people->add("{\"name\": \"Jane Doe\", \"age\": 30, \"country\": \"USA\", \"points\": 200}");
    coll_people->add("{\"name\": \"Adam Smith\", \"age\": 35, \"country\": \"UK\", \"points\": 300}");
    coll_people->add("{\"name\": \"John Smith\", \"age\": 40, \"country\": \"UK\", \"points\": 400}");
    
    Collection* coll_cars = collectionManager.get_collection("coll_cars").get();
    if(!coll_cars) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("price", field_types::INT32, false),
            field("country", field_types::STRING, true),
            field("points", field_types::INT32, true)
        };
        
        coll_cars =collectionManager.create_collection("coll_cars", 4, fields, "points").get();
    }
    
    coll_cars->add("{\"name\": \"Ford\", \"price\": 10000, \"country\": \"USA\", \"points\": 100}");
    coll_cars->add("{\"name\": \"BMW\", \"price\": 20000, \"country\": \"Germany\", \"points\": 200}");
    coll_cars->add("{\"name\": \"Ferrari\", \"price\": 30000, \"country\": \"Italy\", \"points\": 300}");
    coll_cars->add("{\"name\": \"Audi\", \"price\": 40000, \"country\": \"Germany\", \"points\": 400}");
    
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    req->params["sort_by"] = "points:desc";

    nlohmann::json body;

    body["searches"] = nlohmann::json::array();

    nlohmann::json search1, search2;

    search1["q"] = "*";
    search1["collection"] = "coll_people";
    search1["query_by"] = "name";

    search2["q"] = "*";
    search2["collection"] = "coll_cars";
    search2["query_by"] = "name";
    search2["filter_by"] = "points:>=300";

    body["searches"].push_back(search1);
    body["searches"].push_back(search2);
    body["merge_hits"] = true;

    req->body = body.dump();

    req->embedded_params_vec.push_back(nlohmann::json{});
    req->embedded_params_vec.push_back(nlohmann::json{});

    post_multi_search(req, res);

    nlohmann::json response = nlohmann::json::parse(res->body);

    LOG(INFO) << response.dump();

    ASSERT_EQ(response["hits"].size(), 6);
    
    ASSERT_EQ(response["found"], 6);
    ASSERT_EQ(response["out_of"], 8);
    ASSERT_EQ(response["request_params"]["collections"].size(), 2);
    ASSERT_EQ(response["request_params"]["collections"][0], "coll_people");
    ASSERT_EQ(response["request_params"]["collections"][1], "coll_cars");
    ASSERT_EQ(response["request_params"]["per_page"], 10);


    collectionManager.drop_collection("coll_people");
    collectionManager.drop_collection("coll_cars");
}


TEST_F(CollectionMultiSearchAggTest, GroupingTest) {
    
    Collection* coll_people = collectionManager.get_collection("coll_people").get();
    if(!coll_people) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("age", field_types::INT32, false),
            field("country", field_types::STRING, true),
            field("points", field_types::INT32, true)
        };
        
        coll_people =collectionManager.create_collection("coll_people", 4, fields, "points").get();
    }
    
    coll_people->add("{\"name\": \"John Doe\", \"age\": 25, \"country\": \"USA\", \"points\": 100}");
    coll_people->add("{\"name\": \"Jane Doe\", \"age\": 30, \"country\": \"USA\", \"points\": 200}");
    coll_people->add("{\"name\": \"Adam Smith\", \"age\": 35, \"country\": \"UK\", \"points\": 300}");
    coll_people->add("{\"name\": \"John Smith\", \"age\": 40, \"country\": \"UK\", \"points\": 400}");
    
    Collection* coll_cars = collectionManager.get_collection("coll_cars").get();
    if(!coll_cars) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("price", field_types::INT32, false),
            field("country", field_types::STRING, true),
            field("points", field_types::INT32, true)
        };
        
        coll_cars =collectionManager.create_collection("coll_cars", 4, fields, "points").get();
    }
    
    coll_cars->add("{\"name\": \"Ford\", \"price\": 10000, \"country\": \"USA\", \"points\": 100}");
    coll_cars->add("{\"name\": \"BMW\", \"price\": 20000, \"country\": \"Germany\", \"points\": 200}");
    coll_cars->add("{\"name\": \"Ferrari\", \"price\": 30000, \"country\": \"Italy\", \"points\": 300}");
    coll_cars->add("{\"name\": \"Audi\", \"price\": 40000, \"country\": \"Germany\", \"points\": 400}");
    
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    req->params["sort_by"] = "points:desc";
    req->params["group_by"] = "country";

    nlohmann::json body;

    body["searches"] = nlohmann::json::array();

    nlohmann::json search1, search2;

    search1["q"] = "*";
    search1["collection"] = "coll_people";
    search1["query_by"] = "name";

    search2["q"] = "*";
    search2["collection"] = "coll_cars";
    search2["query_by"] = "name";

    body["searches"].push_back(search1);
    body["searches"].push_back(search2);
    body["merge_hits"] = true;


    req->body = body.dump();

    req->embedded_params_vec.push_back(nlohmann::json{});
    req->embedded_params_vec.push_back(nlohmann::json{});

    post_multi_search(req, res);

    nlohmann::json response = nlohmann::json::parse(res->body);


    ASSERT_EQ(response["grouped_hits"].size(), 4);
    ASSERT_EQ(response["grouped_hits"][0]["group_key"][0], "UK");
    ASSERT_EQ(response["grouped_hits"][0]["hits"].size(), 2);
    ASSERT_EQ(response["grouped_hits"][0]["hits"][0]["document"]["name"], "John Smith");
    ASSERT_EQ(response["grouped_hits"][0]["hits"][1]["document"]["name"], "Adam Smith");
    ASSERT_EQ(response["grouped_hits"][1]["group_key"][0], "Germany");
    ASSERT_EQ(response["grouped_hits"][1]["hits"].size(), 2);
    ASSERT_EQ(response["grouped_hits"][1]["hits"][0]["document"]["name"], "Audi");
    ASSERT_EQ(response["grouped_hits"][1]["hits"][1]["document"]["name"], "BMW");
    ASSERT_EQ(response["grouped_hits"][2]["group_key"][0], "Italy");
    ASSERT_EQ(response["grouped_hits"][2]["hits"].size(), 1);
    ASSERT_EQ(response["grouped_hits"][2]["hits"][0]["document"]["name"], "Ferrari");
    ASSERT_EQ(response["grouped_hits"][3]["group_key"][0], "USA");
    ASSERT_EQ(response["grouped_hits"][3]["hits"].size(), 3);
    ASSERT_EQ(response["grouped_hits"][3]["hits"][0]["document"]["name"], "Jane Doe");
    ASSERT_EQ(response["grouped_hits"][3]["hits"][1]["document"]["name"], "John Doe");
    ASSERT_EQ(response["grouped_hits"][3]["hits"][2]["document"]["name"], "Ford");


    collectionManager.drop_collection("coll_people");
    collectionManager.drop_collection("coll_cars");
}


TEST_F(CollectionMultiSearchAggTest, FacetQueryTest) {
    Collection* coll_people = collectionManager.get_collection("coll_people").get();
    if(!coll_people) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("age", field_types::INT32, false),
            field("country", field_types::STRING, true),
            field("points", field_types::INT32, true)
        };
        
        coll_people =collectionManager.create_collection("coll_people", 4, fields, "points").get();
    }
    
    coll_people->add("{\"name\": \"John Doe\", \"age\": 25, \"country\": \"USA\", \"points\": 100}");
    coll_people->add("{\"name\": \"Jane Doe\", \"age\": 30, \"country\": \"USA\", \"points\": 200}");
    coll_people->add("{\"name\": \"Adam Smith\", \"age\": 35, \"country\": \"UK\", \"points\": 300}");
    coll_people->add("{\"name\": \"John Smith\", \"age\": 40, \"country\": \"UK\", \"points\": 400}");
    
    Collection* coll_cars = collectionManager.get_collection("coll_cars").get();
    if(!coll_cars) {
        std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("price", field_types::INT32, false),
            field("country", field_types::STRING, true),
            field("points", field_types::INT32, true)
        };
        
        coll_cars =collectionManager.create_collection("coll_cars", 4, fields, "points").get();
    }
    
    coll_cars->add("{\"name\": \"Ford\", \"price\": 10000, \"country\": \"USA\", \"points\": 100}");
    coll_cars->add("{\"name\": \"BMW\", \"price\": 20000, \"country\": \"Germany\", \"points\": 200}");
    coll_cars->add("{\"name\": \"Ferrari\", \"price\": 30000, \"country\": \"Italy\", \"points\": 300}");
    coll_cars->add("{\"name\": \"Audi\", \"price\": 40000, \"country\": \"Germany\", \"points\": 400}");
    
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    nlohmann::json body;

    body["searches"] = nlohmann::json::array();
    body["merge_hits"] = true;

    nlohmann::json search1, search2;

    search1["q"] = "*";
    search1["collection"] = "coll_people";
    search1["query_by"] = "name";
    search1["facet_query"] = "country:USA";

    search2["q"] = "*";
    search2["collection"] = "coll_cars";
    search2["query_by"] = "name";

    body["searches"].push_back(search1);
    body["searches"].push_back(search2);

    req->body = body.dump();

    req->embedded_params_vec.push_back(nlohmann::json{});
    req->embedded_params_vec.push_back(nlohmann::json{});
    post_multi_search(req, res);

    nlohmann::json response = nlohmann::json::parse(res->body);
    ASSERT_EQ(response["hits"].size(), 0);

    collectionManager.drop_collection("coll_people");
    collectionManager.drop_collection("coll_cars");
}