#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class GeoFilteringOldTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_filtering";
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

TEST_F(GeoFilteringOldTest, GeoPointFiltering) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
            {"Palais Garnier", "48.872576479306765, 2.332291112241466"},
            {"Sacre Coeur", "48.888286721920934, 2.342340862419206"},
            {"Arc de Triomphe", "48.87538726829884, 2.296113163780903"},
            {"Place de la Concorde", "48.86536119187326, 2.321850747347093"},
            {"Louvre Musuem", "48.86065813197502, 2.3381285349616725"},
            {"Les Invalides", "48.856648379569904, 2.3118555692631357"},
            {"Eiffel Tower", "48.85821022164442, 2.294239067890161"},
            {"Notre-Dame de Paris", "48.852455825574495, 2.35071182406452"},
            {"Musee Grevin", "48.872370541246816, 2.3431536410008906"},
            {"Pantheon", "48.84620987789056, 2.345152755563131"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        std::vector<std::string> lat_lng;
        StringUtils::split(records[i][1], lat_lng, ", ");

        double lat = std::stod(lat_lng[0]);
        double lng = std::stod(lat_lng[1]);

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["loc"] = {lat, lng};
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // pick a location close to only the Sacre Coeur
    auto results = coll1->search("*",
                                 {}, "loc: (48.90615915923891, 2.3435897727061175, 3 km)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());


    results = coll1->search("*", {}, "loc: (48.90615, 2.34358, 1 km) || "
                                     "loc: (48.8462, 2.34515, 1 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());

    // pick location close to none of the spots
    results = coll1->search("*",
                            {}, "loc: (48.910544830985785, 2.337218333651177, 2 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    // pick a large radius covering all points

    results = coll1->search("*",
                            {}, "loc: (48.910544830985785, 2.337218333651177, 20 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(10, results["found"].get<size_t>());

    // 1 mile radius

    results = coll1->search("*",
                            {}, "loc: (48.85825332869331, 2.303816427653377, 1 mi)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());

    ASSERT_STREQ("6", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("5", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("3", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // when geo query had NaN
    auto gop = coll1->search("*", {}, "loc: (NaN, nan, 1 mi)",
                             {}, {}, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(gop.ok());
    ASSERT_EQ("Value of filter field `loc`: must be in the `(-44.50, 170.29, 0.75 km)` or "
              "(56.33, -65.97, 23.82, -127.82) format.", gop.error());

    // when geo field is formatted as string, show meaningful error
    nlohmann::json bad_doc;
    bad_doc["id"] = "1000";
    bad_doc["title"] = "Test record";
    bad_doc["loc"] = {"48.91", "2.33"};
    bad_doc["points"] = 1000;

    auto add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a geopoint.", add_op.error());

    bad_doc["loc"] = "foobar";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a 2 element array: [lat, lng].", add_op.error());

    bad_doc["loc"] = "loc: (48.910544830985785, 2.337218333651177, 2k)";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a 2 element array: [lat, lng].", add_op.error());

    bad_doc["loc"] = "loc: (48.910544830985785, 2.337218333651177, 2)";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a 2 element array: [lat, lng].", add_op.error());

    bad_doc["loc"] = {"foo", "bar"};
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a geopoint.", add_op.error());

    bad_doc["loc"] = {"2.33", "bar"};
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a geopoint.", add_op.error());

    bad_doc["loc"] = {"foo", "2.33"};
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a geopoint.", add_op.error());

    // under coercion mode, it should work
    bad_doc["loc"] = {"48.91", "2.33"};
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_TRUE(add_op.ok());

    collectionManager.drop_collection("coll1");
}

TEST_F(GeoFilteringOldTest, GeoPointArrayFiltering) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT_ARRAY, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::vector<std::string>>> records = {
            {   {"Alpha Inc", "Ennore", "13.22112, 80.30511"},
                    {"Alpha Inc", "Velachery", "12.98973, 80.23095"}
            },

            {
                {"Veera Inc", "Thiruvallur", "13.12752, 79.90136"},
            },

            {
                {"B1 Inc", "Bengaluru", "12.98246, 77.5847"},
                    {"B1 Inc", "Hosur", "12.74147, 77.82915"},
                    {"B1 Inc", "Vellore", "12.91866, 79.13075"},
            },

            {
                {"M Inc", "Nashik", "20.11282, 73.79458"},
                    {"M Inc", "Pune", "18.56309, 73.855"},
            }
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0][0];
        doc["points"] = i;

        std::vector<std::vector<double>> lat_lngs;
        for(size_t k = 0; k < records[i].size(); k++) {
            std::vector<std::string> lat_lng_str;
            StringUtils::split(records[i][k][2], lat_lng_str, ", ");

            std::vector<double> lat_lng = {
                    std::stod(lat_lng_str[0]),
                    std::stod(lat_lng_str[1])
            };

            lat_lngs.push_back(lat_lng);
        }

        doc["loc"] = lat_lngs;
        auto add_op = coll1->add(doc.dump());
        ASSERT_TRUE(add_op.ok());
    }

    // pick a location close to Chennai
    auto results = coll1->search("*",
                                 {}, "loc: (13.12631, 80.20252, 100km)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // pick location close to none of the spots
    results = coll1->search("*",
                            {}, "loc: (13.62601, 79.39559, 10 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    // pick a large radius covering all points

    results = coll1->search("*",
                            {}, "loc: (21.20714729927276, 78.99153966917213, 1000 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(4, results["found"].get<size_t>());

    // 1 mile radius

    results = coll1->search("*",
                            {}, "loc: (12.98941, 80.23073, 1mi)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // when geo field is formatted badly, show meaningful error
    nlohmann::json bad_doc;
    bad_doc["id"] = "1000";
    bad_doc["title"] = "Test record";
    bad_doc["loc"] = {"48.91", "2.33"};
    bad_doc["points"] = 1000;

    auto add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must contain 2 element arrays: [ [lat, lng],... ].", add_op.error());

    bad_doc["loc"] = "foobar";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be an array.", add_op.error());

    bad_doc["loc"] = nlohmann::json::array();
    nlohmann::json points = nlohmann::json::array();
    points.push_back("foo");
    points.push_back("bar");
    bad_doc["loc"].push_back(points);

    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be an array of geopoint.", add_op.error());

    bad_doc["loc"][0][0] = "2.33";
    bad_doc["loc"][0][1] = "bar";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be an array of geopoint.", add_op.error());

    bad_doc["loc"][0][0] = "foo";
    bad_doc["loc"][0][1] = "2.33";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be an array of geopoint.", add_op.error());

    // under coercion mode, it should work
    bad_doc["loc"][0][0] = "48.91";
    bad_doc["loc"][0][1] = "2.33";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_TRUE(add_op.ok());

    collectionManager.drop_collection("coll1");
}

TEST_F(GeoFilteringOldTest, GeoPointRemoval) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc1", field_types::GEOPOINT, false),
                                 field("loc2", field_types::GEOPOINT_ARRAY, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Palais Garnier";
    doc["loc1"] = {48.872576479306765, 2.332291112241466};
    doc["loc2"] = nlohmann::json::array();
    doc["loc2"][0] = {48.84620987789056, 2.345152755563131};
    doc["points"] = 100;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("*",
                                 {}, "loc1: (48.87491151802846, 2.343945883701618, 1 km)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("*",
                            {}, "loc2: (48.87491151802846, 2.343945883701618, 10 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    // remove the document, index another document and try querying again
    coll1->remove("0");
    doc["id"] = "1";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    results = coll1->search("*",
                            {}, "loc1: (48.87491151802846, 2.343945883701618, 1 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("*",
                            {}, "loc2: (48.87491151802846, 2.343945883701618, 10 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
}

TEST_F(GeoFilteringOldTest, GeoPolygonFiltering) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
            {"Palais Garnier", "48.872576479306765, 2.332291112241466"},
            {"Sacre Coeur", "48.888286721920934, 2.342340862419206"},
            {"Arc de Triomphe", "48.87538726829884, 2.296113163780903"},
            {"Place de la Concorde", "48.86536119187326, 2.321850747347093"},
            {"Louvre Musuem", "48.86065813197502, 2.3381285349616725"},
            {"Les Invalides", "48.856648379569904, 2.3118555692631357"},
            {"Eiffel Tower", "48.85821022164442, 2.294239067890161"},
            {"Notre-Dame de Paris", "48.852455825574495, 2.35071182406452"},
            {"Musee Grevin", "48.872370541246816, 2.3431536410008906"},
            {"Pantheon", "48.84620987789056, 2.345152755563131"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        std::vector<std::string> lat_lng;
        StringUtils::split(records[i][1], lat_lng, ", ");

        double lat = std::stod(lat_lng[0]);
        double lng = std::stod(lat_lng[1]);

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["loc"] = {lat, lng};
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // pick a location close to only the Sacre Coeur
    auto results = coll1->search("*",
                                 {}, "loc: (48.875223042424125,2.323509661928681, "
                                     "48.85745408145392, 2.3267084486160856, "
                                     "48.859636574404355,2.351469427048221, "
                                     "48.87756059389807, 2.3443610121873206)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_STREQ("8", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("4", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // should work even if points of polygon are clockwise

    results = coll1->search("*",
                            {}, "loc: (48.87756059389807, 2.3443610121873206, "
                                "48.859636574404355,2.351469427048221, "
                                "48.85745408145392, 2.3267084486160856, "
                                "48.875223042424125,2.323509661928681)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    auto search_op = coll1->search("*", {}, "loc: (10, 20, 11, 12, 14, 16, 10, 20, 11, 40)", {}, {}, {0}, 10, 1,
                                   FREQUENCY);
    ASSERT_FALSE(search_op.ok());
    ASSERT_EQ("Polygon is invalid: Edge 2 has duplicate vertex with edge 4", search_op.error());

    search_op = coll1->search("*", {}, "loc: (10, 20, 11, 12, 14, 16, 10, 20)", {}, {}, {0}, 10, 1,
                                   FREQUENCY);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(0, search_op.get()["found"].get<size_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(GeoFilteringOldTest, GeoPolygonFilteringSouthAmerica) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
            {"North of Equator", "4.48615, -71.38049"},
            {"South of Equator", "-8.48587, -71.02892"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        std::vector<std::string> lat_lng;
        StringUtils::split(records[i][1], lat_lng, ", ");

        double lat = std::stod(lat_lng[0]);
        double lng = std::stod(lat_lng[1]);

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["loc"] = {lat, lng};
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // pick a polygon that covers both points

    auto results = coll1->search("*",
                                 {}, "loc: (13.3163, -82.3585, "
                                     "-29.134, -82.3585, "
                                     "-29.134, -59.8528, "
                                     "13.3163, -59.8528)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(GeoFilteringOldTest, GeoPointFilteringWithNonSortableLocationField) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT, false),
                                 field("points", field_types::INT32, false),};

    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string", "sort": false},
            {"name": "loc", "type": "geopoint", "sort": true},
            {"name": "points", "type": "int32", "sort": false}
        ]
    })"_json;

    auto coll_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_op.ok());
    Collection* coll1 = coll_op.get();

    std::vector<std::vector<std::string>> records = {
            {"Palais Garnier", "48.872576479306765, 2.332291112241466"},
            {"Sacre Coeur", "48.888286721920934, 2.342340862419206"},
            {"Arc de Triomphe", "48.87538726829884, 2.296113163780903"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        std::vector<std::string> lat_lng;
        StringUtils::split(records[i][1], lat_lng, ", ");

        double lat = std::stod(lat_lng[0]);
        double lng = std::stod(lat_lng[1]);

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["loc"] = {lat, lng};
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // pick a location close to only the Sacre Coeur
    auto results = coll1->search("*",
                                 {}, "loc: (48.90615915923891, 2.3435897727061175, 3 km)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
}