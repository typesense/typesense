#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class GeoFilteringTest : public ::testing::Test {
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

TEST_F(GeoFilteringTest, GeoPointFiltering) {
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
                                 {}, "loc: ([48.90615915923891, 2.3435897727061175], radius: 3 km)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    // Multiple queries can be clubbed using square brackets [ filterA, filterB, ... ]
    results = coll1->search("*", {}, "loc: [([48.90615, 2.34358], radius: 1 km), ([48.8462, 2.34515], radius: 1 km)]",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());

    // pick location close to none of the spots
    results = coll1->search("*",
                            {}, "loc: [([48.910544830985785, 2.337218333651177], radius: 2 km)]",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    // pick a large radius covering all points

    results = coll1->search("*",
                            {}, "loc: ([48.910544830985785, 2.337218333651177], radius: 20 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(10, results["found"].get<size_t>());

    // 1 mile radius

    results = coll1->search("*",
                            {}, "loc: ([48.85825332869331, 2.303816427653377], radius: 1 mi)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());

    ASSERT_EQ("6", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("5", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["hits"][2]["document"]["id"].get<std::string>());

    // when geo query had NaN
    auto gop = coll1->search("*", {}, "loc: ([NaN, nan], radius: 1 mi)",
                             {}, {}, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(gop.ok());
    ASSERT_EQ("Value of filter field `loc`: must be in the "
              "`([-44.50, 170.29], radius: 0.75 km, exact_filter_radius: 5 km)` or "
              "([56.33, -65.97, 23.82, -127.82], exact_filter_radius: 7 km) format.", gop.error());

    // when geo query does not send radius key
    gop = coll1->search("*", {}, "loc: ([48.85825332869331, 2.303816427653377])",
                        {}, {}, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(gop.ok());
    ASSERT_EQ("Value of filter field `loc`: must be in the "
              "`([-44.50, 170.29], radius: 0.75 km, exact_filter_radius: 5 km)` or "
              "([56.33, -65.97, 23.82, -127.82], exact_filter_radius: 7 km) format.", gop.error());

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

TEST_F(GeoFilteringTest, GeoPointArrayFiltering) {
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
                                 {}, "loc: ([13.12631, 80.20252], radius: 100km, exact_filter_radius: 100km)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // Default value of exact_filter_radius is 10km, exact filtering is not performed.
    results = coll1->search("*",
                            {}, "loc: ([13.12631, 80.20252], radius: 100km,)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][2]["document"]["id"].get<std::string>());

    // pick location close to none of the spots
    results = coll1->search("*",
                            {}, "loc: ([13.62601, 79.39559], radius: 10 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    // pick a large radius covering all points

    results = coll1->search("*",
                            {}, "loc: ([21.20714729927276, 78.99153966917213], radius: 1000 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(4, results["found"].get<size_t>());

    // 1 mile radius

    results = coll1->search("*",
                            {}, "loc: ([12.98941, 80.23073], radius: 1mi)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());

    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

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

TEST_F(GeoFilteringTest, GeoPointRemoval) {
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
                                 {}, "loc1: ([48.87491151802846, 2.343945883701618], radius: 1 km)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("*",
                            {}, "loc2: ([48.87491151802846, 2.343945883701618], radius: 10 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    // remove the document, index another document and try querying again
    coll1->remove("0");
    doc["id"] = "1";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    results = coll1->search("*",
                            {}, "loc1: ([48.87491151802846, 2.343945883701618], radius: 1 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("*",
                            {}, "loc2: ([48.87491151802846, 2.343945883701618], radius: 10 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
}

TEST_F(GeoFilteringTest, GeoPolygonFiltering) {
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
                                 {}, "loc: ([48.875223042424125,2.323509661928681, "
                                     "48.85745408145392, 2.3267084486160856, "
                                     "48.859636574404355,2.351469427048221, "
                                     "48.87756059389807, 2.3443610121873206])",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_EQ("8", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("4", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][2]["document"]["id"].get<std::string>());

    // should work even if points of polygon are clockwise

    results = coll1->search("*",
                            {}, "loc: ([48.87756059389807, 2.3443610121873206, "
                                "48.859636574404355,2.351469427048221, "
                                "48.85745408145392, 2.3267084486160856, "
                                "48.875223042424125,2.323509661928681])",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    // when geo query had NaN
    auto gop = coll1->search("*", {}, "loc: ([48.87756059389807, 2.3443610121873206, NaN, nan])",
                             {}, {}, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(gop.ok());
    ASSERT_EQ("Value of filter field `loc`: must be in the "
              "`([-44.50, 170.29], radius: 0.75 km, exact_filter_radius: 5 km)` or "
              "([56.33, -65.97, 23.82, -127.82], exact_filter_radius: 7 km) format.", gop.error());

    gop = coll1->search("*", {}, "loc: ([56.33, -65.97, 23.82, -127.82], exact_filter_radius: 7k)",
                        {}, {}, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(gop.ok());
    ASSERT_EQ("Unit must be either `km` or `mi`.", gop.error());

    auto search_op = coll1->search("*", {}, "loc: (10, 20, 11, 12, 14, 16, 10, 20, 11, 40)", {}, {}, {0}, 10, 1,
                                   FREQUENCY);
    ASSERT_FALSE(search_op.ok());
    ASSERT_EQ("Polygon is invalid: Edge 2 has duplicate vertex with edge 4", search_op.error());

    search_op = coll1->search("*", {}, "loc: (10, 20, 11, 12, 14, 16, 10, 20)", {}, {}, {0}, 10, 1,
                              FREQUENCY);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(0, search_op.get()["found"].get<size_t>());

    search_op = coll1->search("*", {}, "loc: [([10, 20, 30, 40, 50, 30]), ([10, 20, 11, 12, 14, 16, 10, 20])]", {}, {},
                              {0}, 10, 1, FREQUENCY);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(0, search_op.get()["found"].get<size_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(GeoFilteringTest, GeoPolygonFilteringSouthAmerica) {
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
            {"North of Equator, outside polygon", "4.13377, -56.00459"},
            {"South of Equator, outside polygon", "-4.5041, -57.34523"},
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

    // polygon only covers 2 points but all points are returned since exact filtering is not performed.
    auto results = coll1->search("*",
                                 {}, "loc: ([13.3163, -82.3585, "
                                     "-29.134, -82.3585, "
                                     "-29.134, -59.8528, "
                                     "13.3163, -59.8528])",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());

    results = coll1->search("*",
                            {}, "loc: ([13.3163, -82.3585, "
                                "-29.134, -82.3585, "
                                "-29.134, -59.8528, "
                                "13.3163, -59.8528], exact_filter_radius: 2703km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(GeoFilteringTest, GeoPointFilteringWithNonSortableLocationField) {
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
                                 {}, "loc: ([48.90615915923891, 2.3435897727061175], radius:3 km)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
}

TEST_F(GeoFilteringTest, GeoPolygonTest) {
    nlohmann::json schema = R"({
        "name": "coll_geopolygon",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "area", "type": "geopolygon"}
        ]
    })"_json;

    auto coll_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_op.ok());
    Collection* coll1 = coll_op.get();

    //should be in ccw order to avoid any issues while forming polygon
    std::vector<std::vector<std::string>> records = {
            {"square",    "0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0"},
            {"rectangle", "2.0, 2.0, 5.0, 2.0, 5.0, 4.0, 2.0, 4.0"}
    };

    for (size_t i = 0; i < records.size(); i++) {
        nlohmann::json doc;
        std::vector<double> lat_lng;
        std::vector<std::string> lat_lng_str;

        StringUtils::split(records[i][1], lat_lng_str, ", ");

        doc["id"] = std::to_string(i);
        doc["name"] = records[i][0];

        for (const auto& val: lat_lng_str) {
            lat_lng.push_back(std::stod(val));
        }

        doc["area"] = lat_lng;


        auto op = coll1->add(doc.dump());
        if (!op.ok()) {
            LOG(ERROR) << op.error();
        }
    }

    //search point in square
    auto results = coll1->search("*",
                                 {}, "area:(0.5, 0.5)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("*",
                            {}, "area:(2.5, 3.5)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());


    //add another shape intersecting with existing shape
    nlohmann::json doc;
    std::vector<double> lat_lng;
    std::vector<std::string> lat_lng_str;

    std::vector<std::string> record{"square2", "0.0, 0.0, 2.0, 0.0, 2.0, 2.0, 0.0, 2.0"};
    StringUtils::split(record[1], lat_lng_str, ", ");

    doc["id"] = "2";
    doc["name"] = record[0];

    for (const auto& val: lat_lng_str) {
        lat_lng.push_back(std::stod(val));
    }

    doc["area"] = lat_lng;


    auto op = coll1->add(doc.dump());
    if (!op.ok()) {
        LOG(ERROR) << op.error();
    }

    //search same point
    results = coll1->search("*",
                            {}, "area:(0.5, 0.5)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    //remove a document
    coll1->remove("0");
    results = coll1->search("*",
                            {}, "area:(0.5, 0.5)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    //coordinates should be in ccw or cw loop. otherwise it throws error to form polygon
    std::vector<std::string> record2{"rectangle2", "5.0, 4.0, 5.0, 2.0, 2.0, 2.0, 2.0, 4.0"};
    StringUtils::split(record2[1], lat_lng_str, ", ");

    doc["id"] = "3";
    doc["name"] = record[0];

    for (const auto& val: lat_lng_str) {
        lat_lng.push_back(std::stod(val));
    }

    doc["area"] = lat_lng;

    op = coll1->add(doc.dump());
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Geopolygon for seq_id 3 is invalid: Edge 6 has duplicate vertex with edge 10", op.error());
}

TEST_F(GeoFilteringTest, GeoPolygonTestRealCoordinates) {
    // 1) Create a collection schema with a geopolygon field.
    nlohmann::json schema = R"({
        "name": "coll_geopolygon",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "area", "type": "geopolygon"}
        ]
    })"_json;

    auto coll_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_op.ok());
    Collection* coll = coll_op.get();

    // We'll store {name, polygon-coordinates} in a 2D vector of strings:
    // Each polygon is lat/lon pairs in either CW or CCW order (both can be made valid
    // by S2, as long as no self-intersection occurs).
    // Each entry is { name, "lat1, lon1, lat2, lon2, ... "}

    std::vector<std::vector<std::string>> records = {
            {
                    "central_park",
                    "40.8003, -73.9582, 40.7682, -73.9817, 40.7642, -73.9728, 40.7968, -73.9492"
            },
            {
                    "times_square",
                    "40.7586, -73.9855, 40.7550, -73.9855, 40.7550, -73.9810, 40.7586, -73.9810"
            }
    };

    // 2) Insert these polygons into the collection
    for (size_t i = 0; i < records.size(); i++) {
        nlohmann::json doc;
        std::vector<std::string> lat_lng_str;
        std::vector<double> lat_lng;

        // Extract the comma-separated lat/lng pairs
        StringUtils::split(records[i][1], lat_lng_str, ", ");

        // Build a JSON doc with "id", "name", and the numeric array "area"
        doc["id"] = std::to_string(i);
        doc["name"] = records[i][0];

        for (const auto& val : lat_lng_str) {
            lat_lng.push_back(std::stod(val));
        }
        doc["area"] = lat_lng;

        auto op = coll->add(doc.dump());
        ASSERT_TRUE(op.ok()) << op.error();
    }

    // 3) Query a point that should be inside "central_park"
    // A rough center point: (40.7812, -73.9665)
    {
        auto results = coll->search("*", {},
                                    "area:(40.7812, -73.9665)",  // lat, lon
                                    {}, {}, {0}, 10, 1, FREQUENCY).get();

        ASSERT_EQ(1, results["hits"].size());
        // Expect doc "0" => "central_park"
        ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    }

    // 4) Query a point that should be inside "times_square"
    // A rough center point: (40.7573, -73.9851)
    {
        auto results = coll->search("*", {},
                                    "area:(40.7573, -73.9851)",
                                    {}, {}, {0}, 10, 1, FREQUENCY).get();

        ASSERT_EQ(1, results["hits"].size());
        // Expect doc "1" => "times_square"
        ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    }

    // 5) Add another shape that intersects with Central Park bounding box (a bigger Manhattan bounding box).
    //    This bounding box extends well beyond Central Park, so it should contain that same test point.
    {
        nlohmann::json doc;
        doc["id"] = "2";
        doc["name"] = "manhattan_big";

        std::string bigger_box_coords =
                "40.88, -74.02, 40.7, -74.02, 40.7, -73.93, 40.88, -73.93";

        std::vector<std::string> lat_lng_str;
        std::vector<double> lat_lng;
        StringUtils::split(bigger_box_coords, lat_lng_str, ", ");
        for (const auto& val : lat_lng_str) {
            lat_lng.push_back(std::stod(val));
        }
        doc["area"] = lat_lng;

        auto op = coll->add(doc.dump());
        ASSERT_TRUE(op.ok()) << op.error();
    }

    // 6) Query the same Central Park point again. Now it should return *both*
    //    "central_park" (id=0) and "manhattan_big" (id=2).
    {
        auto results = coll->search("*", {},
                                    "area:(40.7812, -73.9665)",
                                    {}, {}, {0}, 10, 1, FREQUENCY).get();
        ASSERT_EQ(2, results["hits"].size());

        // We expect two hits, but order can vary. Let's just confirm we have ID 0 and ID 2.
        std::unordered_set<std::string> ids;
        for (auto& hit : results["hits"]) {
            ids.insert(hit["document"]["id"].get<std::string>());
        }
        ASSERT_TRUE(ids.count("0") > 0);
        ASSERT_TRUE(ids.count("2") > 0);
    }

    // 7) Remove the "central_park" doc (id=0). Then query the same point again
    //    to confirm we only get the bigger bounding box (id=2).
    {
        coll->remove("0");
        auto results = coll->search("*", {},
                                    "area:(40.7812, -73.9665)",
                                    {}, {}, {0}, 10, 1, FREQUENCY).get();

        ASSERT_EQ(1, results["hits"].size());
        ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    }

    // 8) Insert an invalid polygon
    {
        std::string invalid_polygon_coords = "40.7565, -73.9845";

        nlohmann::json doc;
        doc["id"] = "3";
        doc["name"] = "times_square_invalid";

        std::vector<std::string> lat_lng_str;
        std::vector<double> lat_lng;
        StringUtils::split(invalid_polygon_coords, lat_lng_str, ", ");
        for (const auto &val : lat_lng_str) {
            lat_lng.push_back(std::stod(val));
        }
        doc["area"] = lat_lng;

        auto op = coll->add(doc.dump());
        ASSERT_FALSE(op.ok()); // We expect it to fail
        ASSERT_EQ("Geopolygon for seq_id 3 is invalid: Loop 0: empty loops are not allowed", op.error());
    }
}