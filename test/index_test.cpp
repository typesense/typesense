#include <gtest/gtest.h>
#include "index.h"
#include <vector>
#include <s2/s2loop.h>

TEST(IndexTest, ScrubReindexDoc) {
    tsl::htrie_map<char, field> search_schema;
    search_schema.emplace("title", field("title", field_types::STRING, false));
    search_schema.emplace("points", field("points", field_types::INT32, false));
    search_schema.emplace("cast", field("cast", field_types::STRING_ARRAY, false));
    search_schema.emplace("movie", field("movie", field_types::BOOL, false));

    ThreadPool pool(4);

    Index index("index", 1, nullptr, nullptr, &pool, search_schema, {}, {});
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

    index.scrub_reindex_doc(search_schema, update_doc1, del_doc1, old_doc);
    ASSERT_EQ(1, del_doc1.size());
    ASSERT_STREQ("1", del_doc1["id"].get<std::string>().c_str());

    // when only some fields are updated

    nlohmann::json update_doc2, del_doc2;
    update_doc2["id"] = "1";
    update_doc2["points"] = 100;
    update_doc2["cast"] = {"Jack"};

    del_doc2 = update_doc2;

    index.scrub_reindex_doc(search_schema, update_doc2, del_doc2, old_doc);
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
    index.scrub_reindex_doc(search_schema, update_doc3, del_doc3, old_doc);
    ASSERT_EQ(3, del_doc3.size());
    ASSERT_STREQ("1", del_doc3["id"].get<std::string>().c_str());
    ASSERT_STREQ("The Lawyer", del_doc3["title"].get<std::string>().c_str());
    ASSERT_STREQ("Bar", del_doc3["foo"].get<std::string>().c_str());

    pool.shutdown();
}

/*TEST(IndexTest, PointInPolygon180thMeridian) {
    // somewhere in far eastern russia
    GeoCoord verts[3] = {
        {67.63378886620751, 179.87924212491276},
        {67.6276069384328, -179.8364939577639},
        {67.5749950728145, 179.94421673458666}
    };


    *//*std::vector<S2Point> vertices;
    for(size_t point_index = 0; point_index < 4; point_index++) {
        S2Point vertex = S2LatLng::FromDegrees(verts[point_index].lat, verts[point_index].lon).ToPoint();
        vertices.emplace_back(vertex);
    }

    S2Loop region(vertices);*//*

    Geofence poly1{3, verts};
    double offset = Index::transform_for_180th_meridian(poly1);

    GeoCoord point1 = {67.61896440098865, 179.9998420463554};
    GeoCoord point2 = {67.6332378896519, 179.88828622883355};
    GeoCoord point3 = {67.62717271243574, -179.85954137693625};

    GeoCoord point4 = {67.65842784263879, -179.79268650445243};
    GeoCoord point5 = {67.62016647245217, 179.83764198608083};

    Index::transform_for_180th_meridian(point1, offset);
    Index::transform_for_180th_meridian(point2, offset);
    Index::transform_for_180th_meridian(point3, offset);
    Index::transform_for_180th_meridian(point4, offset);
    Index::transform_for_180th_meridian(point5, offset);

    *//*ASSERT_TRUE(region.Contains(S2LatLng::FromDegrees(point1.lat, point1.lon).ToPoint()));
    ASSERT_TRUE(region.Contains(S2LatLng::FromDegrees(point2.lat, point2.lon).ToPoint()));
    ASSERT_TRUE(region.Contains(S2LatLng::FromDegrees(point3.lat, point3.lon).ToPoint()));
    ASSERT_FALSE(region.Contains(S2LatLng::FromDegrees(point4.lat, point4.lon).ToPoint()));
    ASSERT_FALSE(region.Contains(S2LatLng::FromDegrees(point5.lat, point5.lon).ToPoint()));
*//*
    ASSERT_TRUE(Index::is_point_in_polygon(poly1, point1));
    ASSERT_TRUE(Index::is_point_in_polygon(poly1, point2));
    ASSERT_TRUE(Index::is_point_in_polygon(poly1, point3));

    ASSERT_FALSE(Index::is_point_in_polygon(poly1, point4));
    ASSERT_FALSE(Index::is_point_in_polygon(poly1, point5));
}*/

TEST(IndexTest, GeoPointPackUnpack) {
    std::vector<std::pair<double, double>> latlngs = {
        {43.677223,-79.630556},
        {-0.041935, 65.433296},     // Indian Ocean Equator
        {-66.035056, 173.187202},   // Newzealand
        {-65.015656, -158.336234},  // Southern Ocean
        {84.552144, -159.742483},   // Arctic Ocean
        {84.517046, 171.730040}     // Siberian Sea
    };

    for(auto& latlng: latlngs) {
        int64_t packed_latlng = GeoPoint::pack_lat_lng(latlng.first, latlng.second);
        S2LatLng s2LatLng;
        GeoPoint::unpack_lat_lng(packed_latlng, s2LatLng);
        ASSERT_FLOAT_EQ(latlng.first, s2LatLng.lat().degrees());
        ASSERT_FLOAT_EQ(latlng.second, s2LatLng.lng().degrees());
    }
}
