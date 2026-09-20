#pragma once

#include <string>
#include <unordered_map>
#include <s2/s2polygon.h>
#include <s2/s2region_coverer.h>
#include <s2/s2latlng.h>
#include <s2/s2cell_id.h>
#include <s2/s2loop.h>
#include <s2/s2point.h>
#include <s2/s2region_term_indexer.h>
#include <s2/s2cap.h>
#include <s2/s2builder.h>
#include "option.h"
#include "ids_t.h"

class GeoPolygonIndex {
private:
    S2RegionTermIndexer* indexer = nullptr;

    std::unordered_map<std::string, void*> termToSeqids;
    std::unordered_map<uint32_t, std::vector<std::unique_ptr<S2Polygon>>> seqidToPolygons;
public:
    GeoPolygonIndex() {
        //initialize with all default options
        indexer = new S2RegionTermIndexer();
    }

    ~GeoPolygonIndex() {
        for (auto& kv: termToSeqids) {
            ids_t::destroy_list(kv.second);
        }

        delete indexer;
    }

    GeoPolygonIndex(const GeoPolygonIndex&) = delete;
    GeoPolygonIndex& operator=(const GeoPolygonIndex&) = delete;

    // Add a polygon to the index
    Option<bool> addPolygon(const std::vector<double>& coordinates, uint32_t seq_id);

    // Find all polygons that might contain the given point
    // num_candidates, when given, receives how many polygons the term lookup shortlisted
    std::vector<uint32_t> findContainingPolygonsRecords(double lat, double lng, size_t* num_candidates = nullptr);

    //remove polygon from index
    void removePolygon(uint32_t seq_id);
};
