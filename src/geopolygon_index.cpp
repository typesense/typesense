#include "geopolygon_index.h"
#include "unordered_set"

void GeoPolygonIndex::insertSeqIds(S2CellId value, uint32_t id) {
    if (cellToSeqids.count(value) == 0) {
        cellToSeqids.emplace(value, SET_COMPACT_IDS(compact_id_list_t::create(1, {id})));
    } else {
        auto ids = cellToSeqids[value];
        if (!ids_t::contains(ids, id)) {
            ids_t::upsert(ids, id);
            cellToSeqids[value] = ids;
        }
    }
}

size_t GeoPolygonIndex::getSeqIds(S2CellId value, std::vector<uint32_t>& result_ids) {
    const auto& it = cellToSeqids.find(value);
    if(it == cellToSeqids.end()) {
        return 0;
    }

    uint32_t* ids = ids_t::uncompress(it->second);
    for(size_t i = 0; i < ids_t::num_ids(it->second); i++) {
        result_ids.push_back(ids[i]);
    }

    delete [] ids;

    return ids_t::num_ids(it->second);
}

void GeoPolygonIndex::removeSeqIds(S2CellId value, uint32_t id) {
    if(cellToSeqids.count(value) != 0) {
        void* arr = cellToSeqids[value];
        ids_t::erase(arr, id);

        if(ids_t::num_ids(arr) == 0) {
            ids_t::destroy_list(arr);
            cellToSeqids.erase(value);
        } else {
            cellToSeqids[value] = arr;
        }
    }
}

Option<bool> GeoPolygonIndex::addPolygon(const std::vector<double>& coordinates, uint32_t seq_id) {
    // Convert each ring of coordinates to an S2Loop
    std::vector<S2Point> points;

    const int coordinates_size = coordinates.size();
    for (size_t point_index = 0; point_index < coordinates_size; point_index+=2) {
        double lat = coordinates[point_index];
        double lon = coordinates[point_index + 1];

        S2LatLng latLng(S1Angle::Degrees(coordinates[point_index]),
                        S1Angle::Degrees(coordinates[point_index + 1]));
        points.push_back(latLng.ToPoint());
    }

    auto loop = std::make_unique<S2Loop>(points);
    loop->Normalize();

    // Create polygon from loops
    auto polygon = std::make_unique<S2Polygon>(std::move(loop));
    //polygon->Init(std::move(loop));
    S2Error error;
    if (polygon->FindValidationError(&error)) {
        return Option<bool>(400, "Geopolygon for seq_id " +
                                 std::to_string(seq_id) +
                                 " is invalid: " + error.text());
    }

    // Get covering cells for the polygon
    S2RegionCoverer coverer(covererOptions);
    std::vector<S2CellId> covering;
    coverer.GetCovering(*polygon, &covering);

    for (const S2CellId &cellId: covering) {
        insertSeqIds(cellId, seq_id);
    }
    seqidsToPolygons[seq_id] = std::move(polygon);

    return Option<bool>(true);
}


std::vector<uint32_t> GeoPolygonIndex::findContainingPolygonsRecords(double lat, double lng) {
    S2LatLng latLng(S1Angle::Degrees(lat), S1Angle::Degrees(lng));
    S2Point point = latLng.ToPoint();

    // Get all cells that contain this point
    S2CellId cellId = S2CellId(point);
    std::vector<uint32_t> candidatePolygonRecords;
    std::unordered_set<S2Polygon*> seenPolygons;
    std::vector<uint32_t> seq_ids;

    // Check all parent cells up to min level
    while (cellId.level() >= covererOptions.min_level()) {
        auto it = cellToSeqids.find(cellId);
        if (it != cellToSeqids.end()) {
            seq_ids.clear();
            if(getSeqIds(cellId, seq_ids)) {
                for (auto seq_id: seq_ids) {
                    auto polygonPtr = seqidsToPolygons.at(seq_id).get();
                    if (seenPolygons.insert(polygonPtr).second) {
                        if (polygonPtr->Contains(point)) {
                            candidatePolygonRecords.push_back(seq_id);
                        }
                    }
                }
            }
        }
        cellId = cellId.parent();
    }

    return candidatePolygonRecords;
}

void GeoPolygonIndex::removePolygon(uint32_t seq_id) {
    if(seqidsToPolygons.find(seq_id) != seqidsToPolygons.end()) {
        auto polygonPtr = seqidsToPolygons.at(seq_id).get();

        S2RegionCoverer coverer(covererOptions);
        std::vector<S2CellId> covering;
        coverer.GetCovering(*polygonPtr, &covering);

        for (const S2CellId &cellId: covering) {
            removeSeqIds(cellId, seq_id);
        }

        seqidsToPolygons.erase(seq_id);
    }
}

size_t GeoPolygonIndex::size() const {
    return seqidsToPolygons.size();
}