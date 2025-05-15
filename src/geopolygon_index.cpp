#include "geopolygon_index.h"

Option<bool> GeoPolygonIndex::addPolygon(const std::vector<double>& coordinates, uint32_t seq_id) {
    // Convert each ring of coordinates to an S2Loop
    std::vector <S2Point> points;

    const int coordinates_size = coordinates.size();
    for (size_t point_index = 0; point_index < coordinates_size; point_index += 2) {
        double lat = coordinates[point_index];
        double lon = coordinates[point_index + 1];

        S2LatLng latLng(S1Angle::Degrees(coordinates[point_index]),
                        S1Angle::Degrees(coordinates[point_index + 1]));
        points.push_back(latLng.ToPoint());
    }

    auto loop = std::make_unique<S2Loop>(points);
    loop->Normalize();

    //passing vector of loops to check for empty loops
    std::vector<std::unique_ptr<S2Loop>> loops;
    loops.emplace_back(std::move(loop));

    // Create polygon from loops
    std::unique_ptr<S2Polygon> polygon = std::make_unique<S2Polygon>(std::move(loops));

    std::vector <uint64_t> cell_ids;

    S2Error error;
    if (polygon->FindValidationError(&error)) {
        return Option<bool>(400, "Geopolygon for seq_id " +
                                 std::to_string(seq_id) +
                                 " is invalid: " + error.text());
    }

    for (const auto& term: indexer->GetIndexTerms(*polygon, "")) {
        auto cell = S2CellId::FromToken(term);
        cell_ids.push_back(cell.id());
    }

    for (const auto& cell_id: cell_ids) {
        numericTrie->insert_geopoint(cell_id, seq_id);
    }
    seqidToPolygons[seq_id].emplace_back(std::move(polygon));

    return Option<bool>(true);
}


std::vector<uint32_t> GeoPolygonIndex::findContainingPolygonsRecords(double lat, double lng) {
    S2LatLng latLng(S1Angle::Degrees(lat), S1Angle::Degrees(lng));
    S2Point point = latLng.ToPoint();

    std::vector <uint32_t> candidate_seq_ids, result_seq_ids;

    std::vector <uint64_t> cell_ids;
    for (const auto& term: indexer->GetQueryTerms(point, "")) {
        auto cell = S2CellId::FromToken(term);
        cell_ids.push_back(cell.id());
    }

    numericTrie->search_geopoints(cell_ids, candidate_seq_ids);

    //second pass validation check
    for (const auto& id: candidate_seq_ids) {
        if (seqidToPolygons.find(id) != seqidToPolygons.end()) {
            for (const auto& polygon: seqidToPolygons.at(id)) {
                if (polygon->Contains(point)) {
                    result_seq_ids.push_back(id);
                }
            }
        }
    }

    return result_seq_ids;
}

void GeoPolygonIndex::removePolygon(uint32_t seq_id) {
    if (seqidToPolygons.find(seq_id) != seqidToPolygons.end()) {
        std::vector <uint32_t> cell_ids;

        for (const auto& polygon: seqidToPolygons.at(seq_id)) {
            for (const auto& term: indexer->GetIndexTerms(*polygon, "")) {
                auto cell = S2CellId::FromToken(term);
                cell_ids.push_back(cell.id());
            }

            for (const auto& cell_id: cell_ids) {
                numericTrie->delete_geopoint(cell_id, seq_id);
            }
        }

        seqidToPolygons.erase(seq_id);
    }
}
