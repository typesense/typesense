#include <timsort.hpp>
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

    S2Error error;
    if (polygon->FindValidationError(&error)) {
        return Option<bool>(400, "Geopolygon for seq_id " +
                                 std::to_string(seq_id) +
                                 " is invalid: " + error.text());
    }

    for (const auto& term: indexer->GetIndexTerms(*polygon, "")) {
        const auto& it = termToSeqids.find(term);
        if (it == termToSeqids.end()) {
            termToSeqids.emplace(term, ids_t::create({seq_id}));
        } else {
            ids_t::upsert(it->second, seq_id);
        }
    }

    seqidToPolygons[seq_id].emplace_back(std::move(polygon));

    return Option<bool>(true);
}


std::vector<uint32_t> GeoPolygonIndex::findContainingPolygonsRecords(double lat, double lng,
                                                                     size_t* num_candidates) {
    S2LatLng latLng(S1Angle::Degrees(lat), S1Angle::Degrees(lng));
    S2Point point = latLng.ToPoint();

    std::vector <uint32_t> candidate_seq_ids, result_seq_ids;

    for (const auto& term: indexer->GetQueryTerms(point, "")) {
        const auto& it = termToSeqids.find(term);
        if (it != termToSeqids.end()) {
            ids_t::uncompress(it->second, candidate_seq_ids);
        }
    }

    gfx::timsort(candidate_seq_ids.begin(), candidate_seq_ids.end());
    candidate_seq_ids.erase(std::unique(candidate_seq_ids.begin(), candidate_seq_ids.end()),
                            candidate_seq_ids.end());

    if (num_candidates != nullptr) {
        *num_candidates = candidate_seq_ids.size();
    }

    //second pass validation check
    for (const auto& id: candidate_seq_ids) {
        const auto& it = seqidToPolygons.find(id);
        if (it == seqidToPolygons.end()) {
            continue;
        }

        for (const auto& polygon: it->second) {
            if (polygon->Contains(point)) {
                result_seq_ids.push_back(id);
                break;
            }
        }
    }

    return result_seq_ids;
}

void GeoPolygonIndex::removePolygon(uint32_t seq_id) {
    const auto& seqid_it = seqidToPolygons.find(seq_id);
    if (seqid_it == seqidToPolygons.end()) {
        return;
    }

    for (const auto& polygon: seqid_it->second) {
        for (const auto& term: indexer->GetIndexTerms(*polygon, "")) {
            const auto& it = termToSeqids.find(term);
            if (it == termToSeqids.end()) {
                continue;
            }

            ids_t::erase(it->second, seq_id);
            if (ids_t::num_ids(it->second) == 0) {
                ids_t::destroy_list(it->second);
                termToSeqids.erase(it);
            }
        }
    }

    seqidToPolygons.erase(seqid_it);
}
