#include <s2/s2polygon.h>
#include <s2/s2region_coverer.h>
#include <s2/s2latlng.h>
#include <s2/s2cell_id.h>
#include <s2/s2loop.h>
#include <s2/s2point.h>
#include <s2/s2region_term_indexer.h>
#include <s2/s2cap.h>
#include <s2/s2builder.h>

class GeoPolygonIndex {
private:
    // Map from cell ID to vector of seq_ids
    std::unordered_map<S2CellId, std::set<uint32_t>, S2CellIdHash> cellToSeqids;
    // Map of seq_ids to polygons
    std::unordered_map<uint32_t, std::unique_ptr<S2Polygon>> seqidsToPolygons;
    // Coverer configuration
    S2RegionCoverer::Options covererOptions;

public:
    PolygonIndex(int minLevel = 8, int maxLevel = 12, int maxCells = 50) {
        covererOptions.set_min_level(minLevel);
        covererOptions.set_max_level(maxLevel);
        covererOptions.set_max_cells(maxCells);
    }

    // Add a polygon to the index
    Option<bool> addPolygon(const std::vector<double>& coordinates, uint32_t seq_id) {
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
            return Option<bool>(400, "GeoPolygon for seq_id " +
                                     std::to_string(seq_id) +
                                     " is invalid: " + error.text());
        }

        // Get covering cells for the polygon
        S2RegionCoverer coverer(covererOptions);
        std::vector<S2CellId> covering;
        coverer.GetCovering(*polygon, &covering);

        for (const S2CellId &cellId: covering) {
            cellToSeqids[cellId].insert(seq_id);
        }
        seqidsToPolygons[seq_id] = std::move(polygon);

        return Option<bool>(true);
    }

    // Find all polygons that might contain the given point
    std::vector<uint32_t> findContainingPolygonsRecords(double lat, double lng) const {
        S2LatLng latLng(S1Angle::Degrees(lat), S1Angle::Degrees(lng));
        S2Point point = latLng.ToPoint();

        // Get all cells that contain this point
        S2CellId cellId = S2CellId(point);
        std::vector<uint32_t> candidatePolygonRecords;
        std::unordered_set<S2Polygon*> seenPolygons;

        // Check all parent cells up to min level
        while (cellId.level() >= covererOptions.min_level()) {
            auto it = cellToSeqids.find(cellId);
            if (it != cellToSeqids.end()) {
                for (auto seq_id : it->second) {
                    auto polygonPtr = seqidsToPolygons.at(seq_id).get();
                    if (seenPolygons.insert(polygonPtr).second) {
                        if (polygonPtr->Contains(point)) {
                            candidatePolygonRecords.push_back(seq_id);
                        }
                    }
                }
            }
            cellId = cellId.parent();
        }

        return candidatePolygonRecords;
    }

    void removePolygon(uint32_t seq_id) {
        if(seqidsToPolygons.find(seq_id) != seqidsToPolygons.end()) {
            auto polygonPtr = seqidsToPolygons.at(seq_id).get();

            S2RegionCoverer coverer(covererOptions);
            std::vector<S2CellId> covering;
            coverer.GetCovering(*polygonPtr, &covering);

            for (const S2CellId &cellId: covering) {
                cellToSeqids[cellId].erase(seq_id);
            }

            seqidsToPolygons.erase(seq_id);
        }
    }

    // Get the total number of indexed polygons
    size_t size() const {
        return seqidsToPolygons.size();
    }
};
