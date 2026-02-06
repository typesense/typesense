#include <magic_enum.hpp>
#include "diversity.h"
#include "logger.h"
#include "string_utils.h"
#include "index.h"

Option<bool> diversity_t::parse(const nlohmann::json& json, diversity_t& diversity) {
    // format:
    // "diversity": {
    //          "similarity_metric": [ { "field": "...", "method": "...", "weight": ... }, ... ]
    //      }

    auto it = json.find("diversity");
    if (it == json.end()) {
        return Option<bool>(400, "`diversity` not found: " + json.dump());
    } else if (!it.value().is_object()) {
        return Option<bool>(400, "Invalid `diversity` format: " + it.value().dump());
    } else if (it.value().empty()) {
        return Option<bool>(true);
    }

    auto& rule = it.value();
    if (!rule.is_object() || rule.empty()) {
        return Option<bool>(400, "Invalid `diversity` format, expected an object: " + rule.dump());
    }

    it = rule.find("similarity_metric");
    if (it == rule.end()) {
        return Option<bool>(400, "`similarity_metric` not found: " + rule.dump());
    } else if (!it.value().is_array() || it.value().empty()) {
        return Option<bool>(400, "Invalid `similarity_metric` format: " + it.value().dump());
    }

    std::vector<similarity_metric_t> similarity_metric;
    for (const auto& metric: it.value()) {
        if (!metric.is_object() || metric.empty()) {
            return Option<bool>(400, "Invalid `similarity_metric` format, expected an object: " + metric.dump());
        }

        auto metric_it = metric.find("field");
        if (metric_it == metric.end()) {
            return Option<bool>(400, "`field` not found: " + metric.dump());
        } else if (!metric_it.value().is_string() || metric_it.value().empty()) {
            return Option<bool>(400, "Invalid `field` format: " + metric_it.value().dump());
        }
        std::string field = metric_it.value();

        metric_it = metric.find("method");
        if (metric_it == metric.end()) {
            return Option<bool>(400, "`method` not found: " + metric.dump());
        } else if (!metric_it.value().is_string() || metric_it.value().empty()) {
            return Option<bool>(400, "Invalid `method` format: " + metric_it.value().dump());
        }

        std::string str = metric_it.value().get<std::string>();
        StringUtils::tolowercase(str);
        similarity_methods method;
        auto op = magic_enum::enum_cast<similarity_methods>(str);
        if (op.has_value()) {
            method = op.value();
        } else {
            return Option<bool>(400, "`method` not found: " + metric.dump());
        }

        float weight;
        metric_it = metric.find("weight");
        if (metric_it == metric.end()) {
            weight = 1;
        } else if (!metric_it.value().is_number_float()) {
            return Option<bool>(400, "Invalid `weight` format: `" + metric_it.value().dump() + "`. Expected a float number.");
        } else {
            weight = metric_it.value();
        }

        similarity_metric.emplace_back(field, method, weight);
    }

    diversity.similarity_equation = std::move(similarity_metric);

    return Option<bool>(true);
}

void diversity_t::to_json(const diversity_t &diversity, nlohmann::json& json) {
    for (const auto& item: diversity.similarity_equation) {
        nlohmann::json metric;
        metric["field"] = item.field;
        metric["method"] = magic_enum::enum_name(item.method);
        metric["weight"] = item.weight;
        json["diversity"]["similarity_metric"] += metric;
    }
}

Option<double> similarity_t::calculate(uint32_t seq_id_i, uint32_t seq_id_j, const diversity_t& diversity,
                                       const spp::sparse_hash_map<std::string, spp::sparse_hash_map<uint32_t, int64_t, Hasher32>*>& sort_index,
                                       const facet_index_t* facet_index_v4,
                                       const spp::sparse_hash_map<std::string, hnsw_index_t*>& vector_index) {
    double similarity = 0;
    for (const auto& metric: diversity.similarity_equation) {

        if (metric.method == diversity_t::equality || metric.method == diversity_t::jaccard) {
            if (facet_index_v4->has_hash_index(metric.field)) {
                auto facet_index = facet_index_v4->get_facet_hash_index(metric.field);
                auto facet_it = facet_index->new_iterator();
                if (!facet_it.valid()) {
                    continue;
                }
                facet_it.skip_to(seq_id_i);
                if (!facet_it.valid() || facet_it.id() != seq_id_i) {
                    continue;
                }

                std::set<uint32_t> i_facet_hashes;
                if (metric.is_field_array) {
                    std::vector<uint32_t> facet_hashes;
                    posting_list_t::get_offsets(facet_it, facet_hashes);
                    i_facet_hashes.insert(facet_hashes.begin(), facet_hashes.end());
                } else {
                    i_facet_hashes.insert(facet_it.offset());
                }

                facet_it.skip_to(seq_id_j);
                if (!facet_it.valid() || facet_it.id() != seq_id_j) {
                    continue;
                }

                std::set<uint32_t> j_facet_hashes;
                if (metric.is_field_array) {
                    std::vector<uint32_t> facet_hashes;
                    posting_list_t::get_offsets(facet_it, facet_hashes);
                    j_facet_hashes.insert(facet_hashes.begin(), facet_hashes.end());
                } else {
                    j_facet_hashes.insert(facet_it.offset());
                }

                if (metric.method == diversity_t::jaccard) {
                    if (i_facet_hashes.empty() && j_facet_hashes.empty()) {
                        continue;
                    }
                    std::vector<uint32_t> out{};
                    std::set_intersection(i_facet_hashes.begin(), i_facet_hashes.end(),
                                          j_facet_hashes.begin(), j_facet_hashes.end(),
                                          std::back_inserter(out));
                    const auto intersection_size = out.size();
                    out.clear();

                    std::set_union(i_facet_hashes.begin(), i_facet_hashes.end(),
                                   j_facet_hashes.begin(), j_facet_hashes.end(),
                                   std::back_inserter(out));
                    const auto union_size = out.size();

                    similarity += ((double) intersection_size/union_size) * metric.weight;
                } else if (metric.method == diversity_t::equality) {
                    if (i_facet_hashes.size() != j_facet_hashes.size()) {
                        continue;
                    }
                    std::vector<uint32_t> out{};
                    std::set_difference(i_facet_hashes.begin(), i_facet_hashes.end(),
                                        j_facet_hashes.begin(), j_facet_hashes.end(),
                                        std::back_inserter(out));
                    similarity += metric.weight * out.empty();
                }
            }

            else if (sort_index.count(metric.field) > 0) {
                auto& sort_map = sort_index.at(metric.field);
                auto it = sort_map->find(seq_id_i);
                if (it == sort_map->end()) {
                    continue;
                }
                auto const& i_value = it->second;

                it = sort_map->find(seq_id_j);
                if (it == sort_map->end()) {
                    continue;
                }
                auto const& j_value = it->second;

                if (metric.method == diversity_t::equality && i_value == j_value) {
                    similarity += metric.weight;
                }
            }

            else {
                return Option<double>(400, "`" + metric.field + "` field not found in either facet or sort index.");
            }
        }

        else if (metric.method == diversity_t::vector_distance) {
            auto it = vector_index.find(metric.field);
            if (it == vector_index.end()) {
                return Option<double>(400, "`" + metric.field + "` field not found in vector index.");
            } else if (it->second == nullptr) {
                return Option<double>(400, "`" + metric.field + "` vector index is not initialised.");
            }

            const auto& field_vector_index = it->second;
            std::vector<float> values_i, values_j;
            try {
                values_i = field_vector_index->vecdex->getDataByLabel<float>(seq_id_i);
                values_j = field_vector_index->vecdex->getDataByLabel<float>(seq_id_j);
            } catch (...) {
                // likely not found
                continue;
            }

            // Distance can be [0, 2]. 0 represents that embeddings are identical.
            const auto dist = field_vector_index->space->get_dist_func()(values_i.data(), values_j.data(),
                                                                            &field_vector_index->num_dim);
            // Doing 2-dist since dist 0 means the documents are most similar. We need to return the maximum value for
            // most similar documents from this function.
            similarity += metric.weight * (2 - dist);
        }

        else {
            return Option<double>(400, "`" + std::string(magic_enum::enum_name(metric.method)) + "` method is not supported.");
        }
    }

    return Option<double>(similarity);
}
