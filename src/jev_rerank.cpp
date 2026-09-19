#include "jev_rerank.h"
#include "jev_client.h"
#include "natural_language_search_model_manager.h"
#include "logger.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <unordered_set>

std::string JevRerank::candidate_id(size_t i) {
    return "cand" + std::to_string(i);
}

static void warn_once(std::atomic<bool>& flag, const std::string& message) {
    if(!flag.exchange(true)) {
        LOG(WARNING) << message;
    }
}

// a cut mid codepoint would make the state undumpable
static size_t utf8_boundary(const std::string& value, size_t pos) {
    while(pos > 0 && (static_cast<unsigned char>(value[pos]) & 0xC0) == 0x80) {
        pos--;
    }
    return pos;
}

nlohmann::json JevRerank::candidate_from_hit(const nlohmann::json& hit,
                                             const std::vector<std::vector<std::string>>& query_by_per_search) {
    if(!hit.is_object() || !hit.contains("document") || !hit["document"].is_object()) {
        return nlohmann::json::object();
    }

    const nlohmann::json& document = hit["document"];

    size_t search_index = 0;
    if(hit.contains("search_index") && hit["search_index"].is_number_integer() &&
       hit["search_index"].get<long>() >= 0) {
        search_index = hit["search_index"].get<size_t>();
    }

    std::vector<std::pair<std::string, const nlohmann::json*>> selected;
    std::unordered_set<std::string> seen;
    if(search_index < query_by_per_search.size()) {
        for(const auto& field_name : query_by_per_search[search_index]) {
            if(document.contains(field_name)) {
                if(seen.insert(field_name).second) {
                    selected.emplace_back(field_name, &document[field_name]);
                }
                continue;
            }
            // a nested query_by leaf rides inside its top level object
            const size_t dot = field_name.find('.');
            if(dot != std::string::npos && document.contains(field_name.substr(0, dot)) &&
               seen.insert(field_name.substr(0, dot)).second) {
                selected.emplace_back(field_name.substr(0, dot), &document[field_name.substr(0, dot)]);
            }
        }
    }
    if(selected.empty()) {
        for(auto it = document.begin(); it != document.end(); ++it) {
            selected.emplace_back(it.key(), &it.value());
        }
    }

    nlohmann::json candidate = nlohmann::json::object();
    size_t used = 2;
    for(const auto& [key, value] : selected) {
        const size_t entry_overhead = key.size() + 4;
        if(used + entry_overhead + 2 >= MAX_CANDIDATE_BYTES) {
            continue;
        }
        // an element count bound skips embedding vectors without ever serializing them
        if((value->is_array() || value->is_object()) && value->size() > MAX_CANDIDATE_BYTES / 8) {
            continue;
        }
        if(value->is_string()) {
            // one long field must not starve the rest of the candidate
            const size_t field_cap = std::min(MAX_CANDIDATE_BYTES / 2,
                                              MAX_CANDIDATE_BYTES - used - entry_overhead - 2);
            const std::string& text = value->get_ref<const std::string&>();
            std::string cut = text.substr(0, utf8_boundary(text, std::min(text.size(), field_cap)));
            // dump escaping can grow past the cap, shrink until the escaped form fits
            std::string dumped = nlohmann::json(cut).dump();
            while(dumped.size() > field_cap + 2 && !cut.empty()) {
                cut = cut.substr(0, utf8_boundary(cut, cut.size() / 2));
                dumped = nlohmann::json(cut).dump();
            }
            const size_t entry_size = entry_overhead + dumped.size();
            if(used + entry_size <= MAX_CANDIDATE_BYTES) {
                candidate[key] = std::move(cut);
                used += entry_size;
            }
            continue;
        }
        const size_t entry_size = entry_overhead + value->dump().size();
        if(used + entry_size <= MAX_CANDIDATE_BYTES) {
            candidate[key] = *value;
            used += entry_size;
        }
    }

    return candidate;
}

nlohmann::json JevRerank::build_state(const std::string& query, const nlohmann::json& candidates) {
    nlohmann::json state;
    state["query"] = query;
    state["candidates"] = candidates;
    return state;
}

nlohmann::json JevRerank::build_questions(size_t num_candidates) {
    nlohmann::json questions = nlohmann::json::object();
    for(size_t i = 0; i < num_candidates; i++) {
        const std::string path = "candidates[" + std::to_string(i) + "]";
        questions[candidate_id(i)] = JevClient::noul_question(
                "A searcher typed the `query`. Is the document at `" + path + "` what that searcher "
                "is looking for, does it itself satisfy the query?",
                "the document fulfills the query's intent, the searcher would be satisfied to get it",
                "the document is only superficially or topically related, it does not itself satisfy "
                "what the query asks for");
    }
    return questions;
}

void JevRerank::rerank_with_model(nlohmann::json& hits, const std::string& query,
                                  const std::vector<std::vector<std::string>>& query_by_per_search,
                                  const nlohmann::json& model_config, size_t top_k) {
    if(!hits.is_array() || hits.empty()) {
        return;
    }

    if(query.empty() || query == "*") {
        static std::atomic<bool> warned_star{false};
        warn_once(warned_star, "jev_rerank skipped: there is no query text to judge against");
        return;
    }

    // pinned hits hold their slots, only organic hits are judged and re-sorted
    const size_t page_k = std::min(top_k, hits.size());
    std::vector<size_t> slots;
    for(size_t i = 0; i < page_k; i++) {
        if(hits[i].is_object() && !hits[i].contains("curated")) {
            slots.push_back(i);
        }
    }
    if(slots.empty()) {
        return;
    }

    nlohmann::json candidates = nlohmann::json::array();
    for(size_t j = 0; j < slots.size(); j++) {
        candidates.push_back(candidate_from_hit(hits[slots[j]], query_by_per_search));
    }

    jev_round_stats_t stats;
    stats.purpose = "rerank";
    auto response_op = JevClient::ask(build_state(query, candidates), build_questions(slots.size()),
                                      model_config, &stats);

    if(!response_op.ok()) {
        LOG(WARNING) << "jev rerank failed, original order stands: " << response_op.error();
        LOG(INFO) << "jev timing: rerank status=" << stats.status << " api=" << stats.wall_ms
                  << "ms judged=0 req_bytes=" << stats.request_bytes;
        return;
    }

    // get() returns by value, binding a reference into the temporary would dangle
    const nlohmann::json response = response_op.get();
    const nlohmann::json& answers = response["answers"];

    // negated scores sort ascending, the original rank breaks ties
    std::vector<std::pair<double, size_t>> ranked;
    ranked.reserve(slots.size());
    for(size_t j = 0; j < slots.size(); j++) {
        auto noul_op = JevClient::get_noul(answers, candidate_id(j));
        double score = 0.0;
        if(noul_op.ok()) {
            score = noul_op.get();
            hits[slots[j]]["jev_rerank_score"] = score;
        }
        ranked.emplace_back(-score, j);
    }
    std::sort(ranked.begin(), ranked.end());

    std::vector<nlohmann::json> judged;
    judged.reserve(slots.size());
    for(size_t j = 0; j < slots.size(); j++) {
        judged.push_back(std::move(hits[slots[j]]));
    }
    for(size_t j = 0; j < slots.size(); j++) {
        hits[slots[j]] = std::move(judged[ranked[j].second]);
    }

    LOG(INFO) << "jev timing: rerank api=" << stats.wall_ms << "ms judged=" << slots.size()
              << " req_bytes=" << stats.request_bytes
              << " input_tokens=" << stats.input_tokens;
}

void JevRerank::rerank(nlohmann::json& hits, const std::string& query,
                       const std::vector<std::vector<std::string>>& query_by_per_search,
                       const std::string& model_id, size_t top_k) {
    if(model_id.empty()) {
        static std::atomic<bool> warned_missing{false};
        warn_once(warned_missing, "jev_rerank skipped: `jev_rerank_model_id` is required");
        return;
    }

    auto model_op = NaturalLanguageSearchModelManager::get_model(model_id);
    if(!model_op.ok()) {
        static std::atomic<bool> warned_lookup{false};
        warn_once(warned_lookup, "jev_rerank skipped: model `" + model_id + "`: " + model_op.error());
        return;
    }

    const nlohmann::json model_config = model_op.get();
    if(!JevClient::is_jev_model(model_config)) {
        static std::atomic<bool> warned_namespace{false};
        warn_once(warned_namespace, "jev_rerank skipped: model `" + model_id + "` is not in the `jev` namespace");
        return;
    }

    rerank_with_model(hits, query, query_by_per_search, model_config, top_k);
}
