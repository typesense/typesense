#include <gtest/gtest.h>
#include <chrono>
#include "jev_rerank.h"
#include "jev_client.h"
#include "json.hpp"

class JevRerankTest : public ::testing::Test {
protected:
    void SetUp() override {
        JevClient::clear_mock_responses();
        JevClient::enable_request_capture();
    }

    void TearDown() override {
        JevClient::clear_mock_responses();
        JevClient::disable_request_capture();
    }
};

// matches the clock and unit http_req uses to stamp conn_ts
static uint64_t now_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
}

static nlohmann::json jev_config() {
    return nlohmann::json{{"model_name", "jev/jev-latest"}, {"api_key", "ts-test"}};
}

static nlohmann::json hit_with_doc(const nlohmann::json& document) {
    nlohmann::json hit;
    hit["document"] = document;
    return hit;
}

static nlohmann::json three_title_hits() {
    nlohmann::json hits = nlohmann::json::array();
    hits.push_back(hit_with_doc({{"title", "first"}}));
    hits.push_back(hit_with_doc({{"title", "second"}}));
    hits.push_back(hit_with_doc({{"title", "third"}}));
    return hits;
}

static std::string answers_body(const std::vector<double>& nouls) {
    nlohmann::json body;
    body["model"] = "jev-1.13.0";
    body["answers"] = nlohmann::json::object();
    for(size_t i = 0; i < nouls.size(); i++) {
        body["answers"][JevRerank::candidate_id(i)] = {{"type", "noul"}, {"noul", nouls[i]}};
    }
    body["usage"] = {{"input_tokens", 100}, {"output_tokens", 1}};
    return body.dump();
}

TEST_F(JevRerankTest, CandidateKeepsOnlyQueryByFields) {
    nlohmann::json hit = hit_with_doc({{"title", "Trattoria"}, {"cuisine", "Italian"}, {"internal_id", 42}});
    auto candidate = JevRerank::candidate_from_hit(hit, {{"title", "cuisine"}});

    ASSERT_EQ(2, candidate.size());
    ASSERT_EQ("Trattoria", candidate["title"]);
    ASSERT_EQ("Italian", candidate["cuisine"]);
}

TEST_F(JevRerankTest, CandidateFallsBackToTheWholeDocument) {
    nlohmann::json hit = hit_with_doc({{"title", "Trattoria"}, {"cuisine", "Italian"}});

    auto no_query_by = JevRerank::candidate_from_hit(hit, {});
    ASSERT_EQ(2, no_query_by.size());

    // query_by names that match nothing must not strip the candidate to an empty object
    auto no_overlap = JevRerank::candidate_from_hit(hit, {{"embedding"}});
    ASSERT_EQ(2, no_overlap.size());
}

TEST_F(JevRerankTest, CandidateNestedQueryByRidesItsParentObject) {
    nlohmann::json hit = hit_with_doc({{"open_hours", {{"day", "Mon"}, {"until", "22:00"}}},
                                       {"title", "Trattoria"}});
    auto candidate = JevRerank::candidate_from_hit(hit, {{"open_hours.day"}});

    ASSERT_EQ(1, candidate.size());
    ASSERT_EQ("Mon", candidate["open_hours"]["day"]);
}

TEST_F(JevRerankTest, CandidateUnionHitsPickTheirSearchsFields) {
    nlohmann::json hit = hit_with_doc({{"title", "Trattoria"}, {"name", "Mario"}});
    hit["search_index"] = 1;

    auto candidate = JevRerank::candidate_from_hit(hit, {{"title"}, {"name"}});
    ASSERT_EQ(1, candidate.size());
    ASSERT_EQ("Mario", candidate["name"]);
}

TEST_F(JevRerankTest, CandidateRespectsTheByteBudget) {
    // a multibyte string crossing the cut must land on a codepoint boundary or dump throws
    std::string big;
    while(big.size() < 4 * JevRerank::MAX_CANDIDATE_BYTES) {
        big += "αβγδε ";
    }
    nlohmann::json hit = hit_with_doc({{"title", "Trattoria"}, {"description", big}});

    auto candidate = JevRerank::candidate_from_hit(hit, {});
    ASSERT_LE(candidate.dump().size(), JevRerank::MAX_CANDIDATE_BYTES);
    ASSERT_EQ("Trattoria", candidate["title"]);
    ASSERT_FALSE(candidate["description"].get<std::string>().empty());
}

TEST_F(JevRerankTest, QuestionsReferenceCandidatePaths) {
    auto questions = JevRerank::build_questions(2);

    ASSERT_EQ(2, questions.size());
    ASSERT_TRUE(questions.contains("cand0"));
    ASSERT_TRUE(questions.contains("cand1"));
    ASSERT_EQ("noul", questions["cand1"]["type"]);
    ASSERT_NE(std::string::npos,
              questions["cand1"]["instructions"].get<std::string>().find("`candidates[1]`"));
}

TEST_F(JevRerankTest, RerankReordersByNoulAndExposesScores) {
    nlohmann::json hits = three_title_hits();
    JevClient::add_mock_response(answers_body({0.2, 0.9, 0.5}), 200, {});

    JevRerank::rerank_with_model(hits, "pasta", {{"title"}}, jev_config(), 20);

    ASSERT_EQ("second", hits[0]["document"]["title"]);
    ASSERT_EQ("third", hits[1]["document"]["title"]);
    ASSERT_EQ("first", hits[2]["document"]["title"]);
    ASSERT_DOUBLE_EQ(0.9, hits[0]["jev_rerank_score"].get<double>());
    ASSERT_DOUBLE_EQ(0.5, hits[1]["jev_rerank_score"].get<double>());
    ASSERT_DOUBLE_EQ(0.2, hits[2]["jev_rerank_score"].get<double>());
}

TEST_F(JevRerankTest, RerankTiesKeepTheOriginalOrder) {
    nlohmann::json hits = three_title_hits();
    JevClient::add_mock_response(answers_body({0.5, 0.5, 0.5}), 200, {});

    JevRerank::rerank_with_model(hits, "pasta", {{"title"}}, jev_config(), 20);

    ASSERT_EQ("first", hits[0]["document"]["title"]);
    ASSERT_EQ("second", hits[1]["document"]["title"]);
    ASSERT_EQ("third", hits[2]["document"]["title"]);
}

TEST_F(JevRerankTest, RerankJudgesOnlyTheTopK) {
    nlohmann::json hits = three_title_hits();
    JevClient::add_mock_response(answers_body({0.1, 0.9}), 200, {});

    JevRerank::rerank_with_model(hits, "pasta", {{"title"}}, jev_config(), 2);

    ASSERT_EQ("second", hits[0]["document"]["title"]);
    ASSERT_EQ("first", hits[1]["document"]["title"]);
    ASSERT_EQ("third", hits[2]["document"]["title"]);
    ASSERT_FALSE(hits[2].contains("jev_rerank_score"));

    const auto request = nlohmann::json::parse(JevClient::get_last_request_body());
    ASSERT_EQ(2, request["questions"].size());
    ASSERT_EQ(2, request["state"]["candidates"].size());
    ASSERT_EQ("pasta", request["state"]["query"]);
}

TEST_F(JevRerankTest, RerankFailureKeepsTheOriginalOrder) {
    nlohmann::json hits = three_title_hits();
    JevClient::add_mock_response(R"({"error": {"message": "boom"}})", 500, {});

    JevRerank::rerank_with_model(hits, "pasta", {{"title"}}, jev_config(), 20);

    ASSERT_EQ("first", hits[0]["document"]["title"]);
    ASSERT_EQ("second", hits[1]["document"]["title"]);
    ASSERT_EQ("third", hits[2]["document"]["title"]);
    ASSERT_FALSE(hits[0].contains("jev_rerank_score"));
}

TEST_F(JevRerankTest, RerankSkipsAStarQuery) {
    nlohmann::json hits = three_title_hits();

    JevRerank::rerank_with_model(hits, "*", {{"title"}}, jev_config(), 20);

    ASSERT_EQ(0, JevClient::get_num_captured_requests());
    ASSERT_EQ("first", hits[0]["document"]["title"]);
}

TEST_F(JevRerankTest, RerankCuratedHitsHoldTheirSlots) {
    nlohmann::json hits = three_title_hits();
    hits[0]["curated"] = true;
    JevClient::add_mock_response(answers_body({0.2, 0.9}), 200, {});

    JevRerank::rerank_with_model(hits, "pasta", {{"title"}}, jev_config(), 20);

    ASSERT_EQ("first", hits[0]["document"]["title"]);
    ASSERT_FALSE(hits[0].contains("jev_rerank_score"));
    ASSERT_EQ("third", hits[1]["document"]["title"]);
    ASSERT_EQ("second", hits[2]["document"]["title"]);
}

TEST_F(JevRerankTest, RerankMissingAnswerSortsAsZero) {
    nlohmann::json hits = three_title_hits();
    nlohmann::json body = nlohmann::json::parse(answers_body({0.4, 0.5, 0.8}));
    body["answers"].erase("cand1");
    JevClient::add_mock_response(body.dump(), 200, {});

    JevRerank::rerank_with_model(hits, "pasta", {{"title"}}, jev_config(), 20);

    ASSERT_EQ("third", hits[0]["document"]["title"]);
    ASSERT_EQ("first", hits[1]["document"]["title"]);
    ASSERT_EQ("second", hits[2]["document"]["title"]);
    ASSERT_FALSE(hits[2].contains("jev_rerank_score"));
}

TEST_F(JevRerankTest, RerankSkipsWhenTheRequestBudgetIsSpent) {
    nlohmann::json hits = three_title_hits();
    JevClient::add_mock_response(answers_body({0.2, 0.9, 0.5}), 200, {});

    // a connection a minute old means interpretation already spent the shared budget
    JevRerank::rerank_with_model(hits, "pasta", {{"title"}}, jev_config(), 20,
                                 now_us() - 60ULL * 1000 * 1000);

    ASSERT_EQ(0, JevClient::get_num_captured_requests());
    ASSERT_EQ("first", hits[0]["document"]["title"]);
    ASSERT_EQ("second", hits[1]["document"]["title"]);
    ASSERT_FALSE(hits[0].contains("jev_rerank_score"));
}

TEST_F(JevRerankTest, RerankRunsWithBudgetLeftAndWithoutADeadline) {
    nlohmann::json hits = three_title_hits();
    JevClient::add_mock_response(answers_body({0.2, 0.9, 0.5}), 200, {});

    JevRerank::rerank_with_model(hits, "pasta", {{"title"}}, jev_config(), 20, now_us());

    ASSERT_EQ(1, JevClient::get_num_captured_requests());
    ASSERT_EQ("second", hits[0]["document"]["title"]);

    // zero start_ts is the no-deadline case for callers without a request clock
    nlohmann::json undated = three_title_hits();
    JevClient::add_mock_response(answers_body({0.2, 0.9, 0.5}), 200, {});

    JevRerank::rerank_with_model(undated, "pasta", {{"title"}}, jev_config(), 20, 0);

    ASSERT_EQ(2, JevClient::get_num_captured_requests());
    ASSERT_EQ("second", undated[0]["document"]["title"]);
}

TEST_F(JevRerankTest, RerankByModelIdSkipsUnknownModel) {
    nlohmann::json hits = three_title_hits();

    JevRerank::rerank(hits, "pasta", {{"title"}}, "no_such_model", 20);

    ASSERT_EQ(0, JevClient::get_num_captured_requests());
    ASSERT_EQ("first", hits[0]["document"]["title"]);
}
