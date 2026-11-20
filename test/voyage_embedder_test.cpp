#include <gtest/gtest.h>
#include "text_embedder_remote.h"
#include "embedder_manager.h"

class VoyageEmbedderTest : public ::testing::Test {
protected:
    static nlohmann::json embedding_item(size_t index, size_t dims, float value = 0.1f) {
        return {{"object", "embedding"}, {"index", index}, {"embedding", std::vector<float>(dims, value)}};
    }

    static nlohmann::json chunked_item(size_t index, size_t num_chunks, size_t dims) {
        auto chunks = nlohmann::json::array();
        for(size_t i = 0; i < num_chunks; i++) {
            chunks.push_back(embedding_item(i, dims));
        }
        return {{"object", "list"}, {"index", index}, {"data", chunks}};
    }
};

TEST_F(VoyageEmbedderTest, CatalogueHasOnlyNamedModels) {
    for(const std::string name : {"voyage-4-large", "voyage-4", "voyage-4-lite", "voyage-code-4", "voyage-3-large",
                                  "voyage-3.5", "voyage-3.5-lite", "voyage-code-3", "voyage-finance-2", "voyage-law-2",
                                  "voyage-context-4", "voyage-context-3"}) {
        ASSERT_NE(nullptr, VoyageEmbedder::find_model(name)) << name;
    }

    ASSERT_EQ(nullptr, VoyageEmbedder::find_model("voyage-3"));
    ASSERT_EQ(nullptr, VoyageEmbedder::find_model("text-embedding-3-small"));
    ASSERT_EQ(nullptr, VoyageEmbedder::find_model(""));
    ASSERT_TRUE(VoyageEmbedder::find_model("voyage-context-4")->contextualized);
    ASSERT_FALSE(VoyageEmbedder::find_model("voyage-4")->contextualized);
}

TEST_F(VoyageEmbedderTest, ModelValidationRejectsBadConfig) {
    size_t num_dims = 0;

    // missing model_name / api_key
    auto op = VoyageEmbedder::is_model_valid({{"api_key", "k"}}, num_dims, false);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ(400, op.code());
    ASSERT_EQ("Property `embed.model_config.model_name is missing or is not a string.", op.error());

    op = VoyageEmbedder::is_model_valid({{"model_name", "voyage/voyage-4"}}, num_dims, false);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Property `embed.model_config.api_key is missing or is not a string.", op.error());

    // another provider's model
    op = VoyageEmbedder::is_model_valid({{"model_name", "openai/text-embedding-3-small"}, {"api_key", "k"}}, num_dims, false);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Property `embed.model_config.model_name` malformed.", op.error());

    // unknown model
    op = VoyageEmbedder::is_model_valid({{"model_name", "voyage/voyage-nope"}, {"api_key", "k"}}, num_dims, false);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Property `embed.model_config.model_name` is not a supported Voyage model: `voyage-nope`.", op.error());

    // dimensions the model does not support are rejected before any API call
    num_dims = 300;
    op = VoyageEmbedder::is_model_valid({{"model_name", "voyage/voyage-4-lite"}, {"api_key", "k"}}, num_dims, true);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Voyage model `voyage-4-lite` does not support `num_dim` 300. Supported values: 256, 512, 1024, 2048.", op.error());

    num_dims = 256;
    op = VoyageEmbedder::is_model_valid({{"model_name", "voyage/voyage-law-2"}, {"api_key", "k"}}, num_dims, true);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Voyage model `voyage-law-2` does not support `num_dim` 256. Supported values: 1024.", op.error());
}

TEST_F(VoyageEmbedderTest, EmbedderManagerRejectsUnknownVoyageModel) {
    size_t num_dims = 0;
    auto op = EmbedderManager::get_instance().validate_and_init_model(
        {{"model_name", "voyage/not-a-model"}, {"api_key", "k"}}, num_dims);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ(400, op.code());
    ASSERT_TRUE(EmbedderManager::is_remote_model("voyage/voyage-4"));
}

TEST_F(VoyageEmbedderTest, RequestSendsOutputDimensionOnlyWhenRequested) {
    const auto* spec = VoyageEmbedder::find_model("voyage-4");

    auto body = VoyageEmbedder::build_request(*spec, {"a", "b"}, false, false, 1024);
    ASSERT_EQ("voyage-4", body["model"]);
    ASSERT_EQ("document", body["input_type"]);
    ASSERT_EQ((std::vector<std::string>{"a", "b"}), body["input"].get<std::vector<std::string>>());
    ASSERT_FALSE(body.contains("output_dimension"));

    body = VoyageEmbedder::build_request(*spec, {"a"}, true, true, 512);
    ASSERT_EQ("query", body["input_type"]);
    ASSERT_EQ(512, body["output_dimension"]);
}

TEST_F(VoyageEmbedderTest, ContextualizedDocumentsAreAutoChunkedFlatStrings) {
    const auto* spec = VoyageEmbedder::find_model("voyage-context-4");

    auto body = VoyageEmbedder::build_request(*spec, {"doc one", "doc two"}, false, false, 1024);
    ASSERT_EQ("document", body["input_type"]);
    ASSERT_TRUE(body["enable_auto_chunking"].get<bool>());
    ASSERT_EQ(32000, body["chunk_size"]);
    ASSERT_FALSE(body.contains("input"));
    // one string per document, never a list of lists
    ASSERT_TRUE(body["inputs"].is_array());
    ASSERT_TRUE(body["inputs"][0].is_string());
    ASSERT_EQ(2, body["inputs"].size());

    // auto-chunking is rejected by the API for queries
    body = VoyageEmbedder::build_request(*spec, {"my query"}, true, false, 1024);
    ASSERT_EQ("query", body["input_type"]);
    ASSERT_FALSE(body.contains("enable_auto_chunking"));
    ASSERT_FALSE(body.contains("chunk_size"));
    ASSERT_EQ(nlohmann::json::parse(R"([["my query"]])"), body["inputs"]);
}

TEST_F(VoyageEmbedderTest, ResponseKeepsInputOrderByIndex) {
    const auto* spec = VoyageEmbedder::find_model("voyage-4");

    nlohmann::json res = {{"data", {embedding_item(2, 4, 0.3f), embedding_item(0, 4, 0.1f), embedding_item(1, 4, 0.2f)}}};
    auto parsed = VoyageEmbedder::parse_response(*spec, res.dump(), 3, 4);
    ASSERT_TRUE(parsed.ok());
    ASSERT_EQ(3, parsed.get().size());
    ASSERT_FLOAT_EQ(0.1f, parsed.get()[0].embedding[0]);
    ASSERT_FLOAT_EQ(0.2f, parsed.get()[1].embedding[0]);
    ASSERT_FLOAT_EQ(0.3f, parsed.get()[2].embedding[0]);
}

TEST_F(VoyageEmbedderTest, MalformedResponsesAreErrors) {
    const auto* spec = VoyageEmbedder::find_model("voyage-4");

    // not JSON
    ASSERT_FALSE(VoyageEmbedder::parse_response(*spec, "not json", 1, 4).ok());

    // wrong number of results
    nlohmann::json res = {{"data", {embedding_item(0, 4)}}};
    auto parsed = VoyageEmbedder::parse_response(*spec, res.dump(), 2, 4);
    ASSERT_FALSE(parsed.ok());
    ASSERT_EQ(500, parsed.code());

    // duplicate and out-of-range indexes
    res = {{"data", {embedding_item(0, 4), embedding_item(0, 4)}}};
    ASSERT_FALSE(VoyageEmbedder::parse_response(*spec, res.dump(), 2, 4).ok());
    res = {{"data", {embedding_item(0, 4), embedding_item(5, 4)}}};
    ASSERT_FALSE(VoyageEmbedder::parse_response(*spec, res.dump(), 2, 4).ok());

    // wrong dimension
    res = {{"data", {embedding_item(0, 3)}}};
    parsed = VoyageEmbedder::parse_response(*spec, res.dump(), 1, 4);
    ASSERT_FALSE(parsed.ok());
    ASSERT_EQ("Got malformed response from Voyage API: expected 4 dimensions, got 3.", parsed.error());

    // missing embedding
    res = {{"data", {{{"index", 0}}}}};
    ASSERT_FALSE(VoyageEmbedder::parse_response(*spec, res.dump(), 1, 4).ok());

    // an error body is not a result
    ASSERT_FALSE(VoyageEmbedder::parse_response(*spec, R"({"detail": "bad"})", 1, 4).ok());
}

TEST_F(VoyageEmbedderTest, ContextualizedResponseMustHaveExactlyOneChunkPerInput) {
    const auto* spec = VoyageEmbedder::find_model("voyage-context-3");

    nlohmann::json res = {{"data", {chunked_item(1, 1, 4), chunked_item(0, 1, 4)}}};
    auto parsed = VoyageEmbedder::parse_response(*spec, res.dump(), 2, 4);
    ASSERT_TRUE(parsed.ok());
    ASSERT_EQ(2, parsed.get().size());

    // a document that the API split into several chunks cannot become one vector, but only it fails
    res = {{"data", {chunked_item(0, 1, 4), chunked_item(1, 2, 4), chunked_item(2, 1, 4)}}};
    parsed = VoyageEmbedder::parse_response(*spec, res.dump(), 3, 4);
    ASSERT_TRUE(parsed.ok());
    ASSERT_TRUE(parsed.get()[0].success);
    ASSERT_FALSE(parsed.get()[1].success);
    ASSERT_EQ(500, parsed.get()[1].status_code);
    ASSERT_EQ("Voyage API returned 2 chunks for input 1, expected exactly one.", parsed.get()[1].error["error"]);
    ASSERT_TRUE(parsed.get()[2].success);

    res = {{"data", {chunked_item(0, 0, 4)}}};
    parsed = VoyageEmbedder::parse_response(*spec, res.dump(), 1, 4);
    ASSERT_TRUE(parsed.ok());
    ASSERT_FALSE(parsed.get()[0].success);

    // the flat embeddings shape is not a contextualized response
    res = {{"data", {embedding_item(0, 4)}}};
    ASSERT_FALSE(VoyageEmbedder::parse_response(*spec, res.dump(), 1, 4).ok());
}

TEST_F(VoyageEmbedderTest, BatchesRespectInputLimitAndKeepOrder) {
    auto batches = VoyageEmbedder::split_batches(2500, 1000);
    ASSERT_EQ(3, batches.size());
    ASSERT_EQ(std::make_pair(size_t(0), size_t(1000)), batches[0]);
    ASSERT_EQ(std::make_pair(size_t(1000), size_t(2000)), batches[1]);
    ASSERT_EQ(std::make_pair(size_t(2000), size_t(2500)), batches[2]);

    // a configured batch size above the Voyage limit is capped
    batches = VoyageEmbedder::split_batches(1001, 5000);
    ASSERT_EQ(2, batches.size());
    ASSERT_EQ(std::make_pair(size_t(0), size_t(1000)), batches[0]);
    ASSERT_EQ(std::make_pair(size_t(1000), size_t(1001)), batches[1]);

    // a smaller configured batch size is kept
    batches = VoyageEmbedder::split_batches(5, 2);
    ASSERT_EQ(3, batches.size());
    ASSERT_EQ(std::make_pair(size_t(4), size_t(5)), batches[2]);

    // no input, no request; a zero batch size cannot loop forever
    ASSERT_TRUE(VoyageEmbedder::split_batches(0, 200).empty());
    ASSERT_EQ(3, VoyageEmbedder::split_batches(3, 0).size());
}

TEST_F(VoyageEmbedderTest, ErrorDetailComesFromVoyageErrorBody) {
    ASSERT_EQ("Model nope is not supported.", VoyageEmbedder::get_error_detail(R"({"detail": "Model nope is not supported."})"));
    // not the OpenAI `error.message` shape and not JSON: the raw body is kept
    ASSERT_EQ(R"({"error": {"message": "x"}})", VoyageEmbedder::get_error_detail(R"({"error": {"message": "x"}})"));
    ASSERT_EQ("no response body", VoyageEmbedder::get_error_detail(""));
    ASSERT_EQ("<html>bad gateway</html>", VoyageEmbedder::get_error_detail("<html>bad gateway</html>"));
}
