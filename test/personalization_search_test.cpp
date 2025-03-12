#include <gtest/gtest.h>
#include "collection.h"
#include "collection_manager.h"
#include "personalization_model.h"
#include "field.h"
#include "personalization_model_manager.h"
#include <filesystem>
#include "analytics_manager.h"
#include "json.hpp"

class PersonalizationSearchTest : public ::testing::Test {
protected:
    std::string temp_dir;
    Store *store;
    Store *analytic_store;
    CollectionManager& collectionManager = CollectionManager::get_instance();
    AnalyticsManager& analyticsManager = AnalyticsManager::get_instance();
    std::atomic<bool> quit = false;
    Collection *collection;
    std::string model_id = "test_model";
    uint32_t analytics_minute_rate_limit = 5;

    void SetUp() override {
        temp_dir = (std::filesystem::temp_directory_path() / "personalization_search_test").string();
        system(("rm -rf " + temp_dir + " && mkdir -p " + temp_dir).c_str());

        // Setup model directory
        std::string test_dir = "/tmp/typesense_test/personalization_search_test/models";
        system(("rm -rf " + test_dir + " && mkdir -p " + test_dir).c_str());
        EmbedderManager::set_model_dir(test_dir);

        // Create test collection
        std::string state_dir_path = "/tmp/typesense_test/personalization_search_test/personalization_search_test";
        std::string analytics_dir_path = "/tmp/typesense_test/personalization_search_test/analytics";
        Config::get_instance().set_data_dir(state_dir_path);

        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf " + state_dir_path + " && mkdir -p " + state_dir_path).c_str());
        
        LOG(INFO) << "Truncating and creating: " << analytics_dir_path;
        system(("rm -rf "+ analytics_dir_path +" && mkdir -p "+analytics_dir_path).c_str());
        analytic_store = new Store(analytics_dir_path, 24*60*60, 1024, true, FOURWEEKS_SECS);

        store = new Store(state_dir_path);

        collectionManager.init(store, 1.0, "auth_key", quit);

        analyticsManager.init(store, analytic_store, analytics_minute_rate_limit);
        analyticsManager.resetToggleRateLimit(false);
        PersonalizationModelManager::init(store);
        nlohmann::json model_json = {
            {"id", model_id},
            {"name", "ts/tyrec-1"},
            {"collection", "movies"},
            {"type", "recommendation"}
        };
        std::string archive_name = "test/resources/models.tar.gz";
        std::ifstream archive_file(archive_name, std::ios::binary);
        std::string model_data((std::istreambuf_iterator<char>(archive_file)), std::istreambuf_iterator<char>());
        archive_file.close();
        auto add_result = PersonalizationModelManager::add_model(model_json, model_id, true, model_data);
        ASSERT_EQ(add_result.error(), "");

        nlohmann::json collection_schema = R"({
            "name": "movies",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "description", "type": "string"},
                {"name": "genres", "type": "string[]"},
                {"name": "cast", "type": "string[]"},
                {"name": "directors", "type": "string[]"},
                {"name": "user_embedding", "type": "float[]", "embed": {
                    "from": ["genres", "title", "cast", "directors"],
                    "mapping": ["movie_genres", "movie_title", "actors", "directors"],
                    "model_config": {
                        "model_name": "ts/tyrec-1",
                        "personalization_type": "recommendation",
                        "personalization_model_id": "test_model",
                        "personalization_embedding_type": "user"
                    }
                }},
                {"name": "item_embedding", "type": "float[]", "embed": {
                    "from": ["genres", "title", "cast", "directors"],
                    "mapping": ["movie_genres", "movie_title", "actors", "directors"],
                    "model_config": {
                        "model_name": "ts/tyrec-1",
                        "personalization_type": "recommendation",
                        "personalization_model_id": "test_model",
                        "personalization_embedding_type": "item"
                    }
                }}
            ]
        })"_json;

        auto create_rule_op = collectionManager.create_collection(collection_schema);
        ASSERT_TRUE(create_rule_op.ok());
        collection = collectionManager.get_collection("movies").get();
        nlohmann::json document;
        std::vector<std::string> json_lines = {
            R"({"title": "Inception", "description": "A thief who steals corporate secrets through the use of dream-sharing technology is given the inverse task of planting an idea into the mind of a CEO.", "genres": ["Action", "Adventure", "Sci-Fi"], "cast": ["Leonardo DiCaprio", "Joseph Gordon-Levitt", "Ellen Page"], "directors": ["Christopher Nolan"], "id": "0"})",
            R"({"title": "The Dark Knight", "description": "When the menace known as the Joker wreaks havoc and chaos on the people of Gotham, Batman must accept one of the greatest psychological and physical tests of his ability to fight injustice.", "genres": ["Action", "Crime", "Drama"], "cast": ["Christian Bale", "Heath Ledger", "Aaron Eckhart"], "directors": ["Christopher Nolan"], "id": "1"})"
        };
        auto import_op = collection->add_many(json_lines, document, UPSERT);
        ASSERT_EQ(import_op["success"], true);

        nlohmann::json analytics_rule = R"({
            "name": "personalization_events",
            "type": "log",
            "params": {
                "source": {
                    "collections": ["movies"],
                    "events": [{"type": "click", "name": "test_event"}]
                }
            }
        })"_json;

        auto create_op = analyticsManager.create_rule(analytics_rule, true, true);
        ASSERT_TRUE(create_op.ok());
        // Add events to the event name
        nlohmann::json event1 = R"({
            "type": "click",

            "name": "test_event",
            "data": {
                "doc_id": "0",
                "user_id": "user123"
            }
        })"_json;

        nlohmann::json event2 = R"({
            "type": "click",
            "name": "test_event",
            "data": {
                "doc_id": "1",
                "user_id": "user123"
            }
        })"_json;

        auto add_event_op1 = analyticsManager.add_event("127.0.0.1", "click", "test_event", event1["data"]);
        ASSERT_TRUE(add_event_op1.ok());

        auto add_event_op2 = analyticsManager.add_event("127.0.0.1", "click", "test_event", event2["data"]);
        ASSERT_TRUE(add_event_op2.ok());
    }

    void TearDown() override {
        std::string test_dir = "/tmp/typesense_test";
        system(("rm -rf " + test_dir).c_str());
        collectionManager.dispose();
        PersonalizationModelManager::dispose();
        delete store;
    }
};

TEST_F(PersonalizationSearchTest, ParseAndValidatePersonalizationQuery) {
    vector_query_t vector_query;
    std::string filter_query;
    bool is_wildcard_query = true;
    std::string personalization_user_id = "user123";
    std::string personalization_model_id = "test_model";
    std::string personalization_type = "recommendation";
    std::string personalization_user_field = "user_embedding";
    std::string personalization_item_field = "item_embedding";
    size_t personalization_n_events = 10;
    std::string personalization_event_name = "test_event";

    Option<bool> result = collection->parse_and_validate_personalization_query(
        personalization_user_id,
        personalization_model_id,
        personalization_type,
        personalization_user_field,
        personalization_item_field,
        personalization_n_events,
        personalization_event_name,
        vector_query,
        filter_query,
        is_wildcard_query
    );
    ASSERT_EQ(result.error(), "");
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(vector_query.values.size(), 256);
    ASSERT_EQ(vector_query.field_name, personalization_item_field);
    ASSERT_EQ(filter_query, "id:!=[1,0]");

    result = collection->parse_and_validate_personalization_query(
        "123",
        personalization_model_id,
        personalization_type,
        personalization_user_field,
        personalization_item_field,
        personalization_n_events,
        personalization_event_name,
        vector_query,
        filter_query,
        is_wildcard_query
    );
    ASSERT_EQ(result.error(), "No events found for the user.");
    ASSERT_FALSE(result.ok());

    result = collection->parse_and_validate_personalization_query(
        personalization_user_id,
        personalization_model_id,
        personalization_type,
        personalization_user_field,
        personalization_item_field,
        personalization_n_events,
        "event_doesnt_exist",
        vector_query,
        filter_query,
        is_wildcard_query
    );
    ASSERT_EQ(result.error(), "Analytics event not found");
    ASSERT_FALSE(result.ok());

    result = collection->parse_and_validate_personalization_query(
        personalization_user_id,
        personalization_model_id,
        personalization_type,
        "does_not_exist",
        personalization_item_field,
        personalization_n_events,
        personalization_event_name,
        vector_query,
        filter_query,
        is_wildcard_query
    );
    ASSERT_EQ(result.error(), "Document referenced in event does not contain a valid vector field.");
    ASSERT_FALSE(result.ok());


}
