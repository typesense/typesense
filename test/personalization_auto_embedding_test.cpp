#include <gtest/gtest.h>
#include "collection.h"
#include "collection_manager.h"
#include "personalization_model.h"
#include "field.h"
#include "personalization_model_manager.h"
#include <filesystem>
#include "json.hpp"

class PersonalizationAutoEmbeddingTest : public ::testing::Test {
protected:
    std::string temp_dir;
    Store *store;
    CollectionManager& collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;
    std::string model_id = "test_model";

    void SetUp() override {
        temp_dir = (std::filesystem::temp_directory_path() / "personalization_auto_embedding_test").string();
        system(("rm -rf " + temp_dir + " && mkdir -p " + temp_dir).c_str());

        // Setup model directory
        std::string test_dir = "/tmp/typesense_test/personalization_auto_embedding_test/models";
        system(("rm -rf " + test_dir + " && mkdir -p " + test_dir).c_str());
        EmbedderManager::set_model_dir(test_dir);

        // Create test collection
        std::string state_dir_path = "/tmp/typesense_test/personalization_auto_embedding_test";
        Config::get_instance().set_data_dir(state_dir_path);

        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf " + state_dir_path + " && mkdir -p " + state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
    }

    void TearDown() override {
        std::string test_dir = "/tmp/typesense_test";
        system(("rm -rf " + test_dir).c_str());
        collectionManager.dispose();
        PersonalizationModelManager::dispose();
        delete store;
    }
};

TEST_F(PersonalizationAutoEmbeddingTest, TestAutoEmbeddingFields) {
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
    ASSERT_TRUE(add_result.ok());

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

    auto create_op = collectionManager.create_collection(collection_schema);
    ASSERT_TRUE(create_op.ok());
    Collection *collection = collectionManager.get_collection("movies").get();

    auto fields = collection->get_fields();
    bool has_user_embedding = false;
    bool has_item_embedding = false;
    
    for (const auto& field : fields) {
        if (field.name == "user_embedding") {
            has_user_embedding = true;
            ASSERT_EQ(field.embed["model_config"]["personalization_model_id"], model_id);
            ASSERT_EQ(field.embed["model_config"]["personalization_embedding_type"], "user");
        }
        if (field.name == "item_embedding") {
            has_item_embedding = true;
            ASSERT_EQ(field.embed["model_config"]["personalization_model_id"], model_id);
            ASSERT_EQ(field.embed["model_config"]["personalization_embedding_type"], "item");
        }
    }
    
    ASSERT_TRUE(has_user_embedding);
    ASSERT_TRUE(has_item_embedding);

    nlohmann::json document;
    std::vector<std::string> json_lines = {
        R"({"title": "Inception", "description": "A thief who steals corporate secrets through the use of dream-sharing technology is given the inverse task of planting an idea into the mind of a CEO.", "genres": ["Action", "Adventure", "Sci-Fi"], "cast": ["Leonardo DiCaprio", "Joseph Gordon-Levitt", "Ellen Page"], "directors": ["Christopher Nolan"], "id": "0"})",
        R"({"title": "The Dark Knight", "description": "When the menace known as the Joker wreaks havoc and chaos on the people of Gotham, Batman must accept one of the greatest psychological and physical tests of his ability to fight injustice.", "genres": ["Action", "Crime", "Drama"], "cast": ["Christian Bale", "Heath Ledger", "Aaron Eckhart"], "directors": ["Christopher Nolan"], "id": "1"})"
    };
    auto import_op = collection->add_many(json_lines, document, CREATE);
    ASSERT_EQ(import_op["success"], true);
    ASSERT_EQ(import_op["num_imported"], 2);

    auto id0_op = collection->doc_id_to_seq_id("0");
    ASSERT_TRUE(id0_op.ok());
    auto id1_op = collection->doc_id_to_seq_id("1");
    ASSERT_TRUE(id1_op.ok());

    nlohmann::json document0, document1;
    auto doc_op = collection->get_document_from_store(id0_op.get(), document0);
    ASSERT_TRUE(doc_op.ok());
    ASSERT_EQ(document0["user_embedding"].size(), 256);
    ASSERT_EQ(document0["item_embedding"].size(), 256);

    doc_op = collection->get_document_from_store(id1_op.get(), document1);
    ASSERT_TRUE(doc_op.ok());
    ASSERT_EQ(document1["user_embedding"].size(), 256);
    ASSERT_EQ(document1["item_embedding"].size(), 256);

    std::vector<std::string> json_lines_updated = {
        R"({"title": "Changed Title", "id": "0"})",
        R"({"title": "Changed Title", "id": "1"})"
    };

    auto import_op_updated = collection->add_many(json_lines_updated, document, UPDATE);
    ASSERT_EQ(import_op_updated["success"], true);
    ASSERT_EQ(import_op_updated["num_imported"], 2);

    auto id0_op_updated = collection->doc_id_to_seq_id("0");
    ASSERT_TRUE(id0_op_updated.ok());
    auto id1_op_updated = collection->doc_id_to_seq_id("1");
    ASSERT_TRUE(id1_op_updated.ok());

    nlohmann::json document0_updated, document1_updated;
    auto doc_op_updated = collection->get_document_from_store(id0_op_updated.get(), document0_updated);

    ASSERT_TRUE(doc_op_updated.ok());
    ASSERT_EQ(document0_updated["user_embedding"].size(), 256);
    ASSERT_EQ(document0_updated["item_embedding"].size(), 256);

    doc_op_updated = collection->get_document_from_store(id1_op_updated.get(), document1_updated);
    ASSERT_TRUE(doc_op_updated.ok());
    ASSERT_EQ(document1_updated["user_embedding"].size(), 256);
    ASSERT_EQ(document1_updated["item_embedding"].size(), 256);
    
    bool user_embeddings_different = false;
    for (size_t i = 0; i < document0["user_embedding"].size(); i++) {
        if (document0["user_embedding"][i] != document0_updated["user_embedding"][i]) {
            user_embeddings_different = true;
            break;
        }
    }
    ASSERT_TRUE(user_embeddings_different) << "User embeddings should be different after update";

    // Compare item embeddings
    bool item_embeddings_different = false;
    for (size_t i = 0; i < document0["item_embedding"].size(); i++) {
        if (document0["item_embedding"][i] != document0_updated["item_embedding"][i]) {
            item_embeddings_different = true;
            break;
        }
    }
    ASSERT_TRUE(item_embeddings_different) << "Item embeddings should be different after update";

    user_embeddings_different = false;
    for (size_t i = 0; i < document1["user_embedding"].size(); i++) {
        if (document1["user_embedding"][i] != document1_updated["user_embedding"][i]) {
            user_embeddings_different = true;
            break;
        }
    }
    ASSERT_TRUE(user_embeddings_different) << "User embeddings for document 1 should be different after update";

    item_embeddings_different = false;
    for (size_t i = 0; i < document1["item_embedding"].size(); i++) {
        if (document1["item_embedding"][i] != document1_updated["item_embedding"][i]) {
            item_embeddings_different = true;
            break;
        }
    }
    ASSERT_TRUE(item_embeddings_different) << "Item embeddings for document 1 should be different after update";
}
