#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <cstdint>
#include <queue>

class Compare {
    public:
        bool operator()(const std::pair<int,float>& a, const std::pair<int,float>& b) {
            return a.second >= b.second;
        }
};


struct XTR_token_t {
    uint64_t token;
    int doc_id;
    int token_id;

    bool operator==(const XTR_token_t& other) const {
        return token_id == other.token_id;
    }
};

// hash function for XTR_token_t
struct XTR_token_hash {
    std::size_t operator()(const XTR_token_t& token) const {
        // hash token_id
        return std::hash<int>{}(token.token_id);
    }
};


// Returns the similarity score between two tokens
float similarity_function(const uint64_t& doc_token, const uint64_t& query_token);

// Returns the similarity score for each query token
std::vector<float> get_missing_input_similarities(const std::unordered_map<int, std::unordered_map<XTR_token_t, float, XTR_token_hash>>& top_k);

// Returns the top k documents with the highest similarity scores to the each query token
std::unordered_map<int, std::unordered_map<XTR_token_t, float, XTR_token_hash>> get_top_k(const std::vector<XTR_token_t>& doc_tokens, const std::vector<uint64_t>& query_tokens, const int& k_prime);

// Calculate search results
std::vector<std::pair<int,float>> search(const std::unordered_map<int, std::unordered_map<XTR_token_t, float, XTR_token_hash>>& mapped_top_k);