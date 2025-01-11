#include "xtr_function.h"
#include "logger.h"
#include <thread>
#include <algorithm>
#include <mutex>

float similarity_function(const uint64_t& doc_token, const uint64_t& query_token) {
   auto xor_result = doc_token ^ query_token;
   float res =  ((float)((64 - 2 * __builtin_popcountll(xor_result)))) / 64.0;
    return res;
}

std::vector<float> get_missing_input_similarities(const std::unordered_map<int, std::unordered_map<XTR_token_t, float, XTR_token_hash>>& top_k) {
    std::vector<float> similarities(top_k.size(), 100.0);
    for(auto& pair : top_k) {
        for(auto& doc : pair.second) {
            similarities[pair.first] = std::min(similarities[pair.first], doc.second);
        }
    }
    return similarities;
}

std::mutex mutex_topk;

// Brute force search
std::unordered_map<int, std::unordered_map<XTR_token_t, float, XTR_token_hash>> get_top_k(const std::vector<XTR_token_t>& doc_tokens, const std::vector<uint64_t>& query_tokens, const int& k_prime) {
    std::unordered_map<int, std::unordered_map<XTR_token_t, float, XTR_token_hash>> top_k;

    auto find_top_k = [&top_k](const uint64_t& query_token, const std::vector<XTR_token_t>& doc_tokens, const int& k_prime, const int& i) {
        std::priority_queue<std::pair<int,float>, std::vector<std::pair<int,float>>, Compare> pq;
        for(int j = 0; j < doc_tokens.size(); j++) {
            float score = similarity_function(doc_tokens[j].token, query_token);
            pq.push({j, score});
            if(pq.size() > k_prime) {
                pq.pop();
            }
        }
        while(!pq.empty()) {
            auto pair = pq.top();
            pq.pop();
            std::unique_lock<std::mutex> lock(mutex_topk);
            top_k[i][doc_tokens[pair.first]] = pair.second;
            lock.unlock();
        }
    };

    auto max_num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    for(int i = 0; i < query_tokens.size(); i++) {
        threads.push_back(std::thread(find_top_k, query_tokens[i], doc_tokens, k_prime, i));
        if(threads.size() == max_num_threads) {
            for(auto& thread : threads) {
                thread.join();
            }
            threads.clear();
        }
    }

    for(auto& thread : threads) {
        thread.join();
    }
    return top_k;
}


// map < query_token, map<doc_token, pair<doc_id, score>>>
std::vector<std::pair<int,float>> search(const std::unordered_map<int, std::unordered_map<XTR_token_t, float, XTR_token_hash>>& mapped_top_k) {
    std::unordered_map<int, std::unordered_map<int, float>> did2scores;
    for(auto& q_token_doc : mapped_top_k) {
        for(auto& doc : q_token_doc.second) {
            if(did2scores[doc.first.doc_id].find(q_token_doc.first) == did2scores[doc.first.doc_id].end()) {
                did2scores[doc.first.doc_id][q_token_doc.first] = doc.second;
            } else {
                did2scores[doc.first.doc_id][q_token_doc.first] = std::max(did2scores[doc.first.doc_id][q_token_doc.first], doc.second);
            }
        }
    }

    auto missing_input_similarities = get_missing_input_similarities(mapped_top_k);

    for(auto& doc : did2scores) {
        for(auto& q : mapped_top_k) {
            if(doc.second.find(q.first) == doc.second.end()) {
                did2scores[doc.first][q.first] = missing_input_similarities[q.first];
            }
        }

    }


    std::priority_queue<std::pair<int,float>, std::vector<std::pair<int,float>>, Compare> pq;
    for(auto& doc : did2scores) {
        float score = 0;
        for(auto& q : doc.second) {
            score += q.second;
        }
        pq.push({doc.first, score});
    }

    std::vector<std::pair<int,float>> search_results;
    while(!pq.empty()) {
        search_results.push_back(pq.top());
        pq.pop();
    }

    // reverse the order
    std::reverse(search_results.begin(), search_results.end());

    return search_results;
}
