#pragma once
#include <string>
#include "sparsepp.h"

struct adi_node_t;

class adi_tree_t {

private:

    spp::sparse_hash_map<uint32_t, std::string> id_keys;
    adi_node_t* root = nullptr;

    static void add_node(adi_node_t* node, const std::string& key, size_t key_index);

    static bool rank_aggregate(adi_node_t* node, const std::string& key, size_t key_index, size_t& rank);

    static adi_node_t* get_node(adi_node_t* node, const std::string& key, const size_t key_index,
                                std::vector<adi_node_t*>& path);

    void remove_node(adi_node_t* node, const std::string& key, const size_t key_index);

public:
    static constexpr size_t NOT_FOUND = INT64_MAX;

    adi_tree_t();

    ~adi_tree_t();

    void index(uint32_t id, const std::string& key);

    size_t rank(uint32_t id);

    void remove(uint32_t id);

    const adi_node_t* get_root();
};
