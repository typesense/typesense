#include <cstdint>
#include <vector>
#include "adi_tree.h"
#include "logger.h"

struct adi_node_t {
    uint16_t num_children;
    uint32_t scions;
    char* chars;
    adi_node_t** children;

    adi_node_t(): scions(0), num_children(0), chars(nullptr), children(nullptr) {}

    ~adi_node_t() {
        //LOG(INFO) << "~adi_node: " << this;
        //nodes.erase(this);

        delete [] chars;
        chars = nullptr;

        for(size_t i = 0; i < num_children; i++) {
            delete children[i];
        }
        delete [] children;
        children = nullptr;
        num_children = 0;
    }
};

adi_tree_t::adi_tree_t() {
    root = new adi_node_t();
}

void adi_tree_t::add_node(adi_node_t* node, const std::string& key, const size_t key_index) {
    char c = (key_index == key.size()) ? '\0' : key[key_index];

    // find the slot for `c`
    size_t slot = 0;
    bool found_char = false;

    while(slot < node->num_children) {
        if(node->chars[slot] == c) {
            found_char = true;
            break;
        }

        if(node->chars[slot] > c) {
            break;
        }

        slot++;
    }

    if(!found_char) {
        adi_node_t** new_children = new adi_node_t*[node->num_children+1];
        char* new_chars = new char[node->num_children+1];

        for(size_t i = 0; i < slot; i++) {
            new_children[i] = node->children[i];
            new_chars[i] = node->chars[i];
        }

        new_children[slot] = new adi_node_t();

        /*LOG(INFO) << "new node: " << new_children[slot] << ", slot: " << slot
                  << ", parent node: " << node << ", key: " << key
                  << ", char: " << int(c) << ", node->num_children: " << node->num_children;*/

        //nodes.insert(new_children[slot]);
        new_chars[slot] = c;

        size_t new_index = slot+1;

        for(size_t i = slot; i < node->num_children; i++) {
            new_children[new_index] = node->children[i];
            new_chars[new_index] = node->chars[i];
            new_index++;
        }

        delete [] node->chars;
        delete [] node->children;

        node->chars = new_chars;
        node->children = new_children;

        node->num_children++;
    }

    node->scions++;

    if(c != '\0') {
        add_node(node->children[slot], key, key_index+1);
    } else {
        node->children[slot]->scions++;
    }
}

void adi_tree_t::index(const uint32_t id, const std::string& key) {
    const auto& id_keys_it = id_keys.find(id);
    if(id_keys_it != id_keys.end()) {
        return ;
    }

    if(key.empty()) {
        return;
    }

    if(root == nullptr) {
        root = new adi_node_t();
    }

    add_node(root, key, 0);
    id_keys.emplace(id, key);
}

bool adi_tree_t::rank_aggregate(adi_node_t* node, const std::string& key, const size_t key_index, size_t& rank) {
    char c = (key_index == key.size()) ? '\0' : key[key_index];

    for(size_t i = 0; i < node->num_children; i++) {
        if(node->chars[i] == '\0' && c == '\0') {
            rank += 1;
            return true;
        }

        if(node->chars[i] == '\0') {
            rank += 1;
        } else if(node->chars[i] != c) {
            rank += node->children[i]->scions;
        } else {
            return rank_aggregate(node->children[i], key, key_index+1, rank);
        }
    }

    return false;
}

size_t adi_tree_t::rank(uint32_t id) {
    const auto& id_keys_it = id_keys.find(id);

    if(id_keys_it == id_keys.end()) {
        return NOT_FOUND;
    }

    const std::string& key = id_keys_it->second;
    size_t rank = 0;
    bool found = rank_aggregate(root, key, 0, rank);

    return found ? rank : NOT_FOUND;
}

adi_node_t* adi_tree_t::get_node(adi_node_t* node, const std::string& key, const size_t key_index,
                                 std::vector<adi_node_t*>& path) {
    if(key_index == key.size()) {
        // still push the null node
        if(node->num_children >= 1 && node->chars[0] == '\0') {
            path.push_back(node);
            path.push_back(node->children[0]);
            return node;
        }

        return nullptr;
    }

    for(size_t i = 0; i < node->num_children; i++) {
        if(node->chars[i] == key[key_index]) {
            path.push_back(node);
            return get_node(node->children[i], key, key_index+1, path);
        }
    }

    return nullptr;
}

// assumes that node already exists
void adi_tree_t::remove_node(adi_node_t* node, const std::string& key, const size_t key_index) {
    char c = (key_index == key.size()) ? '\0' : key[key_index];

    for(size_t i = 0; i < node->num_children; i++) {
        if(node->chars[i] == c) {
            if(node->children[i]->scions > 1) {
                // skip to next character in trie as this character is shared by other entries
                node->scions--;
                if(c != '\0') {
                    remove_node(node->children[i], key, key_index+1);
                } else {
                    node->children[i]->scions--;
                }
            } else {
                if(node->num_children == 1) {
                    // solo child, we will have to delete the node itself

                    if(c != '\0') {
                        remove_node(node->children[i], key, key_index+1);
                        node->children[i] = nullptr;
                    }

                    if(root == node) {
                        delete root;
                        root = nullptr;
                    } else {
                        delete node;
                    }
                } else {
                    // delete the char from node

                    adi_node_t** new_children = new adi_node_t*[node->num_children-1];
                    char* new_chars = new char[node->num_children-1];

                    for(size_t j = 0; j < node->num_children; j++) {
                        if(j == i) {
                            continue;
                        }
                        size_t index = (j < i) ? j : j-1;
                        new_children[index] = node->children[j];
                        new_chars[index] = node->chars[j];
                    }

                    if(c != '\0') {
                        remove_node(node->children[i], key, key_index+1);
                    } else {
                        delete node->children[i];
                    }

                    delete [] node->chars;
                    delete [] node->children;

                    node->children = new_children;
                    node->chars = new_chars;

                    node->scions--;
                    node->num_children--;
                }
            }

            break;
        }
    }
}

void adi_tree_t::remove(uint32_t id) {
    const auto& id_keys_it = id_keys.find(id);

    if(id_keys_it == id_keys.end()) {
        return ;
    }

    const std::string& key = id_keys_it->second;
    std::vector<adi_node_t*> path;
    auto leaf_node = get_node(root, key, 0, path);

    //LOG(INFO) << "Removing key: " << key << ", seq_id: " << id << ", id_keys.size: " << id_keys.size()
    //          << ", root.num_children: " << root->num_children;

    if(leaf_node != nullptr) {
        remove_node(root, key, 0);
    }

    id_keys.erase(id);
}

adi_tree_t::~adi_tree_t() {
    std::vector<uint32_t> ids;

    //LOG(INFO) << "ROOT: " << root;

    for(auto& id_key: id_keys) {
        ids.push_back(id_key.first);
    }

    for(auto id: ids) {
        remove(id);
    }

    //LOG(INFO) << "tree destructor, deleting root: " << root;
    delete root;

    //LOG(INFO) << "nodes.size: " << nodes.size();
    //auto missing_node = *nodes.begin();
    //LOG(INFO) << "missing node: " << missing_node;
}

const adi_node_t* adi_tree_t::get_root() {
    return root;
}
