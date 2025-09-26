#pragma once

#include <shared_mutex>
#include <map>
#include <string>
#include <json.hpp>
#include "store.h"
#include "option.h"
#include "curation.h"

class CurationIndex {
private:
    mutable std::shared_mutex mutex;
    Store* store = nullptr;
    // Keyed by curation id to ensure lexicographic ordering
    std::map<std::string, curation_t> curation_definitions;
    std::string name;
public:
    static constexpr const char* COLLECTION_CURATION_SET_PREFIX = "$OI";
    static constexpr const char* OLD_COLLECTION_OVERRIDE_PREFIX = "$CO";

    explicit CurationIndex(Store* store, const std::string& name): store(store), name(name) {}
    ~CurationIndex() = default;

    CurationIndex(const CurationIndex&) = delete;
    CurationIndex& operator=(const CurationIndex&) = delete;

    CurationIndex(CurationIndex&& other) noexcept { swap(*this, other); }
    CurationIndex& operator=(CurationIndex&& other) noexcept { swap(*this, other); return *this; }

    static std::string get_curation_key(const std::string& index_name, const std::string& curation_id) {
        return std::string(COLLECTION_CURATION_SET_PREFIX) + "_" + index_name + "_" + curation_id;
    }

    Option<std::map<std::string, curation_t*>> get_curations(uint32_t limit=0, uint32_t offset=0);
    bool get_curation(const std::string& id, curation_t& ov);
    Option<bool> add_curation(const curation_t& ov, bool write_to_store = true);
    Option<bool> remove_curation(const std::string& id);
    nlohmann::json to_view_json() const;

    friend void swap(CurationIndex& first, CurationIndex& second) noexcept {
        using std::swap;
        swap(first.store, second.store);
        swap(first.curation_definitions, second.curation_definitions);
        swap(first.name, second.name);
    }
};


