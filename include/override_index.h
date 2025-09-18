#pragma once

#include <shared_mutex>
#include <map>
#include <string>
#include <json.hpp>
#include "store.h"
#include "option.h"
#include "override.h"

class OverrideIndex {
private:
    mutable std::shared_mutex mutex;
    Store* store = nullptr;
    // Keyed by override id to ensure lexicographic ordering
    std::map<std::string, override_t> override_definitions;
    std::string name;
public:
    static constexpr const char* COLLECTION_OVERRIDE_SET_PREFIX = "$OI";
    static constexpr const char* OLD_COLLECTION_OVERRIDE_PREFIX = "$CO";

    explicit OverrideIndex(Store* store, const std::string& name): store(store), name(name) {}
    ~OverrideIndex() = default;

    OverrideIndex(const OverrideIndex&) = delete;
    OverrideIndex& operator=(const OverrideIndex&) = delete;

    OverrideIndex(OverrideIndex&& other) noexcept { swap(*this, other); }
    OverrideIndex& operator=(OverrideIndex&& other) noexcept { swap(*this, other); return *this; }

    static std::string get_override_key(const std::string& index_name, const std::string& override_id) {
        return std::string(COLLECTION_OVERRIDE_SET_PREFIX) + "_" + index_name + "_" + override_id;
    }

    Option<std::map<std::string, override_t*>> get_overrides(uint32_t limit=0, uint32_t offset=0);
    bool get_override(const std::string& id, override_t& ov);
    Option<bool> add_override(const override_t& ov, bool write_to_store = true);
    Option<bool> remove_override(const std::string& id);
    nlohmann::json to_view_json() const;

    friend void swap(OverrideIndex& first, OverrideIndex& second) noexcept {
        using std::swap;
        swap(first.store, second.store);
        swap(first.override_definitions, second.override_definitions);
        swap(first.name, second.name);
    }
};


