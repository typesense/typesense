#pragma once

#include "json.hpp"
#include "option.h"

class EventManager {
private:
    EventManager() = default;

    ~EventManager() = default;

    static constexpr char* EVENT_TYPE = "type";
    static constexpr char* EVENT_DATA = "data";
    static constexpr char* EVENT_NAME = "name";

public:
    static EventManager& get_instance() {
        static EventManager instance;
        return instance;
    }

    EventManager(EventManager const&) = delete;
    void operator=(EventManager const&) = delete;

    Option<bool> add_event(const nlohmann::json& event, const std::string& ip);

};