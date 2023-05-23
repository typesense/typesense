#include <analytics_manager.h>
#include "event_manager.h"

bool EventManager::add_event(const nlohmann::json& event) {
    /*
        Sample event payload:

        {
            "type": "search",
            "data": {
                "q": "Nike shoes",
                "collections": ["products"]
            }
        }
    */

    if(!event.contains("type")) {
        return false;
    }

    const auto& event_type_val = event[EVENT_TYPE];

    if(event_type_val.is_string()) {
        const std::string& event_type = event_type_val.get<std::string>();
        if(event_type == "search") {
            if(!event.contains("data")) {
                return false;
            }

            const auto& event_data_val = event[EVENT_DATA];

            if(!event_data_val.is_object()) {
                return false;
            }

            const auto& event_data_query_it = event_data_val["q"];

            if(!event_data_query_it.is_string() || !event_data_val["collections"].is_array()) {
                return false;
            }

            for(const auto& coll: event_data_val["collections"]) {
                if(!coll.is_string()) {
                    return false;
                }

                std::string query = event_data_query_it.get<std::string>();
                AnalyticsManager::get_instance().add_suggestion(coll.get<std::string>(), query, false, "");
            }
        }
    }

    return true;
}
