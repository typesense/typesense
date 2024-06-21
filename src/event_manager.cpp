#include <analytics_manager.h>
#include "event_manager.h"

Option<bool> EventManager::add_event(const nlohmann::json& event, const std::string& client_ip) {
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
        return Option<bool>(404, "key `type` not found.");
    }

    const auto& event_type_val = event[EVENT_TYPE];

    if(event_type_val.is_string()) {
        const std::string& event_type = event_type_val.get<std::string>();
        if(event_type == "search") {
            if(!event.contains("data")) {
                return Option<bool>(404, "key `data` not found.");
            }

            const auto& event_data_val = event[EVENT_DATA];

            if(!event_data_val.is_object()) {
                return Option<bool>(500, "event_data_val is not object.");
            }

            const auto& event_data_query_it = event_data_val["q"];

            if(!event_data_query_it.is_string()) {
                return Option<bool>(500, "`q` value should be string.");
            }

            if(!event_data_val["collections"].is_array() || !event_data_val["collections"][0].is_string()) {
                return Option<bool>(500, "`collections` value should be string array.");
            }

            for(const auto& coll: event_data_val["collections"]) {
                const std::string& query = event_data_query_it.get<std::string>();
                AnalyticsManager::get_instance().add_suggestion(coll.get<std::string>(), query, query, false, "");
            }
        } else if(event_type == AnalyticsManager::CLICK_EVENT || event_type == AnalyticsManager::CONVERSION_EVENT
            || event_type == AnalyticsManager::VISIT_EVENT || event_type == AnalyticsManager::CUSTOM_EVENT) {
            if (!event.contains(EVENT_DATA)) {
                return Option<bool>(404, "key `data` not found.");
            }

            const auto &event_data_val = event[EVENT_DATA];

            if (!event.contains(EVENT_NAME)) {
                return Option<bool>(404, "key `name` not found.");
            }
            const auto &event_name = event[EVENT_NAME];
            if (!event_data_val.is_object()) {
                return Option<bool>(500, "event_data_val is not object.");
            }

            if(event_type != AnalyticsManager::CUSTOM_EVENT) {
                if (!event_data_val.contains("doc_id") || !event_data_val.contains("user_id")) {
                    return Option<bool>(500,
                                        "event json data fields should contain `doc_id`, `user_id`.");
                }

                if (!event_data_val["doc_id"].is_string()) {
                    return Option<bool>(500, "`doc_id` value should be string.");
                }

                if (!event_data_val["user_id"].is_string()) {
                    return Option<bool>(500, "`user_id` value should be string.");
                }
            }

            auto op = AnalyticsManager::get_instance().add_event(client_ip, event_type, event_name, event_data_val);
            if(!op.ok()) {
                return Option<bool>(op.code(), op.error());
            }
        } else {
            return Option<bool>(404, "event_type " + event_type + " not found.");
        }
    } else {
        return Option<bool>(500, "`event_type` value should be string.");
    }

    return Option(true);
}
