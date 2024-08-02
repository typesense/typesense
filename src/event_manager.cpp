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
        if(event_type == AnalyticsManager::CLICK_EVENT || event_type == AnalyticsManager::CONVERSION_EVENT
           || event_type == AnalyticsManager::VISIT_EVENT || event_type == AnalyticsManager::CUSTOM_EVENT
           || event_type == AnalyticsManager::SEARCH_EVENT) {

            if(!event.contains(EVENT_DATA)) {
                return Option<bool>(404, "key `data` not found.");
            }

            const auto& event_data_val = event[EVENT_DATA];

            if(!event.contains(EVENT_NAME)) {
                return Option<bool>(404, "key `name` not found.");
            }
            const auto& event_name = event[EVENT_NAME];
            if(!event_data_val.is_object()) {
                return Option<bool>(500, "event_data_val is not object.");
            }

            if(event_type == AnalyticsManager::SEARCH_EVENT) {
                if(!event_data_val.contains("user_id") || !event_data_val["user_id"].is_string()) {
                    return Option<bool>(500,
                                        "search event json data fields should contain `user_id` as string value.");
                }

                if(!event_data_val.contains("q") || !event_data_val["q"].is_string()) {
                    return Option<bool>(500,
                                        "search event json data fields should contain `q` as string value.");
                }
            } else {
                if(!event_data_val.contains("doc_id") || !event_data_val["doc_id"].is_string()) {
                    return Option<bool>(500, "event should have 'doc_id' as string value.");
                }

                if(event_data_val.contains("user_id") && !event_data_val["user_id"].is_string()) {
                    return Option<bool>(500, "'user_id' should be a string value.");
                }

                if(event_data_val.contains("q") && !event_data_val["q"].is_string()) {
                    return Option<bool>(500, "'q' should be a string value.");
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
