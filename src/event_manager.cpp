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
                return Option<bool>(400, "data is not object.");
            }

            if(event_type == AnalyticsManager::SEARCH_EVENT) {
                if(!event_data_val.contains("user_id") || !event_data_val["user_id"].is_string()) {
                    return Option<bool>(400,
                                        "search event json data fields should contain `user_id` as string value.");
                }

                if(!event_data_val.contains("q") || !event_data_val["q"].is_string()) {
                    return Option<bool>(400,
                                        "search event json data fields should contain `q` as string value.");
                }
            } else {
                // Check that either doc_id or doc_ids is present, but not both
                bool has_doc_id = event_data_val.contains("doc_id");
                bool has_doc_ids = event_data_val.contains("doc_ids");
                
                if(!has_doc_id && !has_doc_ids) {
                    return Option<bool>(400, "Event must contain either 'doc_id' or 'doc_ids' field.");
                }
                
                if(has_doc_id && has_doc_ids) {
                    return Option<bool>(400, "Event cannot contain both 'doc_id' and 'doc_ids' fields.");
                }

                if(has_doc_id && !event_data_val["doc_id"].is_string()) {
                    return Option<bool>(400, "Event's 'doc_id' must be a string value.");
                }

                if(has_doc_ids) {
                    if(!event_data_val["doc_ids"].is_array()) {
                        return Option<bool>(400, "Event's 'doc_ids' must be an array.");
                    }
                    
                    for(const auto& doc_id: event_data_val["doc_ids"]) {
                        if(!doc_id.is_string()) {
                            return Option<bool>(400, "All values in 'doc_ids' must be strings.");
                        }
                    }
                }

                if(event_data_val.contains("collection") && !event_data_val["collection"].is_string()) {
                    return Option<bool>(400, "'collection' should be a string value.");
                }

                if(!event_data_val.contains("user_id") || !event_data_val["user_id"].is_string()) {
                    return Option<bool>(400, "event should have 'user_id' as string value.");
                }

                if(event_data_val.contains("q") && !event_data_val["q"].is_string()) {
                    return Option<bool>(400, "'q' should be a string value.");
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
        return Option<bool>(400, "`event_type` value should be string.");
    }

    return Option(true);
}
