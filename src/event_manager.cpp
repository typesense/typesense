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
                std::string query = event_data_query_it.get<std::string>();
                AnalyticsManager::get_instance().add_suggestion(coll.get<std::string>(), query, false, "");
            }
        } else if(event_type == "query_click") {
            if (!event.contains("data")) {
                return Option<bool>(404, "key `data` not found.");
            }

            const auto &event_data_val = event[EVENT_DATA];

            if (!event_data_val.is_object()) {
                return Option<bool>(500, "event_data_val is not object.");
            }

            if (!event_data_val.contains("q") || !event_data_val.contains("doc_id") || !event_data_val.contains("user_id")
                || !event_data_val.contains("position") || !event_data_val.contains("collection")) {
                return Option<bool>(500, "event json data fields should contain `q`, `doc_id`, `position`, `user_id`, and `collection`.");
            }

            if (!event_data_val["q"].is_string()) {
                return Option<bool>(500, "`q` value should be string.");
            }

            if(!event_data_val["doc_id"].is_string()) {
                return Option<bool>(500, "`doc_id` value should be string.");
            }

            if(!event_data_val["user_id"].is_string()) {
                return Option<bool>(500, "`user_id` value should be string.");
            }

            if(!event_data_val["position"].is_number_unsigned()){
                return Option<bool>(500, "`position` value should be unsigned int.");
            }

            if(!event_data_val["collection"].is_string()) {
                return Option<bool>(500, "`collection` value should be string.");
            }

            const std::string query = event_data_val["q"].get<std::string>();
            const std::string user_id = event_data_val["user_id"].get<std::string>();
            const std::string doc_id = event_data_val["doc_id"].get<std::string>();
            uint64_t position = event_data_val["position"].get<uint64_t>();
            const std::string& collection = event_data_val["collection"].get<std::string>();

            auto op = AnalyticsManager::get_instance().add_click_event(collection, query, user_id, doc_id, position, client_ip);
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
