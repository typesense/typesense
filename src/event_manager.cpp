#include <query_suggestions.h>
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
                QuerySuggestions::get_instance().add_suggestion(coll.get<std::string>(), query, false, "");
            }
        }
    }

    return true;
}

Option<uint32_t> EventManager::create_sink(const nlohmann::json& sink_config, bool write_to_disk) {
    if(!sink_config.contains("name") || !sink_config["name"].is_string()) {
        return Option<uint32_t>(400, "Request payload contains invalid name.");
    }

    if(!sink_config.contains("type") || !sink_config["type"].is_string()) {
        return Option<uint32_t>(400, "Request payload contains invalid type.");
    }

    if(!sink_config.contains("source") || !sink_config["source"].is_object()) {
        return Option<uint32_t>(400, "Request payload contains invalid source.");
    }

    if(!sink_config.contains("destination") || !sink_config["destination"].is_object()) {
        return Option<uint32_t>(400, "Request payload contains invalid destination.");
    }

    if(sink_config.contains("type") && sink_config["type"] == QuerySuggestions::SINK_TYPE) {
        QuerySuggestions::get_instance().create_index(sink_config, write_to_disk);
    } else {
        return Option<uint32_t>(400, ("Missing or invalid event sink type."));
    }

    return Option<uint32_t>(200);
}

Option<bool> EventManager::remove_sink(const std::string& name) {
    return QuerySuggestions::get_instance().remove_suggestion_index(name);
}
