#include "http_data.h"

std::string route_path::_get_action() {
    // `resource:operation` forms an action
    // operations: create, get, list, delete, search, import, export

    std::string resource_path;
    std::string operation;
    size_t identifier_index = 0;

    for(size_t i = 0; i < path_parts.size(); i++) {
        if(i == 0 && path_parts.size() > 2 && path_parts[i] == "collections") {
            // sub-resource of a collection, e.g. /collections/:name/overrides should be treated as
            // top-level resource to maintain backward compatibility
            continue;
        }

        if(path_parts[i][0] == ':') {
            identifier_index = i;
        } else if(resource_path.empty()){
            resource_path = path_parts[i];
        } else {
            resource_path = resource_path + "/" + path_parts[i];
        }
    }

    // special cases to maintain semantics and backward compatibility
    if(resource_path == "multi_search" || resource_path == "documents/search") {
        return "documents:search";
    }

    if(resource_path == "documents/import" || resource_path == "documents/export") {
        StringUtils::replace_all(resource_path, "documents/", "");
        return "documents:" + resource_path;
    }

    // e.g /collections or /collections/:collection/foo or /collections/:collection

    if(http_method == "GET") {
        // GET can be a `get` or `list`
        operation = (identifier_index != 0) ? "get" : "list";
    } else if(http_method == "POST") {
        operation = "create";
    } else if(http_method == "PUT") {
        operation = "upsert";
    } else if(http_method == "DELETE") {
        operation = "delete";
    } else if(http_method == "PATCH") {
        operation = "update";
    } else {
        operation = "unknown";
    }

    return resource_path + ":" + operation;
}
