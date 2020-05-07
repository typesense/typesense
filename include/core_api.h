#pragma once

#include "http_server.h"

bool handle_authentication(http_req& req, const route_path& rpath, const std::string& auth_key);

bool get_collections(http_req & req, http_res & res);

bool post_create_collection(http_req & req, http_res & res);

bool del_drop_collection(http_req & req, http_res & res);

bool get_debug(http_req & req, http_res & res);

bool get_health(http_req & req, http_res & res);

bool get_metrics_json(http_req & req, http_res & res);

bool get_search(http_req & req, http_res & res);

bool get_collection_summary(http_req & req, http_res & res);

bool get_export_documents(http_req & req, http_res & res);

bool post_add_document(http_req & req, http_res & res);

bool post_import_documents(http_req & req, http_res & res);

bool get_fetch_document(http_req & req, http_res & res);

bool del_remove_document(http_req & req, http_res & res);

bool collection_export_handler(http_req* req, http_res* res, void* data);

bool async_write_request(void *data);

static constexpr const char* SEND_RESPONSE_MSG = "send_response";
