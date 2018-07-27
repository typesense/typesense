#pragma once

#include "http_server.h"

bool handle_authentication(const route_path & rpath, const std::string & auth_key);

void get_collections(http_req & req, http_res & res);

void post_create_collection(http_req & req, http_res & res);

void del_drop_collection(http_req & req, http_res & res);

void get_debug(http_req & req, http_res & res);

void get_search(http_req & req, http_res & res);

void get_collection_summary(http_req & req, http_res & res);

void get_collection_export(http_req & req, http_res & res);

void post_add_document(http_req & req, http_res & res);

void get_fetch_document(http_req & req, http_res & res);

void del_remove_document(http_req & req, http_res & res);

void get_replication_updates(http_req &req, http_res &res);

void on_send_response(void *data);

void collection_export_handler(http_req* req, http_res* res, void* data);

static constexpr const char* SEND_RESPONSE_MSG = "send_response";
static constexpr const char* REPLICATION_EVENT_MSG = "replication_event";