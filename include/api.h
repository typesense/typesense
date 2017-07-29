#pragma once

#include "http_server.h"

void get_collections(http_req & req, http_res & res);

void post_create_collection(http_req & req, http_res & res);

void del_drop_collection(http_req & req, http_res & res);

void get_search(http_req & req, http_res & res);

void get_collection_summary(http_req & req, http_res & res);

void post_add_document(http_req & req, http_res & res);

void get_fetch_document(http_req & req, http_res & res);

void del_remove_document(http_req & req, http_res & res);