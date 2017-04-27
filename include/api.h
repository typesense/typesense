#pragma once

#include "http_server.h"

void get_search(http_req & req, http_res & res);

void post_add_document(http_req & req, http_res & res);

void del_remove_document(http_req & req, http_res & res);