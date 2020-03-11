#pragma once

#include <string>
#include <iostream>
#include <cmdline.h>
#include "config.h"
#include "logger.h"
#include "store.h"
#include "collection_manager.h"
#include "replicator.h"
#include <csignal>
#include <sys/types.h>
#include <sys/stat.h>
#include "http_server.h"

extern HttpServer* server;

void catch_interrupt(int sig);

bool directory_exists(const std::string & dir_path);

void stream_response(bool (*handler)(http_req* req, http_res* res, void* data),
                     http_req & request, http_res & response, void* data);

void init_cmdline_options(cmdline::parser & options, int argc, char **argv);

int init_logger(Config & config, const std::string & server_version, std::unique_ptr<g3::LogWorker> & log_worker);

int run_server(const Config & config, const std::string & version,
               void (*master_server_routes)(), void (*replica_server_routes)());
