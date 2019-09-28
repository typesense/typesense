#pragma once

#include <string>
#include <iostream>
#include <cmdline.h>
#include "config.h"
#include "logger.h"
#include "store.h"
#include "collection_manager.h"
#include "http_server.h"
#include "replicator.h"
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>

extern HttpServer* server;

void catch_interrupt(int sig);

bool directory_exists(const std::string & dir_path);

void init_cmdline_options(cmdline::parser & options, int argc, char **argv);

int init_logger(Config & config, const std::string & server_version, std::unique_ptr<g3::LogWorker> & log_worker);

int run_server(Config & config, const std::string & version, void (*master_server_routes)(), void (*replica_server_routes)());