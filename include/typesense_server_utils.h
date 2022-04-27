#pragma once

#include "logger.h"
#include <string>
#include <iostream>
#include <cmdline.h>
#include "config.h"
#include "store.h"
#include "collection_manager.h"
#include <csignal>
#include <sys/types.h>
#include <sys/stat.h>
#include "http_server.h"

extern HttpServer* server;

void catch_interrupt(int sig);

bool directory_exists(const std::string& dir_path);

void init_cmdline_options(cmdline::parser& options, int argc, char **argv);

int init_root_logger(Config& config, const std::string& server_version);

int run_server(const Config& config, const std::string& version,
               void (*master_server_routes)());
