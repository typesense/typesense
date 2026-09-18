#include "http_data.h"
#include "http_server.h"
#include "string_utils.h"
#include <regex>
#include <thread>
#include <signal.h>
#include <h2o.h>
#include <iostream>
#include <auth_manager.h>
#include <app_metrics.h>
#include "raft_server.h"
#include "logger.h"
#include "ratelimit_manager.h"
#include "sole.hpp"
#include "core_api.h"
#include "collection_manager.h"
#include "tsconfig.h"

// The message dispatcher of the event loop the current thread runs. Set by each loop thread on
// startup; read in catch_all_handler to tag every request with its owning loop, so that its
// response is delivered back on that same loop.
static thread_local http_message_dispatcher* tls_res_dispatcher = nullptr;

HttpServer::HttpServer(const std::string & version, const std::string & listen_address,
                       uint32_t listen_port, const std::string & ssl_cert_path, const std::string & ssl_cert_key_path,
                       const uint64_t ssl_refresh_interval_ms, bool cors_enabled,
                       const std::set<std::string>& cors_domains, ThreadPool* thread_pool):
        SSL_REFRESH_INTERVAL_MS(ssl_refresh_interval_ms),
        exit_loop(false), version(version), listen_address(listen_address), listen_port(listen_port),
        ssl_cert_path(ssl_cert_path), ssl_cert_key_path(ssl_cert_key_path),
        cors_enabled(cors_enabled), cors_domains(cors_domains), thread_pool(thread_pool) {
    h2o_config_init(&config);
    hostconf = h2o_config_register_host(&config, h2o_iovec_init(H2O_STRLIT("default")), 65535);
    register_handler(hostconf, "/", catch_all_handler);

    signal(SIGPIPE, SIG_IGN);

    config.server_name.base = nullptr;  // initialized in run()

    // The number of event loops is derived from `thread-pool-size` (resolved the same way as the
    // worker thread pools), capped at the core count since loops beyond that cannot run in
    // parallel anyway.
    const size_t proc_count = std::max<size_t>(1, std::thread::hardware_concurrency());
    size_t pool_size = Config::get_instance().get_thread_pool_size();
    if(pool_size == 0) {
        pool_size = proc_count * 8;
    }
    const size_t num_loops = std::max<size_t>(1, std::min(pool_size, proc_count));

    for(size_t i = 0; i < num_loops; i++) {
        ev_loop_t* loop = new ev_loop_t();
        loop->server = this;
        loop->index = i;
        h2o_context_init(&loop->ctx, h2o_evloop_create(), &config);
        loop->accept_ctx.ctx = &loop->ctx;
        loop->accept_ctx.hosts = config.hosts;
        loop->dispatcher = new http_message_dispatcher;
        loop->dispatcher->init(loop->ctx.loop);

        // used during destructor
        loop->ssl_refresh_timer.timer.expire_at = 0;

        loops.push_back(loop);
    }

    // used during destructor
    metrics_refresh_timer.timer.expire_at = 0;

    meta_thread_pool = new ThreadPool(4);
}

void HttpServer::on_accept(h2o_socket_t *listener, const char *err) {
    // listener->data is the accept context of the event loop that owns this listener, so that the
    // connection is handled entirely on that loop. All loops poll dup()s of one listening socket,
    // so they race accept() on a shared queue: losing the race yields NULL (EAGAIN), and while
    // connections remain queued the level-triggered evloop fires this callback again.
    // Deliberately accepts one connection per wakeup (h2o's own server batches up to 10):
    // returning to the poll loop between accepts gives the other loops a chance to win the next
    // connection, spreading load across loops at the cost of extra wakeups.
    h2o_accept_ctx_t* accept_ctx = reinterpret_cast<h2o_accept_ctx_t*>(listener->data);
    h2o_socket_t *sock;

    if (err != NULL) {
        return;
    }

    if ((sock = h2o_evloop_socket_accept(listener)) == NULL) {
        return;
    }

    h2o_accept(accept_ctx, sock);
}

void HttpServer::on_metrics_refresh_timeout(h2o_timer_t *entry) {
    h2o_custom_timer_t* custom_timer = reinterpret_cast<h2o_custom_timer_t*>(entry);

    AppMetrics::get_instance().window_reset();
    AppMetrics::get_instance().flush_access_log();

    HttpServer *hs = static_cast<HttpServer*>(custom_timer->data);

    // link the timer for the next cycle
    h2o_timer_link(
        hs->loops[0]->ctx.loop,
        AppMetrics::METRICS_REFRESH_INTERVAL_MS,
        &hs->metrics_refresh_timer.timer
    );
}

void HttpServer::on_ssl_refresh_timeout(h2o_timer_t *entry) {
    h2o_custom_timer_t* custom_timer = reinterpret_cast<h2o_custom_timer_t*>(entry);

    // Each event loop refreshes its own SSL context on its own thread, so no SSL state is ever
    // shared across loops.
    ev_loop_t* loop = static_cast<ev_loop_t*>(custom_timer->data);
    HttpServer* hs = loop->server;

    if(loop->index == 0) {
        LOG(INFO) << "Refreshing SSL certs from disk.";
    }

    SSL_CTX* old_ssl_ctx = loop->accept_ctx.ssl_ctx;

    bool refresh_success = initialize_ssl_ctx(hs->ssl_cert_path.c_str(), hs->ssl_cert_key_path.c_str(), &loop->accept_ctx);

    if (refresh_success) {
        // delete the old SSL context but after some time, to allow existing connections to drain
        h2o_custom_timer_t* ssl_ctx_delete_timer = new h2o_custom_timer_t(old_ssl_ctx);
        h2o_timer_init(&ssl_ctx_delete_timer->timer, on_ssl_ctx_delete_timeout);
        uint64_t delete_lag = std::max<uint64_t>(60 * 1000, hs->SSL_REFRESH_INTERVAL_MS / 2);
        h2o_timer_link(loop->ctx.loop, delete_lag, &ssl_ctx_delete_timer->timer);
    } else {
        LOG(ERROR) << "SSL cert refresh failed.";
    }

    // link the timer for the next cycle
    h2o_timer_link(loop->ctx.loop, hs->SSL_REFRESH_INTERVAL_MS, &loop->ssl_refresh_timer.timer);
}

void HttpServer::on_ssl_ctx_delete_timeout(h2o_timer_t *entry) {
    LOG(INFO) << "Deleting old SSL context.";

    h2o_custom_timer_t* custom_timer = reinterpret_cast<h2o_custom_timer_t*>(entry);
    SSL_CTX* old_ssl_ctx = static_cast<SSL_CTX*>(custom_timer->data);
    SSL_CTX_free(old_ssl_ctx);
    delete custom_timer;
}

int HttpServer::setup_ssl(ev_loop_t* loop) {
    // Set up a timer to refresh SSL config from disk. Also, initializing upfront so that destructor works
    loop->ssl_refresh_timer = h2o_custom_timer_t(loop);
    h2o_timer_init(&loop->ssl_refresh_timer.timer, on_ssl_refresh_timeout);
    h2o_timer_link(loop->ctx.loop, SSL_REFRESH_INTERVAL_MS, &loop->ssl_refresh_timer.timer);

    if(loop->index == 0) {
        LOG(INFO) << "SSL cert refresh interval: " << (SSL_REFRESH_INTERVAL_MS / 1000) << "s";
    }

    if(!initialize_ssl_ctx(ssl_cert_path.c_str(), ssl_cert_key_path.c_str(), &loop->accept_ctx)) {
        return -1;
    }

    return 0;
}

int HttpServer::bind_listen_fd() {
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int fd = -1, reuseaddr_flag = 1;
    std::string port_str = std::to_string(listen_port);

    std::string actual_address = listen_address;
    if (actual_address.size() >= 2 && actual_address[0] == '[' &&
        actual_address[actual_address.size()-1] == ']') {
        actual_address = actual_address.substr(1, actual_address.size()-2);
    }

    memset(&hints, 0, sizeof(hints));
    // If :: was explicitly requested, prefer IPv6
    hints.ai_family = (actual_address == "::") ? AF_INET6 : AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM; // TCP socket
    hints.ai_protocol = 0;           // Any protocol

    // Handle special case for any-address binding
    const char* host = NULL;  // NULL with AI_PASSIVE means bind to all interfaces
    if (actual_address != "0.0.0.0" && actual_address != "::") {
        host = actual_address.c_str();
        hints.ai_flags = AI_NUMERICHOST;  // Prevent DNS lookup for IP addresses
    } else {
        hints.ai_flags = AI_PASSIVE;  // Used only when host is NULL
    }

    int s = getaddrinfo(host, port_str.c_str(), &hints, &result);
    if (s != 0) {
        LOG(ERROR) << "getaddrinfo failed: " << gai_strerror(s);
        LOG(ERROR) << "actual_address: " << actual_address;
        return -1;
    }

    // Try each address until we successfully bind.
    // getaddrinfo() returns list with most relevant address first.
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd == -1) {
            continue;
        }

        // For IPv6, ensure we accept both IPv4 and IPv6 connections
        if (rp->ai_family == AF_INET6) {
            int off = 0;
            if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off)) != 0) {
                LOG(WARNING) << "Failed to set IPV6_V6ONLY=0";
            }
        }

        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuseaddr_flag, sizeof(reuseaddr_flag)) != 0) {
            LOG(WARNING) << "Failed to set SO_REUSEADDR";
        }

        if (bind(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            // Successfully bound
            break;
        }

        // Log the specific bind error to help with debugging. errno must be captured before
        // LOG(): the LogMessage constructor runs first and can clobber it.
        const int bind_errno = errno;
        LOG(WARNING) << "Failed to bind to address (family=" << rp->ai_family
                    << "): " << strerror(bind_errno);
        close(fd);
    }

    freeaddrinfo(result);

    if (rp == NULL) {
        LOG(ERROR) << "Could not bind to " << listen_address << ":" << listen_port;
        return -1;
    }

    if (listen(fd, SOMAXCONN) != 0) {
        LOG(ERROR) << "Failed to listen on socket";
        close(fd);
        return -1;
    }

    return fd;
}

int HttpServer::create_listener(ev_loop_t* loop, int fd) {
    if(!ssl_cert_path.empty() && !ssl_cert_key_path.empty()) {
        int ssl_setup_code = setup_ssl(loop);
        if(ssl_setup_code != 0) {
            close(fd);
            return -1;
        }
    }

    loop->listener = h2o_evloop_socket_create(loop->ctx.loop, fd, H2O_SOCKET_FLAG_DONT_READ);
    loop->listener->data = &loop->accept_ctx;
    h2o_socket_read_start(loop->listener, on_accept);

    return 0;
}

int HttpServer::run(ReplicationState* replication_state) {
    this->replication_state = replication_state;

    // h2o global config, shared by the contexts of all event loops
    config.server_name = h2o_strdup(nullptr, "", SIZE_MAX);
    config.http2.active_stream_window_size = ACTIVE_STREAM_WINDOW_SIZE;
    config.http2.idle_timeout = REQ_TIMEOUT_MS;
    config.max_request_entity_size = (size_t(10) * 1024 * 1024 * 1024); // 10 GB

    config.http1.req_timeout = REQ_TIMEOUT_MS;
    config.http1.req_io_timeout = REQ_TIMEOUT_MS;

    metrics_refresh_timer = h2o_custom_timer_t(this);
    h2o_timer_init(&metrics_refresh_timer.timer, on_metrics_refresh_timeout);
    h2o_timer_link(loops[0]->ctx.loop, AppMetrics::METRICS_REFRESH_INTERVAL_MS, &metrics_refresh_timer.timer);

    // One socket is bound to the port; every loop accepts from it via its own dup()'d fd. Idle
    // loops win the accept race, so load follows capacity, and a second process binding the same
    // port still fails with EADDRINUSE (unlike SO_REUSEPORT, which would silently split traffic).
    int listen_fd = bind_listen_fd();
    if(listen_fd < 0) {
        // bind_listen_fd has already logged the specific cause
        LOG(ERROR) << "Failed to listen on " << listen_address << ":" << listen_port;
        return 1;
    }

    for(ev_loop_t* loop : loops) {
        // every dispatcher gets the full set of registered message handlers (registration
        // happens before run(), so a plain copy is safe)
        loop->dispatcher->message_handlers = message_handlers;
        loop->dispatcher->on(STOP_SERVER_MESSAGE, HttpServer::on_stop_server);

        int fd = (loop->index == 0) ? listen_fd : dup(listen_fd);
        if(fd == -1) {
            const int dup_errno = errno;  // capture before LOG() can clobber it
            LOG(ERROR) << "Failed to dup listener fd - " << strerror(dup_errno);
            return 1;
        }

        if(create_listener(loop, fd) != 0) {
            LOG(ERROR) << "Failed to set up listener on " << listen_address << ":" << listen_port;
            return 1;
        }
    }

    LOG(INFO) << "Typesense has started listening on port " << listen_port
              << " (" << loops.size() << " event loops)";

    for(size_t i = 1; i < loops.size(); i++) {
        loops[i]->thread = std::thread([this, loop = loops[i]]() {
            loop_run(loop);
        });
    }

    loop_run(loops[0]);

    for(size_t i = 1; i < loops.size(); i++) {
        if(loops[i]->thread.joinable()) {
            loops[i]->thread.join();
        }
    }

    return 0;
}

void HttpServer::loop_run(ev_loop_t* loop) {
    tls_res_dispatcher = loop->dispatcher;

    while(!exit_loop) {
        h2o_evloop_run(loop->ctx.loop, INT32_MAX);
    }

    // stop accepting new connections; done here so that only the owning thread ever touches
    // this loop's listener socket
    if(loop->listener != nullptr) {
        h2o_socket_read_stop(loop->listener);
        h2o_socket_close(loop->listener);
        loop->listener = nullptr;
    }
}

bool HttpServer::on_stop_server(void *data) {
    // do nothing
    return true;
}

std::string HttpServer::get_version() {
    return version;
}

void HttpServer::clear_timeouts(const std::vector<h2o_timer_t*> & timers, bool trigger_callback) {
    for(h2o_timer_t* timer: timers) {
        h2o_timer_unlink(timer);
    }
}

void HttpServer::stop() {
    // this will break the event loops
    exit_loop = true;

    // send a message to wake up each idle event loop so it can exit; a loop closes its own
    // listener on the way out (only the owning thread may touch its sockets)
    for(ev_loop_t* loop : loops) {
        loop->dispatcher->send_message(STOP_SERVER_MESSAGE, nullptr);
    }
}

h2o_pathconf_t* HttpServer::register_handler(h2o_hostconf_t *hostconf, const char *path,
                                 int (*on_req)(h2o_handler_t *, h2o_req_t *)) {
    // See: https://github.com/h2o/h2o/issues/181#issuecomment-75393049
    h2o_pathconf_t *pathconf = h2o_config_register_path(hostconf, path, 0);
    h2o_custom_req_handler_t *handler = reinterpret_cast<h2o_custom_req_handler_t*>(h2o_create_handler(pathconf, sizeof(*handler)));
    handler->http_server = this;
    handler->super.on_req = on_req;

    // Enable streaming request body
    handler->super.supports_request_streaming = 1;

    compress_args.min_size = 256;       // don't gzip less than this size
    compress_args.brotli.quality = -1;  // disable, not widely supported
    compress_args.gzip.quality = 1;     // fastest
    h2o_compress_register(pathconf, &compress_args);

    return pathconf;
}

uint64_t HttpServer::find_route(const std::vector<std::string> & path_parts, const std::string & http_method,
                                route_path** found_rpath) {
    for (const auto& index_route : route_hash_to_path) {
        const route_path & rpath = index_route.second;

        if(rpath.path_parts.size() != path_parts.size() || rpath.http_method != http_method) {
            continue;
        }

        bool found = true;

        for(size_t j = 0; j < rpath.path_parts.size(); j++) {
            const std::string & rpart = rpath.path_parts[j];
            const std::string & given_part = path_parts[j];
            if(rpart != given_part && rpart[0] != ':') {
                found = false;
                break;
            }
        }

        if(found) {
            *found_rpath = const_cast<route_path *>(&rpath);
            return index_route.first;
        }
    }

    return static_cast<uint64_t>(ROUTE_CODES::NOT_FOUND);
}

void HttpServer::on_res_generator_dispose(void *self) {
    //LOG(INFO) << "on_res_generator_dispose fires";
    h2o_custom_generator_t* custom_generator = *static_cast<h2o_custom_generator_t**>(self);

    // locking to ensure dispose does not happen while the h2o req object is being written to
    {
        std::unique_lock lk(custom_generator->res()->mres);
        custom_generator->res()->final = true;
        custom_generator->res()->generator = nullptr;
        custom_generator->res()->is_alive = false;
        custom_generator->req()->is_diposed = true;
        custom_generator->req()->notify();
        custom_generator->res()->notify();
    }

    // without this, warning about memory allocated by std::string leaking happens
    delete custom_generator;
}

int HttpServer::catch_all_handler(h2o_handler_t *_h2o_handler, h2o_req_t *req) {
    h2o_custom_req_handler_t* h2o_handler = (h2o_custom_req_handler_t *)_h2o_handler;

    const std::string & http_method = std::string(req->method.base, req->method.len);
    const std::string & path = std::string(req->path.base, req->path.len);
    std::vector<std::string> path_with_query_parts;

    // These guards have been added to debug a strange issue of `path_with_query_parts` being empty sometimes
    if(req->path.len == 0 || path.empty()) {
        LOG(ERROR) << "Request path is empty: path.len=" << req->path.len << ", path: " << path;
        nlohmann::json resp;
        resp["message"] = "Request path is empty.";
        return send_response(req, 400, resp.dump());
    } else {
        StringUtils::split(path, path_with_query_parts, "?");
        if(path_with_query_parts.empty()) {
            LOG(ERROR) << "Request path is empty after splitting: path=" << path;
            nlohmann::json resp;
            resp["message"] = "Request path after splitting is empty.";
            return send_response(req, 400, resp.dump());
        }
    }

    const std::string & path_without_query = path_with_query_parts[0];

    std::string metric_identifier = http_method + " " + path_without_query;
    AppMetrics::get_instance().increment_count(metric_identifier, 1);

    std::string client_ip = http_req::get_ip_addr(req).ip;

    std::vector<std::string> path_parts;
    StringUtils::split(path_without_query, path_parts, "/");

    h2o_iovec_t query = req->query_at != SIZE_MAX ?
                        h2o_iovec_init(req->path.base + req->query_at, req->path.len - req->query_at) :
                        h2o_iovec_init(H2O_STRLIT(""));

    std::string query_str(query.base, query.len);
    std::map<std::string, std::string> query_map;
    StringUtils::parse_query_string(query_str, query_map);

    // cache ttl can be applied only from an embedded key: cannot be a get param
    query_map.erase("cache_ttl");

    // Extract auth key from header. If that does not exist, look for a GET parameter.
    ssize_t auth_header_cursor = h2o_find_header_by_str(&req->headers, http_req::AUTH_HEADER, strlen(http_req::AUTH_HEADER), -1);
    std::string api_auth_key_sent;

    if(auth_header_cursor != -1) {
        h2o_iovec_t & slot = req->headers.entries[auth_header_cursor].value;
        api_auth_key_sent = std::string(slot.base, slot.len);
    } else if(query_map.count(http_req::AUTH_HEADER) != 0) {
        api_auth_key_sent = query_map[http_req::AUTH_HEADER];
    }

    std::string api_auth_key_prefix;
    const bool is_multi_search_request = (path_without_query == "/multi_search");
    if(!is_multi_search_request && Config::get_instance().get_enable_access_logging()) {
        uint64_t now = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        auto epoch_millis = now / 1000;
        api_auth_key_prefix = CollectionManager::get_instance().getAuthManager().get_api_key_prefix(api_auth_key_sent);
        AppMetrics::get_instance().write_access_log(epoch_millis, client_ip.c_str(), metric_identifier, api_auth_key_prefix);
    }

    // Handle CORS
    if(h2o_handler->http_server->cors_enabled) {
        h2o_iovec_t response_origin = {(char*)"*", 1};

        if(!h2o_handler->http_server->cors_domains.empty()) {
            auto acl_origin_cursor = h2o_find_header(&req->headers, H2O_TOKEN_ORIGIN, -1);
            if(acl_origin_cursor != -1) {
                response_origin = req->headers.entries[acl_origin_cursor].value;
                std::string origin_str = std::string(response_origin.base, response_origin.len);
                if(h2o_handler->http_server->cors_domains.count(origin_str) == 0) {
                    response_origin = {(char*)"", 0};
                }
            }
        }

        if(response_origin.len != 0) {
            // only send header if origin matches or if wildcard allowed
            h2o_add_header_by_str(&req->pool, &req->res.headers, H2O_STRLIT("access-control-allow-origin"),
                                  0, NULL, response_origin.base, response_origin.len);
        }

        if(http_method == "OPTIONS") {
            // locate request access control headers
            const char* ACL_REQ_HEADERS = "access-control-request-headers";
            ssize_t acl_header_cursor = h2o_find_header_by_str(&req->headers, ACL_REQ_HEADERS,
                                                               strlen(ACL_REQ_HEADERS), -1);

            if(acl_header_cursor != -1) {
                h2o_iovec_t &acl_req_headers = req->headers.entries[acl_header_cursor].value;

                h2o_generator_t generator = {NULL, NULL};
                h2o_iovec_t res_body = h2o_strdup(&req->pool, "", SIZE_MAX);
                req->res.status = 200;
                req->res.reason = http_res::get_status_reason(200);

                h2o_add_header_by_str(&req->pool, &req->res.headers,
                                      H2O_STRLIT("access-control-allow-methods"),
                                      0, NULL, H2O_STRLIT("POST, GET, DELETE, PUT, PATCH, OPTIONS"));
                h2o_add_header_by_str(&req->pool, &req->res.headers,
                                      H2O_STRLIT("access-control-allow-headers"),
                                      0, NULL, acl_req_headers.base, acl_req_headers.len);
                h2o_add_header_by_str(&req->pool, &req->res.headers,
                                      H2O_STRLIT("access-control-max-age"),
                                      0, NULL, H2O_STRLIT("3600"));

                h2o_start_response(req, &generator);
                h2o_send(req, &res_body, 1, H2O_SEND_STATE_FINAL);
                return 0;
            }
        }
    }

    if(query.len > 4000 && http_method == "GET" && !path_parts.empty() && path_parts.back() == "search") {
        nlohmann::json resp;
        resp["message"] = "Query string exceeds max allowed length of 4000. Use the /multi_search end-point for larger payloads.";
        return send_response(req, 400, resp.dump());
    }

    // extract user id from header, if not already present as GET param
    ssize_t user_header_cursor = h2o_find_header_by_str(&req->headers, http_req::USER_HEADER, strlen(http_req::USER_HEADER), -1);

    if(user_header_cursor != -1) {
        h2o_iovec_t & slot = req->headers.entries[user_header_cursor].value;
        std::string user_id_sent = std::string(slot.base, slot.len);
        query_map[http_req::USER_HEADER] = user_id_sent;
    } else if(query_map.count(http_req::USER_HEADER) == 0) {
        query_map[http_req::USER_HEADER] = client_ip;
    }

    route_path *rpath = nullptr;
    uint64_t route_hash = h2o_handler->http_server->find_route(path_parts, http_method, &rpath);

    if(route_hash == static_cast<uint64_t>(ROUTE_CODES::NOT_FOUND)) {
        std::string message = "{ \"message\": \"Not Found\"}";
        return send_response(req, 404, message);
    }

    const std::string& root_resource = (path_parts.empty()) ? "" : path_parts[0];

    bool needs_readiness_check = (root_resource == "collections") ||
         !(
             root_resource == "health" || root_resource == "debug" || root_resource == "proxy" ||
             root_resource == "stats.json" || root_resource == "metrics.json" ||
             root_resource == "sequence" || root_resource == "operations" ||
             root_resource == "config" || root_resource == "status" || root_resource == "proxy_sse"
         );

    bool use_meta_thread_pool = (root_resource == "status" || root_resource == "health");

    if(needs_readiness_check) {
        bool write_op = is_write_request(root_resource, http_method, rpath->handler);
        bool read_op = !write_op;

        std::string message = "{ \"message\": \"Not Ready or Lagging\"}";

        if(read_op && !h2o_handler->http_server->get_replication_state()->is_read_caught_up()) {
            return send_response(req, 503, message);
        }

        else if(write_op && !h2o_handler->http_server->get_replication_state()->is_write_caught_up()) {
            return send_response(req, 503, message);
        }
    }

    // iterate and extract path params
    for(size_t i = 0; i < rpath->path_parts.size(); i++) {
        const std::string & path_part = rpath->path_parts[i];
        if(path_part[0] == ':') {
            std::string value = StringUtils::url_decode(path_parts[i]);
            query_map.emplace(path_part.substr(1), value);
        }
    }

    const std::string& body = std::string(req->entity.base, req->entity.len);
    std::vector<nlohmann::json> embedded_params_vec;

    if(RateLimitManager::getInstance()->is_rate_limited({RateLimitedEntityType::api_key, api_auth_key_sent}, {RateLimitedEntityType::ip, client_ip})) {
        std::string message = "{ \"message\": \"Rate limit exceeded or blocked\"}";
        return send_response(req, 429, message);
    }

    bool is_multi_search_query = (root_resource == "multi_search");

    if(Config::get_instance().get_enable_search_logging()) {
        std::string query_string = "?";
        bool is_search_query = (is_multi_search_query ||
                                StringUtils::ends_with(path_without_query, "/documents/search"));

        if(is_search_query) {
            std::string search_payload;

            if(is_multi_search_query) {
                search_payload = body;
                StringUtils::erase_char(search_payload, '\n');
            }

            // ignore params map of multi_search since it is mutated for every search object in the POST body
            for(const auto& kv: query_map) {
                if(kv.first != http_req::AUTH_HEADER) {
                    query_string += kv.first + "=" + StringUtils::url_encode(kv.second) + "&";
                }
            }

            std::string full_url_path = metric_identifier + query_string;

            // NOTE: we log the `body` ONLY for multi-search query
            LOG(INFO) << "event=search_request" << ", client_ip=" << client_ip << ", endpoint=" << full_url_path
                      << ", body=" << (is_multi_search_query ? search_payload : "");
        }
    }

    if(!is_multi_search_query) {
        // multi_search needs to be handled later because the API key could be part of request body and
        // the whole request body might not be available right now.
        bool authenticated = h2o_handler->http_server->auth_handler(query_map, embedded_params_vec, body, *rpath,
                                                                    api_auth_key_sent, &api_auth_key_prefix);
        if(!authenticated) {
            std::string message = std::string("{\"message\": \"Forbidden - a valid `") + http_req::AUTH_HEADER +
                                  "` header must be sent.\"}";
            return send_response(req, 401, message);
        }
    }

    ssize_t content_type_header_cursor = h2o_find_header_by_str(&req->headers, http_req::CONTENT_TYPE_HEADER, strlen(http_req::CONTENT_TYPE_HEADER), -1);
    bool is_binary_body = false;
    if (content_type_header_cursor != -1) {
        h2o_iovec_t& slot = req->headers.entries[content_type_header_cursor].value;
        std::string content_type = std::string(slot.base, slot.len);
        is_binary_body = (content_type == http_req::OCTET_STREAM_HEADER_VALUE);
    }

    std::shared_ptr<http_req> request = std::make_shared<http_req>(req, rpath->http_method, path_without_query,
                                                                   route_hash, query_map, embedded_params_vec,
                                                                   api_auth_key_sent, api_auth_key_prefix, body, client_ip,
                                                                   is_binary_body);

    // tag the request with the dispatcher of the event loop handling it, so that its response is
    // delivered back on this same loop (the only thread allowed to touch the connection's socket)
    request->res_dispatcher = tls_res_dispatcher;

    // add custom generator with a dispose function for cleaning up resources
    h2o_custom_generator_t* custom_gen = new h2o_custom_generator_t;
    std::shared_ptr<http_res> response = std::make_shared<http_res>(custom_gen);

    custom_gen->h2o_generator = h2o_generator_t {response_proceed, response_abort};
    custom_gen->request = request;
    custom_gen->response = response;
    custom_gen->rpath = rpath;
    custom_gen->h2o_handler = h2o_handler;

    h2o_custom_generator_t** allocated_generator = static_cast<h2o_custom_generator_t**>(
        h2o_mem_alloc_shared(&req->pool, sizeof(*allocated_generator), on_res_generator_dispose)
    );
    *allocated_generator = custom_gen;

    // ensures that the first response need not wait for previous chunk to be done sending
    response->notify();

    //LOG(INFO) << "Init res: " << custom_gen->response << ", ref count: " << custom_gen->response.use_count();

    if(root_resource == "multi_search") {
        // format is <length of api_auth_key_sent>:<api_auth_key_sent><client_ip>
        std::string multi_search_key = std::to_string(api_auth_key_sent.length()) + ":" + api_auth_key_sent + client_ip;
        request->metadata = multi_search_key;
    }

    // routes match and is an authenticated request
    // do any additional pre-request middleware operations here
    if(rpath->action == "keys:create") {
        // we enrich incoming request with a random API key here so that leader and replicas will use the same key
        request->metadata = StringUtils::randstring(AuthManager::GENERATED_KEY_LEN);
    }

    if(rpath->action == "conversations/models:create") {
        try {
            nlohmann::json body_json = nlohmann::json::parse(request->body);
            if(body_json.count("id") != 0 && body_json["id"].is_string()) {
               request->metadata = body_json["id"].get<std::string>();
            } else {
                request->metadata = sole::uuid4().str();
            }
        } catch (const nlohmann::json::parse_error& e) {
            request->metadata = sole::uuid4().str();
        }
    }

    if(rpath->action == "nl_search_models:create") {
        try {
            nlohmann::json body_json = nlohmann::json::parse(request->body);
            if(body_json.count("id") != 0 && body_json["id"].is_string()) {
               request->metadata = body_json["id"].get<std::string>();
            } else {
                request->metadata = sole::uuid4().str();
            }
        } catch (const nlohmann::json::parse_error& e) {
            request->metadata = sole::uuid4().str();
        }
    }

    if(req->proceed_req == nullptr) {
        // Full request body is already available, so we don't care if handler is async or not
        //LOG(INFO) << "Full request body is already available: " << req->entity.len;

        request->last_chunk_aggregate = true;
        return process_request(request, response, rpath, h2o_handler, use_meta_thread_pool);
    } else {
        // Only partial request body is available.
        // If rpath->async_req is true, the request handler function will be invoked multiple times, for each chunk

        //LOG(INFO) << "Partial request body length: " << req->entity.len;

        req->write_req.cb = async_req_cb;
        req->write_req.ctx = custom_gen;
        req->proceed_req(req, NULL);
    }

    return 0;
}


bool HttpServer::is_write_request(const std::string& root_resource, const std::string& http_method,
                                  bool (*rpath_handler)(const std::shared_ptr<http_req>&, const std::shared_ptr<http_res>&)) {
    if(http_method == "GET") {
        return false;
    }

    if(rpath_handler == post_create_event) {
        return false;
    }

    bool write_free_request = (root_resource == "multi_search" || root_resource == "proxy" ||
                               root_resource == "operations" || root_resource == "config"  || 
                               root_resource == "proxy_sse");

    if(!write_free_request &&
       (http_method == "POST" || http_method == "PUT" ||
        http_method == "DELETE" || http_method == "PATCH")) {
        return true;
    }

    return false;
}

bool HttpServer::curl_only_http1(std::string_view ua) {
    constexpr int LIMIT = 7 * 1'000'000 + 71 * 1'000 + 0;   // 7 071 000 (v7.71.0)

    std::size_t p = ua.find("curl/");
    if (p == std::string_view::npos) {
        return false;
    }

    p += 5;

    int component = 0;          // digits of current field
    long long encoded = 0;      // final value (64-bit is plenty)
    long long factor = 1'000'000;   // major → 1 000 000, minor → 1 000, patch → 1

    for (std::size_t i = p; i < ua.size() && factor; ++i) {
        char c = ua[i];
        if (std::isdigit(c)) {
            component = component * 10 + (c - '0');
        } else {
            encoded += component * factor;
            component = 0;
            if (c != '.') {
                break; // stop at any non-dot delimiter
            }
            factor /= 1000;                   // next field weight
        }
    }

    encoded += component * factor;            // commit last field
    return encoded <= LIMIT;
}

int HttpServer::async_req_cb(void *ctx, int is_end_stream) {
    h2o_custom_generator_t* custom_generator = static_cast<h2o_custom_generator_t*>(ctx);

    const std::shared_ptr<http_req>& request = custom_generator->req();
    const std::shared_ptr<http_res>& response = custom_generator->res();

    h2o_iovec_t chunk = request->_req->entity;
    bool async_req = custom_generator->rpath->async_req;
    bool is_http_v1 = (0x101 <= request->_req->version && request->_req->version < 0x200);

    /*LOG(INFO) << "async_req_cb, chunk.len=" << chunk.len
              << ", is_http_v1: " << is_http_v1
              << ", request->req->entity.len=" << request->_req->entity.len
              << ", content_len: " << request->_req->content_length
              << ", is_end_stream=" << is_end_stream;*/

    // disallow specific curl clients from using import call via http2
    // detects: https://github.com/curl/curl/issues/1410
    if(!is_http_v1 && async_req && request->first_chunk_aggregate && request->chunk_len == 0 &&
        request->path_without_query.find("import") != std::string::npos) {

        ssize_t agent_header_cursor = h2o_find_header_by_str(&request->_req->headers,
                                                             http_req::AGENT_HEADER,
                                                             strlen(http_req::AGENT_HEADER), -1);
        if(agent_header_cursor != -1) {
            h2o_iovec_t & slot = request->_req->headers.entries[agent_header_cursor].value;
            const std::string user_agent = std::string(slot.base, slot.len);
            if(curl_only_http1(user_agent)) {
                std::string message = "{ \"message\": \"HTTP2 is not supported by your curl client. "
                                      "You need to use atleast Curl v7.71.0.\"}";
                h2o_iovec_t body = h2o_strdup(&request->_req->pool, message.c_str(), SIZE_MAX);
                request->_req->res.status = 400;
                request->_req->res.reason = http_res::get_status_reason(400);
                h2o_send(request->_req, &body, 1, H2O_SEND_STATE_ERROR);
                return 0;
            }
        }
    }

    std::string chunk_str(chunk.base, chunk.len);
    request->body += chunk_str;
    request->chunk_len += chunk.len;

    /*LOG(INFO) << "entity: " << std::string(request->req->entity.base, std::min<size_t>(40, request->req->entity.len))
              << ", chunk len: " << std::string(chunk.base, std::min<size_t>(40, chunk.len));*/

    //std::this_thread::sleep_for(std::chrono::seconds(30));

    //LOG(INFO) << "request->body.size(): " << request->body.size() << ", request->chunk_len=" << request->chunk_len;
    // LOG(INFO) << "req->entity.len: " << request->req->entity.len << ", request->chunk_len=" << request->chunk_len;

    bool exceeds_chunk_limit = (request->chunk_len >= ACTIVE_STREAM_WINDOW_SIZE);
    bool can_process_async = async_req && exceeds_chunk_limit;

    /*if(is_end_stream == 1) {
        LOG(INFO) << "is_end_stream=1";
    }*/

    // first let's handle the case where we are ready to fire the request handler
    if(can_process_async || is_end_stream) {
        // For async streaming requests, handler should be invoked for every aggregated chunk
        // For a non streaming request, buffer body and invoke only at the end
        if(request->first_chunk_aggregate) {
            request->first_chunk_aggregate = false;
        }

        // default value for last_chunk_aggregate is false
        request->last_chunk_aggregate = (is_end_stream == 1);
        process_request(request, response, custom_generator->rpath, custom_generator->h2o_handler, false);
        return 0;
    }

    request->_req->proceed_req(request->_req, NULL);
    return 0;
}

int HttpServer::process_request(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response,
                                route_path *rpath, const h2o_custom_req_handler_t *handler,
                                const bool use_meta_thread_pool) {

    //LOG(INFO) << "process_request called";
    const std::string& root_resource = (rpath->path_parts.empty()) ? "" : rpath->path_parts[0];

    if(root_resource == "multi_search") {
        // We can authenticate only when the full request body is available
        bool authenticated = handler->http_server->auth_handler(request->params, request->embedded_params_vec,
                                                                request->body, *rpath, request->api_auth_key,
                                                                &request->api_auth_key_prefix);
        if(!authenticated) {
            if(Config::get_instance().get_enable_access_logging()) {
                uint64_t now = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();
                auto epoch_millis = now / 1000;
                const auto metric_identifier = rpath->http_method + " " + request->path_without_query;
                AppMetrics::get_instance().write_access_log(epoch_millis, request->client_ip.c_str(), metric_identifier,
                                                            request->api_auth_key_prefix);
            }

            std::string message = std::string("{\"message\": \"Forbidden - a valid `") + http_req::AUTH_HEADER +
                                  "` header must be sent.\"}";
            return send_response(request->_req, 401, message);
        }

        if(Config::get_instance().get_enable_access_logging()) {
            uint64_t now = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
            auto epoch_millis = now / 1000;
            const auto metric_identifier = rpath->http_method + " " + request->path_without_query;
            AppMetrics::get_instance().write_access_log(epoch_millis, request->client_ip.c_str(), metric_identifier,
                                                        request->api_auth_key_prefix);
        }
    }

    bool is_write = is_write_request(root_resource, rpath->http_method, rpath->handler);

    request->is_write = is_write;

    if(is_write) {
        handler->http_server->get_replication_state()->write(request, response);
        return 0;
    }

    auto thread_pool = use_meta_thread_pool ? handler->http_server->get_meta_thread_pool() :
                       handler->http_server->get_thread_pool();

    // LOG(INFO) << "Before enqueue res: " << response
    thread_pool->log_exhaustion();
    thread_pool->enqueue([rpath, request, response]() {
        // call the API handler
        //LOG(INFO) << "Wait for response " << response.get() << ", action: " << rpath->_get_action();
        (rpath->handler)(request, response);

        if(!rpath->async_res) {
            // lifecycle of non async res will be owned by stream responder
            auto req_res = new async_req_res_t(request, response, true);
            deliver_stream_response(req_res);
        }
        //LOG(INFO) << "Response done " << response.get();
    });

    return 0;
}

void HttpServer::on_deferred_process_request(h2o_timer_t *entry) {
    h2o_custom_timer_t* custom_timer = reinterpret_cast<h2o_custom_timer_t*>(entry);
    deferred_req_res_t* deferred_req_res = static_cast<deferred_req_res_t*>(custom_timer->data);
    //LOG(INFO) << "on_deferred_process_request " << deferred_req_res->req.get();

    route_path* found_rpath = nullptr;
    deferred_req_res->server->get_route(deferred_req_res->req->route_hash, &found_rpath);

    const std::shared_ptr<http_req> request = deferred_req_res->req;
    const std::shared_ptr<http_res> response = deferred_req_res->res;
    HttpServer* server = deferred_req_res->server;

    // done with timer, so we can clear timer and data
    h2o_timer_unlink(&deferred_req_res->req->defer_timer.timer);
    delete deferred_req_res;
    request->defer_timer.data = nullptr;

    if(found_rpath) {
        // must be called on a separate thread so as not to block http thread
        server->thread_pool->enqueue([found_rpath, request, response]() {
            //LOG(INFO) << "Sleeping for 5s req count " << deferred_req_res->req.use_count();
            //std::this_thread::sleep_for(std::chrono::seconds(5));
            //LOG(INFO) << "on_deferred_process_request, calling handler, req use count " << request.use_count();
            found_rpath->handler(request, response);
        });
    }
}

void HttpServer::defer_processing(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res,
                                  size_t timeout_ms) {
    //LOG(INFO) << "defer_processing, exit_loop: " << exit_loop << ", req: " << req.get() << ", use count: " << req.use_count();
    
    if(req->defer_timer.data == nullptr) {
        //LOG(INFO) << "req->defer_timer.data is null";
        auto deferred_req_res = new deferred_req_res_t(req, res, this, false);
        //LOG(INFO) << "req use count " << req.use_count();
        req->defer_timer.data = deferred_req_res;
        h2o_timer_init(&req->defer_timer.timer, on_deferred_process_request);
    } else {
        // This should not happen as data is cleared when defer handler is run
        LOG(ERROR) << "HttpServer::defer_processing, timer data is NOT null";
        h2o_timer_unlink(&req->defer_timer.timer);
    }

    // Schedule the timer on the event loop that owns this request's connection. Linking it to any
    // other loop would race on that loop's timerwheel and fire the continuation on the wrong
    // thread. Both callers (response_proceed, on_deferred_processing_message) already run on the
    // owning loop, so linking here stays single-threaded.
    h2o_loop_t* owning_loop = (req->res_dispatcher != nullptr) ? req->res_dispatcher->loop : loops[0]->ctx.loop;
    h2o_timer_link(owning_loop, timeout_ms, &req->defer_timer.timer);

    if(exit_loop) {
        // otherwise, replication thread could be stuck waiting on a future
        res->is_alive = false;
        req->notify();
        res->notify();
    }
}

int HttpServer::send_response(h2o_req_t *req, int status_code, const std::string & message) {
    h2o_generator_t generator = {nullptr, nullptr};
    h2o_iovec_t body = h2o_strdup(&req->pool, message.c_str(), SIZE_MAX);
    req->res.status = status_code;
    req->res.reason = http_res::get_status_reason(req->res.status);
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, nullptr, H2O_STRLIT("application/json; charset=utf-8"));
    h2o_start_response(req, &generator);
    h2o_send(req, &body, 1, H2O_SEND_STATE_FINAL);
    return 0;
}

void HttpServer::response_abort(h2o_generator_t *generator, h2o_req_t *req) {
    LOG(INFO) << "response_abort called";
    h2o_custom_generator_t* custom_generator = reinterpret_cast<h2o_custom_generator_t*>(generator);

    custom_generator->res()->final = true;
    custom_generator->res()->is_alive = false;
    //LOG(INFO) << "response_abort: fulfilling req & res proceed.";
}

void HttpServer::response_proceed(h2o_generator_t *generator, h2o_req_t *req) {
    //LOG(INFO) << "response_proceed called";
    h2o_custom_generator_t* custom_generator = reinterpret_cast<h2o_custom_generator_t*>(generator);

    //LOG(INFO) << "proxied_stream: " << custom_generator->response->proxied_stream;
    //LOG(INFO) << "response.final: " <<  custom_generator->response->final;

    custom_generator->res()->notify();

    if(custom_generator->res()->proxied_stream) {
        // request progression should not be tied to response generation
        //LOG(INFO) << "Ignoring request proceed";
        return ;
    }

    // if the request itself is NOT async, call the handler since it will be the handler that will be producing content
    // (streaming response but not request)
    if (!custom_generator->rpath->async_req) {
        // call the handler since it will be the handler that will be producing content
        custom_generator->h2o_handler->http_server->defer_processing(custom_generator->req(),
                                                                     custom_generator->res(), 1);
    }
}

void HttpServer::stream_response(stream_response_state_t& state) {
    // LOG(INFO) << "stream_response called";
    //std::this_thread::sleep_for(std::chrono::milliseconds (5000));

    // ***IMPORTANT***
    // We must ensure that fields of `state.req` are not written to for preventing race conditions with indexing thread
    // Check `async_req_res_t` constructor for overlapping writes.

    h2o_req_t* req = state.get_req();
    
    bool start_of_res = (req->res.status == 0);

    if(start_of_res) {
        h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, NULL,
                       state.res_content_type.data(), state.res_content_type.size());
        req->res.status = (state.status == 0 && state.send_state != H2O_SEND_STATE_FINAL) ? 200 : state.status;
        req->res.reason = state.reason;
    }

    if(state.is_req_early_exit) {
        // premature termination of async request: handle this explicitly as otherwise, request is not being closed
        LOG(INFO) << "Premature termination of async request.";
        bool is_http_v1 = (0x101 <= req->version && req->version < 0x200);

        if (req->_generator == nullptr) {
            h2o_start_response(req, state.generator);
        }

        if(is_http_v1) {
            h2o_send(req, &state.res_buff, 1, H2O_SEND_STATE_FINAL);
            h2o_dispose_request(req);
        } else {
            h2o_send(req, &state.res_buff, 1, H2O_SEND_STATE_ERROR);
        }

        return ;
    }

    if (start_of_res) {
        /*LOG(INFO) << "h2o_start_response, content_type=" << state.res_content_type
                  << ",response.status_code=" << state.res_status_code;*/
        h2o_start_response(req, state.generator);
    }

    if(state.res_buff.len == 0 && state.send_state != H2O_SEND_STATE_FINAL) {
        // without this guard, http streaming will break
        state.generator->proceed(state.generator, req);
        return;
    }

    h2o_send(req, &state.res_buff, 1, state.send_state);

    //LOG(INFO) << "stream_response after send";
}

void HttpServer::set_auth_handler(bool (*handler)(std::map<std::string, std::string>& params,
                                                  std::vector<nlohmann::json>& embedded_params_vec,
                                                  const std::string& body,
                                                  const route_path& rpath, const std::string& auth_key,
                                                  std::string* api_key_prefix)) {
    auth_handler = handler;
}

void HttpServer::get(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>&, const std::shared_ptr<http_res>&), bool async_req, bool async_res) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath("GET", path_parts, handler, async_req, async_res);
    route_hash_to_path.emplace_back(rpath.route_hash(), rpath);
    route_hash_to_path_map.emplace(rpath.route_hash(), rpath);
}

void HttpServer::post(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>&, const std::shared_ptr<http_res>&), bool async_req, bool async_res) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath("POST", path_parts, handler, async_req, async_res);
    route_hash_to_path.emplace_back(rpath.route_hash(), rpath);
    route_hash_to_path_map.emplace(rpath.route_hash(), rpath);
}

void HttpServer::put(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>&, const std::shared_ptr<http_res>&), bool async_req, bool async_res) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath("PUT", path_parts, handler, async_req, async_res);
    route_hash_to_path.emplace_back(rpath.route_hash(), rpath);
    route_hash_to_path_map.emplace(rpath.route_hash(), rpath);
}

void HttpServer::patch(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>&, const std::shared_ptr<http_res>&), bool async_req, bool async_res) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath("PATCH", path_parts, handler, async_req, async_res);
    route_hash_to_path.emplace_back(rpath.route_hash(), rpath);
    route_hash_to_path_map.emplace(rpath.route_hash(), rpath);
}

void HttpServer::del(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>&, const std::shared_ptr<http_res>&), bool async_req, bool async_res) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath("DELETE", path_parts, handler, async_req, async_res);
    route_hash_to_path.emplace_back(rpath.route_hash(), rpath);
    route_hash_to_path_map.emplace(rpath.route_hash(), rpath);
}

void HttpServer::on(const std::string & message, bool (*handler)(void*)) {
    // must be called before run(): each loop's dispatcher receives a copy of these handlers
    message_handlers.emplace(message, handler);
}

HttpServer::~HttpServer() {
    if(metrics_refresh_timer.timer.expire_at != 0) {
        // avoid callback since it recreates timeout
        clear_timeouts({&metrics_refresh_timer.timer}, false);
    }

    for(ev_loop_t* loop : loops) {
        delete loop->dispatcher;

        if(loop->ssl_refresh_timer.timer.expire_at != 0) {
            // avoid callback since it recreates timeout
            clear_timeouts({&loop->ssl_refresh_timer.timer}, false);
        }

        h2o_timerwheel_run(loop->ctx.loop->_timeouts, 9999999999999);

        h2o_context_dispose(&loop->ctx);

        // Flaky, sometimes assertion on timeouts occur, preventing a clean shutdown
        //h2o_evloop_destroy(loop->ctx.loop);

        SSL_CTX_free(loop->accept_ctx.ssl_ctx);
        delete loop;
    }

    if(config.server_name.base != nullptr) {
        free(config.server_name.base);
        config.server_name.base = nullptr;
    }

    h2o_config_dispose(&config);

    meta_thread_pool->shutdown();
    delete meta_thread_pool;
}

ReplicationState* HttpServer::get_replication_state() const {
    return replication_state;
}

bool HttpServer::is_alive() const {
    return !is_shutdown_triggered && replication_state->is_alive();
}

bool HttpServer::get_route(uint64_t hash, route_path** found_rpath) {
    auto route_hash_it = route_hash_to_path_map.find(hash);

    if(route_hash_it == route_hash_to_path_map.end()) {
        return false;
    }

    *found_rpath = &route_hash_it->second;
    return true;
}

uint64_t HttpServer::node_state() const {
    return replication_state->node_state();
}

nlohmann::json HttpServer::node_status() {
    return replication_state->get_status();
}

void HttpServer::deliver_stream_response(async_req_res_t* req_res) {
    http_message_dispatcher* dispatcher = req_res->res_dispatcher();

    if(dispatcher != nullptr) {
        dispatcher->send_message(STREAM_RESPONSE_MESSAGE, req_res);
        return;
    }

    // The request has no owning event loop (a write replayed from the log, or tests):
    // mirror the dead-request path of on_stream_response_message.
    req_res->req_notify();
    req_res->res_notify();

    if(req_res->destroy_after_use) {
        delete req_res;
    }
}

void HttpServer::deliver_request_proceed(deferred_req_res_t* req_res) {
    http_message_dispatcher* dispatcher = req_res->req->res_dispatcher;

    if(dispatcher != nullptr) {
        dispatcher->send_message(REQUEST_PROCEED_MESSAGE, req_res);
        return;
    }

    // no owning event loop, so there is no connection to read more input from
    if(req_res->destroy_after_use) {
        delete req_res;
    }
}

void HttpServer::deliver_defer_processing(defer_processing_t* defer) {
    http_message_dispatcher* dispatcher = defer->req->res_dispatcher;

    if(dispatcher != nullptr) {
        dispatcher->send_message(DEFER_PROCESSING_MESSAGE, defer);
        return;
    }

    // No owning event loop, i.e. a write replayed from the log (on a follower or during
    // startup replay). The continuation must still run — chunked handlers like batched
    // deletes re-schedule themselves through here, and dropping the deferral would apply
    // only their first batch and diverge replicas. Without a loop to host the timer,
    // drive the handler directly on the worker pool (mirrors on_deferred_process_request).
    HttpServer* server = defer->server;
    route_path* found_rpath = nullptr;

    if(server != nullptr) {
        server->get_route(defer->req->route_hash, &found_rpath);
    }

    if(found_rpath != nullptr) {
        const std::shared_ptr<http_req> req = defer->req;
        const std::shared_ptr<http_res> res = defer->res;
        auto handler = found_rpath->handler;
        server->thread_pool->enqueue([handler, req, res]() {
            handler(req, res);
        });
        delete defer;
        return;
    }

    // no server (tests) or unknown route: nothing can drive this request anymore,
    // so mark it dead and wake up any waiters
    defer->res->is_alive = false;
    defer->req->notify();
    defer->res->notify();
    delete defer;
}

bool HttpServer::on_stream_response_message(void *data) {
    //LOG(INFO) << "on_stream_response_message";
    auto req_res = static_cast<async_req_res_t *>(data);

    // NOTE: access to `req` and `res` objects must be synchronized and wrapped by `req_res`

    if(req_res->is_alive()) {
        stream_response(req_res->get_res_state());
    } else {
        // serialized request or generator has been disposed (underlying request is probably dead)
        req_res->req_notify();
        req_res->res_notify();
    }

    if(req_res->destroy_after_use) {
        delete req_res;
    }

    return true;
}

bool HttpServer::on_request_proceed_message(void *data) {
    //LOG(INFO) << "on_request_proceed_message";
    // This callback will run concurrently to batch indexer's run() so care must be taken to protect access
    // to variables that are written to by the batch indexer, which for now is only: last_chunk_aggregate (atomic)
    deferred_req_res_t* req_res = static_cast<deferred_req_res_t *>(data);
    if(req_res->res->is_alive) {
        auto stream_state = (req_res->req->last_chunk_aggregate) ? H2O_SEND_STATE_FINAL : H2O_SEND_STATE_IN_PROGRESS;

        size_t written = req_res->req->chunk_len;
        req_res->req->chunk_len = 0;

        if(req_res->req->_req && req_res->req->_req->proceed_req) {
            req_res->req->_req->proceed_req(req_res->req->_req, NULL);
        }
    }

    if(req_res->destroy_after_use) {
        delete req_res;
    }

    return true;
}

bool HttpServer::on_deferred_processing_message(void *data) {
    //LOG(INFO) << "on_deferred_processing_message";
    defer_processing_t* defer = static_cast<defer_processing_t *>(data);
    //LOG(INFO) << "defer req count: " << defer->req.use_count();
    defer->server->defer_processing(defer->req, defer->res, defer->timeout_ms);
    //LOG(INFO) << "req use count: " << defer->req.use_count() << ", req " << defer->req.get();
    delete defer;
    return true;
}

bool HttpServer::has_exited() const {
    return exit_loop;
}

void HttpServer::do_snapshot(const std::string& snapshot_path, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    return replication_state->do_snapshot(snapshot_path, req, res);
}

bool HttpServer::trigger_vote() {
    return replication_state->trigger_vote();
}

bool HttpServer::reset_peers() {
    return replication_state->reset_peers();
}

ThreadPool* HttpServer::get_thread_pool() const {
    return thread_pool;
}

bool HttpServer::initialize_ssl_ctx(const char *cert_file, const char *key_file, h2o_accept_ctx_t* accept_ctx) {
    SSL_CTX* new_ctx = SSL_CTX_new(SSLv23_server_method());

    // As recommended by:
    // https://github.com/ssllabs/research/wiki/SSL-and-TLS-Deployment-Best-Practices#23-use-secure-cipher-suites
    SSL_CTX_set_cipher_list(new_ctx, "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:"
                                         "ECDHE-ECDSA-AES128-SHA:ECDHE-ECDSA-AES256-SHA:ECDHE-ECDSA-AES128-SHA256:ECDHE-ECDSA-AES256-SHA384:"
                                         "ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES128-SHA:ECDHE-RSA-AES256-SHA:"
                                         "ECDHE-RSA-AES128-SHA256:ECDHE-RSA-AES256-SHA384:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384:"
                                         "DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA:DHE-RSA-AES128-SHA256:DHE-RSA-AES256-SHA256");

    // Without this, DH and ECDH ciphers will be ignored by OpenSSL
    int nid = NID_X9_62_prime256v1;
    EC_KEY *key = EC_KEY_new_by_curve_name(nid);
    if (key == nullptr) {
        LOG(ERROR) << "Failed to create DH/ECDH.";
        return -1;
    }

    SSL_CTX_set_tmp_ecdh(new_ctx, key);
    EC_KEY_free(key);

    SSL_CTX_set_options(new_ctx, SSL_OP_NO_SSLv2);
    SSL_CTX_set_options(new_ctx, SSL_OP_NO_SSLv3);
    SSL_CTX_set_options(new_ctx, SSL_OP_NO_TLSv1);
    SSL_CTX_set_options(new_ctx, SSL_OP_NO_TLSv1_1);

    SSL_CTX_set_options(new_ctx, SSL_OP_SINGLE_ECDH_USE);

    if (SSL_CTX_use_certificate_chain_file(new_ctx, cert_file) != 1) {
        LOG(ERROR) << "An error occurred while trying to load server certificate file: " << cert_file;
        SSL_CTX_free(new_ctx);
        return false;
    }

    if (SSL_CTX_use_PrivateKey_file(new_ctx, key_file, SSL_FILETYPE_PEM) != 1) {
        LOG(ERROR) << "An error occurred while trying to load private key file: " << key_file;
        SSL_CTX_free(new_ctx);
        return false;
    }

    if(SSL_CTX_check_private_key(new_ctx) != 1) {
        LOG(ERROR) << "Private key validation failed for: " << key_file;
        SSL_CTX_free(new_ctx);
        return false;
    }

    h2o_ssl_register_alpn_protocols(new_ctx, h2o_http2_alpn_protocols);
    accept_ctx->ssl_ctx = new_ctx;

    return true;
}

void HttpServer::persist_applying_index() {
    return replication_state->persist_applying_index();
}

int64_t HttpServer::get_num_queued_writes() {
    return replication_state->get_num_queued_writes();
}

bool HttpServer::is_leader() const {
    return replication_state->is_leader();
}

ThreadPool* HttpServer::get_meta_thread_pool() const {
    return meta_thread_pool;
}

void HttpServer::decr_pending_writes() {
    return replication_state->decr_pending_writes();
}

void HttpServer::set_shutdown_triggered() {
    is_shutdown_triggered = true;
}