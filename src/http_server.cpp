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

HttpServer::HttpServer(const std::string & version, const std::string & listen_address,
                       uint32_t listen_port, const std::string & ssl_cert_path,
                       const std::string & ssl_cert_key_path, bool cors_enabled):
                       version(version), listen_address(listen_address), listen_port(listen_port),
                       ssl_cert_path(ssl_cert_path), ssl_cert_key_path(ssl_cert_key_path), cors_enabled(cors_enabled) {
    accept_ctx = new h2o_accept_ctx_t();
    h2o_config_init(&config);
    hostconf = h2o_config_register_host(&config, h2o_iovec_init(H2O_STRLIT("default")), 65535);
    register_handler(hostconf, "/", catch_all_handler);

    listener_socket = nullptr; // initialized later

    signal(SIGPIPE, SIG_IGN);
    h2o_context_init(&ctx, h2o_evloop_create(), &config);

    message_dispatcher = new http_message_dispatcher;
    message_dispatcher->init(ctx.loop);

    ssl_refresh_timer.timer.expire_at = 0;  // used during destructor
    metrics_refresh_timer.timer.expire_at = 0;  // used during destructor
}

void HttpServer::on_accept(h2o_socket_t *listener, const char *err) {
    HttpServer* http_server = reinterpret_cast<HttpServer*>(listener->data);
    h2o_socket_t *sock;

    if (err != NULL) {
        return;
    }

    if ((sock = h2o_evloop_socket_accept(listener)) == NULL) {
        return;
    }

    h2o_accept(http_server->accept_ctx, sock);
}

void HttpServer::on_metrics_refresh_timeout(h2o_timer_t *entry) {
    h2o_custom_timer_t* custom_timer = reinterpret_cast<h2o_custom_timer_t*>(entry);

    AppMetrics::get_instance().window_reset();

    HttpServer *hs = static_cast<HttpServer*>(custom_timer->data);

    // link the timer for the next cycle
    h2o_timer_link(
        hs->ctx.loop,
        AppMetrics::METRICS_REFRESH_INTERVAL_MS,
        &hs->metrics_refresh_timer.timer
    );
}

void HttpServer::on_ssl_refresh_timeout(h2o_timer_t *entry) {
    h2o_custom_timer_t* custom_timer = reinterpret_cast<h2o_custom_timer_t*>(entry);

    LOG(INFO) << "Refreshing SSL certs from disk.";

    HttpServer *hs = static_cast<HttpServer*>(custom_timer->data);
    SSL_CTX *ssl_ctx = hs->accept_ctx->ssl_ctx;

    if (SSL_CTX_use_certificate_chain_file(ssl_ctx, hs->ssl_cert_path.c_str()) != 1) {
        LOG(ERROR) << "Error while refreshing SSL certificate file:" << hs->ssl_cert_path;
    } else if (SSL_CTX_use_PrivateKey_file(ssl_ctx, hs->ssl_cert_key_path.c_str(), SSL_FILETYPE_PEM) != 1) {
        LOG(ERROR) << "Error while refreshing SSL private key file: " << hs->ssl_cert_key_path;
    }

    // link the timer for the next cycle
    h2o_timer_link(
        hs->ctx.loop,
        SSL_REFRESH_INTERVAL_MS,
        &hs->ssl_refresh_timer.timer
    );
}

int HttpServer::setup_ssl(const char *cert_file, const char *key_file) {
    // Set up a timer to refresh SSL config from disk. Also, initializing upfront so that destructor works
    ssl_refresh_timer = h2o_custom_timer_t(this);
    h2o_timer_init(&ssl_refresh_timer.timer, on_ssl_refresh_timeout);
    h2o_timer_link(ctx.loop, SSL_REFRESH_INTERVAL_MS, &ssl_refresh_timer.timer);  // every 8 hours

    accept_ctx->ssl_ctx = SSL_CTX_new(SSLv23_server_method());

    // As recommended by:
    // https://github.com/ssllabs/research/wiki/SSL-and-TLS-Deployment-Best-Practices#23-use-secure-cipher-suites
    SSL_CTX_set_cipher_list(accept_ctx->ssl_ctx, "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:"
            "ECDHE-ECDSA-AES128-SHA:ECDHE-ECDSA-AES256-SHA:ECDHE-ECDSA-AES128-SHA256:ECDHE-ECDSA-AES256-SHA384:"
            "ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES128-SHA:ECDHE-RSA-AES256-SHA:"
            "ECDHE-RSA-AES128-SHA256:ECDHE-RSA-AES256-SHA384:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384:"
            "DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA:DHE-RSA-AES128-SHA256:DHE-RSA-AES256-SHA256");

    // Without this, DH and ECDH ciphers will be ignored by OpenSSL
    int nid = NID_X9_62_prime256v1;
    EC_KEY *key = EC_KEY_new_by_curve_name(nid);
    if (key == NULL) {
        LOG(ERROR) << "Failed to create DH/ECDH.";
        return -1;
    }

    SSL_CTX_set_tmp_ecdh(accept_ctx->ssl_ctx, key);
    EC_KEY_free(key);

    SSL_CTX_set_options(accept_ctx->ssl_ctx, SSL_OP_NO_SSLv2);
    SSL_CTX_set_options(accept_ctx->ssl_ctx, SSL_OP_NO_SSLv3);
    SSL_CTX_set_options(accept_ctx->ssl_ctx, SSL_OP_NO_TLSv1);
    SSL_CTX_set_options(accept_ctx->ssl_ctx, SSL_OP_NO_TLSv1_1);

    SSL_CTX_set_options(accept_ctx->ssl_ctx, SSL_OP_SINGLE_ECDH_USE);

    if (SSL_CTX_use_certificate_chain_file(accept_ctx->ssl_ctx, cert_file) != 1) {
        LOG(ERROR) << "An error occurred while trying to load server certificate file: " << cert_file;
        return -1;
    }
    if (SSL_CTX_use_PrivateKey_file(accept_ctx->ssl_ctx, key_file, SSL_FILETYPE_PEM) != 1) {
        LOG(ERROR) << "An error occurred while trying to load private key file: " << key_file;
        return -1;
    }

    h2o_ssl_register_alpn_protocols(accept_ctx->ssl_ctx, h2o_http2_alpn_protocols);
    return 0;
}

int HttpServer::create_listener() {
    struct sockaddr_in addr;
    int fd, reuseaddr_flag = 1;

    if(!ssl_cert_path.empty() && !ssl_cert_key_path.empty()) {
        int ssl_setup_code = setup_ssl(ssl_cert_path.c_str(), ssl_cert_key_path.c_str());
        if(ssl_setup_code != 0) {
            return -1;
        }
    }

    ctx.globalconf->server_name = h2o_strdup(nullptr, "", SIZE_MAX);
    ctx.globalconf->http2.active_stream_window_size = ACTIVE_STREAM_WINDOW_SIZE;
    ctx.globalconf->http2.idle_timeout = REQ_TIMEOUT_MS;
    ctx.globalconf->max_request_entity_size = (size_t(3) * 1024 * 1024 * 1024); // 3 GB

    ctx.globalconf->http1.req_timeout = REQ_TIMEOUT_MS;
    ctx.globalconf->http1.req_io_timeout = REQ_TIMEOUT_MS;

    accept_ctx->ctx = &ctx;
    accept_ctx->hosts = config.hosts;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(listen_port);
    inet_pton(AF_INET, listen_address.c_str(), &(addr.sin_addr));

    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) == -1 ||
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuseaddr_flag, sizeof(reuseaddr_flag)) != 0 ||
        bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
        listen(fd, SOMAXCONN) != 0) {
        return -1;
    }

    listener_socket = h2o_evloop_socket_create(ctx.loop, fd, H2O_SOCKET_FLAG_DONT_READ);
    listener_socket->data = this;
    h2o_socket_read_start(listener_socket, on_accept);

    return 0;
}

int HttpServer::run(ReplicationState* replication_state) {
    this->replication_state = replication_state;

    metrics_refresh_timer = h2o_custom_timer_t(this);
    h2o_timer_init(&metrics_refresh_timer.timer, on_metrics_refresh_timeout);
    h2o_timer_link(ctx.loop, AppMetrics::METRICS_REFRESH_INTERVAL_MS, &metrics_refresh_timer.timer);

    if (create_listener() != 0) {
        LOG(ERROR) << "Failed to listen on " << listen_address << ":" << listen_port << " - " << strerror(errno);
        return 1;
    } else {
        LOG(INFO) << "Typesense has started listening on port " << listen_port;
    }

    message_dispatcher->on(STOP_SERVER_MESSAGE, HttpServer::on_stop_server);

    while(!exit_loop) {
        h2o_evloop_run(ctx.loop, INT32_MAX);
    }

    return 0;
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
    if(listener_socket != nullptr) {
        h2o_socket_read_stop(listener_socket);
        h2o_socket_close(listener_socket);
    }

    // this will break the event loop
    exit_loop = true;

    // send a message to activate the idle event loop to exit, just in case
    message_dispatcher->send_message(STOP_SERVER_MESSAGE, nullptr);
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

std::map<std::string, std::string> HttpServer::parse_query(const std::string& query) {
    std::map<std::string, std::string> query_map;
    std::regex pattern("([\\w+%-]+)=([^&]*)");

    auto words_begin = std::sregex_iterator(query.begin(), query.end(), pattern);
    auto words_end = std::sregex_iterator();

    for (std::sregex_iterator i = words_begin; i != words_end; i++) {
        std::string key = (*i)[1].str();
        std::string raw_value = (*i)[2].str();
        std::string value = StringUtils::url_decode(raw_value);
        if(query_map.count(key) == 0) {
            query_map[key] = value;
        } else if(key == "filter_by") {
            query_map[key] = query_map[key] + "&&" + value;
        } else {
            query_map[key] = value;
        }
    }

    return query_map;
}

uint64_t HttpServer::find_route(const std::vector<std::string> & path_parts, const std::string & http_method,
                                route_path** found_rpath) {
    for (const auto& index_route : routes) {
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
    h2o_custom_generator_t* res_generator = static_cast<h2o_custom_generator_t*>(self);
    destroy_request_response(res_generator->request, res_generator->response);
}

int HttpServer::catch_all_handler(h2o_handler_t *_h2o_handler, h2o_req_t *req) {
    h2o_custom_req_handler_t* h2o_handler = (h2o_custom_req_handler_t *)_h2o_handler;

    const std::string & http_method = std::string(req->method.base, req->method.len);
    const std::string & path = std::string(req->path.base, req->path.len);

    std::vector<std::string> path_with_query_parts;
    StringUtils::split(path, path_with_query_parts, "?");
    const std::string & path_without_query = path_with_query_parts[0];

    std::string metric_identifier = http_method + " " + path_without_query;
    AppMetrics::get_instance().increment_count(metric_identifier, 1);

    // Handle CORS
    if(h2o_handler->http_server->cors_enabled) {
        h2o_add_header_by_str(&req->pool, &req->res.headers, H2O_STRLIT("access-control-allow-origin"),
                              0, NULL, H2O_STRLIT("*"));

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
                                      0, NULL, H2O_STRLIT("86400"));

                h2o_start_response(req, &generator);
                h2o_send(req, &res_body, 1, H2O_SEND_STATE_FINAL);
                return 0;
            }
        }
    }

    // Except for health check, wait for replicating state to be ready before allowing requests
    // Follower or leader must have started AND data must also have been loaded
    if(path_without_query != "/health" && path_without_query != "/debug" && path_without_query != "/sequence" &&
        !h2o_handler->http_server->get_replication_state()->is_ready()) {
        std::string message = "{ \"message\": \"Not Ready\"}";
        return send_response(req, 503, message);
    }

    std::vector<std::string> path_parts;
    StringUtils::split(path_without_query, path_parts, "/");

    h2o_iovec_t query = req->query_at != SIZE_MAX ?
                        h2o_iovec_init(req->path.base + req->query_at, req->path.len - req->query_at) :
                        h2o_iovec_init(H2O_STRLIT(""));

    std::string query_str(query.base, query.len);
    std::map<std::string, std::string> query_map = parse_query(query_str);

    // Extract auth key from header. If that does not exist, look for a GET parameter.
    std::string api_auth_key_sent = "";

    ssize_t auth_header_cursor = h2o_find_header_by_str(&req->headers, http_req::AUTH_HEADER, strlen(http_req::AUTH_HEADER), -1);
    if(auth_header_cursor != -1) {
        h2o_iovec_t & slot = req->headers.entries[auth_header_cursor].value;
        api_auth_key_sent = std::string(slot.base, slot.len);
    } else if(query_map.count(http_req::AUTH_HEADER) != 0) {
        api_auth_key_sent = query_map[http_req::AUTH_HEADER];
    }

    route_path *rpath = nullptr;
    uint64_t route_hash = h2o_handler->http_server->find_route(path_parts, http_method, &rpath);

    if(route_hash == static_cast<uint64_t>(ROUTE_CODES::NOT_FOUND)) {
        std::string message = "{ \"message\": \"Not Found\"}";
        return send_response(req, 404, message);
    }

    // iterate and extract path params
    for(size_t i = 0; i < rpath->path_parts.size(); i++) {
        const std::string & path_part = rpath->path_parts[i];
        if(path_part[0] == ':') {
            query_map.emplace(path_part.substr(1), path_parts[i]);
        }
    }

    bool authenticated = h2o_handler->http_server->auth_handler(query_map, *rpath, api_auth_key_sent);
    if(!authenticated) {
        std::string message = std::string("{\"message\": \"Forbidden - a valid `") + http_req::AUTH_HEADER +
                               "` header must be sent.\"}";
        return send_response(req, 401, message);
    }

    const std::string & body = std::string(req->entity.base, req->entity.len);

    http_req* request = new http_req(req, rpath->http_method, path_without_query, route_hash, query_map, body);
    http_res* response = new http_res();

    // add custom generator with a dispose function for cleaning up resources
    h2o_custom_generator_t* custom_generator = static_cast<h2o_custom_generator_t*>(
        h2o_mem_alloc_shared(&req->pool, sizeof(*custom_generator), on_res_generator_dispose)
    );
    custom_generator->super = h2o_generator_t {response_proceed, response_abort};
    custom_generator->request = request;
    custom_generator->response = response;
    custom_generator->rpath = rpath;
    custom_generator->h2o_handler = h2o_handler;
    response->generator = &custom_generator->super;

    // routes match and is an authenticated request
    // do any additional pre-request middleware operations here
    if(rpath->action == "keys:create") {
        // we enrich incoming request with a random API key here so that leader and replicas will use the same key
        request->metadata = StringUtils::randstring(AuthManager::KEY_LEN);
    }

    if(req->proceed_req == nullptr) {
        // Full request body is already available, so we don't care if handler is async or not
        //LOG(INFO) << "Full request body is already available: " << req->entity.len;
        request->last_chunk_aggregate = true;
        return process_request(request, response, rpath, h2o_handler);
    } else {
        // Only partial request body is available.
        // If rpath->async_req is true, the request handler function will be invoked multiple times, for each chunk

        //LOG(INFO) << "Partial request body length: " << req->entity.len;

        req->write_req.cb = async_req_cb;
        req->write_req.ctx = custom_generator;
        req->proceed_req(req, req->entity.len, H2O_SEND_STATE_IN_PROGRESS);
    }

    return 0;
}


int HttpServer::async_req_cb(void *ctx, h2o_iovec_t chunk, int is_end_stream) {
    // NOTE: this callback is triggered multiple times by HTTP 2 but only once by HTTP 1
    // This quirk is because of the underlying buffer/window sizes. We will have to deal with both cases.
    h2o_custom_generator_t* custom_generator = static_cast<h2o_custom_generator_t*>(ctx);

    http_req* request = custom_generator->request;
    http_res* response = custom_generator->response;

    std::string chunk_str(chunk.base, chunk.len);

    request->body += chunk_str;
    request->chunk_len += chunk.len;

    //LOG(INFO) << "async_req_cb, chunk.len=" << chunk.len << ", aggr_chunk_len=" << request->chunk_len
    //          << ", is_end_stream=" << is_end_stream;

    //LOG(INFO) << "request->body.size(): " << request->body.size() << ", request->chunk_len=" << request->chunk_len;

    size_t CHUNK_LIMIT = 0;
    if(request->is_http_v1()) {
        CHUNK_LIMIT = ACTIVE_STREAM_WINDOW_SIZE;
    } else {
        // For http v2, first window will include both initial request entity and as well as chunks.
        CHUNK_LIMIT = request->first_chunk_aggregate ?
                      (ACTIVE_STREAM_WINDOW_SIZE - request->_req->entity.len) : ACTIVE_STREAM_WINDOW_SIZE;
    }

    bool async_req = custom_generator->rpath->async_req;
    bool can_process_async = async_req && (request->chunk_len >= CHUNK_LIMIT);

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
        process_request(request, response, custom_generator->rpath, custom_generator->h2o_handler);
        return 0;
    }

    // we are not ready to fire the request handler, so that means we need to buffer the request further
    // this could be because we are a) dealing with a HTTP v1 request or b) a synchronous request

    if(request->is_http_v1()) {
        // http v1 callbacks fire on small chunk sizes, so fetch more to match window size of http v2 buffer
        size_t written = chunk.len;
        request->_req->proceed_req(request->_req, written, H2O_SEND_STATE_IN_PROGRESS);
    }

    if(!async_req) {
        // progress ONLY non-streaming type request body since
        // streaming requests will call proceed_req in an async fashion
        size_t written = chunk.len;
        request->_req->proceed_req(request->_req, written, H2O_SEND_STATE_IN_PROGRESS);
    }

    return 0;
}

int HttpServer::process_request(http_req* request, http_res* response, route_path *rpath,
                                const h2o_custom_req_handler_t *handler) {

    //LOG(INFO) << "process_request called";
    bool operations_path = (rpath->path_parts.size() != 0 && rpath->path_parts[0] == "operations");

    //LOG(INFO) << "operations_path: " << operations_path;

    // for writes, we delegate to replication_state to handle response
    if(!operations_path &&
       (rpath->http_method == "POST" || rpath->http_method == "PUT" ||
        rpath->http_method == "DELETE" || rpath->http_method == "PATCH")) {
        handler->http_server->get_replication_state()->write(request, response);
        return 0;
    }

    // for reads, we will invoke the request handler and handle response as well
    (rpath->handler)(*request, *response);

    if(!rpath->async_res) {
        // If a handler is marked as async res, it's responsible for sending the response itself in an async fashion
        // otherwise, we send the whole body response on behalf of the handler
        handler->http_server->send_response(request, response);
    }

    return 0;
}

void HttpServer::on_deferred_process_request(h2o_timer_t *entry) {
    h2o_custom_timer_t* custom_timer = reinterpret_cast<h2o_custom_timer_t*>(entry);
    deferred_req_res_t* deferred_req_res = static_cast<deferred_req_res_t*>(custom_timer->data);

    route_path* found_rpath = nullptr;
    deferred_req_res->server->get_route(deferred_req_res->req->route_hash, &found_rpath);
    if(found_rpath) {
        found_rpath->handler(*deferred_req_res->req, *deferred_req_res->res);
    }
}

void HttpServer::defer_processing(http_req& req, http_res& res, size_t timeout_ms) {
    if(req.defer_timer.data == nullptr) {
        auto deferred_req_res = new deferred_req_res_t{&req, &res, this};
        req.defer_timer.data = deferred_req_res;
        h2o_timer_init(&req.defer_timer.timer, on_deferred_process_request);
    }

    h2o_timer_unlink(&req.defer_timer.timer);
    h2o_timer_link(ctx.loop, timeout_ms, &req.defer_timer.timer);

    //LOG(INFO) << "defer_processing, exit_loop: " << exit_loop << ", res.await: " << res.await;

    if(exit_loop) {
        // otherwise, replication thread could be stuck waiting on a future
        res.await.notify();
    }
}

void HttpServer::send_message(const std::string & type, void* data) {
    message_dispatcher->send_message(type, data);
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

void HttpServer::send_response(http_req* request, http_res* response) {
    //LOG(INFO) << "send_response, request->_req=" << request->_req;

    if(request->_req == nullptr) {
        // indicates serialized request and response -- lifecycle must be managed here
        return destroy_request_response(request, response);
    }

    h2o_req_t* req = request->_req;
    h2o_generator_t& generator = *response->generator;

    h2o_iovec_t body = h2o_strdup(&req->pool, response->body.c_str(), SIZE_MAX);
    req->res.status = response->status_code;
    req->res.reason = http_res::get_status_reason(response->status_code);
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE,
            nullptr, response->content_type_header.c_str(), response->content_type_header.size());
    h2o_start_response(req, &generator);
    h2o_send(req, &body, 1, H2O_SEND_STATE_FINAL);
}

void HttpServer::response_abort(h2o_generator_t *generator, h2o_req_t *req) {
    LOG(INFO) << "response_abort called";
    h2o_custom_generator_t* custom_generator = reinterpret_cast<h2o_custom_generator_t*>(generator);

    custom_generator->request->_req = nullptr;
    custom_generator->response->final = true;

    // returns control back to caller (raft replication or follower forward)
    LOG(INFO) << "response_abort: fulfilling req & res proceed.";
    custom_generator->response->await.notify();
    custom_generator->request->await.notify();
}

void HttpServer::response_proceed(h2o_generator_t *generator, h2o_req_t *req) {
    //LOG(INFO) << "response_proceed called";
    h2o_custom_generator_t* custom_generator = reinterpret_cast<h2o_custom_generator_t*>(generator);

    custom_generator->response->await.notify();

    //LOG(INFO) << "proxied_stream: " << custom_generator->response->proxied_stream;
    //LOG(INFO) << "response.final: " <<  custom_generator->response->final;

    if(custom_generator->response->proxied_stream) {
        // request progression should not be tied to response generation
        //LOG(INFO) << "Ignoring request proceed";
        return ;
    }

    // if the request itself is async, we will proceed the request to fetch input content
    if (custom_generator->rpath->async_req) {
        auto stream_state = (custom_generator->response->final) ? H2O_SEND_STATE_FINAL : H2O_SEND_STATE_IN_PROGRESS;

        // `written` is ignored by http1.1 implementation, so meant only for http 2+
        size_t written = custom_generator->request->chunk_len;
        custom_generator->request->chunk_len = 0;

        if(custom_generator->request->_req->proceed_req) {
            //LOG(INFO) << "response_proceed: proceeding req";
            custom_generator->request->_req->proceed_req(custom_generator->request->_req, written, stream_state);
        }
    } else {
        // otherwise, call the handler since it will be the handler that will be producing content
        // (streaming response but not request)
        custom_generator->h2o_handler->http_server->defer_processing(*custom_generator->request,
                                                                     *custom_generator->response, 1);
    }
}

void HttpServer::stream_response(http_req& request, http_res& response) {
    //LOG(INFO) << "stream_response called";
    if(request._req == nullptr) {
        // raft log replay or when underlying request is aborted
        //LOG(INFO) << "request._req == nullptr";
        destroy_request_response(&request, &response);
        return;
    }

    h2o_req_t* req = request._req;
    h2o_custom_generator_t* custom_generator = reinterpret_cast<h2o_custom_generator_t *>(response.generator);

    if (req->res.status == 0) {
        //LOG(INFO) << "h2o_start_response, content_type=" << response.content_type_header
        //          << ",response.status_code=" << response.status_code;
        response.status_code = (response.status_code == 0) ? 503 : response.status_code; // just to be sure
        req->res.status = response.status_code;
        req->res.reason = http_res::get_status_reason(response.status_code);
        h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, NULL,
                       response.content_type_header.c_str(),
                       response.content_type_header.size());
        h2o_start_response(req, &custom_generator->super);
    }

    //LOG(INFO) << "stream_response, body_size: " << response.body.size() << ", response_final="
    //          << custom_generator->response->final;

    h2o_iovec_t body = h2o_strdup(&req->pool, response.body.c_str(), SIZE_MAX);
    response.body = "";

    const h2o_send_state_t state = custom_generator->response->final ? H2O_SEND_STATE_FINAL : H2O_SEND_STATE_IN_PROGRESS;
    h2o_send(req, &body, 1, state);
}

void HttpServer::destroy_request_response(http_req* request, http_res* response) {
    if(request->defer_timer.data != nullptr) {
        deferred_req_res_t* deferred_req_res = static_cast<deferred_req_res_t*>(request->defer_timer.data);
        h2o_timer_unlink(&request->defer_timer.timer);
        delete deferred_req_res;
    }

    /*LOG(INFO) << "destroy_request_response, response->proxied_stream=" << response->proxied_stream
              << ", request->_req=" << request->_req << ", response->await=" << &response->await;*/

    if(response->auto_dispose) {
        //LOG(INFO) << "destroy_request_response: deleting req/res";
        delete request;
        delete response;
    } else {
        // lifecycle of proxied/replicated resources are managed externally
        // we will just nullify _req to indicate that original request is dead
        //LOG(INFO) << "Ignoring request/response cleanup since auto_dispose is false.";
        response->final = true;
        request->_req = nullptr;
        response->await.notify();
    }
}

void HttpServer::set_auth_handler(bool (*handler)(std::map<std::string, std::string>& params, const route_path& rpath,
                                                  const std::string& auth_key)) {
    auth_handler = handler;
}

void HttpServer::get(const std::string & path, bool (*handler)(http_req &, http_res &), bool async_req, bool async_res) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath("GET", path_parts, handler, async_req, async_res);
    routes.emplace_back(rpath.route_hash(), rpath);
}

void HttpServer::post(const std::string & path, bool (*handler)(http_req &, http_res &), bool async_req, bool async_res) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath("POST", path_parts, handler, async_req, async_res);
    routes.emplace_back(rpath.route_hash(), rpath);
}

void HttpServer::put(const std::string & path, bool (*handler)(http_req &, http_res &), bool async_req, bool async_res) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath("PUT", path_parts, handler, async_req, async_res);
    routes.emplace_back(rpath.route_hash(), rpath);
}

void HttpServer::patch(const std::string & path, bool (*handler)(http_req &, http_res &), bool async_req, bool async_res) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath("PATCH", path_parts, handler, async_req, async_res);
    routes.emplace_back(rpath.route_hash(), rpath);
}

void HttpServer::del(const std::string & path, bool (*handler)(http_req &, http_res &), bool async_req, bool async_res) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath("DELETE", path_parts, handler, async_req, async_res);
    routes.emplace_back(rpath.route_hash(), rpath);
}

void HttpServer::on(const std::string & message, bool (*handler)(void*)) {
    message_dispatcher->on(message, handler);
}

HttpServer::~HttpServer() {
    delete message_dispatcher;

    if(ssl_refresh_timer.timer.expire_at != 0) {
        // avoid callback since it recreates timeout
        clear_timeouts({&ssl_refresh_timer.timer}, false);
    }

    if(metrics_refresh_timer.timer.expire_at != 0) {
        // avoid callback since it recreates timeout
        clear_timeouts({&metrics_refresh_timer.timer}, false);
    }

    h2o_timerwheel_run(ctx.loop->_timeouts, 9999999999999);

    h2o_context_dispose(&ctx);

    free(ctx.globalconf->server_name.base);
    ctx.globalconf->server_name.base = nullptr;

    h2o_evloop_destroy(ctx.loop);
    h2o_config_dispose(&config);

    SSL_CTX_free(accept_ctx->ssl_ctx);
    delete accept_ctx;
}

http_message_dispatcher* HttpServer::get_message_dispatcher() const {
    return message_dispatcher;
}

ReplicationState* HttpServer::get_replication_state() const {
    return replication_state;
}

bool HttpServer::is_alive() const {
    return replication_state->is_alive();
}

bool HttpServer::get_route(uint64_t hash, route_path** found_rpath) {
    for (auto& hash_route : routes) {
        if(hash_route.first == hash) {
            *found_rpath = &hash_route.second;
            return true;
        }
    }

    return false;
}

uint64_t HttpServer::node_state() const {
    return replication_state->node_state();
}

bool HttpServer::on_stream_response_message(void *data) {
    //LOG(INFO) << "on_stream_response_message";
    deferred_req_res_t* req_res = static_cast<deferred_req_res_t *>(data);
    stream_response(*req_res->req, *req_res->res);
    return true;
}

bool HttpServer::on_request_proceed_message(void *data) {
    //LOG(INFO) << "on_request_proceed_message";
    deferred_req_res_t* req_res = static_cast<deferred_req_res_t *>(data);
    auto stream_state = (req_res->req->last_chunk_aggregate) ? H2O_SEND_STATE_FINAL : H2O_SEND_STATE_IN_PROGRESS;

    size_t written = req_res->req->chunk_len;
    req_res->req->chunk_len = 0;

    if(req_res->req->_req && req_res->req->_req->proceed_req) {
        req_res->req->_req->proceed_req(req_res->req->_req, written, stream_state);
    }

    return true;
}

bool HttpServer::has_exited() const {
    return exit_loop;
}

void HttpServer::do_snapshot(const std::string& snapshot_path, http_req& req, http_res& res) {
    return replication_state->do_snapshot(snapshot_path, req, res);
}

bool HttpServer::trigger_vote() {
    return replication_state->trigger_vote();
}
