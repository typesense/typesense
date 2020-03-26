#include "http_data.h"
#include "http_server.h"
#include "string_utils.h"
#include <regex>
#include <thread>
#include <signal.h>
#include <h2o.h>
#include <iostream>
#include "raft_server.h"
#include "logger.h"

struct h2o_custom_req_handler_t {
    h2o_handler_t super;
    HttpServer* http_server;
};

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

int HttpServer::setup_ssl(const char *cert_file, const char *key_file) {
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
    SSL_CTX_set_options(accept_ctx->ssl_ctx, SSL_OP_SINGLE_ECDH_USE);

    if (SSL_CTX_use_certificate_chain_file(accept_ctx->ssl_ctx, cert_file) != 1) {
        LOG(ERROR) << "An error occurred while trying to load server certificate file:" << cert_file;
        return -1;
    }
    if (SSL_CTX_use_PrivateKey_file(accept_ctx->ssl_ctx, key_file, SSL_FILETYPE_PEM) != 1) {
        LOG(ERROR) << "An error occurred while trying to load private key file: " << key_file;
        return -1;
    }

    h2o_ssl_register_alpn_protocols(accept_ctx->ssl_ctx, h2o_http2_alpn_protocols);
    return 0;
}

int HttpServer::create_listener(void) {
    struct sockaddr_in addr;
    int fd, reuseaddr_flag = 1;

    if(!ssl_cert_path.empty() && !ssl_cert_key_path.empty()) {
        int ssl_setup_code = setup_ssl(ssl_cert_path.c_str(), ssl_cert_key_path.c_str());
        if(ssl_setup_code != 0) {
            return -1;
        }
    }

    ctx.globalconf->server_name = h2o_strdup(NULL, "", SIZE_MAX);

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

    if (create_listener() != 0) {
        LOG(ERROR) << "Failed to listen on " << listen_address << ":" << listen_port << " - " << strerror(errno);
        return 1;
    } else {
        LOG(INFO) << "Typesense has started. Ready to accept requests on port " << listen_port;
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

void HttpServer::clear_timeouts(const std::vector<h2o_timeout_t*> & timeouts) {
    for(h2o_timeout_t* timeout: timeouts) {
        while (!h2o_linklist_is_empty(&timeout->_entries)) {
            h2o_timeout_entry_t *entry = H2O_STRUCT_FROM_MEMBER(h2o_timeout_entry_t, _link, timeout->_entries.next);
            h2o_linklist_unlink(&entry->_link);
            entry->registered_at = 0;
            entry->cb(entry);
            h2o_timeout__do_post_callback(ctx.loop);
        }
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
        } else {
            query_map[key] = query_map[key] + "&&" + value;
        }
    }

    return query_map;
}

int HttpServer::find_route(const std::vector<std::string> & path_parts, const std::string & http_method, route_path** found_rpath) {
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

    return static_cast<int>(ROUTE_CODES::NOT_FOUND);
}

int HttpServer::catch_all_handler(h2o_handler_t *_self, h2o_req_t *req) {
    h2o_custom_req_handler_t *self = (h2o_custom_req_handler_t *)_self;

    const std::string & http_method = std::string(req->method.base, req->method.len);
    const std::string & path = std::string(req->path.base, req->path.len);

    std::vector<std::string> path_with_query_parts;
    StringUtils::split(path, path_with_query_parts, "?");
    const std::string & path_without_query = path_with_query_parts[0];

    std::vector<std::string> path_parts;
    StringUtils::split(path_without_query, path_parts, "/");

    h2o_iovec_t query = req->query_at != SIZE_MAX ?
                        h2o_iovec_init(req->path.base + req->query_at, req->path.len - req->query_at) :
                        h2o_iovec_init(H2O_STRLIT(""));

    std::string query_str(query.base, query.len);
    std::map<std::string, std::string> query_map = parse_query(query_str);
    const std::string & req_body = std::string(req->entity.base, req->entity.len);

    // Extract auth key from header. If that does not exist, look for a GET parameter.
    std::string auth_key_from_header = "";

    ssize_t auth_header_cursor = h2o_find_header_by_str(&req->headers, AUTH_HEADER, strlen(AUTH_HEADER), -1);
    if(auth_header_cursor != -1) {
        h2o_iovec_t & slot = req->headers.entries[auth_header_cursor].value;
        auth_key_from_header = std::string(slot.base, slot.len);
    } else if(query_map.count(AUTH_HEADER) != 0) {
        auth_key_from_header = query_map[AUTH_HEADER];
    }

    // Handle CORS
    if(self->http_server->cors_enabled) {
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

    route_path *rpath = nullptr;
    int route_hash = self->http_server->find_route(path_parts, http_method, &rpath);

    if(route_hash != -1) {
        bool authenticated = self->http_server->auth_handler(*rpath, auth_key_from_header);
        if(!authenticated) {
            return send_401_unauthorized(req);
        }

        // routes match and is an authenticated request - iterate and extract path params
        for(size_t i = 0; i < rpath->path_parts.size(); i++) {
            const std::string & path_part = rpath->path_parts[i];
            if(path_part[0] == ':') {
                query_map.emplace(path_part.substr(1), path_parts[i]);
            }
        }

        http_req* request = new http_req(req, http_method, route_hash, query_map, req_body);
        http_res* response = new http_res();

        // for writes, we defer to replication_state
        if(http_method != "GET") {
            self->http_server->get_replication_state()->write(request, response);
            return 0;
        }

        (rpath->handler)(*request, *response);

        if(!rpath->async) {
            // If a handler is marked async, it's assumed that it's responsible for sending the response itself
            // later in an async fashion by calling into the main http thread via a message
            self->http_server->send_response(request, response);
        }

        return 0;
    }

    h2o_generator_t generator = {NULL, NULL};
    h2o_iovec_t res_body = h2o_strdup(&req->pool, "{ \"message\": \"Not Found\"}", SIZE_MAX);
    req->res.status = 404;
    req->res.reason = http_res::get_status_reason(404);
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, NULL, H2O_STRLIT("application/json; charset=utf-8"));
    h2o_start_response(req, &generator);
    h2o_send(req, &res_body, 1, H2O_SEND_STATE_FINAL);

    return 0;
}

void HttpServer::send_message(const std::string & type, void* data) {
    message_dispatcher->send_message(type, data);
}

void HttpServer::send_response(http_req* request, const http_res* response) {
    h2o_req_t* req = request->_req;
    h2o_generator_t generator = {NULL, NULL};

    h2o_iovec_t body = h2o_strdup(&req->pool, response->body.c_str(), SIZE_MAX);
    req->res.status = response->status_code;
    req->res.reason = http_res::get_status_reason(response->status_code);
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, NULL, H2O_STRLIT("application/json; charset=utf-8"));
    h2o_start_response(req, &generator);
    h2o_send(req, &body, 1, H2O_SEND_STATE_FINAL);

    delete request;
    delete response;
}

int HttpServer::send_401_unauthorized(h2o_req_t *req) {
    h2o_generator_t generator = {NULL, NULL};
    std::string res_body = std::string("{\"message\": \"Forbidden - a valid `") + AUTH_HEADER +
                                       "` header must be sent.\"}";
    h2o_iovec_t body = h2o_strdup(&req->pool, res_body.c_str(), SIZE_MAX);
    req->res.status = 401;
    req->res.reason = http_res::get_status_reason(req->res.status);
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, NULL, H2O_STRLIT("application/json; charset=utf-8"));
    h2o_start_response(req, &generator);
    h2o_send(req, &body, 1, H2O_SEND_STATE_FINAL);
    return 0;
}

void HttpServer::set_auth_handler(bool (*handler)(const route_path & rpath, const std::string & auth_key)) {
    auth_handler = handler;
}

void HttpServer::get(const std::string & path, bool (*handler)(http_req &, http_res &), bool async) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath = {"GET", path_parts, handler, async};
    routes.emplace(rpath.route_hash(), rpath);
}

void HttpServer::post(const std::string & path, bool (*handler)(http_req &, http_res &), bool async) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath = {"POST", path_parts, handler, async};
    routes.emplace(rpath.route_hash(), rpath);
}

void HttpServer::put(const std::string & path, bool (*handler)(http_req &, http_res &), bool async) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath = {"PUT", path_parts, handler, async};
    routes.emplace(rpath.route_hash(), rpath);
}

void HttpServer::del(const std::string & path, bool (*handler)(http_req &, http_res &), bool async) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath = {"DELETE", path_parts, handler, async};
    routes.emplace(rpath.route_hash(), rpath);
}

void HttpServer::on(const std::string & message, bool (*handler)(void*)) {
    message_dispatcher->on(message, handler);
}

HttpServer::~HttpServer() {
    delete message_dispatcher;

    // remove all timeouts defined in: https://github.com/h2o/h2o/blob/v2.2.2/lib/core/context.c#L142
    std::vector<h2o_timeout_t*> timeouts = {
        &ctx.zero_timeout,
        &ctx.one_sec_timeout,
        &ctx.hundred_ms_timeout,
        &ctx.handshake_timeout,
        &ctx.http1.req_timeout,
        &ctx.http2.idle_timeout,
        &ctx.http2.graceful_shutdown_timeout,
        &ctx.proxy.io_timeout
    };

    clear_timeouts(timeouts);
    clear_timeouts({&ctx.zero_timeout});  // needed to clear a deferred timeout that crops up

    h2o_context_dispose(&ctx);
    free(ctx.globalconf->server_name.base);
    free(ctx.queue);
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

void HttpServer::get_route(size_t index, route_path** found_rpath) {
    if(routes.count(index) > 0) {
        *found_rpath = &routes[index];
    }
}
