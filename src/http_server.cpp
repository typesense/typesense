#include "http_data.h"
#include "http_server.h"
#include "string_utils.h"
#include <regex>
#include <thread>
#include <signal.h>
#include <h2o.h>

struct h2o_custom_req_handler_t {
    h2o_handler_t super;
    HttpServer* http_server;
};

struct h2o_custom_res_message_t {
    h2o_multithread_message_t super;
    HttpServer* http_server;
    std::string type;
    void* data;
};

HttpServer::HttpServer(std::string listen_address, uint32_t listen_port):
                       listen_address(listen_address), listen_port(listen_port) {
    accept_ctx = new h2o_accept_ctx_t();
    h2o_config_init(&config);
    hostconf = h2o_config_register_host(&config, h2o_iovec_init(H2O_STRLIT("default")), 65535);
    register_handler(hostconf, "/", catch_all_handler);
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

int HttpServer::create_listener(void) {
    struct sockaddr_in addr;
    int fd, reuseaddr_flag = 1;

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

    ctx.globalconf->server_name = h2o_strdup(NULL, "", SIZE_MAX);
    h2o_socket_t *listener = h2o_evloop_socket_create(ctx.loop, fd, H2O_SOCKET_FLAG_DONT_READ);
    listener->data = this;
    h2o_socket_read_start(listener, on_accept);

    return 0;
}

int HttpServer::run() {
    signal(SIGPIPE, SIG_IGN);
    h2o_context_init(&ctx, h2o_evloop_create(), &config);

    message_queue = h2o_multithread_create_queue(ctx.loop);
    message_receiver = new h2o_multithread_receiver_t();
    h2o_multithread_register_receiver(message_queue, message_receiver, on_message);

    if (create_listener() != 0) {
        std::cerr << "Failed to listen on " << listen_address << ":" << listen_port << std::endl
                  << "Error: " << strerror(errno) << std::endl;
        return 1;
    }

    while (h2o_evloop_run(ctx.loop, INT32_MAX) == 0);

    return 0;
}

void HttpServer::on_message(h2o_multithread_receiver_t *receiver, h2o_linklist_t *messages) {
    while (!h2o_linklist_is_empty(messages)) {
        h2o_generator_t generator = {NULL, NULL};
        h2o_multithread_message_t *message = H2O_STRUCT_FROM_MEMBER(h2o_multithread_message_t, link, messages->next);
        h2o_custom_res_message_t *custom_message = reinterpret_cast<h2o_custom_res_message_t*>(message);

        if(custom_message->http_server->message_handlers.count(custom_message->type) != 0) {
            auto handler = custom_message->http_server->message_handlers.at(custom_message->type);
            (handler)(custom_message->data);
        }

        h2o_linklist_unlink(&message->link);
        delete custom_message;
    }
}

h2o_pathconf_t* HttpServer::register_handler(h2o_hostconf_t *hostconf, const char *path,
                                 int (*on_req)(h2o_handler_t *, h2o_req_t *)) {
    // See: https://github.com/h2o/h2o/issues/181#issuecomment-75393049
    h2o_pathconf_t *pathconf = h2o_config_register_path(hostconf, path, 0);
    h2o_custom_req_handler_t *handler = reinterpret_cast<h2o_custom_req_handler_t*>(h2o_create_handler(pathconf, sizeof(*handler)));
    handler->http_server = this;
    handler->super.on_req = on_req;
    return pathconf;
}

const char* HttpServer::get_status_reason(uint32_t status_code) {
    switch(status_code) {
        case 200: return "OK";
        case 201: return "Created";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 409: return "Conflict";
        case 422: return "Unprocessable Entity";
        case 500: return "Internal Server Error";
        default: return "";
    }
}


std::map<std::string, std::string> HttpServer::parse_query(const std::string& query) {
    std::map<std::string, std::string> query_map;
    std::regex pattern("([\\w+%]+)=([^&]*)");

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

    for(const route_path & rpath: self->http_server->routes) {
        if(rpath.path_parts.size() != path_parts.size() || rpath.http_method != http_method) {
            continue;
        }

        bool found = true;

        for(size_t i = 0; i < rpath.path_parts.size(); i++) {
            const std::string & rpart = rpath.path_parts[i];
            const std::string & given_part = path_parts[i];
            if(rpart != given_part && rpart[0] != ':') {
                found = false;
                goto check_next_route;
            }
        }

        check_next_route:

        if(found) {
            // routes match - iterate and extract path params
            for(size_t i = 0; i < rpath.path_parts.size(); i++) {
                const std::string & path_part = rpath.path_parts[i];
                if(path_part[0] == ':') {
                    query_map.emplace(path_part.substr(1), path_parts[i]);
                }
            }

            if(rpath.authenticated) {
                CollectionManager & collectionManager = CollectionManager::get_instance();
                ssize_t auth_header_cursor = h2o_find_header_by_str(&req->headers, AUTH_HEADER,
                                                                    strlen(AUTH_HEADER), -1);

                if(auth_header_cursor == -1) {
                    // requires authentication, but API Key is not present in the headers
                    return send_401_unauthorized(req);
                } else {
                    // api key is found, let's validate
                    h2o_iovec_t & slot = req->headers.entries[auth_header_cursor].value;
                    std::string auth_key_from_header = std::string(slot.base, slot.len);

                    if(!collectionManager.auth_key_matches(auth_key_from_header)) {
                        return send_401_unauthorized(req);
                    }
                }
            }

            http_req* request = new http_req{req, query_map, req_body};
            http_res* response = new http_res();
            response->server = self->http_server;
            (rpath.handler)(*request, *response);

            if(!rpath.async) {
                // If a handler is marked async, it's assumed that it's responsible for sending the response itself
                // later in an async fashion by calling into the main http thread via a message
                self->http_server->send_response(request, response);
            }

            return 0;
        }
    }

    h2o_generator_t generator = {NULL, NULL};
    h2o_iovec_t res_body = h2o_strdup(&req->pool, "{ \"message\": \"Not Found\"}", SIZE_MAX);
    req->res.status = 404;
    req->res.reason = get_status_reason(404);
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, NULL, H2O_STRLIT("application/json; charset=utf-8"));
    h2o_start_response(req, &generator);
    h2o_send(req, &res_body, 1, H2O_SEND_STATE_FINAL);

    return 0;
}

void HttpServer::send_message(const std::string & type, void* data) {
    h2o_custom_res_message_t* message = new h2o_custom_res_message_t{{{NULL}}, this, type.c_str(), data};
    h2o_multithread_send_message(message_receiver, &message->super);
}

void HttpServer::send_response(http_req* request, const http_res* response) {
    h2o_req_t* req = request->_req;
    h2o_generator_t generator = {NULL, NULL};

    h2o_iovec_t body = h2o_strdup(&req->pool, response->body.c_str(), SIZE_MAX);
    req->res.status = response->status_code;
    req->res.reason = get_status_reason(response->status_code);
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, NULL, H2O_STRLIT("application/json; charset=utf-8"));
    h2o_start_response(req, &generator);
    h2o_send(req, &body, 1, H2O_SEND_STATE_FINAL);

    delete request;
    delete response;
}

int HttpServer::send_401_unauthorized(h2o_req_t *req) {
    h2o_generator_t generator = {NULL, NULL};
    std::string res_body = std::string("{\"message\": \"Forbidden - ") + AUTH_HEADER + " header is invalid or not present.\"}";
    h2o_iovec_t body = h2o_strdup(&req->pool, res_body.c_str(), SIZE_MAX);
    req->res.status = 401;
    req->res.reason = get_status_reason(req->res.status);
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, NULL, H2O_STRLIT("application/json; charset=utf-8"));
    h2o_start_response(req, &generator);
    h2o_send(req, &body, 1, H2O_SEND_STATE_FINAL);
    return 0;
}

void HttpServer::get(const std::string & path, void (*handler)(http_req &, http_res &), bool authenticated, bool async) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath = {"GET", path_parts, handler, authenticated, async};
    routes.push_back(rpath);
}

void HttpServer::post(const std::string & path, void (*handler)(http_req &, http_res &), bool authenticated, bool async) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath = {"POST", path_parts, handler, authenticated, async};
    routes.push_back(rpath);
}

void HttpServer::put(const std::string & path, void (*handler)(http_req &, http_res &), bool authenticated, bool async) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath = {"PUT", path_parts, handler, authenticated, async};
    routes.push_back(rpath);
}

void HttpServer::del(const std::string & path, void (*handler)(http_req &, http_res &), bool authenticated, bool async) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath = {"DELETE", path_parts, handler, authenticated, async};
    routes.push_back(rpath);
}


void HttpServer::on(const std::string & message, void (*handler)(void*)) {
    message_handlers.emplace(message, handler);
}

HttpServer::~HttpServer() {
    delete accept_ctx;
    delete message_receiver;
}