#include "http_server.h"
#include "string_utils.h"
#include <regex>
#include <signal.h>

h2o_globalconf_t HttpServer::config;
h2o_context_t HttpServer::ctx;
h2o_accept_ctx_t HttpServer::accept_ctx;
std::vector<route_path> HttpServer::routes;

HttpServer::HttpServer() {
    h2o_config_init(&config);
    hostconf = h2o_config_register_host(&config, h2o_iovec_init(H2O_STRLIT("default")), 65535);
    register_handler(hostconf, "/", catch_all_handler);
}

void HttpServer::on_accept(h2o_socket_t *listener, const char *err) {
    h2o_socket_t *sock;

    if (err != NULL) {
        return;
    }

    if ((sock = h2o_evloop_socket_accept(listener)) == NULL) {
        return;
    }

    h2o_accept(&accept_ctx, sock);
}

int HttpServer::create_listener(void) {
    struct sockaddr_in addr;
    int fd, reuseaddr_flag = 1;
    h2o_socket_t *sock;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY); //htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(8088);

    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) == -1 ||
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuseaddr_flag, sizeof(reuseaddr_flag)) != 0 ||
        bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
        listen(fd, SOMAXCONN) != 0) {
        return -1;
    }

    ctx.globalconf->server_name = h2o_strdup(NULL, "", SIZE_MAX);
    sock = h2o_evloop_socket_create(ctx.loop, fd, H2O_SOCKET_FLAG_DONT_READ);
    h2o_socket_read_start(sock, on_accept);

    return 0;
}

int HttpServer::run() {

    signal(SIGPIPE, SIG_IGN);
    h2o_context_init(&ctx, h2o_evloop_create(), &config);

    accept_ctx.ctx = &ctx;
    accept_ctx.hosts = config.hosts;

    if (create_listener() != 0) {
        fprintf(stderr, "failed to listen to 127.0.0.1:1088:%s\n", strerror(errno));
        return 1;
    }

    while (h2o_evloop_run(ctx.loop) == 0);

    return 0;
}

h2o_pathconf_t* HttpServer::register_handler(h2o_hostconf_t *hostconf, const char *path,
                                 int (*on_req)(h2o_handler_t *, h2o_req_t *)) {
    h2o_pathconf_t *pathconf = h2o_config_register_path(hostconf, path, 0);
    h2o_handler_t *handler = h2o_create_handler(pathconf, sizeof(*handler));
    handler->on_req = on_req;
    return pathconf;
}

const char* HttpServer::get_status_reason(uint32_t status_code) {
    switch(status_code) {
        case 200: return "OK";
        case 201: return "Created";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 409: return "Conflict";
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
        if(query_map.count(value) == 0) {
            query_map[key] = value;
        } else {
            query_map[key] = query_map[key] + "&&" + value;
        }
    }

    return query_map;
}

int HttpServer::catch_all_handler(h2o_handler_t *self, h2o_req_t *req) {
    const std::string & http_method = std::string(req->method.base, req->method.len);
    const std::string & path = std::string(req->path.base, req->path.len);
    h2o_generator_t generator = {NULL, NULL};

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

    for(const route_path & rpath: routes) {
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

            http_req request = {query_map, req_body};
            http_res response;
            (rpath.handler)(request, response);

            h2o_iovec_t body = h2o_strdup(&req->pool, response.body.c_str(), SIZE_MAX);
            req->res.status = response.status_code;
            req->res.reason = get_status_reason(response.status_code);
            h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, H2O_STRLIT("application/json; charset=utf-8"));
            h2o_start_response(req, &generator);
            h2o_send(req, &body, 1, 1);

            return 0;
        }
    }

    h2o_iovec_t res_body = h2o_strdup(&req->pool, "{ \"message\": \"Not Found\"}", SIZE_MAX);
    req->res.status = 404;
    req->res.reason = get_status_reason(404);
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, H2O_STRLIT("application/json; charset=utf-8"));
    h2o_start_response(req, &generator);
    h2o_send(req, &res_body, 1, 1);

    return 0;
}

void HttpServer::get(const std::string & path, void (*handler)(http_req &, http_res &)) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath = {"GET", path_parts, handler};
    routes.push_back(rpath);
}

void HttpServer::post(const std::string & path, void (*handler)(http_req &, http_res &)) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath = {"POST", path_parts, handler};
    routes.push_back(rpath);
}

void HttpServer::put(const std::string & path, void (*handler)(http_req &, http_res &)) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath = {"PUT", path_parts, handler};
    routes.push_back(rpath);
}

void HttpServer::del(const std::string & path, void (*handler)(http_req &, http_res &)) {
    std::vector<std::string> path_parts;
    StringUtils::split(path, path_parts, "/");
    route_path rpath = {"DELETE", path_parts, handler};
    routes.push_back(rpath);
}

HttpServer::~HttpServer() {

}