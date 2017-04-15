#include "http_server.h"

h2o_globalconf_t HttpServer::config;
h2o_context_t HttpServer::ctx;
h2o_accept_ctx_t HttpServer::accept_ctx;
std::map<std::string, int (*)(h2o_handler_t *, h2o_req_t *)> HttpServer::route_map;

HttpServer::HttpServer(const std::string & data_dir): data_dir(data_dir) {
    h2o_config_init(&config);
    hostconf = h2o_config_register_host(&config, h2o_iovec_init(H2O_STRLIT("default")), 65535);
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

    sock = h2o_evloop_socket_create(ctx.loop, fd, H2O_SOCKET_FLAG_DONT_READ);
    h2o_socket_read_start(sock, on_accept);

    return 0;
}

int HttpServer::run() {
    store = new Store(data_dir);
    collectionManager.init(store);

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

int HttpServer::catch_all_handler(h2o_handler_t *self, h2o_req_t *req) {
    std::string http_method = std::string(req->method.base, req->method.len);
    const std::string & route_key = http_method + "_" + std::string(req->path.base, req->path.len);
    if(route_map.count(route_key) == 0) {
        // send 404
        return 1;
    }

    (route_map[route_key])(self, req);

    return 0;
}

void HttpServer::get(const std::string & path, int (*handler)(h2o_handler_t *, h2o_req_t *)) {
    route_map.emplace(std::string("GET") + "_" + path, handler);
    register_handler(hostconf, path.c_str(), catch_all_handler);
}

void HttpServer::post() {

}

void HttpServer::put() {

}

void HttpServer::del() {

}

HttpServer::~HttpServer() {
    delete store;
}