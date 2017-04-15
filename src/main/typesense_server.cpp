#include "http_server.h"
#include <cmdline.h>

int main(int argc, char **argv) {
    cmdline::parser options;
    options.add<std::string>("data-dir", 'd', "Directory where data will be stored.", true);
    options.add<std::string>("listen-address", 'a', "Address to which Typesense server binds.", false, "0.0.0.0");
    options.add<uint32_t>("listen-port", 'p', "Port on which Typesense server listens.", false, 8080);
    options.parse_check(argc, argv);

    HttpServer server(options.get<std::string>("data-dir"));
    server.get("/foo", [](h2o_handler_t *self, h2o_req_t *req) {
        h2o_generator_t generator = {NULL, NULL};
        h2o_iovec_t body = h2o_strdup(&req->pool, "{\"foo\": 123}", SIZE_MAX);
        req->res.status = 200;
        req->res.reason = "OK";
        h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, H2O_STRLIT("application/json; charset=utf-8"));
        h2o_start_response(req, &generator);
        h2o_send(req, &body, 1, 1);
        return 0;
    });

    server.run();
    return 0;
}