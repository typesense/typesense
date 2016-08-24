#define H2O_USE_LIBUV 0

#include <errno.h>
#include <limits.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <regex>
#include "string_utils.h"
#include "collection.h"
#include "json.hpp"

#include "h2o.h"
#include "h2o/http1.h"
#include "h2o/http2.h"
#include "h2o/memcached.h"

static h2o_globalconf_t config;
static h2o_context_t ctx;
static h2o_accept_ctx_t accept_ctx;
static Collection *collection = new Collection();


static h2o_pathconf_t *register_handler(h2o_hostconf_t *hostconf, const char *path,
                                        int (*on_req)(h2o_handler_t *, h2o_req_t *)) {
    h2o_pathconf_t *pathconf = h2o_config_register_path(hostconf, path, 0);
    h2o_handler_t *handler = h2o_create_handler(pathconf, sizeof(*handler));
    handler->on_req = on_req;
    return pathconf;
}

std::map<std::string, std::string> parse_query(const std::string& query) {
    std::map<std::string, std::string> query_map;
    std::regex pattern("([\\w+%]+)=([^&]*)");

    auto words_begin = std::sregex_iterator(query.begin(), query.end(), pattern);
    auto words_end = std::sregex_iterator();

    for (std::sregex_iterator i = words_begin; i != words_end; i++) {
        std::string key = (*i)[1].str();
        std::string value = (*i)[2].str();
        query_map[key] = value;
    }

    return query_map;
}


static int chunked_test(h2o_handler_t *self, h2o_req_t *req) {
    /*
        nlohmann::json_j;

        // add a number that is stored as double (note the implicit conversion of j to an object)
        j["pi"] = 3.141;

        // add a Boolean that is stored as bool
        j["happy"] = true;
     */

    static h2o_generator_t generator = {NULL, NULL};

    if (!h2o_memis(req->method.base, req->method.len, H2O_STRLIT("GET"))) {
        return -1;
    }

    h2o_iovec_t query = req->query_at != SIZE_MAX ?
                        h2o_iovec_init(req->path.base + req->query_at, req->path.len - req->query_at) :
                        h2o_iovec_init(H2O_STRLIT(""));

    printf("Query: %.*s\n", (int) query.len, query.base);

    std::string query_str(query.base, query.len);
    std::map<std::string, std::string> query_map = parse_query(query_str);

    auto begin = std::chrono::high_resolution_clock::now();
    collection->search(query_map["q"], 100);
    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    std::cout << "Time taken: " << timeMillis << "us" << std::endl;

    h2o_iovec_t body = h2o_strdup(&req->pool, "hello world\n", SIZE_MAX);
    req->res.status = 200;
    req->res.reason = "OK";
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, H2O_STRLIT("text/plain"));
    h2o_start_response(req, &generator);
    h2o_send(req, &body, 1, 1);

    return 0;
}

static int post_test(h2o_handler_t *self, h2o_req_t *req) {
    if (h2o_memis(req->method.base, req->method.len, H2O_STRLIT("POST")) &&
        h2o_memis(req->path_normalized.base, req->path_normalized.len, H2O_STRLIT("/post-test/"))) {
        static h2o_generator_t generator = {NULL, NULL};
        req->res.status = 200;
        req->res.reason = "OK";
        h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, H2O_STRLIT("text/plain; charset=utf-8"));
        h2o_start_response(req, &generator);
        h2o_send(req, &req->entity, 1, 1);
        return 0;
    }

    return -1;
}

static void on_accept(h2o_socket_t *listener, const char *err) {
    h2o_socket_t *sock;

    if (err != NULL) {
        return;
    }

    if ((sock = h2o_evloop_socket_accept(listener)) == NULL)
        return;
    h2o_accept(&accept_ctx, sock);
}

static int create_listener(void) {
    struct sockaddr_in addr;
    int fd, reuseaddr_flag = 1;
    h2o_socket_t *sock;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(0x7f000001);
    addr.sin_port = htons(7890);

    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) == -1 ||
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuseaddr_flag, sizeof(reuseaddr_flag)) != 0 ||
        bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(fd, SOMAXCONN) != 0) {
        return -1;
    }

    sock = h2o_evloop_socket_create(ctx.loop, fd, H2O_SOCKET_FLAG_DONT_READ);
    h2o_socket_read_start(sock, on_accept);

    return 0;
}

void index_documents() {
    std::ifstream infile("/Users/kishore/others/wreally/typesense/test/documents.txt");
    //std::ifstream infile("/Users/kishore/Downloads/hnstories.tsv");

    std::string line;

    while (std::getline(infile, line)) {
        std::vector<std::string> parts;
        StringUtils::tokenize(line, parts, "\t", true);
        line = StringUtils::replace_all(line, "\"", "");

        std::vector<std::string> tokens;
        StringUtils::tokenize(parts[0], tokens, " ", true);

        if(parts.size() != 2) continue;
        collection->add(tokens, stoi(parts[1]));
    }

    std::cout << "FINISHED INDEXING!" << std::endl << std::flush;
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    index_documents();

    h2o_config_init(&config);
    h2o_hostconf_t *hostconf = h2o_config_register_host(&config, h2o_iovec_init(H2O_STRLIT("default")), 65535);
    register_handler(hostconf, "/post-test", post_test);
    register_handler(hostconf, "/search", chunked_test);
    h2o_file_register(h2o_config_register_path(hostconf, "/", 0), "examples/doc_root", NULL, NULL, 0);

    h2o_context_init(&ctx, h2o_evloop_create(), &config);

    accept_ctx.ctx = &ctx;
    accept_ctx.hosts = config.hosts;

    if (create_listener() != 0) {
        fprintf(stderr, "failed to listen to 127.0.0.1:7890:%s\n", strerror(errno));
        return 1;
    }

    while (h2o_evloop_run(ctx.loop) == 0);

    delete collection;
    return 0;
}