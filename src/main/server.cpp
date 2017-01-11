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
#include "collection_manager.h"
#include <sys/resource.h>

#include "h2o.h"
#include "h2o/http1.h"
#include "h2o/http2.h"
#include "h2o/memcached.h"

static h2o_globalconf_t config;
static h2o_context_t ctx;
static h2o_accept_ctx_t accept_ctx;
std::vector<field> search_fields = {field("title", field_types::STRING)};
std::vector<std::string> rank_fields = {"points"};
Store *store = new Store("/tmp/typesense-data");

CollectionManager & collectionManager = CollectionManager::get_instance();
Collection *collection;

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
        query_map[key] = StringUtils::replace_all(value, "%20", " ");
    }

    return query_map;
}


static int get_search(h2o_handler_t *self, h2o_req_t *req) {
    static h2o_generator_t generator = {NULL, NULL};
    h2o_iovec_t query = req->query_at != SIZE_MAX ?
                        h2o_iovec_init(req->path.base + req->query_at, req->path.len - req->query_at) :
                        h2o_iovec_init(H2O_STRLIT(""));

    std::string query_str(query.base, query.len);
    std::map<std::string, std::string> query_map = parse_query(query_str);
    const char *NUM_TYPOS = "num_typos";
    const char *PREFIX = "prefix";
    const char *TOKEN_ORDERING = "token_ordering";

    if(query_map.count(NUM_TYPOS) == 0) {
        query_map[NUM_TYPOS] = "2";
    }

    if(query_map.count(PREFIX) == 0) {
        query_map[PREFIX] = "false";
    }

    if(query_map.count(TOKEN_ORDERING) == 0) {
        query_map[TOKEN_ORDERING] = "FREQUENCY";
    }

    token_ordering token_order = (query_map[TOKEN_ORDERING] == "MAX_SCORE") ? MAX_SCORE : FREQUENCY;

    //printf("Query: %s\n", query_map["q"].c_str());
    auto begin = std::chrono::high_resolution_clock::now();

    std::vector<std::string> search_fields = {"title"};

    std::vector<nlohmann::json> results = collection->search(query_map["q"], search_fields,
                                                             std::stoi(query_map[NUM_TYPOS]), 100, token_order, false);
    nlohmann::json json_array = nlohmann::json::array();
    for(nlohmann::json& result: results) {
        json_array.push_back(result);
    }

    std::string json_str = json_array.dump();
    //std::cout << "JSON:" << json_str << std::endl;
    struct rusage r_usage;
    getrusage(RUSAGE_SELF,&r_usage);

    //std::cout << "Memory usage: " << r_usage.ru_maxrss << std::endl;

    h2o_iovec_t body = h2o_strdup(&req->pool, json_str.c_str(), SIZE_MAX);
    req->res.status = 200;
    req->res.reason = "OK";
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, H2O_STRLIT("application/json; charset=utf-8"));
    h2o_start_response(req, &generator);
    h2o_send(req, &body, 1, 1);

    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    std::cout << "Time taken: " << timeMillis << "us" << std::endl;
    return 0;
}

static int post_add_document(h2o_handler_t *self, h2o_req_t *req) {
    std::string document(req->entity.base, req->entity.len);
    std::string inserted_id = collection->add(document);

    static h2o_generator_t generator = {NULL, NULL};
    req->res.status = 200;
    req->res.reason = "OK";
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, H2O_STRLIT("application/json; charset=utf-8"));
    h2o_start_response(req, &generator);

    nlohmann::json json_response;
    json_response["id"] = inserted_id;
    json_response["status"] = "SUCCESS";

    h2o_iovec_t body = h2o_strdup(&req->pool, json_response.dump().c_str(), SIZE_MAX);
    h2o_send(req, &body, 1, 1);
    return 0;
}

static int delete_remove_document(h2o_handler_t *self, h2o_req_t *req) {
    h2o_iovec_t query = req->query_at != SIZE_MAX ?
                        h2o_iovec_init(req->path.base + req->query_at, req->path.len - req->query_at) :
                        h2o_iovec_init(H2O_STRLIT(""));

    std::string query_str(query.base, query.len);
    std::map<std::string, std::string> query_map = parse_query(query_str);

    std::string doc_id = query_map["id"];

    auto begin = std::chrono::high_resolution_clock::now();
    collection->remove(doc_id);
    long long int time_micro = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - begin).count();
    std::cout << "Time taken: " << time_micro << "us" << std::endl;

    nlohmann::json json_response;
    json_response["id"] = doc_id;
    json_response["status"] = "SUCCESS";

    static h2o_generator_t generator = {NULL, NULL};
    req->res.status = 200;
    req->res.reason = "OK";
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, H2O_STRLIT("application/json; charset=utf-8"));
    h2o_start_response(req, &generator);
    h2o_iovec_t body = h2o_strdup(&req->pool, json_response.dump().c_str(), SIZE_MAX);
    h2o_send(req, &body, 1, 1);
    return 0;
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
    addr.sin_port = htons(1088);

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
//    std::ifstream infile("/Users/kishore/others/wreally/typesense/test/documents.jsonl");
    std::ifstream infile("/Users/kishore/Downloads/hnstories.jsonl");

    std::string json_line;

    while (std::getline(infile, json_line)) {
        collection->add(json_line);
    }

    infile.close();
    std::cout << "FINISHED INDEXING!" << std::endl << std::flush;
    struct rusage r_usage;
    getrusage(RUSAGE_SELF,&r_usage);

    std::cout << "Memory usage: " << r_usage.ru_maxrss << std::endl;
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    collectionManager.init(store);
    collection = collectionManager.get_collection("collection");
    if(collection == nullptr) {
        collection = collectionManager.create_collection("collection", search_fields, rank_fields);
    }

    index_documents();

    h2o_config_init(&config);
    h2o_hostconf_t *hostconf = h2o_config_register_host(&config, h2o_iovec_init(H2O_STRLIT("default")), 65535);
    register_handler(hostconf, "/add", post_add_document);
    register_handler(hostconf, "/delete", delete_remove_document);
    register_handler(hostconf, "/search", get_search);

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