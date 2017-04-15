#define H2O_USE_LIBUV 0

extern "C" {
    #include "h2o.h"
    #include "h2o/http1.h"
    #include "h2o/http2.h"
}

#include <map>
#include <string>
#include <stdio.h>
#include "collection.h"
#include "collection_manager.h"

struct http_res {
    uint32_t status;
    std::string body;
};

class HttpServer {
private:
    static h2o_globalconf_t config;
    static h2o_context_t ctx;
    static h2o_accept_ctx_t accept_ctx;
    static std::map<std::string, int (*)(h2o_handler_t *, h2o_req_t *)> route_map;

    h2o_hostconf_t *hostconf;
    const std::string data_dir;

    Store *store = nullptr;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection *collection;

    static void on_accept(h2o_socket_t *listener, const char *err);

    int create_listener();

    h2o_pathconf_t *register_handler(h2o_hostconf_t *hostconf, const char *path,
                                     int (*on_req)(h2o_handler_t *, h2o_req_t *));

    static int catch_all_handler(h2o_handler_t *self, h2o_req_t *req);

public:
    HttpServer(const std::string & data_dir);

    ~HttpServer();

    void get(const std::string & path, int (*handler)(h2o_handler_t *, h2o_req_t *));

    void post();

    void put();

    void del();

    int run();
};