#pragma once

#include <cstdint>
#include <string>
#include <map>
#include <vector>
#include <future>
#include "json.hpp"

#define H2O_USE_LIBUV 0
extern "C" {
    #include "h2o.h"
}

struct http_res {
    uint32_t status_code;
    std::string content_type_header;
    std::string body;
    bool final;

    http_res(): status_code(501), content_type_header("application/json; charset=utf-8"), final(true) {

    }

    static const char* get_status_reason(uint32_t status_code) {
        switch(status_code) {
            case 200: return "OK";
            case 201: return "Created";
            case 400: return "Bad Request";
            case 401: return "Unauthorized";
            case 404: return "Not Found";
            case 409: return "Conflict";
            case 422: return "Unprocessable Entity";
            case 500: return "Internal Server Error";
            default: return "";
        }
    }

    void send_200(const std::string & res_body) {
        status_code = 200;
        body = res_body;
    }

    void send_201(const std::string & res_body) {
        status_code = 201;
        body = res_body;
    }

    void send_400(const std::string & message) {
        status_code = 400;
        body = "{\"message\": \"" + message + "\"}";
    }

    void send_401(const std::string & message) {
        status_code = 400;
        body = "{\"message\": \"" + message + "\"}";
    }

    void send_403() {
        status_code = 403;
        body = "{\"message\": \"Forbidden\"}";
    }

    void send_404() {
        status_code = 404;
        body = "{\"message\": \"Not Found\"}";
    }

    void send_405(const std::string & message) {
        status_code = 405;
        body = "{\"message\": \"" + message + "\"}";
    }

    void send_409(const std::string & message) {
        status_code = 409;
        body = "{\"message\": \"" + message + "\"}";
    }

    void send_422(const std::string & message) {
        status_code = 422;
        body = "{\"message\": \"" + message + "\"}";
    }

    void send_500(const std::string & res_body) {
        status_code = 500;
        body = res_body;
    }

    void send(uint32_t code, const std::string & message) {
        status_code = code;
        body = "{\"message\": \"" + message + "\"}";
    }
};

enum class ROUTE_CODES {
    NOT_FOUND = -1,
    RETURN_EARLY = -2,
};

struct http_req {
    h2o_req_t* _req;
    std::string http_method;
    int route_index;
    std::map<std::string, std::string> params;
    std::string body;

    http_req(): route_index(-1) {}

    http_req(h2o_req_t* _req, const std::string & http_method, size_t route_index,
            const std::map<std::string, std::string> & params,
            std::string body): _req(_req), http_method(http_method), route_index(route_index),
            params(params), body(body) {}

    void deserialize(const std::string& serialized_content) {
        nlohmann::json content = nlohmann::json::parse(serialized_content);
        route_index = content["route_index"];
        body = content["body"];

        for (nlohmann::json::iterator it = content["params"].begin(); it != content["params"].end(); ++it) {
            params.emplace(it.key(), it.value());
        }

        _req = nullptr;
    }

    std::string serialize() const {
        nlohmann::json content;
        content["route_index"] = route_index;
        content["params"] = params;
        content["body"] = body;

        return content.dump();
    }
};

struct request_response {
    http_req* req;
    http_res* res;
};

struct route_path {
    std::string http_method;
    std::vector<std::string> path_parts;
    bool (*handler)(http_req &, http_res &);
    bool async;

    inline bool operator< (const route_path& rhs) const {
        return true;
    }
};

struct h2o_custom_generator_t {
    h2o_generator_t super;
    bool (*handler)(http_req* req, http_res* res, void* data);
    http_req* req;
    http_res* res;
    void* data;
};

struct h2o_custom_res_message_t {
    h2o_multithread_message_t super;
    std::map<std::string, bool (*)(void*)> *message_handlers;
    std::string type;
    void* data;
};

struct http_message_dispatcher {
    h2o_multithread_queue_t* message_queue;
    h2o_multithread_receiver_t* message_receiver;
    std::map<std::string, bool (*)(void*)> message_handlers;

    void init(h2o_loop_t *loop) {
        message_queue = h2o_multithread_create_queue(loop);
        message_receiver = new h2o_multithread_receiver_t();
        h2o_multithread_register_receiver(message_queue, message_receiver, on_message);
    }

    ~http_message_dispatcher() {
        // drain existing messages
        on_message(message_receiver, &message_receiver->_messages);

        h2o_multithread_unregister_receiver(message_queue, message_receiver);
        h2o_multithread_destroy_queue(message_queue);
        free(message_queue);
        delete message_receiver;
    }

    static void on_message(h2o_multithread_receiver_t *receiver, h2o_linklist_t *messages) {
        while (!h2o_linklist_is_empty(messages)) {
            h2o_multithread_message_t *message = H2O_STRUCT_FROM_MEMBER(h2o_multithread_message_t, link, messages->next);
            h2o_custom_res_message_t *custom_message = reinterpret_cast<h2o_custom_res_message_t*>(message);

            if(custom_message->message_handlers->count(custom_message->type) != 0) {
                auto handler = custom_message->message_handlers->at(custom_message->type);
                (handler)(custom_message->data);
            }

            h2o_linklist_unlink(&message->link);
            delete custom_message;
        }
    }

    void send_message(const std::string & type, void* data) {
        h2o_custom_res_message_t* message = new h2o_custom_res_message_t{{{NULL, NULL}}, &message_handlers, type.c_str(), data};
        h2o_multithread_send_message(message_receiver, &message->super);
    }

    void on(const std::string & message, bool (*handler)(void*)) {
        message_handlers.emplace(message, handler);
    }
};

struct AsyncIndexArg {
    http_req* req;
    http_res* res;
    std::promise<bool>* promise;
};
