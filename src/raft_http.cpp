#include "raft_http.h"
#include <zlib.h>
#include <logger.h>

// Raft HTTP Request Processing Module

namespace raft {
    namespace http {

Option<bool> handle_gzip(const std::shared_ptr<http_req>& request) {
    if (!request->zstream_initialized) {
        request->zs.zalloc = Z_NULL;
        request->zs.zfree = Z_NULL;
        request->zs.opaque = Z_NULL;
        request->zs.avail_in = 0;
        request->zs.next_in = Z_NULL;

        if (inflateInit2(&request->zs, 16 + MAX_WBITS) != Z_OK) {
            return Option<bool>(400, "inflateInit failed while decompressing");
        }

        request->zstream_initialized = true;
    }

    std::string outbuffer;
    outbuffer.resize(10 * request->body.size());

    request->zs.next_in = (Bytef *) request->body.c_str();
    request->zs.avail_in = request->body.size();
    std::size_t size_uncompressed = 0;
    int ret = 0;
    do {
        request->zs.avail_out = static_cast<unsigned int>(outbuffer.size());
        request->zs.next_out = reinterpret_cast<Bytef *>(&outbuffer[0] + size_uncompressed);
        ret = inflate(&request->zs, Z_FINISH);
        if (ret != Z_STREAM_END && ret != Z_OK && ret != Z_BUF_ERROR) {
            std::string error_msg = request->zs.msg;
            inflateEnd(&request->zs);
            return Option<bool>(400, error_msg);
        }

        size_uncompressed += (outbuffer.size() - request->zs.avail_out);
    } while (request->zs.avail_out == 0);

    if (ret == Z_STREAM_END) {
        request->zstream_initialized = false;
        inflateEnd(&request->zs);
    }

    outbuffer.resize(size_uncompressed);

    request->body = outbuffer;
    request->chunk_len = outbuffer.size();

    return Option<bool>(true);
}

    } // namespace http
} // namespace raft
