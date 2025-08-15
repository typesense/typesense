#pragma once

#include <memory>
#include "http_data.h"
#include "option.h"

/**
 * Namespace for HTTP-related utilities
 */
namespace raft_http {

    /**
     * Handle gzip compression/decompression for incoming requests.
     * Modifies the request body in-place if gzip is detected.
     *
     * @param request The HTTP request to process
     * @return Success(true) or Error with status code and message
     */
    Option<bool> handle_gzip(const std::shared_ptr<http_req>& request);

}
