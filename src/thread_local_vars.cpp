#include <cstdint>
#include "thread_local_vars.h"

thread_local int64_t write_log_index = 0;
thread_local std::chrono::high_resolution_clock::time_point begin;
thread_local int64_t search_stop_ms;
thread_local bool search_cutoff = false;
