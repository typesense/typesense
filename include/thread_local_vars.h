#include <chrono>

extern thread_local int64_t write_log_index;
extern thread_local std::chrono::high_resolution_clock::time_point begin;
extern thread_local int64_t search_stop_ms;
extern thread_local bool search_cutoff;