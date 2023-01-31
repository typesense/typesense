#include <chrono>

extern thread_local int64_t write_log_index;

// These are used for circuit breaking search requests
// NOTE: if you fork off main search thread, care must be taken to initialize these from parent thread values
extern thread_local uint64_t search_begin_us;
extern thread_local uint64_t search_stop_us;
extern thread_local bool search_cutoff;