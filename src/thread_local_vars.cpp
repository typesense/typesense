#include <cstdint>
#include "thread_local_vars.h"

thread_local int64_t write_log_index = 0;
thread_local uint64_t search_begin_us;
thread_local uint64_t search_stop_us;
thread_local bool search_cutoff = false;
