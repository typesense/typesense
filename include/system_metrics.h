#include <string>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <fstream>
#include <sstream>
#include <thread>
#include <sys/stat.h>
#include "json.hpp"

const int NUM_CPU_STATES = 10;
const int NUM_NETWORK_STATS = 16;

struct cpu_data_t {
    std::string cpu;
    size_t times[NUM_CPU_STATES];
};

enum CPUStates {
    S_USER = 0,
    S_NICE,
    S_SYSTEM,
    S_IDLE,
    S_IOWAIT,
    S_IRQ,
    S_SOFTIRQ,
    S_STEAL,
    S_GUEST,
    S_GUEST_NICE
};

struct cpu_stat_t {
    std::string active;
    std::string idle;
};

struct mallctl_stats_t {
    size_t memory_mapped_bytes = 1;
    size_t memory_retained_bytes = 1;
    size_t memory_active_bytes = 1;
    size_t memory_metadata_bytes = 1;
};

class SystemMetrics {
private:

    const uint64_t MALLCTL_STATS_UPDATE_INTERVAL_SECONDS = 5;
    std::atomic<uint64_t> mallctl_stats_last_access = 0;
    mallctl_stats_t mallctl_stats;

    mutable std::shared_mutex mutex;

    size_t _get_idle_time(const cpu_data_t &e) {
        // we will consider iowait as cpu being idle
        return e.times[S_IDLE] +
               e.times[S_IOWAIT];
    }

    size_t get_total_time(const cpu_data_t &e) {
        return e.times[S_USER] +
               e.times[S_NICE] +
               e.times[S_SYSTEM] +
               e.times[S_IDLE] +
               e.times[S_IOWAIT] +
               e.times[S_IRQ] +
               e.times[S_SOFTIRQ] +
               e.times[S_STEAL];
    }

    // https://stackoverflow.com/a/52173118/131050
    size_t get_active_time(const cpu_data_t &e) {
        return get_total_time(e) - _get_idle_time(e);
    }

    std::vector<cpu_stat_t> compute_cpu_stats(const std::vector<cpu_data_t>& cpu_data_prev,
                                              const std::vector<cpu_data_t>& cpu_data_now) {
        std::vector<cpu_stat_t> stats;
        const size_t NUM_ENTRIES = cpu_data_prev.size();

        for (size_t i = 0; i < NUM_ENTRIES; ++i) {
            const cpu_data_t &prev = cpu_data_prev[i];
            const cpu_data_t &now = cpu_data_now[i];

            auto prev_active = get_active_time(prev);
            auto now_active = get_active_time(now);

            auto prev_total = get_total_time(prev);
            auto now_total = get_total_time(now);

            auto total_diff = float(now_total - prev_total);
            auto active_diff = float(now_active - prev_active);

            // take care to avoid division by zero!
            float active_percentage = (now_total == prev_total) ? 0 : ((active_diff / total_diff) * 100);
            float idle_percentage = 100 - active_percentage;

            cpu_stat_t stat;
            stat.active = format_dp(active_percentage);
            stat.idle = format_dp(idle_percentage);
            stats.push_back(stat);
        }

        return stats;
    }

    std::string format_dp(float value) const {
        std::stringstream active_ss;
        active_ss.setf(std::ios::fixed, std::ios::floatfield);
        active_ss.precision(2);
        active_ss << value;
        return active_ss.str();
    }

    void read_cpu_data(std::vector<cpu_data_t> &entries) {
        std::ifstream stat_file("/proc/stat");

        std::string line;

        const std::string STR_CPU("cpu");
        const std::string STR_TOT("tot");

        while (std::getline(stat_file, line)) {
            // cpu stats line found
            if (!line.compare(0, STR_CPU.size(), STR_CPU)) {
                std::istringstream ss(line);

                // store entry
                entries.emplace_back(cpu_data_t());
                cpu_data_t &entry = entries.back();

                // read cpu label
                ss >> entry.cpu;

                if (entry.cpu.size() > STR_CPU.size()) {
                    entry.cpu.erase(0, STR_CPU.size());
                } else {
                    entry.cpu = STR_TOT;
                }

                // read times
                for (int i = 0; i < NUM_CPU_STATES; ++i) {
                    ss >> entry.times[i];
                }
            }
        }
    }

    mallctl_stats_t get_cached_mallctl_stats();

    SystemMetrics() {}

    ~SystemMetrics() {}

public:

    static SystemMetrics &get_instance() {
        static SystemMetrics instance;
        return instance;
    }

    SystemMetrics(SystemMetrics const &) = delete;

    void operator=(SystemMetrics const &) = delete;

    uint64_t get_proc_memory_active_bytes();

    void linux_get_network_data(const std::string & stat_path, uint64_t& received_bytes, uint64_t& sent_bytes);

    void get(const std::string & data_dir_path, nlohmann::json& result);

    std::vector<cpu_stat_t> get_cpu_stats() {
        // snapshot 1
        std::vector<cpu_data_t> cpu_data_prev;
        read_cpu_data(cpu_data_prev);

        // 100ms pause
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // snapshot 2
        std::vector<cpu_data_t> cpu_data_now;
        read_cpu_data(cpu_data_now);

        // compute
        return compute_cpu_stats(cpu_data_prev, cpu_data_now);
    }

    void get_proc_meminfo(uint64_t& memory_total_bytes, uint64_t& memory_available_bytes,
                          uint64_t& swap_total_bytes, uint64_t& swap_free_bytes);

    uint64_t get_memory_total_bytes();

    uint64_t get_memory_used_bytes();

    uint64_t get_cached_jemalloc_unused_memory();
};