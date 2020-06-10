#include <string>
#include <fstream>
#include <sstream>
#include <thread>
#include "json.hpp"

const int NUM_CPU_STATES = 10;

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

class SystemMetrics {
private:

    size_t get_idle_time(const cpu_data_t &e) {
        return e.times[S_IDLE] +
               e.times[S_IOWAIT];
    }

    size_t get_active_time(const cpu_data_t &e) {
        return e.times[S_USER] +
               e.times[S_NICE] +
               e.times[S_SYSTEM] +
               e.times[S_IRQ] +
               e.times[S_SOFTIRQ] +
               e.times[S_STEAL] +
               e.times[S_GUEST] +
               e.times[S_GUEST_NICE];
    }

    std::vector<cpu_stat_t> compute_cpu_stats(const std::vector<cpu_data_t>& cpu_data1,
                                              const std::vector<cpu_data_t>& cpu_data2) {
        std::vector<cpu_stat_t> stats;
        const size_t NUM_ENTRIES = cpu_data1.size();

        for (size_t i = 0; i < NUM_ENTRIES; ++i) {
            cpu_stat_t stat;

            const cpu_data_t &d1 = cpu_data1[i];
            const cpu_data_t &d2 = cpu_data2[i];

            const float active_time = static_cast<float>(get_active_time(d2) - get_active_time(d1));
            const float idle_time = static_cast<float>(get_idle_time(d2) - get_idle_time(d1));
            const float total_time = active_time + idle_time;

            float active_percentage = 100.f * (active_time / total_time);
            float idle_percentage = 100.f * (idle_time / total_time);

            std::stringstream active_ss;
            active_ss.setf(std::ios::fixed, std::ios::floatfield);
            active_ss.precision(2);
            active_ss << active_percentage;
            stat.active = active_ss.str();

            std::stringstream idle_ss;
            idle_ss.setf(std::ios::fixed, std::ios::floatfield);
            idle_ss.precision(2);
            idle_ss << idle_percentage;
            stat.idle = idle_ss.str();

            stats.push_back(stat);
        }

        return stats;
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

    static unsigned long linux_get_mem_available_bytes() {
        std::string token;
        std::ifstream file("/proc/meminfo");
        while(file >> token) {
            if(token == "MemAvailable:") {
                unsigned long mem_kb;
                if(file >> mem_kb) {
                    return mem_kb * 1000;
                } else {
                    return 0;
                }
            }
        }

        return 0; // nothing found
    }

public:
    void get(const std::string & data_dir_path, nlohmann::json& result);

    std::vector<cpu_stat_t> get_cpu_stats() {
        std::vector<cpu_data_t> cpu_data1;
        std::vector<cpu_data_t> cpu_data2;

        // snapshot 1
        read_cpu_data(cpu_data1);

        // 100ms pause
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // snapshot 2
        read_cpu_data(cpu_data2);

        // compute
        return compute_cpu_stats(cpu_data1, cpu_data2);
    }
};