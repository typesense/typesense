#pragma once
#include <iostream>
#include <chrono>
#include "logger.h"

class ExecTime {
    static auto begin = std::chrono::high_resolution_clock::now();

    static void start() {
        begin = std::chrono::high_resolution_clock::now();
    }

    static void log(std::string operation) {
        long long int timeMicros = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - begin).count();
        LOG(INFO) << "Time taken for " << operation << ": " << timeMicros << "us";
    }
};