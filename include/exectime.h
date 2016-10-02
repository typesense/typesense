#pragma once
#include <iostream>
#include <chrono>

class ExecTime {
    static auto begin = std::chrono::high_resolution_clock::now();

    static void start() {
        begin = std::chrono::high_resolution_clock::now();
    }

    static void log(std::string operation) {
        long long int timeMicros = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - begin).count();
        std::cout << "Time taken for " << operation << ": " << timeMicros << "us" << std::endl;
    }
};