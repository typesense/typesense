#pragma once

#include <string>
#include <iostream>
#include <g3log/g3log.hpp>
#include <g3log/logworker.hpp>
#include <g3log/logmessage.hpp>

/* Custom logging levels for use with g3log */
const LEVELS ERR     {WARNING.value + 10, {"ERROR"}};

/* https://github.com/KjellKod/g3sinks/blob/master/snippets/ColorCoutSink.hpp */
struct ConsoleLoggingSink {
    enum FG_Color {YELLOW = 33, RED = 31, GREEN=32, WHITE = 97};

    void ReceiveLogMessage(g3::LogMessageMover logEntry) {
        auto level = logEntry.get()._level;

        if (level.value >= ERR.value) {
            std::cerr << "\033[" << RED << "m"
                      << logEntry.get().toString() << "\033[m" << std::flush;
        } else {
            std::cout << logEntry.get().toString() << std::flush;
        }
    }
};