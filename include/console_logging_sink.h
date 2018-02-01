#pragma once

#include <string>
#include <iostream>
#include <g3log/logmessage.hpp>

// https://github.com/KjellKod/g3sinks/blob/master/snippets/ColorCoutSink.hpp

struct ConsoleLoggingSink {
    enum FG_Color {YELLOW = 33, RED = 31, GREEN=32, WHITE = 97};

    void ReceiveLogMessage(g3::LogMessageMover logEntry) {
        auto level = logEntry.get()._level;

        if (g3::internal::wasFatal(level)) {
            std::cerr << "\033[" << RED << "m"
                      << logEntry.get().toString() << "\033[m";
        } else {
            std::cout << logEntry.get().toString();
        }
    }
};