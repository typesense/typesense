#include <gtest/gtest.h>
#include "logger.h"

class TypesenseTestEnvironment : public testing::Environment {
public:
    virtual void SetUp() {
        auto log_worker = g3::LogWorker::createLogWorker();
        auto sink_handle = log_worker->addSink(std2::make_unique<ConsoleLoggingSink>(),
                                               &ConsoleLoggingSink::ReceiveLogMessage);
        g3::initializeLogging(log_worker.get());
    }

    virtual void TearDown() {

    }
};

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::AddGlobalTestEnvironment(new TypesenseTestEnvironment);
    int exitCode = RUN_ALL_TESTS();
    return exitCode;
}
