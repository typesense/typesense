#include <gtest/gtest.h>
#include <butil/at_exit.h>
#include "logger.h"

class TypesenseTestEnvironment : public testing::Environment {
public:
    virtual void SetUp() {

    }

    virtual void TearDown() {

    }
};

int main(int argc, char **argv) {
    // required by brpc servers started in tests, as in the typesense server
    butil::AtExitManager exit_manager;

    ::testing::InitGoogleTest(&argc, argv);
    ::testing::AddGlobalTestEnvironment(new TypesenseTestEnvironment);
    int exitCode = RUN_ALL_TESTS();
    return exitCode;
}
