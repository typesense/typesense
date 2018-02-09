#include <gtest/gtest.h>
#include "logger.h"

class TypesenseTestEnvironment : public testing::Environment {
public:
    virtual void SetUp() {

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
