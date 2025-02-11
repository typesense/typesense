#!/bin/bash
set -e

# Set default target if not specified
TYPESENSE_TARGET=${TYPESENSE_TARGET:-typesense-server}

# Detect architecture and set build flags
BUILD_FLAGS="$(if [ "$(dpkg --print-architecture)" = "arm64" ]; then echo '--arm'; else echo ''; fi) --jobs=$(nproc)"

cd /build/typesense

# Build the target
/bin/bash ci_build_v2.sh $BUILD_FLAGS

if [ "$TYPESENSE_TARGET" = "typesense-test" ]; then
    # Run the tests
    bazel test --cache_test_results=no --test_output=all //:typesense-test
else
    # Forward signals to the child process
    trap 'kill -TERM $child' TERM INT

    # Start the server and store its PID
    ./dist/$TYPESENSE_TARGET "$@" &
    child=$!

    # Wait for the server to exit
    wait $child
fi 