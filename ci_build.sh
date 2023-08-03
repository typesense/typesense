#!/bin/bash
# TYPESENSE_VERSION=nightly TYPESENSE_TARGET=typesense-server|typesense-test bash ci_build.sh

set -ex
PROJECT_DIR=`dirname $0 | while read a; do cd $a && pwd && break; done`
BUILD_DIR=bazel-bin

if [ -z "$TYPESENSE_VERSION" ]; then
  TYPESENSE_VERSION="nightly"
fi

ARCH_NAME="amd64"

if [[ "$@" == *"--graviton2"* ]] || [[ "$@" == *"--arm"* ]]; then
  ARCH_NAME="arm64"
fi

docker run --user $UID:$GID --volume="/etc/group:/etc/group:ro" --volume="/etc/passwd:/etc/passwd:ro" \
--volume="/etc/shadow:/etc/shadow:ro" -it --rm -v /bazeld:/bazeld -v $PROJECT_DIR:/src \
--workdir /src typesense/bazel_dev:24032023 bazel --output_user_root=/bazeld/cache build --verbose_failures \
--jobs=6 --action_env=LD_LIBRARY_PATH="/usr/local/gcc-10.3.0/lib64" \
--define=TYPESENSE_VERSION=\"$TYPESENSE_VERSION\" //:$TYPESENSE_TARGET

if [[ "$@" == *"--build-deploy-image"* ]]; then
    echo "Creating deployment image for Typesense $TYPESENSE_VERSION server ..."
    docker build --platform linux/${ARCH_NAME} --file $PROJECT_DIR/docker/deployment.Dockerfile \
          --tag typesense/typesense:$TYPESENSE_VERSION $PROJECT_DIR/$BUILD_DIR
fi

if [[ "$@" == *"--package-binary"* ]]; then
    OS_FAMILY=linux
    RELEASE_NAME=typesense-server-$TYPESENSE_VERSION-$OS_FAMILY-$ARCH_NAME
    printf `md5sum $PROJECT_DIR/$BUILD_DIR/typesense-server | cut -b-32` > $PROJECT_DIR/$BUILD_DIR/typesense-server.md5.txt
    tar -cvzf $PROJECT_DIR/$BUILD_DIR/$RELEASE_NAME.tar.gz -C $PROJECT_DIR/$BUILD_DIR typesense-server typesense-server.md5.txt
    echo "Built binary successfully: $PROJECT_DIR/$BUILD_DIR/$RELEASE_NAME.tar.gz"
fi
