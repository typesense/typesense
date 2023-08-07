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

if [[ "$@" == *"--with-cuda"* ]]; then
  CUDA_FLAGS="--define use_cuda=on --action_env=CUDA_HOME=/usr/local/cuda --action_env=CUDNN_HOME=/usr/local/cuda"
fi

bazel build --verbose_failures --jobs=6 $CUDA_FLAGS \
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

    GPU_DEPS_NAME=typesense-gpu-deps-$TYPESENSE_VERSION-$OS_FAMILY-$ARCH_NAME
    tar -cvzf $PROJECT_DIR/$BUILD_DIR/$GPU_DEPS_NAME.tar.gz -C $PROJECT_DIR/$BUILD_DIR libonnxruntime_providers_cuda.so libonnxruntime_providers_shared.so
    echo "Built binary successfully: $PROJECT_DIR/$BUILD_DIR/$GPU_DEPS_NAME.tar.gz"
fi
