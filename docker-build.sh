#!/bin/bash

set -ex
PROJECT_DIR=`dirname $0 | while read a; do cd $a && pwd && break; done`
SYSTEM_NAME=Linux
BUILD_DIR=build-$SYSTEM_NAME
TEST_BUILD_DIR=test-$BUILD_DIR

if [ -z "$TYPESENSE_VERSION" ]; then
  TYPESENSE_VERSION="nightly"
fi

if [[ "$@" == *"--clean"* ]]; then
  echo "Cleaning..."
  rm -rf $PROJECT_DIR/$BUILD_DIR
  mkdir $PROJECT_DIR/$BUILD_DIR
fi

if [[ "$@" == *"--clean-test"* ]]; then
  echo "Cleaning..."
  rm -rf $PROJECT_DIR/$TEST_BUILD_DIR
  mkdir $PROJECT_DIR/$TEST_BUILD_DIR
fi

if [[ "$@" == *"--depclean"* ]]; then
  echo "Cleaning dependencies..."
  rm -rf $PROJECT_DIR/external-$SYSTEM_NAME
  mkdir $PROJECT_DIR/external-$SYSTEM_NAME
fi


TYPESENSE_DEV_IMAGE="typesense-development:27-JUN-2022-1"
ARCH_NAME="amd64"

if [[ "$@" == *"--graviton2"* ]] || [[ "$@" == *"--arm"* ]]; then
  TYPESENSE_DEV_IMAGE="typesense-development-arm:27-JUN-2022-1"
  ARCH_NAME="arm64"
fi

echo "Building Typesense $TYPESENSE_VERSION..."
docker run -it --platform linux/${ARCH_NAME} -v $PROJECT_DIR:/typesense typesense/$TYPESENSE_DEV_IMAGE cmake -DTYPESENSE_VERSION=$TYPESENSE_VERSION \
 -DCMAKE_BUILD_TYPE=Release -H/typesense -B/typesense/$BUILD_DIR
docker run -it --platform linux/${ARCH_NAME} -v $PROJECT_DIR:/typesense typesense/$TYPESENSE_DEV_IMAGE make typesense-server -C/typesense/$BUILD_DIR

if [[ "$@" == *"--test"* ]]; then
    echo "Running tests"
    docker run -it --platform linux/${ARCH_NAME} -v $PROJECT_DIR:/typesense typesense/$TYPESENSE_DEV_IMAGE cp /typesense/$BUILD_DIR/Makefile /typesense/$TEST_BUILD_DIR
    docker run -it --platform linux/${ARCH_NAME} -v $PROJECT_DIR:/typesense typesense/$TYPESENSE_DEV_IMAGE cp -R /typesense/$BUILD_DIR/CMakeFiles /typesense/$TEST_BUILD_DIR/
    docker run -it --platform linux/${ARCH_NAME} -v $PROJECT_DIR:/typesense typesense/$TYPESENSE_DEV_IMAGE make typesense-test -C/typesense/$TEST_BUILD_DIR
    docker run -it --platform linux/${ARCH_NAME} -v $PROJECT_DIR:/typesense typesense/$TYPESENSE_DEV_IMAGE chmod +x /typesense/$TEST_BUILD_DIR/typesense-test
    docker run -it --platform linux/${ARCH_NAME} -v $PROJECT_DIR:/typesense typesense/$TYPESENSE_DEV_IMAGE /typesense/$TEST_BUILD_DIR/typesense-test
fi

if [[ "$@" == *"--build-deploy-image"* ]]; then
    echo "Creating deployment image for Typesense $TYPESENSE_VERSION server ..."

    cp $PROJECT_DIR/docker/deployment.Dockerfile $PROJECT_DIR/$BUILD_DIR
    docker build --platform linux/${ARCH_NAME} --file $PROJECT_DIR/$BUILD_DIR/deployment.Dockerfile --tag typesense/typesense:$TYPESENSE_VERSION \
                        $PROJECT_DIR/$BUILD_DIR
fi

if [[ "$@" == *"--package-binary"* ]]; then
    OS_FAMILY=linux
    RELEASE_NAME=typesense-server-$TYPESENSE_VERSION-$OS_FAMILY-$ARCH_NAME
    printf `md5sum $PROJECT_DIR/$BUILD_DIR/typesense-server | cut -b-32` > $PROJECT_DIR/$BUILD_DIR/typesense-server.md5.txt
    tar -cvzf $PROJECT_DIR/$BUILD_DIR/$RELEASE_NAME.tar.gz -C $PROJECT_DIR/$BUILD_DIR typesense-server typesense-server.md5.txt
    echo "Built binary successfully: $PROJECT_DIR/$BUILD_DIR/$RELEASE_NAME.tar.gz"
fi

#
#if [[ "$@" == *"--create-deb-upload"* ]]; then
#    docker run -it --platform linux/${ARCH_NAME} -v $PROJECT_DIR:/typesense typesense/typesense-development:09-AUG-2021-1 cmake -DTYPESENSE_VERSION=$TYPESENSE_VERSION \
#    -DCMAKE_BUILD_TYPE=Debug -H/typesense -B/typesense/$BUILD_DIR
#fi

echo "Done... quitting."
