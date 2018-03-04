#!/bin/sh

set -ex
PROJECT_DIR=`dirname $0 | while read a; do cd $a && pwd && break; done`

if [ -z "$TYPESENSE_VERSION" ]; then
  TYPESENSE_VERSION="nightly"
fi

if [[ "$@" == *"--clean"* ]]; then
  echo "Cleaning..."
  rm -rf $PROJECT_DIR/build
  mkdir $PROJECT_DIR/build
fi

if [[ "$@" == *"--depclean"* ]]; then
  echo "Cleaning dependencies..."
  rm -rf $PROJECT_DIR/external-Linux
  mkdir $PROJECT_DIR/external-Linux
fi

echo "Creating development image..."
docker build --file $PROJECT_DIR/docker/development.Dockerfile --tag typesense/typesense-development:latest $PROJECT_DIR/docker

echo "Building Typesense $TYPESENSE_VERSION..."
docker run -it -v $PROJECT_DIR:/typesense typesense/typesense-development cmake -DTYPESENSE_VERSION=$TYPESENSE_VERSION \
-DCMAKE_BUILD_TYPE=Release -H/typesense -B/typesense/build

docker run -it -v $PROJECT_DIR:/typesense typesense/typesense-development make -C/typesense/build

if [[ "$@" == *"--build-deploy-image"* ]]; then
    echo "Creating deployment image for Typesense $TYPESENSE_VERSION server ..."

    cp $PROJECT_DIR/docker/deployment.Dockerfile $PROJECT_DIR/build
    docker build --file $PROJECT_DIR/build/deployment.Dockerfile --tag typesense/typesense:$TYPESENSE_VERSION \
                        $PROJECT_DIR/build
fi

if [[ "$@" == *"--create-binary"* ]]; then
    OS_FAMILY=linux
    RELEASE_NAME=typesense-server-$TYPESENSE_VERSION-$OS_FAMILY-amd64
    cp $PROJECT_DIR/build/typesense-server $PROJECT_DIR/build/$RELEASE_NAME
    printf `md5sum $PROJECT_DIR/build/${RELEASE_NAME} | cut -b-32` > $PROJECT_DIR/build/$RELEASE_NAME.md5.txt
    tar -cvzf $PROJECT_DIR/build/$RELEASE_NAME.tar.gz -C $PROJECT_DIR/build $RELEASE_NAME $RELEASE_NAME.md5.txt
    echo "Built binary successfully: $PROJECT_DIR/build/$RELEASE_NAME.tar.gz"
fi

echo "Done... quitting."