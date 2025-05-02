# First time setup:
# 1) Note: If using Docker Desktop, increase memory limit in Docker Desktop > Settings > Resources > Memory (requires at least 16GB)
# 2) docker volume create typesense-bazel-cache
# 3) docker build -t typesense-builder -f docker/development-macos.Dockerfile .

# Build & Run typesense-server:
#    docker run -p 8108:8108 \
#                 -v "$(pwd)":/build/typesense \
#                 -v typesense-bazel-cache:/root/.cache/bazel \
#                 -v"$(pwd)"/typesense-data:/data \
#                 -e TYPESENSE_TARGET=typesense-server \
#                 typesense-builder \
#                 --data-dir=/data \
#                 --api-key=xyz \
#                 --enable-cors
#
# Build & Test entire test suite:
#    docker run -v "$(pwd)":/build/typesense \
#                 -v typesense-bazel-cache:/root/.cache/bazel \
#                 -e TYPESENSE_TARGET=typesense-test \
#                 typesense-builder
#
# Build & Test single test:
#    docker run -v "$(pwd)":/build/typesense \
#                 -v typesense-bazel-cache:/root/.cache/bazel \
#                 -e TYPESENSE_TARGET=typesense-test \
#                 typesense-builder \
#                 --gtest_filter="TestSuiteName.TestName"
#

FROM ubuntu:20.04

# Set environment variables to avoid interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# Set up locale, important, otherwise onnx compilation errors out!
RUN apt-get update && apt-get install -y locales \
    && locale-gen en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8

# Install dependencies
RUN apt-get update && apt-get install -y software-properties-common \
    && add-apt-repository -y ppa:ubuntu-toolchain-r/test \
    && apt-get update

RUN apt-get install -y \
    build-essential \
    g++-10 \
    make \
    git \
    zlib1g-dev \
    m4 \
    htop \
    curl \
    wget \
    jq \
    xz-utils \
    python3-distutils \
    moreutils \
    iftop \
    iotop \
    net-tools \
    iperf \
    nvme-cli \
    alien \
    lld \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Configure GCC 10 as the default compiler
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 30 && \
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-10 30 && \
    update-alternatives --install /usr/bin/cc cc /usr/bin/gcc 30 && \
    update-alternatives --set cc /usr/bin/gcc && \
    update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++ 30 && \
    update-alternatives --set c++ /usr/bin/g++

# Install pip
RUN curl -sSL https://bootstrap.pypa.io/pip/3.8/get-pip.py -o /tmp/get-pip.py \
    && python3 /tmp/get-pip.py \
    && rm /tmp/get-pip.py

# Detect architecture for Bazel installation
RUN dpkg --print-architecture > /tmp/arch.txt \
    && if [ "$(cat /tmp/arch.txt)" = "arm64" ]; then \
       echo "arm64" > /tmp/bazel_arch.txt; \
    else \
       echo "x86_64" > /tmp/bazel_arch.txt; \
    fi

# Install Bazel
RUN BAZEL_ARCH=$(cat /tmp/bazel_arch.txt) \
    && curl -fsSL -o /usr/bin/bazel https://github.com/bazelbuild/bazel/releases/download/6.2.0/bazel-6.2.0-linux-${BAZEL_ARCH} \
    && chmod +x /usr/bin/bazel

WORKDIR /build/typesense

# Script to handle build and run commands
COPY docker/development-macos-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/development-macos-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/development-macos-entrypoint.sh"] 