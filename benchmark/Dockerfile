FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies including locales
RUN apt-get update && apt-get install -y \
    software-properties-common \
    curl \
    locales \
    && add-apt-repository -y ppa:ubuntu-toolchain-r/test \
    && apt-get update \
    && apt-get install -y \
    g++-10 \
    make \
    git \
    zlib1g-dev \
    m4 \
    lld \
    && rm -rf /var/lib/apt/lists/*

# Set up compiler alternatives
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 30 \
    && update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-10 30 \
    && update-alternatives --install /usr/bin/cc cc /usr/bin/gcc 30 \
    && update-alternatives --set cc /usr/bin/gcc \
    && update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++ 30 \
    && update-alternatives --set c++ /usr/bin/g++

# Install architecture-specific Bazelisk
ARG TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then \
    curl -LO "https://github.com/bazelbuild/bazelisk/releases/download/v1.16.0/bazelisk-linux-arm64" && \
    mv bazelisk-linux-arm64 /usr/local/bin/bazel; \
    else \
    curl -LO "https://github.com/bazelbuild/bazelisk/releases/download/v1.16.0/bazelisk-linux-amd64" && \
    mv bazelisk-linux-amd64 /usr/local/bin/bazel; \
    fi && \
    chmod +x /usr/local/bin/bazel

# Set up locale
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# Set the filesystem encoding
ENV LANG C.UTF-8

VOLUME /root/.cache/bazel

WORKDIR /app
ENTRYPOINT ["/bin/bash"]
