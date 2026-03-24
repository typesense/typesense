# Base image
FROM ubuntu:20.04

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
	build-essential \
	tar \
	libmpfr-dev \
	libgmp3-dev \
	libmpc-dev \
	flex \
	bison \
	zlib1g-dev \
	libssl-dev \
	git \
	locales \
	wget \
	curl && \
	rm -rf /var/lib/apt/lists/*

# Set up UTF-8 locale
RUN locale-gen en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

# Install GCC 10.3.0
ADD https://ftp.gnu.org/gnu/gcc/gcc-10.3.0/gcc-10.3.0.tar.gz /opt/
RUN tar -C /opt -xf /opt/gcc-10.3.0.tar.gz && \
	mkdir /opt/gcc-10.3.0/build && \
	cd /opt/gcc-10.3.0/build && \
	../configure --disable-checking --enable-languages=c,c++ \
	--disable-multilib --enable-shared --enable-threads=posix \
	--with-gmp=/usr/lib --with-mpc=/usr/lib --with-mpfr=/usr/lib \
	--without-included-gettext --prefix=/usr/local/gcc-10.3.0 && \
	make -j8 && make install && \
	rm -rf /opt/gcc-10.3.0 /opt/gcc-10.3.0.tar.gz

# Set GCC as default compiler
ENV CC=/usr/local/gcc-10.3.0/bin/gcc \
	CXX=/usr/local/gcc-10.3.0/bin/g++ \
	PATH=/usr/local/gcc-10.3.0/bin/:$PATH \
	LD_LIBRARY_PATH=/usr/local/gcc-10.3.0/lib64

# Install Bazel (ARM64 version)
ADD https://github.com/bazelbuild/bazel/releases/download/5.2.0/bazel-5.2.0-linux-arm64 /opt/
RUN chmod +x /opt/bazel-5.2.0-linux-arm64 && \
	mv /opt/bazel-5.2.0-linux-arm64 /usr/bin/bazel && \
	bazel --version

# Install Python 3.6.3 (fast version without optimizations)
ADD https://www.python.org/ftp/python/3.6.3/Python-3.6.3.tgz /opt
RUN tar -C /opt -xf /opt/Python-3.6.3.tgz && \
	cd /opt/Python-3.6.3 && \
	./configure && \
	make -j8 && make install && \
	rm -rf /opt/Python-3.6.3 /opt/Python-3.6.3.tgz

# Install pip for Python 3.6
ADD https://bootstrap.pypa.io/pip/3.6/get-pip.py /opt
RUN /usr/local/bin/python3.6 /opt/get-pip.py && \
	update-alternatives --install /usr/bin/python python /usr/local/bin/python3.6 1 && \
	rm -f /opt/get-pip.py

# Set working directory
WORKDIR /app
