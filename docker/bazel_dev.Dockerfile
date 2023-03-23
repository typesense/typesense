# $ docker build --file docker/bazel_dev.Dockerfile --tag  typesense/bazel_dev:DDMMYYYY ./docker
# NOTE: Default build image is bloated. Before publishing, export from a container to squash the image:
# $ docker run -it typesense/bazel_dev:DDMMYYYY bash -c "exit"
# $ docker ps -a | grep bazel_dev
# $ docker export <container_id> | docker import - typesense/bazel_dev:latest
# $ docker push typesense/bazel_dev:latest

FROM ubuntu:14.10

RUN sed -i -re 's/([a-z]{2}\.)?archive.ubuntu.com|security.ubuntu.com/old-releases.ubuntu.com/g' /etc/apt/sources.list
RUN apt-get update && apt-get install -y \
	build-essential \
	tar \
	libmpfr-dev \
	libgmp3-dev \
	libmpc-dev \
	flex \
	bison \
	zlib1g-dev

ADD https://ftp.gnu.org/gnu/gcc/gcc-10.3.0/gcc-10.3.0.tar.gz /opt/
RUN tar -C /opt -xf /opt/gcc-10.3.0.tar.gz

RUN mkdir /opt/gcc-10.3.0/build && cd /opt/gcc-10.3.0/build && ../configure --disable-checking --enable-languages=c,c++ \
      --enable-multiarch --disable-multilib --enable-shared --enable-threads=posix \
      --with-gmp=/usr/lib --with-mpc=/usr/lib --with-mpfr=/usr/lib \
      --build=x86_64-linux-gnu --host=x86_64-linux-gnu --target=x86_64-linux-gnu \
      --without-included-gettext --with-tune=generic --prefix=/usr/local/gcc-10.3.0
RUN cd /opt/gcc-10.3.0/build && make -j8
RUN cd /opt/gcc-10.3.0/build && make install

RUN apt-get remove -y gcc
RUN rm -rf /opt/gcc-10.3.0 /opt/gcc-10.3.0.tar.gz

ADD https://github.com/bazelbuild/bazel/releases/download/5.2.0/bazel-5.2.0-linux-x86_64 /opt/
RUN chmod 777 /opt/bazel-5.2.0-linux-x86_64
RUN mv /opt/bazel-5.2.0-linux-x86_64 /usr/bin/bazel

# To force extraction of installation
RUN /usr/bin/bazel --version

RUN apt-get -y install git

#RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

ENV CC /usr/local/gcc-10.3.0/bin/gcc
ENV CXX /usr/local/gcc-10.3.0/bin/g++
ENV PATH /usr/local/gcc-10.3.0/bin/:$PATH
ENV LD_LIBRARY_PATH /usr/local/gcc-10.3.0/lib64

RUN locale-gen en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

RUN apt install -y libssl-dev

ADD https://www.python.org/ftp/python/3.6.3/Python-3.6.3.tgz /opt
RUN tar -C /opt -xf /opt/Python-3.6.3.tgz
RUN cd /opt/Python-3.6.3 && ./configure --enable-optimizations && make install

ADD https://bootstrap.pypa.io/pip/3.6/get-pip.py /opt
RUN /usr/local/bin/python3.6 /opt/get-pip.py

RUN update-alternatives --install /usr/bin/python python /usr/local/bin/python3.6 1


