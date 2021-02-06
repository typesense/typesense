# $ docker build --file $PROJECT_DIR/docker/ubuntu-10-04-gcc.Dockerfile --tag typesense/ubuntu-12.04-gcc:latest --tag typesense/ubuntu-12.04-gcc:6.4 $PROJECT_DIR/docker
# 
# NOTE: Default build image is bloated. Before publishing, export from a container to squash the image:
# $ docker run -it typesense/ubuntu-12.04-gcc:latest bash -c "exit"
# $ docker ps -a | grep typesense
# $ docker export <container_id> | docker import - typesense/ubuntu-12.04-gcc:latest
# $ docker push typesense/ubuntu-12.04-gcc

FROM ubuntu:12.04

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

ADD https://ftp.gnu.org/gnu/gcc/gcc-10.1.0/gcc-10.1.0.tar.gz /opt/
RUN tar -C /opt -xf /opt/gcc-10.1.0.tar.gz

RUN mkdir /opt/gcc-10.1.0/build && cd /opt/gcc-10.1.0/build && ../configure --disable-checking --enable-languages=c,c++ \
      --enable-multiarch --disable-multilib --enable-shared --enable-threads=posix \
      --with-gmp=/usr/lib --with-mpc=/usr/lib --with-mpfr=/usr/lib \
      --build=x86_64-linux-gnu --host=x86_64-linux-gnu --target=x86_64-linux-gnu \
      --without-included-gettext --with-tune=generic --prefix=/usr/local/gcc-10.1.0
RUN cd /opt/gcc-10.1.0/build && make -j8
RUN cd /opt/gcc-10.1.0/build && make install

RUN apt-get remove -y gcc
RUN rm -rf /opt/gcc-10.1.0 /opt/gcc-10.1.0.tar.gz

ENV PATH /usr/local/gcc-10.1.0/bin/:$PATH
ENV LD_LIBRARY_PATH /usr/local/gcc-10.1.0/lib64
