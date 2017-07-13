# NOTE: Default build image is bloated. Before publishing, export from a container to squash the image:
# $ docker run -it typesense/ubuntu-10.04-gcc:4.9.2 bash -c "exit"
# $ docker ps -a | grep typesense
# $ docker export <container_id> | docker import - typesense/ubuntu-10.04-gcc:4.9.2

FROM ubuntu:10.04

RUN sed -i -re 's/([a-z]{2}\.)?archive.ubuntu.com|security.ubuntu.com/old-releases.ubuntu.com/g' /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y build-essential

RUN apt-get -y install curl tar
RUN apt-get -y install libmpfr-dev libgmp3-dev libmpc-dev flex bison zlib1g-dev

RUN curl -o /opt/gcc-4.9.2.tar.bz2 -L http://ftp.gnu.org/gnu/gcc/gcc-4.9.2/gcc-4.9.2.tar.bz2
RUN tar -C /opt -xf /opt/gcc-4.9.2.tar.bz2

RUN mkdir /opt/gcc-4.9.2/build && cd /opt/gcc-4.9.2/build && ../configure --disable-checking --enable-languages=c,c++ \
      --enable-multiarch --disable-multilib --enable-shared --enable-threads=posix \
      --with-gmp=/usr/local/lib --with-mpc=/usr/lib --with-mpfr=/usr/lib \
      --build=x86_64-linux-gnu --host=x86_64-linux-gnu --target=x86_64-linux-gnu \
      --without-included-gettext --with-tune=generic --prefix=/usr/local/gcc-4.9.2
RUN cd /opt/gcc-4.9.2/build && make -j8
RUN cd /opt/gcc-4.9.2/build && make install

RUN apt-get remove -y gcc
RUN rm -rf /opt/gcc-4.9.2 /opt/gcc-4.9.2.tar.bz2

ENV PATH /usr/local/gcc-4.9.2/bin/:$PATH
ENV LD_LIBRARY_PATH /usr/local/gcc-4.9.2/lib64
