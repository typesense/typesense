FROM ubuntu:12.04

RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y python-software-properties
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN apt-get update

RUN apt-get install -y gcc-4.9
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.9 50
RUN apt-get install -y g++-4.9
RUN update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.9 50

RUN apt-get install -y zlib1g-dev
RUN apt-get install -y liblist-compare-perl
RUN apt-get install -y wget
RUN wget https://openssl.org/source/openssl-1.0.2k.tar.gz -P /opt
RUN tar -C /opt -xvzf /opt/openssl-1.0.2k.tar.gz
RUN cd /opt/openssl-1.0.2k && sh ./config --prefix=/usr --openssldir=/usr zlib-dynamic
RUN make -C /opt/openssl-1.0.2k depend
RUN make -C /opt/openssl-1.0.2k -j4
RUN make -C /opt/openssl-1.0.2k install

RUN apt-get install -y libsnappy-dev

RUN wget http://www.cmake.org/files/v3.3/cmake-3.3.2-Linux-x86_64.tar.gz -P /opt
RUN tar -C /opt -xvzf /opt/cmake-3.3.2-Linux-x86_64.tar.gz
RUN cp -r /opt/cmake-3.3.2-Linux-x86_64/* /usr