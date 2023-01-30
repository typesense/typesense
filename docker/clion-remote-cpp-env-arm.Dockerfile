# Source: https://github.com/JetBrains/clion-remote/blob/3db8912219cbe572f98677704942fda2e280cf55/Dockerfile.remote-cpp-env
#
# CLion remote docker environment
#
# Build:
#   docker build -t typesense/typesense-clion-remote-cpp-env-arm:11-OCT-2022-1 -f ./docker/clion-remote-cpp-env-arm.Dockerfile ./docker
#   docker push typesense/typesense-clion-remote-cpp-env-arm:11-OCT-2022-1
#
# Run:
#   docker run -d --cap-add sys_ptrace -v`pwd`/typesense-server-data/:/var/lib/typesense -p 127.0.0.1:2222:22 -p 127.0.0.1:8108:8108 --name clion_remote_env typesense/typesense-clion-remote-cpp-env-arm:11-OCT-2022-1
#   docker stop clion_remote_env ; docker rm clion_remote_env
#
# If you need to SSH into the container directly:
#   ssh -p 2222 user@localhost
#   Password: password
#
# Setup remote host in CLion with the following details:
#   Hostname: localhost
#   Port: 2222
#   Username: user
#   Password: password
#
#   GDB: /usr/local/bin/gdb
#

FROM typesense/typesense-development-arm:27-JUN-2022-1

RUN DEBIAN_FRONTEND="noninteractive" apt-get update && apt-get -y install tzdata

RUN apt-get update \
  && apt-get install -y ssh \
      ssh \
      htop \
      vim \
      sudo \
      locales-all \
      dos2unix \
      rsync \
      tar \
      python \
      python-dev \
      git \
  && apt-get clean

ARG GDB_VERSION=12.1
ADD http://ftp.gnu.org/gnu/gdb/gdb-${GDB_VERSION}.tar.gz /opt/gdb-${GDB_VERSION}.tar.gz
RUN tar -C /opt -xf /opt/gdb-${GDB_VERSION}.tar.gz
RUN cd /opt/gdb-${GDB_VERSION} && ./configure --with-python && make -j8 && make install

ADD https://sourceware.org/pub/valgrind/valgrind-3.17.0.tar.bz2 /opt/valgrind-3.17.0.tar.bz2
RUN tar -C /opt -xf /opt/valgrind-3.17.0.tar.bz2
RUN cd /opt/valgrind-3.17.0 && ./configure --prefix=/usr && make -j8 && make install

RUN ( \
    echo 'LogLevel DEBUG2'; \
    echo 'PermitRootLogin yes'; \
    echo 'PasswordAuthentication yes'; \
    echo 'Subsystem sftp /usr/lib/openssh/sftp-server'; \
  ) > /etc/ssh/sshd_config_test_clion \
  && mkdir /run/sshd

RUN useradd -m user \
  && yes password | passwd user

RUN usermod -s /bin/bash user
RUN usermod -aG sudo user

CMD ["/usr/sbin/sshd", "-D", "-e", "-f", "/etc/ssh/sshd_config_test_clion"]