FROM dockcross/linux-x64

# Required for OpenSSL 1.0.2+
RUN echo "deb http://ftp.debian.org/debian jessie-backports main" > /etc/apt/sources.list.d/jessie-backport.list
RUN apt-get update

RUN apt-get install -y -t jessie-backports openssl
RUN apt-get install -y -t jessie-backports libssl-dev
RUN apt-get install -y zlib1g-dev
RUN apt-get install -y libsnappy-dev