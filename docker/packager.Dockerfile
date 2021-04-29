FROM ubuntu:16.04

RUN apt-get -y update && apt-get -y install ca-certificates alien
