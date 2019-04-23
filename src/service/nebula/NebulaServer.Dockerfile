
FROM ubuntu:14.04

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:ubuntu-toolchain-r/test
RUN apt-get update
RUN apt-get install -y curl build-essential supervisor wget libstdc++6
RUN apt-get upgrade -y
RUN apt-get dist-upgrade

EXPOSE 9090
COPY ./gen/greeter_server /etc/nebula/greeter_server
CMD ["/etc/nebula/greeter_server"]