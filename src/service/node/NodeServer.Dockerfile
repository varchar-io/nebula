FROM ubuntu:20.04

RUN apt-get update
RUN apt-get install -y software-properties-common \
  build-essential \
  libunwind-dev \
  curl \
  supervisor \
  wget \
  libstdc++6
RUN apt-get upgrade -y
RUN apt-get dist-upgrade

EXPOSE 9199
COPY ./gen/nebula/NodeServer /etc/nebula/NodeServer
RUN chmod +x /etc/nebula/NodeServer

CMD ["/etc/nebula/NodeServer"]
