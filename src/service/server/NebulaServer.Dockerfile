FROM ubuntu:20.04

# Set timezone:
RUN ln -snf /usr/share/zoneinfo/$CONTAINER_TIMEZONE /etc/localtime && echo $CONTAINER_TIMEZONE > /etc/timezone

RUN apt update
RUN apt install -y software-properties-common \
  build-essential \
  libunwind-dev \
  curl \
  supervisor \
  wget \
  libstdc++6
RUN apt upgrade -y
RUN apt dist-upgrade

EXPOSE 9190
COPY ./gen/nebula/NebulaServer /etc/nebula/NebulaServer
COPY ./gen/nebula/configs/cluster.yml /etc/nebula/configs/cluster.yml
RUN chmod +x /etc/nebula/NebulaServer

# This will enable verbose GRPC tracing log if needed
# ENV GRPC_VERBOSITY=DEBUG
# ENV GRPC_TRACE=all

CMD ["/etc/nebula/NebulaServer", "--CLS_CONF", "/etc/nebula/configs/cluster.yml", "--CLS_CONF_UPDATE_INTERVAL", "10000"]
