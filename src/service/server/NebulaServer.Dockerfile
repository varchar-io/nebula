
FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN apt-get update
RUN apt-get install -y libunwind-dev
RUN apt-get update
RUN apt-get install -y curl build-essential supervisor wget libstdc++6
RUN apt-get upgrade -y
RUN apt-get dist-upgrade

EXPOSE 9190
COPY ./gen/nebula/NebulaServer /etc/nebula/NebulaServer
COPY ./gen/nebula/configs/cluster.yml /etc/nebula/configs/cluster.yml
RUN chmod +x /etc/nebula/NebulaServer

# This will enable verbose GRPC tracing log if needed
# ENV GRPC_VERBOSITY=DEBUG
# ENV GRPC_TRACE=all

CMD ["/etc/nebula/NebulaServer", "--CLS_CONF", "/etc/nebula/configs/cluster.yml", "--CLS_CONF_UPDATE_INTERVAL", "10000"]
