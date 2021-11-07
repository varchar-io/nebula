FROM ubuntu:20.04

# install basic dependencies (ca-certificates is used by SASL application such as kafka)
RUN apt update && apt install -y software-properties-common ca-certificates
RUN apt update --option Acquire::Retries=100 --option Acquire::http::Timeout="300" \
    && apt install -y \
    build-essential \
    libssl-dev \
    libunwind-dev \
    libcurl4-gnutls-dev \
    gnutls-dev \
    libstdc++6 \
    gcc-10 \
    g++-10
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
