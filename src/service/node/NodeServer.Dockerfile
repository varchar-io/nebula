FROM ubuntu:20.04

# install basic dependencies
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

EXPOSE 9199
COPY ./gen/nebula/NodeServer /etc/nebula/NodeServer
RUN chmod +x /etc/nebula/NodeServer

CMD ["/etc/nebula/NodeServer"]
