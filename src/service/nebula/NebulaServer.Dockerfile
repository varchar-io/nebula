
FROM ubuntu:14.04

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:ubuntu-toolchain-r/test
RUN apt-get update
RUN apt-get install -y curl build-essential supervisor wget libstdc++6
RUN apt-get upgrade -y
RUN apt-get dist-upgrade

EXPOSE 9190
COPY ./gen/nebula/NebulaServer /etc/nebula/NebulaServer
RUN chmod +x /etc/nebula/NebulaServer
COPY ./gen/nebula/pin.trends.csv /tmp/pin.trends.csv
CMD ["/etc/nebula/NebulaServer", "--HOST_ADDR", "10.1.51.48", "--COMMENTS_FILE", "/home/shawncao/hadoop/full.txt", "--COMMENTS_MAX", "100000"]