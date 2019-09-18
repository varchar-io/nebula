
FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:ubuntu-toolchain-r/test
RUN apt-get update
RUN apt-get install -y libunwind-dev
RUN apt-get install -y curl build-essential supervisor wget libstdc++6
RUN apt-get upgrade -y
RUN apt-get dist-upgrade

EXPOSE 9199
COPY ./gen/nebula/NodeServer /etc/nebula/NodeServer
RUN chmod +x /etc/nebula/NodeServer

CMD ["/etc/nebula/NodeServer"]

####################################################################################################

# This section is running node server with gperftools
# RUN apt-get update && apt-get upgrade  -y && \
#     apt-get install -y git autoreconf

# # install gperftools by default
# ENV GPERFTOOLS_REPO=https://github.com/gperftools/gperftools.git
# RUN set -x \
#     && mkdir -p /usr/src/gperftools \
#     && git clone "$GPERFTOOLS_REPO" /usr/src/gperftools \
#     && cd /usr/src/gperftools \
#     && ./autogen.sh \
#     && ./configure \
#     && make \
#     && sudo make install

# CMD ["LD_PRELOAD=/usr/local/lib/libprofiler.so CPUPROFILE=prof.out /etc/nebula/NodeServer"]