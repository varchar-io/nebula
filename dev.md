# Work On Nebula

## Build the project

These steps are tested for Ubuntu LTS 18. 
Other linux os or macos uses different package manager such as homebrew.

### Install dependencies and build
run `~/nebula/build.sh` for automate setup (All steps in the build.sh script also listed on the bottom of this page as well)
a little hiccup of this script is we still need to fix below issue manually, hopefully this manual step won't be required after a version upgrade

### fix grpc
A local change to solve the the build break (in `~/nebula/build.sh`):
0. ensure `flatbuffers` and `grpc` downloaded by command `make flatbuffers && make grpc`
1. grpc/src/grpc/include/grpcpp/impl/codegen/byte_buffer.h
   move method to public grpc_byte_buffer* c_buffer() { return buffer_; }
2. flatbuffers/src/flatbuffers/include/flatbuffers/grpc.h
   use ByteBuffer as parameter type of Deserialize
     + static grpc::Status Deserialize(ByteBuffer *bb, flatbuffers::grpc::Message<T> *msg) {
     + grpc_byte_buffer* buffer = nullptr;
     + if (!bb || !(buffer = bb->c_buffer())) {
         return ::grpc::Status(::grpc::StatusCode::INTERNAL, "No payload");
       }

### run nebula
After you build nebula successfully, you can run this script to run all services locally by `./run.sh`.

## Code Convention
### Style - use clang-format

- install clang-format (user dir): `npm install clang-format`
- open preferences/settings `CMD+,`
- enter value for "clang-format.executable": `~/node_modules/clang-format/bin/darwin_x64/clang-format`
- check above path exists (it may change across versions depending on time run the installation)
- my format key binding is `SHIFT+ALT+F` (check your own bindings)

### Basic Code Convention

- All path are in lower case - single word preferred.
- All files follow camel naming convention.
- Every module has its own folder and its cmake file as {module}.cmake


## Perf Profiling
### Code profile C++ service running in a container 
1. Run perf to profile the running server inside docker container
    update> apt-get update
    install perf > apt-get install linux-tools-generic
    link perf > ln -fs /usr/lib/linux-tools/3.13.0-169-generic/perf /usr/bin/perf
    (version may differ depends on when it's running)
2. Find PID to attach
    list > docker ps
    enter > docker exec -ti service_server_1 /bin/bash
3. Attach perf to running server and issue slow query 
    attach > perf record -p <pid> -g
4. Failed to run perf due to permission? 
    try > https://blog.alicegoldfuss.com/enabling-perf-in-kubernetes/
    something like > docker run --cap-add SYS_ADMIN ...
5. Copy perf data from container to host
    copy > docker cp <containerId>:/perf.data /tmp/perf.data
6. View perf report (if no input, it will look for perf.data in current folder). 
    report > perf report -i /tmp/perf.data
7. To have readable report, make sure the binary has symbols in it.
    build with symbols > -g?
8. more tools can be found here http://euccas.github.io/blog/20170827/cpu-profiling-tools-on-linux.html
9. Use gperftools
   1.  clone source from github https://github.com/gperftools/gperftools.git
   2.  install the tool set on machine (tested on both mac and ubuntu)
       1.  ./autogen.sh
       2.  ./configure
       3.  sudo make install
   3.  use it in the app
       1.  make nebula with -DPPROF=1 using "gperftools"
       2.  run NodeServer `LD_PRELOAD=/usr/local/lib/libprofiler.so CPUPROFILE=/tmp/prof_ns.out ./NodeServer`
           1.  can add cpu frequency to tune the freuqency to profile `CPUPROFILE_FREQUENCY=400`
           2.  to see reports with graph, install grpahviz `sudo apt-get install graphviz gv`
           3.  convert: `pprof --svg NodeServer /tmp/prof_ns.out > prof.svg`
   4.  To make perfiler to flush/write perf results out, we need the app to exit normally. Hence implemented a hook to shutdown first node.
       1.  http://dev-shawncao:8088/?api=nuclear

## Misc
### Docker images
Docker images are available here: https://hub.docker.com/search?q=caoxhua%2Fnebula&type=image
I published these images from a Ubuntu machine via these commands:
1. docker login (my account - check /etc/.docker/config.json to see if it manages docker repo for you, delete it)
2. docker tag nebula/envoy caoxhua/nebula.envoy && docker push caoxhua/nebula.envoy
3. docker tag nebula/web caoxhua/nebula.web && docker push caoxhua/nebula.web
4. docker tag nebula/server caoxhua/nebula.server && docker push caoxhua/nebula.server
5. docker tag nebula/node caoxhua/nebula.node && docker push caoxhua/nebula.node


### Run End to End Nebula On Macbook
After you can make a successful build on your macbook, run #1 in /build folder, and #2 in `/src/service/http/nebula`
1. Run `NodeServer` and `NebulaServer` in a terminal window: `./NodeServer & ./NebulaServer --CLS_CONF configs/cluster.yml &`
2. Run `Nebula Web` to serve UI and connect Nebula Engine: `NS_ADDR=localhost:9190 NODE_PORT=8088 node node.js &`
3. Open browser and hit `http://localhost:8088`s


### Service Discovery
There are many options we can use to build a generic interface for service discovery. 
However, we don't want to pull dependencies like `zookeeper` or `etcd` to maintain out of Nebula Service.
Even though they are really great tools, especially `etcd`, I love it. 

For Nebula Server - we need a metadata DB to persist all metadata snapshot to reflect Nebula runtime states.
At the same time, this metadata DB will be served as service registry through Nebula Web API interface. 
So we need 
- An embeded DB that managed by nebula server itself.
- Build home made service discovery story on top of this metadata DB. 

Based on this consideration, I chose leveldb as our internal meta store. We will add web API for service registry.


### Manual setup dev environment and build
### Install CMake
Your system may not have new version of cmake required to build nebula.
Recommend building a version of cmake from source (take 3.18.1 as example):
1. wget https://cmake.org/files/v3.18/cmake-3.18.1.tar.gz
2. tar -xzvf cmake-3.18.1.tar.gz
3. cd cmake-3.18.1/
4. ./bootstrap
5. make -j$(nproc)
6. sudo make install
7. (Optional) sudo ln -s /usr/local/bin/cmake /usr/bin/cmake

### Install GCC-10
1. sudo apt-get update
2. sudo apt-get install -y gcc-10 g++-10
3. sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 900 --slave /usr/bin/g++ g++ /usr/bin/g++-10
4. (Optional) check version: `gcc -v`


### Install CURL, UNWIND, IBERTY
1. sudo apt-get install -y libcurl4-gnutls-dev libunwind-dev libiberty-dev
2. sudo apt-get install -y gnutls-dev libgcrypt-dev libkrb5-dev libldap-dev
3. sudo apt-get install -y librtmp-dev libnghttp2-dev libpsl-dev
4. sudo apt-get install -y libutf8proc-dev

### Install DOUBLE-CONVERSION
1. git clone https://github.com/google/double-conversion.git
2. cd double-conversion
3. cmake -DBUILD_SHARED_LIBS=OFF . && make && sudo make install

### Install GFLAGS, GLOG, GTEST
1. sudo apt-get install -y libgflags-dev
2. git clone https://github.com/google/glog.git
3. cd glog && cmake . && make . && sudo make install
1. git clone https://github.com/google/googletest.git
2. mkdir googletest/build && cd googletest/build
3. cmake -DCMAKE_CXX_FLAGS=-fPIC -DCMAKE_C_FLAGS=-fPIC .. && make && sudo make install

### install MBEDTLS
1. git clone https://github.com/ARMmbed/mbedtls.git
2. cd mbedtls && mkdir build && cd build
3. cmake -DCMAKE_CXX_FLAGS=-fPIC -DCMAKE_C_FLAGS=-fPIC .. && make -j$(nproc)
4. sudo make install

### Install LIBEVENT
1. git clone https://github.com/libevent/libevent.git
2. cd libevent
3. cmake . && make -j$(nproc)
4. sudo make install

### Install FMT
1. git clone https://github.com/fmtlib/fmt.git
2. cd fmt
3. cmake . && make -j$(nproc)
4. sudo make install

### Install BOOST
1. sudo apt-get install -y libboost-dev

### Install compression libs
1. sudo apt-get install -y liblz4-dev
2. sudo apt-get install -y libzstd-dev
3. sudo apt-get install -y libsnappy-dev
4. sudo apt-get install -y liblzma-dev

### Install OpenSSL
1. git clone https://github.com/openssl/openssl.git
2. cd openssl && ./config && make -j$(nproc) && sudo make install

### Install Facebook Folly
1. git clone https://github.com/facebook/folly.git
2. cd folly && git checkout v2020.09.21.00
3. mkdir _build && cd _build && cmake -DFOLLY_HAVE_PREADV=OFF -DFOLLY_HAVE_PWRITEV=OFF .. && make -j$(nproc) && sudo make install

### Install gperftools
1. sudo apt-get install -y autoconf
2. git clone https://github.com/gperftools/gperftools.git
3. cd gperftools && ./autogen.sh && ./configure && sudo make install

### Install protobuf
1. git clone https://github.com/protocolbuffers/protobuf.git
2. cd protobuf && git submodule update --init --recursive && ./autogen.sh
3. ./configure && make -j$(nproc) && sudo make install && sudo ldconfig

### Install flex bison rapidjson-dev
1. sudo apt-get install -y rapidjson-dev
2. sudo apt-get install -y flex
3. sudo apt-get install -y bison

### fix grpc
A local change to solve the the build break (in `~/nebula/build`):
0. ensure `flatbuffers` and `grpc` downloaded by command `make flatbuffers && make grpc`
1. grpc/src/grpc/include/grpcpp/impl/codegen/byte_buffer.h
   move method to public grpc_byte_buffer* c_buffer() { return buffer_; }
2. flatbuffers/src/flatbuffers/include/flatbuffers/grpc.h
   use ByteBuffer as parameter type of Deserialize
     + static grpc::Status Deserialize(ByteBuffer *bb, flatbuffers::grpc::Message<T> *msg) {
     + grpc_byte_buffer* buffer = nullptr;
     + if (!bb || !(buffer = bb->c_buffer())) {
         return ::grpc::Status(::grpc::StatusCode::INTERNAL, "No payload");
       }

### build nebula
- mkdir build && cd build
- cmake ..
- make
