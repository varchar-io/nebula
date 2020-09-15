# nebula

## Build the project

### Install CMake

- https://cmake.org/download/ 
- (MACOS: After intall the GUI, use this command to install the command line tool: sudo "/Applications/CMake.app/Contents/bin/cmake-gui" --install)
- CMake Tutorial - https://cmake.org/cmake-tutorial/
- CMake build system - https://cmake.org/cmake/help/latest/manual/cmake-buildsystem.7.html
- Promote this video - https://www.youtube.com/watch?v=bsXLMQ6WgIk
- A quick sample of quick module setup https://github.com/vargheseg/test

### Use CMake to build the project
- mkdir build && cd build
- cmake ..
- make
- make install

### Use Facebook/folly library

Folly is complex library so we don't embed it in our build system
Manual installation steps (Macos) https://github.com/facebook/folly/tree/master/folly/build
- git clone https://github.com/facebook/folly.git
- ./folly/folly/build/bootstrap-osx-homebrew.sh
- (may need sudo to grant permission)
- "brew install folly" should just work for MacOS
- If you don't have brew on your mac, install it from here https://brew.sh/

On linux - which makes folly dependency consistent
- Now we have linuxbrew - https://docs.brew.sh/Homebrew-on-Linux

### Use clang-format

- VS Code is the default IDE which has extension for clang-format to format our code
- install clang-format so that IDE can invoke the formatter automatically on saving.
- https://packagecontrol.io/packages/Clang%20Format
- On MacOS (npm install clang-format) and edit user-settings with below settings
- "clang-format.executable": "/absolute-path-to/clang-format"
- If you don't have npm on your mac, install node from here https://nodejs.org/en/download/


### Use Glog

- GLog https://github.com/google/glog/blob/master/cmake/INSTALL.md
- Glog has an issue as external project of missing log_severity.h in interface folder, just copy it
-- "~/nebula/build/glogp-prefix/src/glogp-build/glog/logging.h:512:10: fatal error: 'glog/log_severity.h' file not found"
-- build> cp ./glogp-prefix/src/glogp/src/glog/log_severity.h glogp-prefix/src/glogp-build/glog/
- (tip - apply to others too, failed to download *.git file? delete invisible special character in URL)


### GRPC + Flatbuffers
- There are some incompatible interface between GRPC + flattbuffers on byte buffers and deserialize method
- Apply these two changes respectively before it's fixed or submitted to both grpc/flatbuffers repo
- GRPC: 
(include/grpcpp/impl/codegen/byte_buffer.h)
https://gist.github.com/shawncao/abc91b05ce6167ace226ead2383f2302
- FB: 
(include/flatbuffers/grpc.h)
https://gist.github.com/shawncao/7f3d6bb26feb2e0f48888a5ea4ab0f53


### Code Convention

- All path are in lower case - single word preferred.
- All files follow camel naming convention.
- Every module has its own folder and its cmake file as {module}.cmake


### C++ Patterns References
- SNIFAE - http://jguegant.github.io/blogs/tech/sfinae-introduction.html


### profile C++ service running in a container 
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
       1.  make nebula with -DGPROF=1 using "gprof"
       2.  OR make nebula with -DPPROF=1 using "gperftools"
       3.  Run Node server separately from other images by removing "node" service from docker-compose.yaml
           1.  LD_PRELOAD=/usr/local/lib/libprofiler.so CPUPROFILE=/tmp/prof_ns.out CPUPROFILE_FREQUENCY=400 ./NodeServer
       4.  if using gperftools, run NodeServer like this "LD_PRELOAD=/usr/local/lib/libprofiler.so CPUPROFILE=/tmp/prof_ns5.out ./NodeServer"
           1.  A bit more info on using gperftools: https://gperftools.github.io/gperftools/cpuprofile.html
           2.  to see reports with graph, install grpahviz "sudo apt-get install graphviz gv"
           3.  then we can do "pprof --gv <bin> x.out"
           4.  We can also (more practical since difficult to config gv on linux) gen svg file and copy it to mac to open by browsers.
               1.  such as "pprof --svg NodeServer /tmp/prof_ns.out > prof.svg"
       5.  if using gprof, run NodeServer normally
   4.  To make perfiler to flush/write perf results out, we need the app to exit normally. Hence implemented a hook to shutdown first node.
       1.  http://dev-shawncao:8088/?api=nuclear

### new fresh setup on ubuntu 18.04 recordings
Source build usually are "cmake .. -DCMAKE_BUILD_TYPE=Release && make -j36 && sudo make install" except those tar.gz with bootstrap such as cmake, boost
We will automate these steps one day.
1.  install cmake (source build, tar, bootstrap)
2.  install autoconf (apt-get)
3.  install brew (sh -c "$(curl -fsSL https://raw.githubusercontent.com/Linuxbrew/install/master/install.sh)")
4.  install gcc-9 and clang-8
5.  install gflags (source build, github, cmake)
6.  install glog (source build, github, cmake)
7.  install double-conversion (source build, github, cmake)
8.  install jemalloc (source build, github, autoconf, make OR "sudo apt-get install -y libjemalloc-dev")
9.  install lz4 (source build, github, make)
10. install zstd (source build, github, make)
11. install snappy (source build, github, cmake)
12. install libevent (source build, github, cmake)
13. install libunwind (apt-get libunwind-dev)
14. install libelf (apt-get libelf-dev)
15. install libdwarf (source build, https://github.com/tomhughes/libdwarf, configure/make)
16. "cp /usr/include/libdwarf/dwarf.h /usr/include/dwarf.h"
17. install folly (source build, github, cmake)
18. install boost 1.69.0 (check boost_ext.cmake)
19. install flex (apt-get)
20. install bison (apt-get)
21. install openssl (source build, github, config, make install)
22. install gtest (source build, github, cmake)
23. install golang (apt-get install will not place go in /usr/local expected by grpc)
24. install golang (tar unpack to /usr/local "tar -C /usr/local -xzf go$VERSION.$OS-$ARCH.tar.gz", export to path)
25. install protobuf (source build, github, follow instructions strictly including "sudo ldconfig")
26. install libcurl (apt-get, libcurl4-gnutls-dev and dependencies: libkrb5-dev, libgnutls28-dev, libgcrypt-dev, libldap-dev, librtmp-dev, libidn11-dev, libnghttp2-dev, libpsl-dev)
27. install rapidjson (nebula build, rapidjson/buid, sudo make install)
28. install libiberty (apt-get libiberty-dev)


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