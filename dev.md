# Work On Nebula

## Build the project
These steps are tested for MacOS and Ubuntu (LTS 18) only.

### Install dependencies and build
run `~/nebula/build.sh` for automate setup (All steps in the build.sh script also listed on the bottom of this page as well)
A little hiccup of this script is we still need to fix below issue manually, hopefully this manual step won't be required after a version upgrade.

In build script, we use `sudo make` because some of the components needs to be installed in system path.

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