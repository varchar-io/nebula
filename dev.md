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