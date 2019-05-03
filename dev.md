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


### Code Convention

- All path are in lower case - single word preferred.
- All files follow camel naming convention.
- Every module has its own folder and its cmake file as {module}.cmake


### C++ Patterns References
- SNIFAE - http://jguegant.github.io/blogs/tech/sfinae-introduction.html
