# nebula

## Build the project

### Install CMake

- https://cmake.org/download/ 
- (MACOS: After intall the GUI, use this command to install the command line tool: sudo "/Applications/CMake.app/Contents/bin/cmake-gui" --install)
- CMake Tutorial - https://cmake.org/cmake-tutorial/
- CMake build system - https://cmake.org/cmake/help/latest/manual/cmake-buildsystem.7.html

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
