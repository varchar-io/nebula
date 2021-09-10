#!/bin/bash -e

# Support build on Ubuntu (tested on Ubuntu 20.04 LTS)
# If the script doesn't work for you, look up manual steps in dev.md
# This script is basically automation script of dev.md
# It is supposed to run in same folder where the file lives (root).

# Unfortunately we still need to manually fix the GRPC issue listed in dev.md

if [ "$(uname -s)" != "Linux" ]; then
  echo "Must be on Linux machine to run this script"
  exit
fi

ROOT=$(git rev-parse --show-toplevel)
BUILD_DIR=$ROOT/build
# enter into nebula build folder
mkdir -p $BUILD_DIR && cd $BUILD_DIR

# on a complete new system -
# before everything starts - need at least c compiler
sudo apt install -y build-essential libssl-dev cmake libboost-all-dev

# packages could be installed by apt install
aptGetInstallPackages=(
  "libcurl4-gnutls-dev"
  "libunwind-dev"
  "libiberty-dev"
  "gnutls-dev"
  "libgcrypt-dev"
  "libkrb5-dev"
  "libldap-dev"
  "librtmp-dev"
  "libnghttp2-dev"
  "libpsl-dev"
  "libutf8proc-dev"
  "libgflags-dev"
  "libboost-dev"
  "liblz4-dev"
  "libzstd-dev"
  "libsnappy-dev"
  "liblzma-dev"
  "autoconf"
  "flex"
  "bison"
)

# Install Prerequisites
# install cmake and gcc-10
(
  if ! command -v cmake &> /dev/null
  then
    MJ_VER=3.19
    MN_VER=3.19.2
    wget https://cmake.org/files/v$MJ_VER/cmake-$MN_VER.tar.gz
    tar -xzvf cmake-$MN_VER.tar.gz
    cd cmake-$MN_VER/
    ./bootstrap
    make -j$(nproc)
    sudo make install
  fi

  # sudo apt update
  sudo apt install -y gcc-10 g++-10
  sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 900 --slave /usr/bin/g++ g++ /usr/bin/g++-10
)

# loop through aptGetInstallPackages to install
for package in "${aptGetInstallPackages[@]}"; do
    sudo apt install -y "$package"
done

# Install MBEDTLS
(
  if [ -z "$(ls -A ./mbedtls)" ]; then
    git clone --depth 1 --branch v3.0.0 https://github.com/ARMmbed/mbedtls.git
    cd mbedtls && mkdir build && cd build
    cmake -DCMAKE_CXX_FLAGS=-fPIC -DCMAKE_C_FLAGS=-fPIC .. && make -j$(nproc)
    sudo make install
  fi
)

# Install LIBEVENT
(
  if [ -z "$(ls -A ./libevent)" ]; then
    git clone --depth 1 --branch release-2.1.12-stable https://github.com/libevent/libevent.git
    cd libevent
    cmake . && make -j$(nproc)
    sudo make install
  fi
)

# Install Abseil
(
  if [ -z "$(ls -A ./abseil-cpp)" ]; then
    git clone --depth 1 --branch 20200923.3 https://github.com/abseil/abseil-cpp.git
    cd abseil-cpp && mkdir build && cd build
    cmake -DBUILD_TESTING=OFF -DBUILD_SHARED_LIBS=OFF -DABSL_USES_STD_STRING_VIEW=ON -DABSL_USES_STD_OPTIONAL=ON -DCMAKE_CXX_STANDARD=11 ..
    make -j$(nproc)
    sudo make install
  fi
)

# Install crc32c
(
  if [ -z "$(ls -A ./crc32c)" ]; then
    git clone --depth 1 --branch 1.1.1 https://github.com/google/crc32c.git
    cd crc32c && mkdir build && cd build
    cmake -DCRC32C_BUILD_TESTS=OFF -DCRC32C_BUILD_BENCHMARKS=OFF -DCRC32C_USE_GLOG=OFF ..
    make -j$(nproc)
    sudo make install
  fi
)

# run nebula cmake
echo "enter password for sudo..."
echo "-----------------------"
sudo cmake .. -DCMAKE_BUILD_TYPE=Release -DSYM=1 -DPPROF=2 -DOPENSSL_ROOT_DIR=/usr/local/openssl

# execute make
sudo make -j$(nproc)
