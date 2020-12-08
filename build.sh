#!/bin/bash -e

# Support build on Ubuntu (tested on Ubuntu 18.0)
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

# packages could be installed by apt-get install
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
  cd $BUILD_DIR
  wget https://cmake.org/files/v3.18/cmake-3.18.1.tar.gz
  tar -xzvf cmake-3.18.1.tar.gz
  cd cmake-3.18.1/
  ./bootstrap
  make -j$(nproc)
  sudo make install

  sudo apt-get update
  sudo apt-get install -y gcc-10 g++-10
  sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 900 --slave /usr/bin/g++ g++ /usr/bin/g++-10
)

# loop through aptGetInstallPackages to install
for package in "${aptGetInstallPackages[@]}"; do
    sudo apt-get install -y "$package"
done

# Install DOUBLE-CONVERSION
(
  cd $BUILD_DIR
  if [ -z "$(ls -A ./double-conversion)" ]; then
    git clone https://github.com/google/double-conversion.git
    cd double-conversion
    cmake -DBUILD_SHARED_LIBS=OFF . && make && sudo make install
  fi
)

# Install GLOG, GTEST
(
  cd $BUILD_DIR
  if [ -z "$(ls -A ./glog)" ]; then
    git clone https://github.com/google/glog.git
    cd glog && cmake . && make . && sudo make install
  fi
  if [ -z "$(ls -A ./googletest)" ]; then
    git clone https://github.com/google/googletest.git
    mkdir googletest/build && cd googletest/build
    cmake -DCMAKE_CXX_FLAGS=-fPIC -DCMAKE_C_FLAGS=-fPIC .. && make && sudo make install
  fi
)

# Install MBEDTLS
(
  cd $BUILD_DIR
  if [ -z "$(ls -A ./mbedtls)" ]; then
    git clone https://github.com/ARMmbed/mbedtls.git
    cd mbedtls && mkdir build && cd build
    cmake -DCMAKE_CXX_FLAGS=-fPIC -DCMAKE_C_FLAGS=-fPIC .. && make -j$(nproc)
    sudo make install
  fi
)

# Install LIBEVENT
(
  cd $BUILD_DIR
  if [ -z "$(ls -A ./libevent)" ]; then
    git clone https://github.com/libevent/libevent.git
    cd libevent
    cmake . && make -j$(nproc)
    sudo make install
  fi
)

# Install FMT
(
  cd $BUILD_DIR
  if [ -z "$(ls -A ./fmt)" ]; then
    git clone https://github.com/fmtlib/fmt.git
    cd fmt
    cmake . && make -j$(nproc)
    sudo make install
  fi
)

# Install OpenSSL
(
  cd $BUILD_DIR
  if [ -z "$(ls -A ./openssl)" ]; then
    git clone https://github.com/openssl/openssl.git
    cd openssl && ./config && make -j$(nproc) && sudo make install
  fi
)

# Install Facebook Folly
(
  cd $BUILD_DIR
  if [ -z "$(ls -A ./folly)" ]; then
    git clone https://github.com/facebook/folly.git
    cd folly && git checkout v2020.09.21.00
    mkdir _build && cd _build && cmake -DFOLLY_HAVE_PREADV=OFF -DFOLLY_HAVE_PWRITEV=OFF .. && make -j$(nproc) && sudo make install
  fi
)

# Install gperftools
(
  cd $BUILD_DIR
  if [ -z "$(ls -A ./gperftools)" ]; then
    git clone https://github.com/gperftools/gperftools.git
    cd gperftools && ./autogen.sh && ./configure && sudo make install
  fi
)

# Install protobuf
(
  cd $BUILD_DIR
  if [ -z "$(ls -A ./protobuf)" ]; then
    git clone https://github.com/protocolbuffers/protobuf.git
    cd protobuf && git submodule update --init --recursive && ./autogen.sh
    ./configure && make -j$(nproc) && sudo make install && sudo ldconfig
  fi
)

# make nebula
cmake .. && make
