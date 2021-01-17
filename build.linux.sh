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

  sudo apt-get update
  sudo apt-get install -y gcc-10 g++-10
  sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 900 --slave /usr/bin/g++ g++ /usr/bin/g++-10
)

# loop through aptGetInstallPackages to install
for package in "${aptGetInstallPackages[@]}"; do
    sudo apt-get install -y "$package"
done

# Install MBEDTLS
(
  if [ -z "$(ls -A ./mbedtls)" ]; then
    git clone https://github.com/ARMmbed/mbedtls.git
    cd mbedtls && mkdir build && cd build
    cmake -DCMAKE_CXX_FLAGS=-fPIC -DCMAKE_C_FLAGS=-fPIC .. && make -j$(nproc)
    sudo make install
  fi
)

# Install LIBEVENT
(
  if [ -z "$(ls -A ./libevent)" ]; then
    git clone https://github.com/libevent/libevent.git
    cd libevent
    cmake . && make -j$(nproc)
    sudo make install
  fi
)

# Install OpenSSL
SSL_ROOT=/usr/local/openssl
(
  if [ -z "$(ls -A ./openssl)" ]; then
    git clone https://github.com/openssl/openssl.git
    cd openssl && ./config --prefix=${SSL_ROOT} && make -j$(nproc) && sudo make install
  fi
)

# run nebula cmake
echo "enter password for sudo..."
echo "-----------------------"
sudo cmake .. -DCMAKE_BUILD_TYPE=Release -DSYM=1 -DPPROF=2 -DOPENSSL_ROOT_DIR=$SSL_ROOT

# execute make
sudo make -j$(nproc)
