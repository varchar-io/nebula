#!/bin/bash -e

# Support build on MacOs (tested on M1/arm based)
# If the script doesn't work for you, look up manual steps in dev.md
# This script is basically automation script of dev.md
# It is supposed to run in same folder where the file lives (root).

if [ "$(uname -s)" != "Darwin" ]; then
  echo "Must be on Mac OS to run this script."
  exit
fi

# On new Macbook (M1 chip), there are changes compared to intel chip.
# such as homebrew uses different locations
IS_ARM=0
OPT_DIR="/usr/local/opt"
if [ "$(uname -p)" == "arm" ]; then
  IS_ARM=1
  OPT_DIR="/opt/homebrew/opt"
fi

# create build folder
ROOT=$(git rev-parse --show-toplevel)
BUILD_DIR=$ROOT/build
mkdir -p $BUILD_DIR && cd $BUILD_DIR

# ensure brew is installed and build tool cmake is installed
if ! command -v brew &> /dev/null
then
  echo 'installing homebrew...'
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

(
  if ! command -v cmake &> /dev/null
  then
    echo 'installing cmake...'
    brew install cmake
  fi
)

# Install Prerequisites
preq() {
  local target=$1
  if brew ls --versions $target > /dev/null; then
    echo "$target is ready."
  else
    echo "installing $target..."
    brew install $target -q
  fi
}

## gcc
preq gcc

## fmt
# preq fmt

## boost
preq boost

## openssl
preq openssl

# curl
preq curl

# nlohmann json
preq nlohmann-json

# compressions: zlib, zstd, snappy, lz4, bzip2
preq gzip
preq zlib
preq zstd
preq snappy
preq lz4
preq bzip2
preq xz
preq bison

# folly dependencies
preq libevent
preq icu4c

# perf tools build using automake
preq automake
preq libtool

# azure sdk requires libxml2
preq libxml2

# build and install gflags - brew has only dylib
(
  if [ -z "$(ls -A /usr/local/lib/libgflags.a)" ]; then
    sudo rm -rf ./gflags
    git clone --depth 1 --branch v2.2.2 https://github.com/gflags/gflags.git
    cd gflags && mkdir bd && cd bd
    cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DBUILD_STATIC_LIBS=ON -DBUILD_TESTING=OFF ..
    make && sudo make install
  fi
)

# Install Abseil - brew has wrong version
(
  if [ -z "$(ls -A /usr/local/lib/libabsl_strings.a)" ]; then
    sudo rm -rf ./abseil-cpp
    git clone --depth 1 --branch 20240722.1 https://github.com/abseil/abseil-cpp.git
    cd abseil-cpp && mkdir build && cd build
    cmake -DBUILD_TESTING=OFF -DBUILD_SHARED_LIBS=OFF -DABSL_USES_STD_STRING_VIEW=ON -DABSL_USES_STD_OPTIONAL=ON -DCMAKE_CXX_STANDARD=17 ..
    make -j$(nproc)
    sudo make install
  fi
)

# Install crc32c - brew has wrong version
(
  if [ -z "$(ls -A /usr/local/lib/libcrc32c.a)" ]; then
    sudo rm -rf ./crc32c
    git clone --depth 1 --branch 1.1.2 https://github.com/google/crc32c.git
    cd crc32c && mkdir build && cd build
    cmake -DCRC32C_BUILD_TESTS=OFF -DCRC32C_BUILD_BENCHMARKS=OFF -DCRC32C_USE_GLOG=OFF ..
    make -j$(nproc)
    sudo make install
  fi
)


# build type from env name, default to release
BTYPE=${BUILD_TYPE}
if [ "$BTYPE" == "" ]; then
  BTYPE="Release"
fi

# flag to strip symbols
SYM=0
if [ "$BTYPE" == "Release" ]; then
  SYM=1
fi

# on MacOS, we use command line tools to locate SDK
# consider run below command if you saw wrong SDK path in build log
# sudo xcode-select --switch /Library/Developer/CommandLineTools

# execute cmake config with preset configs
echo "enter password for sudo..."
echo "-----------------------"
echo "build type: $BTYPE"
SSL_ROOT=$OPT_DIR/openssl
sudo cmake .. -DCMAKE_BUILD_TYPE=$BTYPE -DARM=$IS_ARM -DSYM=$SYM -DPPROF=2 -DOPENSSL_ROOT_DIR=$SSL_ROOT

# execute make
sudo make -j$(sysctl -n hw.logicalcpu)
