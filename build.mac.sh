#!/bin/bash -e

# Support build on MacOs (tested on M1/arm based)
# If the script doesn't work for you, look up manual steps in dev.md
# This script is basically automation script of dev.md
# It is supposed to run in same folder where the file lives (root).

# Unfortunately we still need to manually fix the GRPC issue listed in dev.md

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

## fmt
preq fmt

## boost
preq boost

## openssl
preq openssl

# curl
preq curl

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

# execute cmake config with preset configs
echo "enter password for sudo..."
echo "-----------------------"
SSL_ROOT=$OPT_DIR/openssl
sudo cmake .. -DCMAKE_BUILD_TYPE=Release -DARM=1 -DSYM=1 -DPPROF=2 -DOPENSSL_ROOT_DIR=$SSL_ROOT

# execute make
sudo make -j$(sysctl -n hw.logicalcpu)