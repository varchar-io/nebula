#!/bin/bash -e

# Support build on Ubuntu (tested on Ubuntu 18.0)
# If the script doesn't work for you, look up manual steps in dev.md
# This script is basically automation script of dev.md
# It is supposed to run in same folder where the file lives (root).

# Unfortunately we still need to manually fix the GRPC issue listed in dev.md

if [ "$(uname -s)" != "Darwin" ]; then
  echo "Must be on Mac OS to run this script."
  exit
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

## boost
preq boost

## openssl
preq openssl

# curl
preq curl

# compressions: zlib, zstd, snappy, lz4, bzip2
preq zlib
preq zstd
preq snappy
preq lz4
preq bzip2
preq bison

# folly dependencies
preq libevent
preq icu4c

# perf tools build using automake
preq automake

# execute cmake config with preset configs
SSL_ROOT=/usr/local/opt/openssl
cmake .. -DCMAKE_BUILD_TYPE=Release -DSYM=1 -DPPROF=2 -DOPENSSL_ROOT_DIR=$SSL_ROOT

# execute make
make -j$(nproc)