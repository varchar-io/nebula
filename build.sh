#!/bin/bash -e

# root build script to dispath to different platform
ROOT=$(git rev-parse --show-toplevel)

# turn off git warning on specific branch checkout
git config --global advice.detachedHead false

# use vcpkg to manage major packages
# install vcpkg and use vcpkg to install our dependencies
# 1. install vcpkg to $HOME
# 2. install boost, glog, gflags, gtest
# 3. install folly, aws, gcs

if [ "$(uname -s)" == "Darwin" ]; then
  ( "$ROOT/build.mac.sh" ) 
  exit
fi

if [ "$(uname -s)" == "Linux" ]; then
  ( "$ROOT/build.linux.sh" )
  exit
fi

echo "The platform $(uname -s) is not supported yet."
exit 1
