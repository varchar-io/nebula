#!/bin/bash -e

# root build script to dispath to different platform
ROOT=$(git rev-parse --show-toplevel)

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
