#!/bin/bash -e

# Support build on Ubuntu (tested on Ubuntu 18.0)
# If the script doesn't work for you, look up manual steps in dev.md
# This script is basically automation script of dev.md
# It is supposed to run in same folder where the file lives (root).

# 1. enter into build folder
mkdir -p build && cd build

# 2. install all required dependencies sync to dev.md
# TODO(cao) - placeholder

# 2. make nebula
cmake .. && make