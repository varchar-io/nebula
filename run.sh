#!/bin/bash -e
# do a case insensitive compare to see
# if asked to bring down all services
shopt -s nocasematch
case "$1" in
"down")
    # kill NodeServer
    ns1=$(ps aux | grep NodeServer | grep -v grep | awk '{print $2}')
    if ! [ "$ns1" = "" ]; then
        kill -9 $ns1
    fi

    # kill NebulaServer
    ns2=$(ps aux | grep NebulaServer | grep -v grep | awk '{print $2}')
    if ! [ "$ns2" = "" ]; then
        kill -9 $ns2
    fi

    # kill NebulaAPI - probably unnecessary
    api=$(ps aux | grep NS_ADDR | grep -v grep | awk '{print $2}')
    if ! [ "$api" = "" ]; then
        kill -9 $api
    fi

    echo "all services down."
    exit 0
esac
shopt -u nocasematch

# get os name
os=$(uname -s)
os_dir=
if [ "$os" == "Linux" ]; then
    os_dir="linux"
fi

if [ "$os" == "Darwin" ]; then
    os_dir="macos"
fi

# assuming nebula code is ~/nebula
base=$(git rev-parse --show-toplevel)

# run the nebula node and nebula server on current box
build=$base/build
prebuilt=$base/test/bin/$os_dir
node=$build/NodeServer
#   check if file not exists, use pre-built version
if ! [ -z "$node" ]; then
    node=$prebuilt/NodeServer
fi

if ! grep -q "$node" <<< `ps aux` ; then
    $node &
else
    echo 'Nebula Node is already running.'
fi

server=$build/NebulaServer
config=$build/configs/test.yml
#   check if file not exists, use pre-built version
if ! [ -z "$server" ]; then
    server=$prebuilt/NebulaServer
    config=$base/src/configs/test.yml
fi

if ! grep -q "$server" <<< `ps aux` ; then
    $server --CLS_CONF  $config &
else
    echo 'Nebula Server is already running.'
fi

# run nebula API using node.js
api=$base/src/service/http/nebula
if ! grep -q "$api" <<< `ps aux` ; then
    pushd $api
    yarn
    NS_ADDR=localhost:9190 NODE_PORT=8088 node node.js
else
    echo 'Nebula API is already running.'
fi
