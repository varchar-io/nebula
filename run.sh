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

# assuming nebula code is ~/nebula
base=~/nebula

# run the nebula node and nebula server on current box
build=$base/build
node=$build/NodeServer
#   check if file exists
if ! [ -z "$GIT_DIR" ]; then
    echo 'please build first - looking for $node';
    exit 1
fi

if ! grep -q "$node" <<< `ps aux` ; then
    $node &
else
    echo 'Nebula Node is already running.'
fi

server=$build/NebulaServer
config=$build/configs/test.yml
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
