#!/bin/bash

# 
# generate random payload and send it to the redpanda cluster
# requires node.js to run web server
# requires brew to install redpanda
# run this script in nebula repo
# 

# STEP 1: follow https://vectorized.io/docs/quick-start-macos to start a local cluster
brew install vectorizedio/tap/redpanda
rpk container purge 
BROKERS=$(rpk container start -n 3 | grep 127.0.0.1 | rev | awk '{$1=$1};1' | cut -d' ' -f1 | rev | paste -sd "," -)

# STEP 2: create topic
echo "BROKERS: $BROKERS"
TOPIC=nebula_redpanda
rpk topic create $TOPIC --brokers $BROKERS


# STEP 3: bring up nebula local cluster to serve

# decide which binary to use based on current mac is arm versioned or not
# get os name
os=$(uname -s)
arch=$(uname -p)
if [ "$os" == "Darwin" ]; then
    os_dir="macos"
    if [ "$arch" == "arm" ]; then
        os_dir=$os_dir/arm
    fi
fi

# simple check
if [ "$os_dir" == "" ]; then
  echo "only suppot run this demo on mac".
  exit 1
fi

# launch nebula node/server/web
PORT=8011
base=$(git rev-parse --show-toplevel)
prebuilt=$base/test/bin/$os_dir
node=$prebuilt/NodeServer
server=$prebuilt/NebulaServer
web=$base/src/service/http/nebula
# kill all of them in case they are already running
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
ns3=$(ps aux | grep 'node node.js' | grep -v grep | awk '{print $2}')
if ! [ "$ns3" = "" ]; then
    kill -9 $ns3
fi

# replace brokers
config_temp=$base/test/redpanda/config.yaml
config=$base/test/redpanda/config_copy.yaml
yes | cp $config_temp $config
echo "Update config file $config..."
sed -i '' "s/<BROKERS>/$BROKERS/" $config
sed -i '' "s/<TOPIC>/$TOPIC/" $config

## start them up
echo "starting $node..."
$node &

echo "starting $server --CLS_CONF $config ..."
$server --CLS_CONF  $config &

echo "starting nebula web server...."
pushd $web
npm install >> /dev/null
NS_ADDR=localhost:9190 NODE_PORT=$PORT node node.js &

echo ""
echo "==========================="
echo "go to http://localhost:$PORT/#$TOPIC to analyze topic data"
echo "==========================="
echo ""

# STEP 4: start to send fake data to this cluster
# just make a inifinite loop and send messages in each iteration
NAMES=("redpanda" "is" "awesome" "with" "nebula" "super" "great")
gen()
{
  payload="{\"name\": \"${NAMES[$RANDOM % 8]}\", \"value\":$RANDOM}"
  echo $payload | rpk topic produce $TOPIC -n 1 --brokers $BROKERS
}

i=0
while true
do
  gen >> /dev/null
  sleep .1
  let i++
  if [ "$(($i % 100))" == "0" ]; then
    echo "has sent $i messages to redpanda, continue..."
  fi
done
