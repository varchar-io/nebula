#!/bin/bash -e
# publish docker images to https://hub.docker.com/u/columns
# it prompts for user name and password.
# run this `publish.sh` only after `build.sh` succeeds

# source root
ROOT=$(git rev-parse --show-toplevel)

# prompt for login info early so we don't need to wait build
echo 'Make sure you have run `build.sh` successfully before this!'
echo 'Enter username (columns) and password to continue...'
sudo docker login

# to support latest syntax, we need latest docker-compose
$ROOT/deploy/install-latest-docker-compose.sh

# go to docker-compose folder
cd $ROOT/src/service
sudo docker-compose build

# push tag by name
WEB=columns/nebula.web
SERVER=columns/nebula.server
NODE=columns/nebula.node
pushtag()
{
    TAG=$1
    # is it really good to abuse tag for component or we should create multiple repo for 1:1 map?
    sudo docker tag nebula/web $WEB:$TAG
    sudo docker tag nebula/server $SERVER:$TAG
    sudo docker tag nebula/node $NODE:$TAG
    
    # push all images to container repo
    sudo docker push $WEB:$TAG
    sudo docker push $SERVER:$TAG
    sudo docker push $NODE:$TAG
}

# Just push or maybe we should do sanity check on outcome of the images
# in case this is not run by jenkins, we will use current time stamp as git commit hash
GIT_COMMIT=`git rev-parse --short HEAD`
if [ -z "$GIT_COMMIT" ]
then
    GIT_COMMIT=$(date +%s)
fi
echo "Current git commit: $GIT_COMMIT"

pushtag $GIT_COMMIT
pushtag latest

echo 'Images are ready: [columns/nebula.web, columns/nebula.server, columns/nebula.node]'
echo 'DONE!'

cd $ROOT
