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
pushtag()
{
    TAG=$1
    # is it really good to abuse tag for component or we should create multiple repo for 1:1 map?
    # docker tag $COSERVER $REGISTRY/$COSERVER:$TAG
    docker tag nebula/web columns/nebula.web:$TAG
    docker tag nebula/server columns/nebula.server:$TAG
    docker tag nebula/node columns/nebula.node:$TAG
    
    # push all images to container repo
    # docker push $REGISTRY/$COSERVER:$TAG
    docker push columns/nebula.web:$TAG
    docker push columns/nebula.server:$TAG
    docker push columns/nebula.node:$TAG
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