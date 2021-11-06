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

# Just push or maybe we should do sanity check on outcome of the images
sudo docker tag nebula/web columns/nebula.web && sudo docker push columns/nebula.web
sudo docker tag nebula/server columns/nebula.server && sudo docker push columns/nebula.server
sudo docker tag nebula/node columns/nebula.node && sudo docker push columns/nebula.node

echo 'Images are ready: [columns/nebula.web, columns/nebula.server, columns/nebula.node]'
echo 'DONE!'
