#!/bin/bash -e
# publish docker images to https://hub.docker.com/u/columns
# it prompts for user name and password.
# this script is only used when we want to revert to previous version
echo 'Enter username (columns) and password to continue...'
sudo docker login

# sometimes we want to rollback to pervious version, use below command to do so
# the call of sequence mark specific version as latest
TARGET=latest
WEB=columns/nebula.web
SERVER=columns/nebula.server
NODE=columns/nebula.node
retag()
{
    TAG=$1
    
    # nebula.web
    sudo docker pull $WEB:$TAG
    sudo docker tag $WEB:$TAG $WEB:$TARGET
    sudo docker push $WEB:$TARGET
    
    # nebula.node
    sudo docker pull $NODE:$TAG
    sudo docker tag $NODE:$TAG $NODE:$TARGET
    sudo docker push $NODE:$TARGET
    
    # nebula.web
    sudo docker pull $SERVER:$TAG
    sudo docker tag $SERVER:$TAG $SERVER:$TARGET
    sudo docker push $SERVER:$TARGET
}

# run the script as so:
# sudo retag <hash>
retag $1
