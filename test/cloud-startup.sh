#! /bin/bash
apt update
apt -y install docker-compose

# clone git 
git clone https://github.com/shawncao/nebula.git

# go to target folder to get cluster config and docker config
cd nebula/test

# replace the single node with current host name
sed -i "s/<hostname>/$HOSTNAME/g" local-cluster.yml

# pull images and fire them up
docker pull caoxhua/nebula.server
docker pull caoxhua/nebula.node
docker pull caoxhua/nebula.envoy
docker pull caoxhua/nebula.web
docker-compose down
docker-compose up -d
