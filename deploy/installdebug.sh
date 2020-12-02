#!/bin/bash

#-----------------------------
version=$(cat VERSION)
echo 'Installing Phew ('${version}')...'

echo 'Building docker image...'
cd ..
cp ./deploy/Dockerfile ./
docker build --build-arg VER=1.0.0 -f ./Dockerfile -t phew:${version} .
rm Dockerfile
cd ./deploy

#make dir for data
if [ ! -d "/data" ]; then
	mkdir /data
fi
if [ ! -d "/data/phewdata" ]; then
	echo 'Making path (/data/phewdata) for cache...'
	mkdir /data/phewrdata
fi

echo 'starting Phew Debug mode...'
#start container
docker run \
--name phew_debug \
--network=host \
-v /data/phewdata:/phewdata \
-v /data/yn/dzjz/:/juanzong \
-it phew:${version} \
bash

