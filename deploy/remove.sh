#!/bin/bash

version=$(cat VERSION)

echo "Removing Phew service..."
docker stop phew &&  docker rm phew

echo "Removing docker image..."
docker rmi phew:${version}

