#!/bin/bash

version=$(cat VERSION)

echo "Removing Phew Debug service..."
docker stop phew_debug &&  docker rm phew_debug
