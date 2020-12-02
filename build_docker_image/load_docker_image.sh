#!/bin/bash

version=$(cat ../VERSION)

#load
docker load -i phew_image_${version}.tar
