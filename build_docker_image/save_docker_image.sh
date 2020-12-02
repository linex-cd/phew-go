#!/bin/bash

version=$(cat ../VERSION)

#save image
docker save -o phew_image_${version}.tar phew_image:${version}