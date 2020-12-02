#!/bin/bash

#pull base image
docker pull golang:latest

#-----------------------------
version=$(cat ../VERSION)

#build image
docker build -f ./BuildImageDockerfile -t phew_image:${version} .

