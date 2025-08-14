#!/bin/bash

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 690944315170.dkr.ecr.us-west-2.amazonaws.com
docker buildx build --platform linux/amd64,linux/arm64 -t 690944315170.dkr.ecr.us-west-2.amazonaws.com/go_stream --push .
