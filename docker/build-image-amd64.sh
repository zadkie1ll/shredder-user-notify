#!/bin/bash

docker buildx build \
  --platform linux/amd64 \
  -t monkey-island-user-notify:v0.1 \
  -f Dockerfile \
  --output type=docker,dest=monkey-island-user-notify-amd64.tar ..
