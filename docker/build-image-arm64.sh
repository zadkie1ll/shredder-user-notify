#!/bin/bash

docker buildx build \
  --platform linux/arm64 \
  -t monkey-island-user-notify:v0.1 \
  -f Dockerfile \
  --output type=docker,dest=monkey-island-user-notify-arm64.tar ..
