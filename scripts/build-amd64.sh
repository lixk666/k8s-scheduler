#!/usr/bin/env bash
set -euo pipefail

IMAGE="${IMAGE:-10.14.2.6:8091/bigdata/load-aware-scheduler:v0.1.8}"
PLATFORM="${PLATFORM:-linux/amd64}"

docker buildx build \
  --platform "${PLATFORM}" \
  -t "${IMAGE}" \
  --push \
  .
