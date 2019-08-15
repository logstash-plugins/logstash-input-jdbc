#!/usr/bin/env bash

# This is intended to be run inside the docker container as the command of the docker-compose.
set -ex
docker-compose -f ci/unit/docker-compose.yml up --exit-code-from logstash
