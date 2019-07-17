#!/bin/bash
# version: 1
########################################################
#
# AUTOMATICALLY GENERATED! DO NOT EDIT
#
########################################################
set -e

echo "Starting build process in: `pwd`"
source ./ci/setup.sh

if [[ -z "${TZ}" ]]; then
  export TZ='UTC'
fi

if [[ -f "ci/run.sh" ]]; then
    echo "Running custom build script in: `pwd`/ci/run.sh"
    source ./ci/run.sh
else
    echo "Running default build scripts in: `pwd`/ci/build.sh"
    bundle install
    bundle exec rake vendor
    # The postgres driver prints a HUGE no connection stack trace to stderr
    bundle exec rspec spec 2> /dev/null
fi
