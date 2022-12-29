#!/bin/bash

set -e
set -o pipefail

. /etc/os-release

if [ "$ID" = "ubuntu" ] || [ "$ID" = "debian" ]; then
  apt install --yes \
    bundler \
    libpq-dev \
    libsqlite3-dev \
    nodejs \
    npm \
    protobuf-compiler 
elif [ "$ID" = "fedora" ]; then
  dnf install -y \
    libpq-devel \
    libsqlite3x-devel \
    nodejs \
    npm \
    protobuf-compiler \
    rubygem-bundler
else
  echo "Operating system $ID is not supported by this installer."
fi
