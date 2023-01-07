#!/bin/bash

set -e
set -o pipefail

. /etc/os-release

if [ "$ID" = "ubuntu" ] || [ "$ID" = "debian" ]; then
  curl -sL https://deb.nodesource.com/setup_14.x | sudo bash -
  apt install --yes \
    bundler \
    libpq-dev \
    libsqlite3-dev \
    nodejs \
    protobuf-compiler 
elif [ "$ID" = "fedora" ]; then
  dnf install -y \
    libpq-devel \
    libsqlite3x-devel \
    nodejs \
    npm \
    protobuf-compiler \
    rubygem-bundler \
    rubygem-sqlite3 
else
  echo "Operating system $ID is not supported by this installer."
fi
