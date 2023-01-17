#!/bin/bash

set -e
set -o pipefail

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  if [ -f "/etc/os-release" ]; then
    . /etc/os-release
  fi

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
      rubygem-sqlite3 \
      ruby-devel
  else
    echo "Linux distribution $ID is not supported by this installer."
  fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
  brew install protobuf
else
  echo "Your operating system is not supported by this installer."
fi
