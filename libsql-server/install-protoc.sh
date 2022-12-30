#!/bin/sh

ARCH=$(arch)

# download protoc
case $ARCH in
    aarch64)
        wget --output-document protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protoc-21.12-linux-aarch_64.zip;;
    x86_64)
        wget --output-document protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protoc-21.12-linux-x86_64.zip;;
esac

unzip protoc.zip -d protoc
mv protoc/bin/protoc /bin/protoc
