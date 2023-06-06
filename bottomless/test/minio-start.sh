#!/usr/bin/env bash

export AWS_ACCESS_KEY_ID=miniotest
export AWS_DEFAULT_REGION=eu-west-1
export AWS_SECRET_ACCESS_KEY=miniotest
export LIBSQL_BOTTOMLESS_ENDPOINT=http://127.0.0.1:9000/

DIRECTORY=$(cd `dirname $0` && pwd)
docker run -dt                                  \
  -p 9000:9000 -p 9090:9090                     \
  -v $DIRECTORY/minio-mnt/      \
  --name "minio_local"                          \
  quay.io/minio/minio server $DIRECTORY/minio-mnt/ --console-address ":9090"