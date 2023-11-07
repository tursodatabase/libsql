#!/usr/bin/env sh

docker run --net=host --rm --name libsql-server-prometheus -v $(dirname $0)/prometheus_docker.yml:/etc/prometheus/prometheus.yml prom/prometheus
