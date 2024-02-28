#!/bin/bash

set -Eeuo pipefail

SQLD_NODE="${SQLD_NODE:-primary}"

SQLD_DB_PATH="${SQLD_DB_PATH:-iku.db}"
SQLD_HTTP_LISTEN_ADDR="${SQLD_HTTP_LISTEN_ADDR:-"0.0.0.0:8080"}"

if [ "$1" = '/bin/sqld' ]; then
  # We are running the server.
  declare -a server_args=()

  server_args+=("--db-path" "$SQLD_DB_PATH")

  # Listen on HTTP 8080 port by default.
  server_args+=("--http-listen-addr" "$SQLD_HTTP_LISTEN_ADDR")

  # Set remaining arguments depending on what type of node we are.
  case "$SQLD_NODE" in
    primary)
      SQLD_GRPC_LISTEN_ADDR="${SQLD_GRPC_LISTEN_ADDR:-"0.0.0.0:5001"}"
      server_args+=("--grpc-listen-addr" "$SQLD_GRPC_LISTEN_ADDR")
      ;;
    replica)
      server_args+=("--primary-grpc-url" "$SQLD_PRIMARY_URL")
      ;;
    standalone)
      ;;
  esac

  # Append server arguments.
  set -- "$@" ${server_args[@]}
fi

exec "$@"
