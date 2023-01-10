#!/bin/bash

set -Eeuo pipefail

SQLD_NODE="${SQLD_NODE:-primary}"

if [ "$1" = '/bin/sqld' ]; then
  # We are running the server.
  declare -a server_args=()

  # Listen to PostgreSQL port by default.
  server_args+=("--pg-listen-addr" "0.0.0.0:5000")

  # Listen on HTTP 8080 port by default.
  server_args+=("--http-listen-addr" "0.0.0.0:8080")

  # Set remaining arguments depending on what type of node we are.
  case "$SQLD_NODE" in
    primary)
      server_args+=("--grpc-listen-addr" "0.0.0.0:5001")
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
