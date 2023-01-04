#!/bin/bash

set -Eeuo pipefail

SQLD_NODE="${SQLD_NODE:-primary}"

if [ "$1" = '/bin/sqld' ]; then
  # We are running the server.
  declare -a server_args=()

  # Listen to PostgreSQL port by default.
  server_args+=("--pg-listen-addr" "0.0.0.0:5000")

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
