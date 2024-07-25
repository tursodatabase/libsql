#!/bin/bash

set -Eeuo pipefail

SQLD_DB_PATH="${SQLD_DB_PATH:-iku.db}"
mkdir -p $SQLD_DB_PATH
chown -R sqld:sqld $SQLD_DB_PATH
exec gosu sqld docker-entrypoint.sh "$@"
