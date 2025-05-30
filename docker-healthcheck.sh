#!/usr/bin/env bash
set -euo pipefail

SQLD_HTTP_LISTEN_ADDR="${SQLD_HTTP_LISTEN_ADDR:-"0.0.0.0:8080"}"
SQLD_HTTP_LISTEN_ADDR="${SQLD_HTTP_LISTEN_ADDR//:/\/}"

exec 3<>"/dev/tcp/$SQLD_HTTP_LISTEN_ADDR"
echo -e "GET /health HTTP/1.1\r\nConnection: close\r\n\r\n" >&3
RESPONSE=$(cat <&3)
exec 3<&- && exec 3>&-

if echo "$RESPONSE" | grep -q "HTTP/1.1 200 OK"; then
    exit 0
else
    echo "Did not receive HTTP 200 response"
    echo "$RESPONSE"
    exit 1
fi
