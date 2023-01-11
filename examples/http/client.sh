#!/usr/bin/env bash

ADDR=${1:-localhost:8000}

if ! command -v curl &> /dev/null; then
    echo "Install curl first"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "Install jq first"
    exit 1
fi

if ! command -v jtbl &> /dev/null; then
    echo "Install jtbl first (pip install --user jtbl)"
    exit 1
fi

function ctrl_c() {
    printf "\033[7h"
    echo "use Ctrl+D to exit"
    echo -n "sqld> "
}
trap ctrl_c INT

# Connection check
if ! curl -s --show-error $ADDR; then
    exit 1
fi
echo "Connected to $ADDR"

# Main loop
while read -p "sqld> " -r line; do
    if [ -z "$line" ]; then
        continue
    fi
    output=$(curl -s --show-error -X POST -d "{\"statements\": [\"$line\"]}" $ADDR)
    if [ $? -ne "0" ]; then
        exit 1
    fi
    length=$(echo $output | jq length)
    if [ $length -gt 0 ]; then
        echo $output | jq --sort-keys | jtbl
    fi
done
