#!/bin/bash

URL=$1

curl -X POST -H "Content-Type: application/json" -d @setup.json $URL

ab -c 10 -n 10000 -p query.json -T application/json $URL
