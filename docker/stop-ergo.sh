#!/bin/bash

if [ -z "$SCOREX_REST_API_KEY_HASH" ]
  then
    echo "Please export SCOREX_REST_API_KEY_HASH env var to shutdown ergo node properly"
    exit 1
fi

docker compose stop uexplorer

echo "Shutting down Ergo node..."

curl -X POST "http://127.0.0.1:9053/node/shutdown" -H "accept: application/json" -H "api_key: $SCOREX_REST_API_KEY_HASH"

sleep 5

docker compose stop ergo

