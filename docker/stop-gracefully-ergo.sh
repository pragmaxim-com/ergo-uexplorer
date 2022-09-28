#!/bin/bash

if [ -z "$SCOREX_REST_API_KEY" ]
  then
    echo "Please export SCOREX_REST_API_KEY env var to shutdown ergo node properly"
    exit 1
fi

echo "Shutting down Ergo node..."

curl -X POST "http://127.0.0.1:9053/node/shutdown" -H "accept: application/json" -H "api_key: $SCOREX_REST_API_KEY"

echo ""
sleep 5

