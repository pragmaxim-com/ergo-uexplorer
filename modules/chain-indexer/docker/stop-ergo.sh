#!/bin/bash

SECRET=$1

if [ -z "$SECRET" ]
  then
    echo "Please pass scorex.restApi.apiKeyHash as argument to shutdown ergo node properly"
    exit 1
fi

docker compose stop uexplorer

echo "Shutting down Ergo node..."

curl -X POST "http://127.0.0.1:9053/node/shutdown" -H "accept: application/json" -H "api_key: $SECRET"

sleep 5

docker compose stop ergo

