#!/bin/bash

NAMES=("rockset" "is" "awesome" "with" "nebula" "super" "great")
gen()
{
  payload="{\"data\": [{\"name\": \"${NAMES[$RANDOM % 8]}\", \"value\":$RANDOM}]}"
  echo $payload
  curl --request POST \
    --url https://api.rs2.usw2.rockset.com/v1/orgs/self/ws/commons/collections/test-rt/docs \
    -H 'Authorization: ApiKey <API_KEY>' \
    -H 'Content-Type: application/json' \
    -d "$payload"
}

# gen
i=0
while true
do
  gen >> /dev/null
  sleep .1
  let i++
  if [ "$(($i % 100))" == "0" ]; then
    echo "has sent $i messages to rockset, continue..."
  fi
done