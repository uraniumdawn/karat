#!/bin/sh
set -e

# Registers the connectors the local karat test environment expects.
# Idempotent: uses PUT /connectors/<name>/config, so re-running just updates the config.

CONNECT_URL=${CONNECT_URL:-http://kafka-connect:8083}

apk add --no-cache curl >/dev/null

echo "Waiting for Kafka Connect at $CONNECT_URL..."
timeout=120
while ! curl -sf "$CONNECT_URL/connectors" >/dev/null && [ $timeout -gt 0 ]; do
  echo "Still waiting..."
  sleep 3
  timeout=$((timeout - 3))
done
if [ $timeout -le 0 ]; then
  echo "Kafka Connect did not become ready in time."
  exit 1
fi

register() {
  name=$1
  config=$2

  code=$(curl -s -o /tmp/resp -w "%{http_code}" -X PUT "$CONNECT_URL/connectors/$name/config" \
    -H "Content-Type: application/json" -d "$config")

  if [ "$code" -ge 400 ]; then
    echo "Failed to register connector '$name'. HTTP $code:"
    cat /tmp/resp
    exit 1
  fi
  echo "Connector '$name' registered."
}

register file-source '{
  "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
  "tasks.max": "1",
  "file": "/data/lines.txt",
  "topic": "connect-file-lines"
}'

register file-sink '{
  "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
  "tasks.max": "1",
  "file": "/tmp/connect-file-sink.out",
  "topics": "connect-file-lines"
}'

# Reads plain strings through the JSON converter with schemas enabled, so the first record
# blows up the task. Config validation passes, the task ends up FAILED — the Connectors page
# needs a non-RUNNING connector to inspect / restart.
register file-sink-broken '{
  "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
  "tasks.max": "1",
  "file": "/tmp/connect-file-sink-broken.out",
  "topics": "connect-file-lines",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true",
  "errors.tolerance": "none"
}'

echo "Connectors registered successfully."
curl -s "$CONNECT_URL/connectors" && echo
