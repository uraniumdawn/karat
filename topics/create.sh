#!/bin/bash
set -e

# Creates the topics and ACLs the local karat test environment expects.
# Idempotent: safe to re-run against an existing cluster.

PATH=/opt/kafka/bin:$PATH

KAFKA_HOST=${KAFKA_BROKER:-kafka:29092}
MAX_RETRIES=30
RETRY_INTERVAL=5

echo "Waiting for Kafka broker at $KAFKA_HOST..."

ready=0
for ((i = 1; i <= MAX_RETRIES; i++)); do
  if echo >/dev/tcp/$(echo $KAFKA_HOST | cut -d: -f1)/$(echo $KAFKA_HOST | cut -d: -f2) 2>/dev/null; then
    ready=1
    break
  fi
  echo "Attempt $i/$MAX_RETRIES: Kafka not ready yet..."
  sleep $RETRY_INTERVAL
done

if [ "$ready" -ne 1 ]; then
  echo "Kafka did not become ready in time. Exiting."
  exit 1
fi

echo "Kafka is available! Creating topics..."

create_topic() {
  local topic=$1 partitions=$2
  shift 2
  local configs=()
  for cfg in "$@"; do
    configs+=(--config "$cfg")
  done

  kafka-topics.sh --create --if-not-exists --bootstrap-server "$KAFKA_HOST" \
    --replication-factor 1 --partitions "$partitions" --topic "$topic" "${configs[@]}"
}

# Topics carrying generated Avro traffic
create_topic stream-alpha 4
create_topic stream-beta 4

# Target of the transactional producer (docker-compose --profile txn)
create_topic txn-events 3

# Target of the Connect file source connector
create_topic connect-file-lines 1

# Compacted topic, exercises the config view and compaction-specific columns
create_topic user-profiles 2 cleanup.policy=compact min.cleanable.dirty.ratio=0.1 segment.ms=60000

# Short retention, exercises the topic config editor
create_topic audit-log 1 retention.ms=3600000 segment.bytes=10485760 max.message.bytes=1048576

# Matches the default internal_topic_patterns, so <i> on the Topics page has an effect
create_topic orders-changelog 1 cleanup.policy=compact
create_topic orders-repartition 2

# Edge case: a topic that stays empty
create_topic empty-topic 1

echo "Kafka topics created successfully."

echo "Creating ACLs..."

# The broker runs StandardAuthorizer with allow.everyone.if.no.acl.found=true and
# User:ANONYMOUS as super user, so these entries only populate the ACLs page; they do not
# restrict any client in this environment.
add_acl() {
  kafka-acls.sh --bootstrap-server "$KAFKA_HOST" --add --force "$@"
}

add_acl --allow-principal User:alice --operation Read --operation Describe --topic stream-alpha
add_acl --allow-principal User:alice --operation Read --group stream-alpha-live
add_acl --allow-principal User:bob --operation Write --operation Describe --topic stream-beta
add_acl --allow-principal User:bob --operation All --topic 'orders-' --resource-pattern-type prefixed
add_acl --deny-principal User:mallory --operation All --topic '*'
add_acl --allow-principal User:connect --operation All --cluster

echo "ACLs created successfully."
kafka-acls.sh --bootstrap-server "$KAFKA_HOST" --list
