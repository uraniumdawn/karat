// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

// Package consumer provides a native Kafka message consumer backed by confluent-kafka-go.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/rs/zerolog/log"
)

// Format controls how message values are decoded.
type Format int

const (
	// FormatJSON attempts to parse and pretty-print values as JSON.
	FormatJSON Format = iota
	// FormatAvro decodes values using the Confluent Schema Registry Avro serde.
	FormatAvro
)

// FromSpec describes where consumption starts.
type FromSpec struct {
	// Type is one of "beginning", "end", "offset", "tail", "timestamp".
	// "tail" means start Offset messages before the current high-water mark.
	Type      string
	Offset    int64 // used when Type == "offset" or "tail"
	Timestamp int64 // unix ms, used when Type == "timestamp"
}

// ToSpec describes the optional stop condition.
type ToSpec struct {
	// Type is one of "" (no stop), "offset", "timestamp".
	Type      string
	Offset    int64 // exclusive upper bound on partition offset
	Timestamp int64 // unix ms exclusive upper bound on message timestamp
}

// Params holds all parameters required to start a consumer session.
type Params struct {
	// KafkaConf is the base cluster config (bootstrap.servers + auth properties).
	// Consume will overlay consumer-specific settings on top of a copy.
	KafkaConf *kafka.ConfigMap
	Topic     string
	From      FromSpec
	To        ToSpec
	Format    Format
	// SRClient is required when Format == FormatAvro; nil otherwise.
	SRClient schemaregistry.Client
	// ExitOnEnd stops consumption as soon as every partition reaches its current
	// high-water mark (equivalent to kcat -e).
	ExitOnEnd bool
	// Partitions restricts consumption to the given partition IDs.
	// An empty slice means consume all partitions.
	Partitions []int32
	// MaxCount stops consumption after this many messages per partition have been delivered.
	// Zero means unlimited.
	MaxCount int64
	// KeySerdes and ValueSerdes override Format-based decoding when non-zero.
	KeySerdes   Serdes
	ValueSerdes Serdes
}

// Record is a single decoded Kafka message.
type Record struct {
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       string
	Value     string   // compact JSON or raw string on decode failure
	Headers   []string // "key=value" pairs
	Size      int      // original payload size in bytes
}

// Consume streams records from the configured topic into records until ctx is
// cancelled or the ToSpec stop-condition is reached on every partition.
// Both channels are closed when Consume returns.
func Consume(ctx context.Context, params Params, records chan<- Record, errs chan<- error) {
	defer close(records)
	defer close(errs)

	c, cleanup, err := newConsumer(params)
	if err != nil {
		errs <- err
		return
	}
	defer func() { go cleanup() }()

	partitions, err := resolvePartitions(c, params)
	if err != nil {
		errs <- err
		return
	}

	if err := c.Assign(partitions); err != nil {
		errs <- fmt.Errorf("assign partitions: %w", err)
		return
	}

	// One decoder serves both key and value: schemas are resolved by the id inside each
	// payload, so the key and the value need no separate configuration.
	var decoder *avroDecoder
	if params.SRClient != nil {
		decoder = newAvroDecoder(params.SRClient)
	}

	numPartitions := len(partitions)
	stoppedPartitions := make(map[int32]bool, numPartitions)
	partitionDelivered := make(map[int32]int64, numPartitions)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		ev := c.Poll(50)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			partition := e.TopicPartition.Partition
			if stoppedPartitions[partition] {
				continue
			}

			offset := int64(e.TopicPartition.Offset)

			if shouldStop(params.To, offset, e.Timestamp) {
				stoppedPartitions[partition] = true
				if len(stoppedPartitions) >= numPartitions {
					return
				}
				continue
			}

			key := string(e.Key)
			if params.KeySerdes.Kind != SerdesRaw {
				decoded, decErr := decodeWithSerdes(e.Key, params.KeySerdes, decoder)
				if decErr != nil {
					select {
					case errs <- decErr:
					case <-ctx.Done():
						return
					}
				} else {
					key = decoded
				}
			}

			size := len(e.Value)
			var value string
			var decErr error
			if params.ValueSerdes.Kind != SerdesRaw {
				value, decErr = decodeWithSerdes(e.Value, params.ValueSerdes, decoder)
			} else {
				value, decErr = decodeValue(e.Value, params.Format, decoder)
			}
			if decErr != nil {
				select {
				case errs <- decErr:
				case <-ctx.Done():
					return
				}
				value = string(e.Value)
			}

			headers := make([]string, 0, len(e.Headers))
			for _, h := range e.Headers {
				headers = append(headers, fmt.Sprintf("%s=%s", h.Key, string(h.Value)))
			}

			select {
			case records <- Record{
				Partition: partition,
				Offset:    offset,
				Timestamp: e.Timestamp,
				Key:       key,
				Value:     value,
				Headers:   headers,
				Size:      size,
			}:
			case <-ctx.Done():
				return
			}
			if params.MaxCount > 0 {
				partitionDelivered[partition]++
				if partitionDelivered[partition] >= params.MaxCount {
					stoppedPartitions[partition] = true
					if len(stoppedPartitions) >= numPartitions {
						return
					}
				}
			}

		case kafka.Error:
			log.Error().Err(e).Msg("consumer error")
			select {
			case errs <- e:
			case <-ctx.Done():
				return
			}
			if e.IsFatal() {
				return
			}

		case kafka.PartitionEOF:
			if params.ExitOnEnd {
				stoppedPartitions[e.Partition] = true
				if len(stoppedPartitions) >= numPartitions {
					return
				}
			}

		default:
			// Ignore other event types (stats, log events handled via channel).
		}
	}
}

// newConsumer creates a kafka.Consumer using params.KafkaConf as the base and
// overlaying consumer-specific settings. Returns a cleanup func that closes
// both the consumer and its log drain goroutine.
func newConsumer(params Params) (*kafka.Consumer, func(), error) {
	conf := make(kafka.ConfigMap)
	if params.KafkaConf != nil {
		for k, v := range *params.KafkaConf {
			conf[k] = v
		}
	}

	// Override/add consumer-specific settings.
	//
	// The group id is always karat's own and never the user's: partitions are assigned
	// explicitly and nothing is ever committed, so naming a real group would put karat's
	// client id on it without it ever joining or moving its offsets — a group that looks
	// consumed and is not.
	_ = conf.SetKey("group.id", fmt.Sprintf("karat-%d", time.Now().UnixNano()))
	_ = conf.SetKey("enable.auto.commit", false)
	// explicit assignment via Assign(), reject out-of-range offsets
	_ = conf.SetKey("auto.offset.reset", "error")
	if params.ExitOnEnd {
		// Required for librdkafka to emit PartitionEOF events via Poll().
		_ = conf.SetKey("enable.partition.eof", true)
	}

	logChan := make(chan kafka.LogEvent, 32)
	_ = conf.SetKey("go.logs.channel.enable", true)
	_ = conf.SetKey("go.logs.channel", logChan)

	c, err := kafka.NewConsumer(&conf)
	if err != nil {
		close(logChan)
		return nil, nil, fmt.Errorf("create consumer: %w", err)
	}

	logDone := make(chan struct{})
	go func() {
		defer close(logDone)
		for ev := range logChan {
			log.Debug().Str("kafka", ev.Message).Msg("librdkafka")
		}
	}()

	cleanup := func() {
		_ = c.Close()
		close(logChan)
		<-logDone
	}

	return c, cleanup, nil
}

// resolvePartitions builds the []TopicPartition list with correct starting
// offsets derived from params.From.
func resolvePartitions(c *kafka.Consumer, params Params) ([]kafka.TopicPartition, error) {
	meta, err := c.GetMetadata(&params.Topic, false, 10_000)
	if err != nil {
		return nil, fmt.Errorf("get metadata for %q: %w", params.Topic, err)
	}

	topicMeta, ok := meta.Topics[params.Topic]
	if !ok || len(topicMeta.Partitions) == 0 {
		return nil, fmt.Errorf("topic %q not found or has no partitions", params.Topic)
	}

	sourceParts := topicMeta.Partitions
	if len(params.Partitions) > 0 {
		existing := make(map[int32]bool, len(topicMeta.Partitions))
		for _, p := range topicMeta.Partitions {
			existing[p.ID] = true
		}
		for _, id := range params.Partitions {
			if !existing[id] {
				return nil, fmt.Errorf("partition %d does not exist in topic %q", id, params.Topic)
			}
		}
		allowed := make(map[int32]bool, len(params.Partitions))
		for _, id := range params.Partitions {
			allowed[id] = true
		}
		filtered := topicMeta.Partitions[:0:0]
		for _, p := range topicMeta.Partitions {
			if allowed[p.ID] {
				filtered = append(filtered, p)
			}
		}
		sourceParts = filtered
	}

	partitions := make([]kafka.TopicPartition, len(sourceParts))
	for i, p := range sourceParts {
		partitions[i] = kafka.TopicPartition{
			Topic:     &params.Topic,
			Partition: p.ID,
		}
	}

	switch params.From.Type {
	case "beginning":
		for i := range partitions {
			partitions[i].Offset = kafka.OffsetBeginning
		}

	case "end":
		for i := range partitions {
			partitions[i].Offset = kafka.OffsetEnd
		}

	case "tail":
		for i := range partitions {
			partitions[i].Offset = kafka.OffsetTail(kafka.Offset(params.From.Offset))
		}

	case "offset":
		for i := range partitions {
			partitions[i].Offset = kafka.Offset(params.From.Offset)
		}

	case "timestamp":
		// OffsetsForTimes uses the Offset field as the target timestamp in ms.
		for i := range partitions {
			partitions[i].Offset = kafka.Offset(params.From.Timestamp)
		}
		resolved, err := c.OffsetsForTimes(partitions, 10_000)
		if err != nil {
			return nil, fmt.Errorf("resolve timestamp offsets: %w", err)
		}
		// OffsetsForTimes returns OffsetEnd when no message exists at/after
		// the timestamp; treat that as "no existing messages, wait for new ones".
		partitions = resolved

	default:
		return nil, fmt.Errorf("unknown From.Type %q", params.From.Type)
	}

	return partitions, nil
}

// shouldStop returns true when the given message offset/timestamp has reached
// or exceeded the ToSpec bound.
func shouldStop(to ToSpec, offset int64, ts time.Time) bool {
	switch to.Type {
	case "offset":
		return offset >= to.Offset
	case "timestamp":
		return ts.UnixMilli() >= to.Timestamp
	}
	return false
}

// confluentMagicBytes are the valid first bytes of a Confluent Schema Registry
// wire-format payload (MagicByteV0 = 0x00, MagicByteV1 = 0x01).
func isConfluentWireFormat(payload []byte) bool {
	return len(payload) > 5 && (payload[0] == 0x00 || payload[0] == 0x01)
}

// decodeValue decodes a raw Kafka message value based on the requested format.
// For Avro, the payload must start with a Confluent Schema Registry magic byte
// (0x00 or 0x01). If it doesn't, the function falls back to JSON/raw display
// rather than returning an error, since the message was simply not Avro-encoded.
func decodeValue(payload []byte, format Format, decoder *avroDecoder) (string, error) {
	if len(payload) == 0 {
		return "", nil
	}

	switch format {
	case FormatAvro:
		if decoder != nil && isConfluentWireFormat(payload) {
			return decoder.Decode(payload)
		}
		// Not a Confluent SR payload — fall through to JSON/raw display.
		fallthrough

	default: // FormatJSON (and Avro fallback)
		var raw json.RawMessage
		if err := json.Unmarshal(payload, &raw); err != nil {
			return string(payload), nil
		}
		out, err := json.Marshal(raw)
		if err != nil {
			return string(payload), nil
		}
		return string(out), nil
	}
}
