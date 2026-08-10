// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package consumer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/hamba/avro/v2"
)

// wireFormatHeaderLen is the size of the Confluent framing: one magic byte plus a 4-byte
// big-endian schema id.
const wireFormatHeaderLen = 5

// avroDecoder turns Confluent-framed Avro payloads into JSON. Schemas are looked up by the id
// in the payload and parsed once per id.
//
// The confluent-kafka-go GenericDeserializer is deliberately not used here: it unmarshals into
// a value produced by a caller-supplied MessageFactory, so it needs a Go type per schema, which
// karat does not have — it decodes whatever the topic happens to carry.
type avroDecoder struct {
	client schemaregistry.Client

	mu      sync.Mutex
	schemas map[int]avro.Schema
}

// newAvroDecoder returns a decoder that resolves schemas through the given registry client.
func newAvroDecoder(client schemaregistry.Client) *avroDecoder {
	return &avroDecoder{client: client, schemas: make(map[int]avro.Schema)}
}

// Decode returns the payload as JSON. The payload must be Confluent-framed; check with
// isConfluentWireFormat first.
func (d *avroDecoder) Decode(payload []byte) (string, error) {
	if len(payload) < wireFormatHeaderLen {
		return "", fmt.Errorf("avro payload is %d bytes, too short to be Confluent-framed", len(payload))
	}

	id := int(binary.BigEndian.Uint32(payload[1:wireFormatHeaderLen]))

	schema, err := d.schema(id)
	if err != nil {
		return "", err
	}

	var value any
	if err := avro.Unmarshal(schema, payload[wireFormatHeaderLen:], &value); err != nil {
		return "", fmt.Errorf("avro deserialize with schema id %d: %w", id, err)
	}

	// A plain string schema — the usual key schema — is returned unquoted, so that an Avro key
	// reads the same as a raw one and the output format can quote it itself.
	if s, ok := value.(string); ok {
		return s, nil
	}

	out, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value), nil
	}

	return string(out), nil
}

// schema returns the parsed schema for an id, fetching it from the registry on first use.
func (d *avroDecoder) schema(id int) (avro.Schema, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if schema, ok := d.schemas[id]; ok {
		return schema, nil
	}

	if d.client == nil {
		return nil, fmt.Errorf("no schema registry is selected, cannot resolve schema id %d", id)
	}

	// An empty subject asks the registry for the schema by id alone.
	info, err := d.client.GetBySubjectAndID("", id)
	if err != nil {
		return nil, fmt.Errorf("fetching schema id %d: %w", id, err)
	}

	schema, err := avro.Parse(info.Schema)
	if err != nil {
		return nil, fmt.Errorf("parsing schema id %d: %w", id, err)
	}

	d.schemas[id] = schema

	return schema, nil
}
