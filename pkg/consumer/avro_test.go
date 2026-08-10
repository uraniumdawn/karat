// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package consumer

import (
	"encoding/binary"
	"testing"

	"github.com/hamba/avro/v2"
)

// framed builds a Confluent-framed payload: magic byte, schema id, Avro binary.
func framed(t *testing.T, id int, schema avro.Schema, value any) []byte {
	t.Helper()

	body, err := avro.Marshal(schema, value)
	if err != nil {
		t.Fatalf("marshalling the test value: %v", err)
	}

	payload := make([]byte, wireFormatHeaderLen+len(body))
	binary.BigEndian.PutUint32(payload[1:wireFormatHeaderLen], uint32(id))
	copy(payload[wireFormatHeaderLen:], body)

	return payload
}

// decoderWith returns a decoder that already knows the schema, so no registry is needed.
func decoderWith(id int, schema avro.Schema) *avroDecoder {
	return &avroDecoder{schemas: map[int]avro.Schema{id: schema}}
}

func TestAvroDecoderDecode(t *testing.T) {
	const id = 7

	t.Run("a record becomes JSON", func(t *testing.T) {
		schema := avro.MustParse(`{
			"type": "record", "name": "Alpha", "namespace": "io.karat.local",
			"fields": [
				{"name": "id", "type": "string"},
				{"name": "amount", "type": "double"},
				{"name": "note", "type": ["null", "string"]}
			]
		}`)

		payload := framed(t, id, schema, map[string]any{
			"id":     "a-1",
			"amount": 1.5,
			"note":   map[string]any{"string": "hello"},
		})

		got, err := decoderWith(id, schema).Decode(payload)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		want := `{"amount":1.5,"id":"a-1","note":"hello"}`
		if got != want {
			t.Errorf("Decode() = %s, want %s", got, want)
		}
	})

	t.Run("a string key keeps no JSON quotes", func(t *testing.T) {
		schema := avro.MustParse(`{"type": "string"}`)

		got, err := decoderWith(id, schema).Decode(framed(t, id, schema, "k-1"))
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		if got != "k-1" {
			t.Errorf("Decode() = %q, want k-1", got)
		}
	})

	t.Run("a payload shorter than the framing is an error", func(t *testing.T) {
		if _, err := decoderWith(id, avro.MustParse(`{"type":"string"}`)).Decode([]byte{0, 0}); err == nil {
			t.Error("Decode() accepted a truncated payload")
		}
	})

	t.Run("an unknown schema id without a registry is an error", func(t *testing.T) {
		schema := avro.MustParse(`{"type": "string"}`)
		decoder := &avroDecoder{schemas: map[int]avro.Schema{}}

		if _, err := decoder.Decode(framed(t, id, schema, "k-1")); err == nil {
			t.Error("Decode() accepted an id it has no schema for")
		}
	})
}
