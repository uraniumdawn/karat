// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package client

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestComputeGroupLags(t *testing.T) {
	tp := func(topic string, partition int32) TopicPartition {
		return TopicPartition{Topic: topic, Partition: partition}
	}

	committed := map[string]map[TopicPartition]kafka.Offset{
		"lagging": {
			tp("orders", 0): 100,
			tp("orders", 1): 50,
		},
		"caught-up": {
			tp("orders", 0): 200,
		},
		"ahead": { // committed past end (e.g. after a reset) -> clamped to 0
			tp("orders", 0): 250,
		},
		"missing-end": { // no end offset known for this partition -> skipped
			tp("events", 0): 10,
		},
		"empty": {},
	}

	end := map[TopicPartition]kafka.Offset{
		tp("orders", 0): 200,
		tp("orders", 1): 75,
	}

	want := map[string]int64{
		"lagging":     125, // (200-100) + (75-50)
		"caught-up":   0,   // 200-200
		"ahead":       0,   // 200-250 clamped
		"missing-end": 0,   // events/0 has no end offset
		"empty":       0,
	}

	got := computeGroupLags(committed, end)

	if len(got) != len(want) {
		t.Fatalf("got %d groups, want %d: %v", len(got), len(want), got)
	}
	for group, wantLag := range want {
		if got[group] != wantLag {
			t.Errorf("group %q: got lag %d, want %d", group, got[group], wantLag)
		}
	}
}
