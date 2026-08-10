// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"maps"
	"slices"
	"strings"
	"testing"
)

func TestTopicChanges(t *testing.T) {
	tests := []struct {
		name              string
		currentPartitions int
		newPartitions     int
		current           map[string]string
		edited            map[string]string
		wantSet           map[string]string
		wantRemoved       []string
		wantEmpty         bool
	}{
		{
			name:              "an untouched document changes nothing",
			currentPartitions: 12,
			newPartitions:     12,
			current:           map[string]string{"cleanup.policy": "compact"},
			edited:            map[string]string{"cleanup.policy": "compact"},
			wantSet:           map[string]string{},
			wantEmpty:         true,
		},
		{
			name:              "a changed value is set, an unchanged sibling is not",
			currentPartitions: 12,
			newPartitions:     12,
			current: map[string]string{
				"cleanup.policy": "delete",
				"retention.ms":   "604800000",
			},
			edited: map[string]string{
				"cleanup.policy": "compact",
				"retention.ms":   "604800000",
			},
			wantSet: map[string]string{"cleanup.policy": "compact"},
		},
		{
			name:              "a new key is set",
			currentPartitions: 3,
			newPartitions:     3,
			current:           map[string]string{},
			edited:            map[string]string{"max.message.bytes": "10485760"},
			wantSet:           map[string]string{"max.message.bytes": "10485760"},
		},
		{
			name:              "a dropped key is reset to the cluster default",
			currentPartitions: 3,
			newPartitions:     3,
			current: map[string]string{
				"cleanup.policy": "compact",
				"retention.ms":   "604800000",
			},
			edited:      map[string]string{"cleanup.policy": "compact"},
			wantSet:     map[string]string{},
			wantRemoved: []string{"retention.ms"},
		},
		{
			name:              "growing the partition count alone is a change",
			currentPartitions: 12,
			newPartitions:     24,
			current:           map[string]string{},
			edited:            map[string]string{},
			wantSet:           map[string]string{},
		},
		{
			// validateTopicDocumentEdit rejects a decrease before this point, but an equal
			// count must not read as a change either.
			name:              "an unchanged partition count is not a change",
			currentPartitions: 12,
			newPartitions:     12,
			current:           map[string]string{},
			edited:            map[string]string{},
			wantSet:           map[string]string{},
			wantEmpty:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := topicChanges(
				"orders",
				tt.currentPartitions,
				tt.newPartitions,
				tt.current,
				tt.edited,
			)

			if !maps.Equal(got.Set, tt.wantSet) {
				t.Errorf("Set = %v, want %v", got.Set, tt.wantSet)
			}
			if !slices.Equal(got.Removed, tt.wantRemoved) {
				t.Errorf("Removed = %v, want %v", got.Removed, tt.wantRemoved)
			}
			if got.empty() != tt.wantEmpty {
				t.Errorf("empty() = %v, want %v", got.empty(), tt.wantEmpty)
			}
			if got.Name != "orders" {
				t.Errorf("Name = %q, want %q", got.Name, "orders")
			}
		})
	}
}

func TestRenderTopicChanges(t *testing.T) {
	change := topicChanges(
		"orders",
		12,
		24,
		map[string]string{"cleanup.policy": "delete", "retention.ms": "604800000"},
		map[string]string{"cleanup.policy": "compact", "max.message.bytes": "10485760"},
	)

	got := renderTopicChanges(change)

	for _, want := range []string{
		"partitions  12 -> 24",
		"cleanup.policy = compact",
		"max.message.bytes = 10485760",
		"retention.ms (reset to cluster default)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("renderTopicChanges() is missing %q:\n%s", want, got)
		}
	}

	// A key whose value did not change must not be listed as if it were being written.
	if strings.Contains(got, "retention.ms = ") {
		t.Errorf("renderTopicChanges() lists a reset key as a write:\n%s", got)
	}
}

// The partition line is only meaningful when the count actually grows.
func TestRenderTopicChangesOmitsUnchangedPartitions(t *testing.T) {
	change := topicChanges("orders", 12, 12, nil, map[string]string{"cleanup.policy": "compact"})

	if got := renderTopicChanges(change); strings.Contains(got, "partitions") {
		t.Errorf("renderTopicChanges() mentions partitions when they did not change:\n%s", got)
	}
}

func TestRenderNewTopic(t *testing.T) {
	got := renderNewTopic(TopicParams{
		TopicName:         "orders",
		ReplicationFactor: 3,
		Partitions:        12,
		Config:            map[string]string{"cleanup.policy": "compact"},
	})

	for _, want := range []string{"orders", "3", "12", "cleanup.policy = compact"} {
		if !strings.Contains(got, want) {
			t.Errorf("renderNewTopic() is missing %q:\n%s", want, got)
		}
	}
}

// A topic with no overrides must not grow an empty "configs" heading.
func TestRenderNewTopicWithoutConfigs(t *testing.T) {
	got := renderNewTopic(TopicParams{TopicName: "orders", ReplicationFactor: 1, Partitions: 1})

	if strings.Contains(got, "configs") {
		t.Errorf("renderNewTopic() has a configs heading with no configs:\n%s", got)
	}
}
