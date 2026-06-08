// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"regexp"
	"testing"
)

func TestIsInternalTopic(t *testing.T) {
	patterns := []*regexp.Regexp{
		regexp.MustCompile("^__.*"),
		regexp.MustCompile(".*-changelog$"),
		regexp.MustCompile(".*-repartition$"),
	}

	tests := []struct {
		name string
		want bool
	}{
		{"__consumer_offsets", true},
		{"__transaction_state", true},
		{"my-app-store-changelog", true},
		{"my-app-repartition", true},
		{"orders", false},
		{"orders-events", false},
		{"changelog-topic", false},
	}

	for _, tt := range tests {
		if got := isInternalTopic(tt.name, patterns); got != tt.want {
			t.Errorf("isInternalTopic(%q, patterns) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestIsInternalTopicNoPatterns(t *testing.T) {
	if isInternalTopic("__consumer_offsets", nil) {
		t.Error("isInternalTopic with no patterns should never report a topic as internal")
	}
}

func TestIsInternalTopicCustomPatterns(t *testing.T) {
	patterns := []*regexp.Regexp{
		regexp.MustCompile("^_schemas$"),
		regexp.MustCompile("^connect-.*"),
	}

	tests := []struct {
		name string
		want bool
	}{
		{"_schemas", true},
		{"connect-configs", true},
		{"connect-offsets", true},
		{"orders", false},
		{"__consumer_offsets", false}, // not matched unless configured
	}

	for _, tt := range tests {
		if got := isInternalTopic(tt.name, patterns); got != tt.want {
			t.Errorf("isInternalTopic(%q, patterns) = %v, want %v", tt.name, got, tt.want)
		}
	}
}
