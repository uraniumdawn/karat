// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"regexp"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/franz"
	"github.com/uraniumdawn/karat/pkg/util"
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

func topicMetadata(names ...string) map[string]*kafka.TopicMetadata {
	metadata := make(map[string]*kafka.TopicMetadata, len(names))
	for _, name := range names {
		metadata[name] = &kafka.TopicMetadata{
			Topic: name,
			Partitions: []kafka.PartitionMetadata{
				{ID: 0, Replicas: []int32{1, 2}},
			},
		}
	}
	return metadata
}

func TestSortTopicsTableSizeColumnDisabled(t *testing.T) {
	size := sizeColumn{
		enabled: false,
		sizes:   map[string]franz.TopicLogDirSummary{"orders": {TotalSizeBytes: 2048}},
	}

	table := tview.NewTable()
	sortTopicsTable(table, topicMetadata("orders"), size, 0, false, tcell.ColorWhite, false, nil)

	if got := table.GetColumnCount(); got != 3 {
		t.Errorf("column count = %d, want 3 (Name, Partitions, Replication)", got)
	}
	if got := table.GetCell(0, 2).Text; got != "Replication" {
		t.Errorf("last header = %q, want %q", got, "Replication")
	}
}

func TestSortTopicsTableSizeColumnEnabled(t *testing.T) {
	size := sizeColumn{
		enabled: true,
		sizes: map[string]franz.TopicLogDirSummary{
			"orders": {TotalSizeBytes: 2048},
			"events": {TotalSizeBytes: 1024, Hint: "some replicas did not report"},
		},
	}

	table := tview.NewTable()
	// Sort by Size descending: orders (2 KiB) before events (~1 KiB), payments unknown.
	sortTopicsTable(
		table,
		topicMetadata("orders", "events", "payments"),
		size,
		3,
		true,
		tcell.ColorWhite,
		false,
		nil,
	)

	if got := table.GetColumnCount(); got != 4 {
		t.Fatalf("column count = %d, want 4", got)
	}
	if got := table.GetCell(0, 3).Text; got != "Size[↓]" {
		t.Errorf("size header = %q, want %q", got, "Size[↓]")
	}

	wantRows := [][2]string{
		{"orders", util.FormatBytes(2048)},
		{"events", "~" + util.FormatBytes(1024)},
		{"payments", "-"},
	}
	for i, want := range wantRows {
		row := i + 1
		if got := table.GetCell(row, 0).Text; got != want[0] {
			t.Errorf("row %d name = %q, want %q", row, got, want[0])
		}
		if got := table.GetCell(row, 3).Text; got != want[1] {
			t.Errorf("row %d size = %q, want %q", row, got, want[1])
		}
	}
}

func TestFilterTopicsTableSizeColumnDisabled(t *testing.T) {
	size := sizeColumn{enabled: false, sizes: map[string]franz.TopicLogDirSummary{}}

	table := tview.NewTable()
	filterTopicsTable(table, topicMetadata("orders", "events"), size, "ord", tcell.ColorWhite, false, nil)

	if got := table.GetColumnCount(); got != 3 {
		t.Errorf("column count = %d, want 3", got)
	}
	if got := table.GetCell(1, 0).Text; got != "orders" {
		t.Errorf("first match = %q, want %q", got, "orders")
	}
}

func TestTopicsTableHeaderMarksLoading(t *testing.T) {
	tests := []struct {
		name    string
		size    sizeColumn
		sortCol int
		want    string
	}{
		{
			name: "loading",
			size: sizeColumn{enabled: true, loading: true, sizes: map[string]franz.TopicLogDirSummary{}},
			want: "Size" + loadingMarker,
		},
		{
			name: "loaded",
			size: sizeColumn{
				enabled: true,
				sizes:   map[string]franz.TopicLogDirSummary{"orders": {TotalSizeBytes: 1024}},
			},
			want: "Size",
		},
		{
			name:    "loading while sorted by size",
			size:    sizeColumn{enabled: true, loading: true, sizes: map[string]franz.TopicLogDirSummary{}},
			sortCol: 3,
			want:    "Size" + loadingMarker + "[↓]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := tview.NewTable()
			sortTopicsTable(
				table,
				topicMetadata("orders"),
				tt.size,
				tt.sortCol,
				true,
				tcell.ColorWhite,
				false,
				nil,
			)

			if got := table.GetCell(0, 3).Text; got != tt.want {
				t.Errorf("size header = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTopicsTableNoLoadingMarkerWhenDisabled(t *testing.T) {
	// A disabled column never renders, so it cannot claim to be loading either.
	size := sizeColumn{enabled: false, loading: true, sizes: map[string]franz.TopicLogDirSummary{}}

	table := tview.NewTable()
	sortTopicsTable(table, topicMetadata("orders"), size, 0, false, tcell.ColorWhite, false, nil)

	if got := table.GetColumnCount(); got != 3 {
		t.Errorf("column count = %d, want 3", got)
	}
}
