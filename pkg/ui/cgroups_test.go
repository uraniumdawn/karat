// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func groupListing(ids ...string) []kafka.ConsumerGroupListing {
	listing := make([]kafka.ConsumerGroupListing, 0, len(ids))
	for _, id := range ids {
		listing = append(listing, kafka.ConsumerGroupListing{
			GroupID: id,
			State:   kafka.ConsumerGroupStateStable,
		})
	}
	return listing
}

func TestSortGroupsTableLagColumnDisabled(t *testing.T) {
	lags := lagColumn{enabled: false, lags: map[string]int64{"a": 10}}

	table := tview.NewTable()
	sortGroupsTable(table, groupListing("a"), lags, 0, false, tcell.ColorWhite)

	if got := table.GetColumnCount(); got != 2 {
		t.Errorf("column count = %d, want 2 (Name, State)", got)
	}
	if got := table.GetCell(0, 1).Text; got != "State" {
		t.Errorf("last header = %q, want %q", got, "State")
	}
}

func TestSortGroupsTableByLagDescending(t *testing.T) {
	lags := lagColumn{enabled: true, lags: map[string]int64{"a": 5, "b": 100}}

	table := tview.NewTable()
	// "c" has no lag yet — it sorts as 0 and renders as "-".
	sortGroupsTable(table, groupListing("a", "b", "c"), lags, 2, true, tcell.ColorWhite)

	if got := table.GetColumnCount(); got != 3 {
		t.Fatalf("column count = %d, want 3", got)
	}
	if got := table.GetCell(0, 2).Text; got != "Lag[↓]" {
		t.Errorf("lag header = %q, want %q", got, "Lag[↓]")
	}

	wantRows := [][2]string{{"b", "100"}, {"a", "5"}, {"c", "-"}}
	for i, want := range wantRows {
		row := i + 1
		if got := table.GetCell(row, 0).Text; got != want[0] {
			t.Errorf("row %d group = %q, want %q", row, got, want[0])
		}
		if got := table.GetCell(row, 2).Text; got != want[1] {
			t.Errorf("row %d lag = %q, want %q", row, got, want[1])
		}
	}
}

func TestFilterConsumerGroupsTableLagColumnDisabled(t *testing.T) {
	lags := lagColumn{enabled: false, lags: map[string]int64{}}

	table := tview.NewTable()
	filterConsumerGroupsTable(table, groupListing("orders-app", "events-app"), lags, "", tcell.ColorWhite)

	if got := table.GetColumnCount(); got != 2 {
		t.Errorf("column count = %d, want 2", got)
	}
	if got := table.GetCell(1, 0).Text; got != "events-app" {
		t.Errorf("first row = %q, want %q (alphabetical with empty filter)", got, "events-app")
	}
}

func TestFilterConsumerGroupsTableLagColumnEnabled(t *testing.T) {
	lags := lagColumn{enabled: true, lags: map[string]int64{"orders-app": 42}}

	table := tview.NewTable()
	filterConsumerGroupsTable(table, groupListing("orders-app", "events-app"), lags, "ord", tcell.ColorWhite)

	if got := table.GetCell(1, 0).Text; got != "orders-app" {
		t.Fatalf("first match = %q, want %q", got, "orders-app")
	}
	if got := table.GetCell(1, 2).Text; got != "42" {
		t.Errorf("lag cell = %q, want %q", got, "42")
	}
}

func TestGroupsTableHeaderMarksLoading(t *testing.T) {
	tests := []struct {
		name    string
		lags    lagColumn
		sortCol int
		want    string
	}{
		{
			name: "loading",
			lags: lagColumn{enabled: true, loading: true, lags: map[string]int64{}},
			want: "Lag" + loadingMarker,
		},
		{
			name: "loaded",
			lags: lagColumn{enabled: true, lags: map[string]int64{"a": 1}},
			want: "Lag",
		},
		{
			name:    "loading while sorted by lag",
			lags:    lagColumn{enabled: true, loading: true, lags: map[string]int64{}},
			sortCol: 2,
			want:    "Lag" + loadingMarker + "[↓]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := tview.NewTable()
			sortGroupsTable(table, groupListing("a"), tt.lags, tt.sortCol, true, tcell.ColorWhite)

			if got := table.GetCell(0, 2).Text; got != tt.want {
				t.Errorf("lag header = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFilterConsumerGroupsTableHeaderMarksLoading(t *testing.T) {
	lags := lagColumn{enabled: true, loading: true, lags: map[string]int64{}}

	table := tview.NewTable()
	filterConsumerGroupsTable(table, groupListing("a"), lags, "a", tcell.ColorWhite)

	if got := table.GetCell(0, 2).Text; got != "Lag"+loadingMarker {
		t.Errorf("lag header = %q, want %q", got, "Lag"+loadingMarker)
	}
}
