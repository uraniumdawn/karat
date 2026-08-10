// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package util

import (
	"testing"

	"github.com/rivo/tview"
)

func TestSetColumnMarkerMovesMarkerToActiveRow(t *testing.T) {
	table := tview.NewTable()
	SetTableHeaders(table, 0, "Name", "Active")
	for row := 1; row <= 3; row++ {
		table.SetCell(row, 0, tview.NewTableCell("entry"))
	}
	table.SetCell(1, 1, tview.NewTableCell("✓"))

	SetColumnMarker(table, 1, 3, "✓")

	want := []string{"", "", "✓"}
	for i, w := range want {
		row := i + 1
		if got := table.GetCell(row, 1).Text; got != w {
			t.Errorf("row %d marker = %q, want %q", row, got, w)
		}
	}
	if got := table.GetCell(0, 1).Text; got != "Active" {
		t.Errorf("header = %q, want %q", got, "Active")
	}
}

func TestSetColumnMarkerClearsAllWhenNoActiveRow(t *testing.T) {
	table := tview.NewTable()
	SetTableHeaders(table, 0, "Name", "Active")
	table.SetCell(1, 0, tview.NewTableCell("entry"))
	table.SetCell(1, 1, tview.NewTableCell("✓"))

	SetColumnMarker(table, 1, 0, "✓")

	if got := table.GetCell(1, 1).Text; got != "" {
		t.Errorf("row 1 marker = %q, want empty", got)
	}
}
