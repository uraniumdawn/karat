// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"testing"

	"github.com/rivo/tview"
)

// newNamesTable builds a list-page table: a header row plus one row per name.
func newNamesTable(names ...string) *tview.Table {
	table := tview.NewTable()
	table.SetSelectable(true, false)
	table.SetCell(0, 0, tview.NewTableCell("Name"))
	for i, name := range names {
		table.SetCell(i+1, 0, tview.NewTableCell(name))
	}
	return table
}

func TestSelectedRow(t *testing.T) {
	tests := []struct {
		name    string
		table   *tview.Table
		select_ int
		want    string
		wantOK  bool
	}{
		{
			name:    "a data row",
			table:   newNamesTable("a", "b", "c"),
			select_: 2,
			want:    "b",
			wantOK:  true,
		},
		{
			// The header is never a topic, however the cursor got there.
			name:    "the header row",
			table:   newNamesTable("a"),
			select_: 0,
			wantOK:  false,
		},
		{
			// What a filter leaves behind: the selection index outlives the rows it pointed
			// at, and tview answers an out-of-range index with an empty cell.
			name:    "an index past the end",
			table:   newNamesTable("a"),
			select_: 7,
			wantOK:  false,
		},
		{
			name:    "a table with nothing but a header",
			table:   newNamesTable(),
			select_: 1,
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.table.Select(tt.select_, 0)

			got, ok := selectedRow(tt.table, 0, afterHeaderRow)
			if ok != tt.wantOK {
				t.Fatalf("selectedRow() ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("selectedRow() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRestoreSelection(t *testing.T) {
	const page = "local:topics"

	t.Run("keeps the same row across a rebuild", func(t *testing.T) {
		app := &App{SelectedRows: map[string]string{}}

		table := newNamesTable("a", "b", "c")
		app.TrackSelection(page, table, afterHeaderRow)
		table.Select(3, 0) // c

		// A refresh: a new table, the same rows in a different order.
		rebuilt := newNamesTable("c", "a", "b")
		app.RestoreSelection(page, rebuilt, afterHeaderRow)

		if got, _ := selectedRow(rebuilt, 0, afterHeaderRow); got != "c" {
			t.Errorf("selected %q after the rebuild, want c", got)
		}
	})

	t.Run("falls back to the first row when the row is gone", func(t *testing.T) {
		app := &App{SelectedRows: map[string]string{}}

		table := newNamesTable("a", "b", "c")
		app.TrackSelection(page, table, afterHeaderRow)
		table.Select(3, 0) // c

		// A filter that keeps neither the row nor its neighbours.
		filtered := newNamesTable("a")
		app.RestoreSelection(page, filtered, afterHeaderRow)

		name, ok := selectedRow(filtered, 0, afterHeaderRow)
		if !ok {
			t.Fatal("nothing is selected after the rebuild")
		}
		if name != "a" {
			t.Errorf("selected %q, want the first row a", name)
		}
	})

	t.Run("leaves a header-only table alone", func(t *testing.T) {
		app := &App{SelectedRows: map[string]string{page: "c"}}

		empty := newNamesTable()
		app.RestoreSelection(page, empty, afterHeaderRow)

		if _, ok := selectedRow(empty, 0, afterHeaderRow); ok {
			t.Error("a table with no data rows reports a selection")
		}
	})

	t.Run("a rebuilt table never keeps an out-of-range selection", func(t *testing.T) {
		app := &App{SelectedRows: map[string]string{}}

		table := newNamesTable("a", "b", "c", "d")
		app.TrackSelection(page, table, afterHeaderRow)
		table.Select(4, 0) // d

		// Filtering in place: same table, fewer rows, cursor left at row 4.
		table.Clear()
		table.SetCell(0, 0, tview.NewTableCell("Name"))
		table.SetCell(1, 0, tview.NewTableCell("b"))
		app.RestoreSelection(page, table, afterHeaderRow)

		if _, ok := selectedRow(table, 0, afterHeaderRow); !ok {
			t.Error("selection is still outside the rebuilt table")
		}
	})
}

// newHeaderlessTable builds a subject or version list: one name per row, starting at row 0.
func newHeaderlessTable(names ...string) *tview.Table {
	table := tview.NewTable()
	table.SetSelectable(true, false)
	for i, name := range names {
		table.SetCell(i, 0, tview.NewTableCell(name))
	}
	return table
}

// The subject and version lists have no header, so their row 0 is a selectable name — reading
// it as a header is what made 'd' on the only version report "nothing selected".
func TestSelectedRowOnAHeaderlessTable(t *testing.T) {
	table := newHeaderlessTable("1")
	table.Select(0, 0)

	got, ok := selectedRow(table, 0, firstRow)
	if !ok {
		t.Fatal("the only row of a headerless table reports no selection")
	}
	if got != "1" {
		t.Errorf("selectedRow() = %q, want 1", got)
	}
}

func TestRestoreSelectionOnAHeaderlessTable(t *testing.T) {
	const page = "sr:subject:versions"

	app := &App{SelectedRows: map[string]string{}}

	table := newHeaderlessTable("1", "2", "3")
	app.TrackSelection(page, table, firstRow)
	table.Select(0, 0) // the first version, which a headered table would call the header

	if got := app.SelectedRows[page]; got != "1" {
		t.Fatalf("remembered %q, want the first row 1", got)
	}

	rebuilt := newHeaderlessTable("3", "2", "1")
	app.RestoreSelection(page, rebuilt, firstRow)

	if got, _ := selectedRow(rebuilt, 0, firstRow); got != "1" {
		t.Errorf("selected %q after the rebuild, want 1", got)
	}
}
