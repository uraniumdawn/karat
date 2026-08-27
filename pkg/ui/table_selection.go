// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"strings"

	"github.com/rivo/tview"
)

// Where a table's rows begin. Every list page labels its columns in row 0 and keeps it fixed,
// so data starts at row 1; firstRow is for a table without a header row.
const (
	afterHeaderRow = 1
	firstRow       = 0
)

// selectedRow returns the text of the given column on the selected row, and reports whether
// the selection is on a data row at all. Rows before first are the page's header.
//
// It is false for the header row, for a table holding nothing but a header, and for a row
// index a rebuild has left behind — filtering, sorting and refreshing all replace the rows
// under the cursor. tview answers an out-of-range index with an empty cell rather than an
// error, so without this check a key pressed in that state reaches the cluster as an empty
// topic or connector name.
func selectedRow(table *tview.Table, column, first int) (string, bool) {
	row, _ := table.GetSelection()
	if row < first || row >= table.GetRowCount() {
		return "", false
	}
	value := strings.TrimSpace(table.GetCell(row, column).Text)
	return value, value != ""
}

// selectedName is selectedRow on the first column, which is the name on every list page. It
// says so in the status line when there is nothing selected, so a key pressed on an empty
// list is visibly a no-op instead of a failed call against "".
func selectedName(table *tview.Table, first int) (string, bool) {
	name, ok := selectedRow(table, 0, first)
	if !ok {
		SendStatusNote("nothing selected")
	}
	return name, ok
}

// TrackSelection remembers what pageKey has selected, by the name in the table's first
// column. Row indices do not survive a filter, a sort or a refresh; names do.
func (app *App) TrackSelection(pageKey string, table *tview.Table, first int) {
	app.rememberSelection(pageKey, table, first)
	table.SetSelectionChangedFunc(func(_, _ int) {
		app.rememberSelection(pageKey, table, first)
	})
}

// RestoreSelection puts the cursor back on the row remembered for pageKey once the table has
// been rebuilt. A row that is gone — filtered out, or deleted — falls back to the first data
// row, so the selection is never left past the end of the table.
func (app *App) RestoreSelection(pageKey string, table *tview.Table, first int) {
	rows := table.GetRowCount()
	if rows <= first {
		return
	}

	target := first
	if name := app.SelectedRows[pageKey]; name != "" {
		for row := first; row < rows; row++ {
			if strings.TrimSpace(table.GetCell(row, 0).Text) == name {
				target = row
				break
			}
		}
	}
	table.Select(target, 0)
}

func (app *App) rememberSelection(pageKey string, table *tview.Table, first int) {
	if name, ok := selectedRow(table, 0, first); ok {
		app.SelectedRows[pageKey] = name
	}
}
