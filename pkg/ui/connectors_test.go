// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/connect"
)

// taskActionsTable digs the task table out of the modal the registry holds, so the test reads
// what the user sees rather than a copy of it.
func taskActionsTable(tb testing.TB, app *App) *tview.Table {
	tb.Helper()

	var table *tview.Table
	var find func(p tview.Primitive)
	find = func(p tview.Primitive) {
		switch v := p.(type) {
		case *tview.Table:
			if table == nil {
				table = v
			}
		case *tview.Flex:
			for i := range v.GetItemCount() {
				find(v.GetItem(i))
			}
		}
	}

	_, page := app.Layout.PagesRegistry.UI.Pages.GetFrontPage()
	find(page)
	if table == nil {
		tb.Fatal("task actions modal has no table")
	}

	return table
}

func taskDetail(states ...string) *connect.ConnectorDetail {
	tasks := make([]connect.TaskStateInfo, 0, len(states))
	for i, state := range states {
		tasks = append(tasks, connect.TaskStateInfo{ID: i, State: state, WorkerID: "worker"})
	}

	return &connect.ConnectorDetail{
		Name: "orders-sink",
		Status: &connect.ConnectorStatus{
			Name:      "orders-sink",
			Connector: connect.ConnectorStateInfo{State: "RUNNING"},
			Tasks:     tasks,
		},
	}
}

// TestTaskActionsModalPrefillsAction covers the one thing a task row can be asked to do:
// with a single action available, the row arrives ready to submit instead of blank.
func TestTaskActionsModalPrefillsAction(t *testing.T) {
	app := newRegistryApp(t)
	app.TaskActionsModal(taskDetail("RUNNING", "PAUSED"))
	app.ShowModalPage(TaskActions)

	table := taskActionsTable(t, app)
	for row := 1; row <= 2; row++ {
		if got := table.GetCell(row, 3).Text; got != "RESTART" {
			t.Errorf("row %d action = %q, want %q", row, got, "RESTART")
		}
	}
}

// TestTaskActionsModalTabKeepsAnAction guards the other half: cycling with Tab must not
// leave a row with nothing to submit.
func TestTaskActionsModalTabKeepsAnAction(t *testing.T) {
	app := newRegistryApp(t)
	app.TaskActionsModal(taskDetail("RUNNING"))
	app.ShowModalPage(TaskActions)

	table := taskActionsTable(t, app)
	table.Select(1, 0)

	capture := table.InputHandler()
	if capture == nil {
		t.Fatal("task table has no input handler")
	}

	for range 3 {
		capture(tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone), func(tview.Primitive) {})
		if got := table.GetCell(1, 3).Text; got == "" {
			t.Fatal("Tab cleared the action, leaving the row unsubmittable")
		}
	}
}
