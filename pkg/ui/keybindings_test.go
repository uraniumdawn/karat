// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/config"
)

// tview quits on <Ctrl-C> only when the application input capture returns the very event it was
// given (application.go: `event == originalEvent`). Both modes below consume every other key,
// so returning nil for this one leaves no way out of karat at all.
func TestCtrlCIsNeverSwallowed(t *testing.T) {
	tests := []struct {
		name  string
		setUp func(app *App)
	}{
		{
			name:  "with a question standing",
			setUp: func(app *App) { app.Confirm("delete it?", func() {}) },
		},
		{
			name:  "in auto-update mode",
			setUp: func(app *App) { app.autoUpdateMode = true },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer drainStatus()

			app := newConfirmApp(t, config.Confirm)
			app.Application = tview.NewApplication()
			app.MainOperationKeyHandler()
			tt.setUp(app)

			ctrlC := tcell.NewEventKey(tcell.KeyCtrlC, 0, tcell.ModNone)
			if got := app.GetInputCapture()(ctrlC); got != ctrlC {
				t.Errorf("input capture returned %v for <Ctrl-C>, want the event unchanged", got)
			}
		})
	}
}

// The question is still answered by the keys that answer it, and still swallows the rest.
func TestStandingQuestionStillConsumesOtherKeys(t *testing.T) {
	defer drainStatus()

	app := newConfirmApp(t, config.Confirm)
	app.Application = tview.NewApplication()
	app.MainOperationKeyHandler()

	yes := 0
	app.Confirm("delete it?", func() { yes++ })

	if got := app.GetInputCapture()(keyRune('j')); got != nil {
		t.Errorf("input capture returned %v for an unrelated key, want it consumed", got)
	}
	if got := app.GetInputCapture()(keyRune('Y')); got != nil {
		t.Errorf("input capture returned %v for <Y>, want it consumed", got)
	}
	if yes != 1 {
		t.Errorf("the operation ran %d times, want 1", yes)
	}
}
