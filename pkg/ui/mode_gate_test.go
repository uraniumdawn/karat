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

// newGateApp builds an application with the key bindings installed and the given mode.
func newGateApp(tb testing.TB, mode config.Mode) *App {
	tb.Helper()
	drainStatus()
	tb.Cleanup(drainStatus)

	app := newRegistryApp(tb)
	app.Config.SetMode(mode)
	app.MainOperationKeyHandler()

	return app
}

// The mode is the value karat holds, and every mutation reads it when it runs — not when the
// editor or the confirmation page that leads to it was opened. Anything else would let a
// switch to read-only be ignored by an operation that was already on its way.
func TestModeIsReadWhenTheMutationRuns(t *testing.T) {
	app := newGateApp(t, config.Yolo)

	// The state an editor or a confirmation page is opened in.
	if !app.Allowed() {
		t.Fatal("yolo refused an operation")
	}

	// The user switches while it is open; what counts is the mode now, not then.
	app.Config.SetMode(config.ReadOnly)

	if app.Allowed() {
		t.Error("a mutation was allowed after the switch to read-only")
	}

	app.Config.SetMode(config.Yolo)
	if !app.Allowed() {
		t.Error("a mutation stayed refused after the switch back to yolo")
	}
}

// Modify reads the mode at the same moment: yolo runs the operation, read-only refuses it, and
// confirm asks instead of running it.
func TestModifyReadsTheModeItRunsUnder(t *testing.T) {
	app := newGateApp(t, config.Yolo)

	ran := false
	app.Modify("delete it?", func() { ran = true })
	if !ran {
		t.Error("yolo did not run the operation")
	}

	ran = false
	app.Config.SetMode(config.ReadOnly)
	app.Modify("delete it?", func() { ran = true })
	if ran {
		t.Error("read-only ran the operation")
	}
	if app.confirmPending() {
		t.Error("read-only asked a question instead of refusing")
	}

	app.Config.SetMode(config.Confirm)
	app.Modify("delete it?", func() { ran = true })
	if ran {
		t.Error("confirm ran the operation without an answer")
	}
	if !app.confirmPending() {
		t.Error("confirm did not ask")
	}
}

// A confirmation page cannot be left for the Clusters page, which is the only place <Tab>
// cycles the mode. The keys that would take the user there are refused while it stands.
func TestConfirmationPageBlocksTheWayToTheModeSwitch(t *testing.T) {
	app := newGateApp(t, config.Confirm)
	registry := app.Layout.PagesRegistry
	capture := app.GetInputCapture()

	// The modals those keys open are pages of their own, as app.Run() registers them.
	registry.UI.Pages.AddPage(Resources, tview.NewBox(), true, false)
	registry.UI.Pages.AddPage(OpenedPages, registry.UI.Main, true, false)

	app.AddToPagesRegistry(Clusters, tview.NewTable(), ClustersPageMenu, false)
	app.AddToPagesRegistry("local:topics", tview.NewTable(), TopicsPageMenu, false)

	resources := tcell.NewEventKey(tcell.KeyRune, ':', tcell.ModNone)
	openedPages := tcell.NewEventKey(tcell.KeyCtrlP, 0, tcell.ModNone)

	front := func() string {
		name, _ := registry.UI.Pages.GetFrontPage()
		return name
	}

	capture(resources)
	if front() != Resources {
		t.Fatalf("front page = %q, want the resource menu to open on an ordinary page", front())
	}
	app.HideModalPage(Resources)

	app.addTransientPage(TopicConfirm, tview.NewTextView())

	capture(resources)
	if front() != TopicConfirm {
		t.Errorf("the resource menu opened from a confirmation page: front is %q", front())
	}
	capture(openedPages)
	if front() != TopicConfirm {
		t.Errorf("the opened-pages modal opened from a confirmation page: front is %q", front())
	}

	app.removeTransientPage(TopicConfirm)

	capture(resources)
	if front() != Resources {
		t.Errorf("front page = %q, want the resource menu to work again", front())
	}
}

// A standing question owns the keyboard: <Tab> reaches nothing, so the mode cannot change
// between asking and answering either.
func TestStandingQuestionSwallowsTheModeKey(t *testing.T) {
	app := newGateApp(t, config.Confirm)
	capture := app.GetInputCapture()

	app.Modify("delete it?", func() {})
	if !app.confirmPending() {
		t.Fatal("no question stands")
	}

	if capture(tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone)) != nil {
		t.Error("<Tab> reached the application while a question stood")
	}
	if got := app.Config.Mode(); got != config.Confirm {
		t.Errorf("mode = %q, want it unchanged at %q", got, config.Confirm)
	}
}
