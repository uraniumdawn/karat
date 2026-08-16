// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"github.com/gdamore/tcell/v2"

	"github.com/uraniumdawn/karat/pkg/config"
)

// refusal is what the status line says when a modifying action is refused.
const refusal = "[red]karat is in read-only mode"

// confirmation is the question standing in the status line, waiting for an answer.
type confirmation struct {
	onYes func()
}

// Modify runs a modifying operation as far as the mode karat is running in allows:
//
//   - read-only refuses it and says so in the status line;
//   - confirm puts the question in the status line and runs it on <Y>;
//   - yolo runs it straight away.
//
// It is the single gate every mutation goes through, so a new call site cannot implement
// half of it — asking without checking the mode, or checking without asking.
//
// It must be called on the UI goroutine, which is where a keypress handler runs. run is
// called there too, and in yolo it runs before Modify returns — inside the queued update the
// caller may itself be in. So run must not block: publishing an event from it deadlocks the
// application, the reader of that channel being the goroutine parked in QueueUpdate waiting
// for this very update to finish. Start the operation and let its own goroutine publish.
func (app *App) Modify(question string, run func()) {
	switch app.Config.Mode() {
	case config.ReadOnly:
		SendStatusNote(refusal)
	case config.Yolo:
		run()
	default:
		app.Confirm(question, run)
	}
}

// Allowed reports whether the mode permits a modifying operation at all, sending the reason to
// the status line when it does not.
//
// It is for the paths Modify does not fit: opening an editor or a form, where the mutation
// itself happens on submit, and applying a change the user has already confirmed by reading
// it on a confirmation page.
func (app *App) Allowed() bool {
	if app.Config.Mode() == config.ReadOnly {
		SendStatusNote(refusal)
		return false
	}
	return true
}

// Confirm asks question in the status line and runs onYes once the user answers <Y>. <N> and
// <Esc> abandon the operation; every other key is ignored while the question stands, so
// nothing else in the application reacts until the user decides — see answer.
func (app *App) Confirm(question string, onYes func()) {
	app.confirm = &confirmation{onYes: onYes}
	SendStatusPrompt(question + " [Y/N]")
}

// answer resolves the standing confirmation and reports whether the keypress was consumed.
//
// It is the gate the application-wide input capture runs first: while a question stands, it
// consumes every keypress, so no page, modal or search field sees any of them.
func (app *App) answer(event *tcell.EventKey) bool {
	pending := app.confirm
	if pending == nil {
		return false
	}

	switch {
	case IsKey(event, 'y'), IsKey(event, 'Y'):
		app.confirm = nil
		// Cleared before onYes runs: the operation puts its own message in the status line.
		ClearStatus()
		pending.onYes()
	case IsKey(event, 'n'), IsKey(event, 'N'), event.Key() == tcell.KeyEsc:
		app.confirm = nil
		SendStatusNote("cancelled")
	}

	return true
}

// confirmPending reports whether a confirmation is waiting for an answer.
//
// The status line consults it before showing or clearing anything: a standing question
// outlives both a background message and the TTL of the message it displaced.
func (app *App) confirmPending() bool {
	return app.confirm != nil
}
