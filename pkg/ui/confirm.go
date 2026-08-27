// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/config"
)

// refusal is what the status line says when a modifying action is refused.
const refusal = "[red]karat is in read-only mode"

// confirmation is the question standing in the status line, waiting for an answer.
type confirmation struct {
	onYes func()
	// onDone runs on both branches once the answer is in. It is for a question asked over a page
	// the user reads it on: the page comes down whichever way it is answered, and one field that
	// always runs cannot be set for <Y> and forgotten for <N>.
	onDone func()
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
// <Esc> abandon the operation; every other key is ignored while the question stands — lowercase
// <y> and <n> included — so nothing else in the application reacts until the user decides. See
// answer.
//
// The keybinding bar carries the answer while the question stands, since nothing the page
// underneath advertises works until it is given, and goes back to that page's own bindings once
// it is.
func (app *App) Confirm(question string, onYes func()) {
	app.ask(question, onYes, nil)
}

// ask installs the question and takes the keybinding bar down with it. It is the only place the
// standing-question state is set up, so a caller cannot install half of it.
//
// It must not be called while a question already stands: the earlier one's onDone would be
// dropped, stranding whatever page it was asked over.
func (app *App) ask(question string, onYes, onDone func()) {
	app.confirm = &confirmation{onYes: onYes, onDone: onDone}
	app.Layout.Menu.Pin(ConfirmationMenu)
	SendStatusPrompt(question + " [Y/N]")
}

// ConfirmPage puts a page up for the user to read and asks the question in the status line.
// apply runs on <Y>; <N> and <Esc> abandon it. The page comes down either way — the answer is
// the only thing that resolves it, and there is no way back to a page once it is gone.
//
// The page carries no input capture of its own and must not be given one: while the question
// stands the application-wide capture consumes every keypress before any page sees it, so a
// capture there would never be reached. The keys that scroll the page through are the one
// exception — see answer.
//
// The mode is re-checked before apply runs rather than when the page opened, because a mutation
// reads the mode at the moment it runs and nothing read earlier can be trusted to still hold.
//
// It must be called on the UI goroutine, with no question already standing, and apply must not
// block — see Modify for why.
func (app *App) ConfirmPage(name string, page tview.Primitive, question string, apply func()) {
	app.ask(
		question,
		func() {
			if !app.Allowed() {
				return
			}
			apply()
		},
		func() { app.removeTransientPage(name) },
	)
	app.addTransientPage(name, page)
}

// newConfirmView builds the page a confirmation is read on: the change itself in full, with no
// colours to misread and no keys of its own.
func newConfirmView(title, body string) *tview.TextView {
	view := tview.NewTextView().
		SetText(body).
		SetTextAlign(tview.AlignLeft).
		SetDynamicColors(false)

	view.SetBorder(true).
		SetTitle(title).
		SetBorderPadding(0, 0, 1, 1)

	return view
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

	// A question asked over a page must not swallow the keys that let the user read it: the
	// change being confirmed is often longer than the terminal, and answering it unread is the
	// one thing the page exists to prevent.
	if app.confirmationInFront() && isScrollKey(event) {
		return false
	}

	// Uppercase only. A question stands over an operation the user cannot undo, and the shifted
	// key is the one they cannot hit by brushing the keyboard on the way past.
	switch {
	case IsKey(event, 'Y'):
		app.resolve(pending)
		// Cleared before onYes runs: the operation puts its own message in the status line.
		ClearStatus()
		pending.onYes()
	case IsKey(event, 'N'), event.Key() == tcell.KeyEsc:
		app.resolve(pending)
		SendStatusNote("cancelled")
	}

	return true
}

// resolve takes the standing question down: the page it was asked over first, so that the bar
// going back up is the one belonging to the page underneath rather than to the page that has
// just been removed.
//
// The bar is released before onYes runs, so that an operation opening a page of its own sets the
// menu it wants over a bar that is already showing.
func (app *App) resolve(pending *confirmation) {
	app.confirm = nil
	if pending.onDone != nil {
		pending.onDone()
	}
	app.Layout.Menu.Unpin()
}

// confirmPending reports whether a confirmation is waiting for an answer.
//
// The status line consults it before showing or clearing anything: a standing question
// outlives both a background message and the TTL of the message it displaced.
func (app *App) confirmPending() bool {
	return app.confirm != nil
}
