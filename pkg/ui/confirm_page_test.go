// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"strings"
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/config"
)

// underlying is the page a confirmation is asked over, so that a test can tell the confirmation
// came down and left the user where they were.
const underlying = "local:topics"

// showUnderlyingPage puts a page up for a confirmation to stand over and returns the app.
func showUnderlyingPage(tb testing.TB, mode config.Mode) *App {
	tb.Helper()

	app := newRegistryApp(tb)
	app.Config.SetMode(mode)
	app.AddToPagesRegistry(underlying, tview.NewTable(), TopicsPageMenu, false)

	return app
}

func confirmPage(app *App, apply func()) {
	app.ConfirmPage(
		TopicConfirm,
		newConfirmView(" Confirm Topic Update: orders ", "partitions  3 -> 6"),
		"apply these changes to topic 'orders'?",
		apply,
	)
}

// However it is answered, the page comes down and the user is left on the page it was asked
// over. A page still standing with no question over it has no keys at all.
func TestConfirmPageIsGoneWhicheverWayItIsAnswered(t *testing.T) {
	answers := map[string]*tcell.EventKey{
		"yes":       keyRune('Y'),
		"no":        keyRune('N'),
		"abandoned": tcell.NewEventKey(tcell.KeyEsc, 0, tcell.ModNone),
	}

	for name, event := range answers {
		t.Run(name, func(t *testing.T) {
			defer drainStatus()

			app := showUnderlyingPage(t, config.Confirm)
			confirmPage(app, func() {})

			if front, _ := app.Layout.PagesRegistry.UI.Pages.GetFrontPage(); front != TopicConfirm {
				t.Fatalf("front page = %q, want %q", front, TopicConfirm)
			}
			if !app.confirmPending() {
				t.Fatalf("no question standing over the confirmation page")
			}

			app.answer(event)

			if app.confirmPending() {
				t.Errorf("the question still stands after the answer")
			}
			if app.Layout.PagesRegistry.UI.Pages.HasPage(TopicConfirm) {
				t.Errorf("the confirmation page is still there after the answer")
			}
			if app.Layout.PagesRegistry.IsTransientPage(TopicConfirm) {
				t.Errorf("the page is still marked transient after the answer")
			}
			if front, _ := app.Layout.PagesRegistry.UI.Pages.GetFrontPage(); front != underlying {
				t.Errorf("front page = %q, want the page it was asked over (%q)", front, underlying)
			}
		})
	}
}

func TestConfirmPageAppliesOnYesOnly(t *testing.T) {
	tests := map[string]struct {
		event *tcell.EventKey
		want  int
	}{
		"yes applies":         {keyRune('Y'), 1},
		"no does not":         {keyRune('N'), 0},
		"abandoning does not": {tcell.NewEventKey(tcell.KeyEsc, 0, tcell.ModNone), 0},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			defer drainStatus()

			app := showUnderlyingPage(t, config.Confirm)
			applied := 0
			confirmPage(app, func() { applied++ })

			app.answer(tt.event)
			if applied != tt.want {
				t.Errorf("applied %d times, want %d", applied, tt.want)
			}

			// The answer is given once: a second keypress must not apply it again.
			app.answer(keyRune('Y'))
			if applied != tt.want {
				t.Errorf("applied %d times after a second answer, want %d", applied, tt.want)
			}
		})
	}
}

// The shifted key is the one the user cannot hit by brushing the keyboard on the way past, so
// the unshifted one answers nothing at all and the page stays up waiting for a real answer.
func TestConfirmPageIgnoresLowercaseAnswers(t *testing.T) {
	for _, event := range []*tcell.EventKey{keyRune('y'), keyRune('n')} {
		defer drainStatus()

		app := showUnderlyingPage(t, config.Confirm)
		applied := 0
		confirmPage(app, func() { applied++ })

		app.answer(event)

		if applied != 0 {
			t.Errorf("%v applied the change", event.Rune())
		}
		if !app.confirmPending() {
			t.Errorf("%v answered the question", event.Rune())
		}
		if !app.Layout.PagesRegistry.UI.Pages.HasPage(TopicConfirm) {
			t.Errorf("%v took the confirmation page down", event.Rune())
		}
	}
}

// The mode is read when the mutation runs, not when the page opened. A refusal must still take
// the page down, or the user is left on a page with nothing left to answer.
func TestConfirmPageRefusesInReadOnly(t *testing.T) {
	defer drainStatus()

	app := showUnderlyingPage(t, config.ReadOnly)
	applied := 0
	confirmPage(app, func() { applied++ })

	app.answer(keyRune('Y'))

	if applied != 0 {
		t.Errorf("the change was applied in read-only mode")
	}
	if app.Layout.PagesRegistry.UI.Pages.HasPage(TopicConfirm) {
		t.Errorf("the confirmation page survived the refusal")
	}

	var last string
	for {
		select {
		case status := <-StatusLineCh:
			last = status.Message
			continue
		default:
		}
		break
	}
	if last != refusal {
		t.Errorf("status line = %q, want %q", last, refusal)
	}
}

func TestConfirmPageRestoresTheBarOfThePageUnderneath(t *testing.T) {
	defer drainStatus()

	app := showUnderlyingPage(t, config.Confirm)
	menu := app.Layout.Menu
	standing := menu.Content.GetRowCount()
	if standing == 0 {
		t.Fatalf("the page underneath has an empty bar, nothing to restore")
	}

	confirmPage(app, func() {})
	assertAnswerBar(t, menu)

	app.answer(keyRune('N'))
	if got := menu.Content.GetRowCount(); got != standing {
		t.Errorf("bar rows after the answer = %d, want %d", got, standing)
	}
}

// A confirmation page has no bindings of its own, so it must not claim a menu or a help section.
func TestConfirmPageHasNoMenuOfItsOwn(t *testing.T) {
	defer drainStatus()

	app := showUnderlyingPage(t, config.Confirm)
	confirmPage(app, func() {})

	if menu, ok := app.Layout.PagesRegistry.PageMenuMap[TopicConfirm]; ok {
		t.Errorf("the confirmation page claims menu %q", menu)
	}
	if help := app.helpText(TopicConfirm); strings.Contains(help, TopicConfirm) {
		t.Errorf("the help body carries a section for the confirmation page:\n%s", help)
	}
}

// The change being confirmed is often longer than the terminal. The keys that scroll it have to
// reach the page, or it would have to be answered unread.
func TestConfirmPagePassesScrollKeysThrough(t *testing.T) {
	defer drainStatus()

	app := showUnderlyingPage(t, config.Confirm)
	app.MainOperationKeyHandler()
	applied := 0
	confirmPage(app, func() { applied++ })

	for _, event := range []*tcell.EventKey{
		keyRune('j'),
		keyRune('k'),
		keyRune('G'),
		tcell.NewEventKey(tcell.KeyPgDn, 0, tcell.ModNone),
	} {
		if got := app.GetInputCapture()(event); got != event {
			t.Errorf("input capture returned %v for a scroll key, want the event handed on", got)
		}
		if !app.confirmPending() {
			t.Fatalf("a scroll key answered the question")
		}
	}

	if got := app.GetInputCapture()(keyRune('Y')); got != nil {
		t.Errorf("input capture returned %v for <Y>, want it consumed", got)
	}
	if applied != 1 {
		t.Errorf("applied %d times, want 1", applied)
	}
}

// A question with no page under it swallows everything, scroll keys included: there is nothing
// to scroll, and a key that does nothing must not reach the page behind the status line.
func TestStandingQuestionWithNoPageSwallowsScrollKeys(t *testing.T) {
	defer drainStatus()

	app := showUnderlyingPage(t, config.Confirm)
	app.MainOperationKeyHandler()
	app.Confirm("delete topic 'orders'?", func() {})

	if got := app.GetInputCapture()(keyRune('j')); got != nil {
		t.Errorf("input capture returned %v for <j>, want it consumed", got)
	}
}
