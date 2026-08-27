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

// newConfirmApp builds the least App a confirmation needs: a mode, and the keybinding bar the
// question takes down and puts back.
//
// It restores the tview package-level borders NewLayout writes, so a test cannot leak them into
// the next one.
func newConfirmApp(tb testing.TB, mode config.Mode) *App {
	tb.Helper()

	borders := tview.Borders
	tb.Cleanup(func() { tview.Borders = borders })

	colors, err := config.LoadColorConfig("")
	if err != nil {
		tb.Fatalf("LoadColorConfig() error = %v", err)
	}

	cfg := &config.Config{}
	cfg.SetMode(mode)

	app := &App{Config: cfg, Colors: colors}
	app.Layout = NewLayout(NewPagesRegistry(colors), colors, cfg, true, true)

	return app
}

// drainStatus empties the status channel, which has no handler running in a test.
func drainStatus() {
	for {
		select {
		case <-StatusLineCh:
		default:
			return
		}
	}
}

func keyRune(r rune) *tcell.EventKey {
	return tcell.NewEventKey(tcell.KeyRune, r, tcell.ModNone)
}

func TestConfirmAnswers(t *testing.T) {
	tests := []struct {
		name    string
		event   *tcell.EventKey
		wantYes bool
		// wantPending is whether the question still stands after the keypress.
		wantPending bool
	}{
		{name: "Y confirms", event: keyRune('Y'), wantYes: true},
		{name: "N abandons", event: keyRune('N')},
		// The shifted key is the one the user cannot hit by brushing the keyboard on the way
		// past, so the unshifted one answers nothing and the question keeps standing.
		{name: "lowercase y is ignored", event: keyRune('y'), wantPending: true},
		{name: "lowercase n is ignored", event: keyRune('n'), wantPending: true},
		{
			name:  "esc abandons",
			event: tcell.NewEventKey(tcell.KeyEsc, 0, tcell.ModNone),
		},
		{name: "any other rune is ignored", event: keyRune('j'), wantPending: true},
		{
			name:        "enter is ignored",
			event:       tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone),
			wantPending: true,
		},
		{
			name:        "ctrl-d is ignored, so the question cannot re-ask itself",
			event:       tcell.NewEventKey(tcell.KeyCtrlD, 0, tcell.ModNone),
			wantPending: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer drainStatus()

			app := newConfirmApp(t, "")
			yes := 0
			app.Confirm("delete it?", func() { yes++ })

			if !app.confirmPending() {
				t.Fatalf("no question pending after Confirm()")
			}

			// Every keypress is consumed while a question stands: that is what blocks the
			// application, whether or not the key means anything.
			if !app.answer(tt.event) {
				t.Errorf("answer() = false, want the keypress consumed")
			}
			if got := yes > 0; got != tt.wantYes {
				t.Errorf("onYes ran = %v, want %v", got, tt.wantYes)
			}
			if got := app.confirmPending(); got != tt.wantPending {
				t.Errorf("question still pending = %v, want %v", got, tt.wantPending)
			}
		})
	}
}

// A question is answered once: the operation must not run twice on a second <y>.
func TestConfirmRunsOnceOnRepeatedYes(t *testing.T) {
	defer drainStatus()

	app := newConfirmApp(t, "")
	yes := 0
	app.Confirm("delete it?", func() { yes++ })

	app.answer(keyRune('Y'))
	if consumed := app.answer(keyRune('Y')); consumed {
		t.Errorf("answer() = true with no question pending, want the keypress passed through")
	}
	if yes != 1 {
		t.Errorf("onYes ran %d times, want 1", yes)
	}
}

func TestAnswerPassesKeysThroughWithNoQuestion(t *testing.T) {
	app := newConfirmApp(t, "")
	if app.answer(keyRune('Y')) {
		t.Errorf("answer() = true with no question pending, want false")
	}
}

// What the mode does to a modifying operation is the whole point of the gate.
func TestModifyByMode(t *testing.T) {
	modes := []struct {
		mode config.Mode
		// wantRan is whether the operation ran without the user answering anything.
		wantRan     bool
		wantPending bool
	}{
		{mode: config.ReadOnly},
		{mode: config.Confirm, wantPending: true},
		{mode: "", wantPending: true},
		{mode: config.Yolo, wantRan: true},
	}

	for _, m := range modes {
		t.Run(string(m.mode), func(t *testing.T) {
			defer drainStatus()

			app := newConfirmApp(t, m.mode)
			ran := 0
			app.Modify("delete it?", func() { ran++ })

			// Only yolo runs an operation the user has not answered for.
			if got := ran > 0; got != m.wantRan {
				t.Errorf("operation ran unasked = %v, want %v", got, m.wantRan)
			}
			if got := app.confirmPending(); got != m.wantPending {
				t.Errorf("question pending = %v, want %v", got, m.wantPending)
			}

			// Only a standing question swallows keys: refused and already run both leave the
			// application answering to its own keys again.
			if got := app.answer(keyRune('Y')); got != m.wantPending {
				t.Errorf("answer() consumed the keypress = %v, want %v", got, m.wantPending)
			}
			if m.mode == config.ReadOnly && ran != 0 {
				t.Errorf("operation ran %d times in read-only, want 0", ran)
			}
		})
	}
}

// Allowed is the mode check on its own, for the paths that confirm elsewhere.
func TestAllowed(t *testing.T) {
	defer drainStatus()

	if app := newConfirmApp(t, config.ReadOnly); app.Allowed() {
		t.Errorf("Allowed() = true in read-only mode")
	}

	app := newConfirmApp(t, config.Confirm)
	if !app.Allowed() {
		t.Errorf("Allowed() = false in confirm mode")
	}
	if app.confirmPending() {
		t.Errorf("Allowed() raised a question, want the mode check alone")
	}
}

// A config that never came through the loader carries no mode at all. The gate must read that
// as the careful branch rather than as yolo.
func TestModifyWithNoModeSet(t *testing.T) {
	defer drainStatus()

	app := newConfirmApp(t, "")
	ran := 0
	app.Modify("delete it?", func() { ran++ })

	if ran != 0 {
		t.Errorf("operation ran unasked with no mode set")
	}
	if !app.confirmPending() {
		t.Errorf("no question pending with no mode set")
	}
}

// The question is the one message the status line shows while it stands; a background fetch's
// progress must not paint over it.
func TestConfirmSendsAPromptStatus(t *testing.T) {
	defer drainStatus()

	app := newConfirmApp(t, "")
	app.Confirm("delete topic 'orders'?", func() {})

	select {
	case status := <-StatusLineCh:
		if !status.Prompt {
			t.Errorf("status.Prompt = false, want the question marked as a prompt")
		}
		if status.TTL != 0 {
			t.Errorf("status.TTL = %v, want no auto-clear", status.TTL)
		}
		if status.Spinner {
			t.Errorf("status.Spinner = true, want no spinner on a question")
		}
		if want := "delete topic 'orders'? [Y/N]"; status.Message != want {
			t.Errorf("status.Message = %q, want %q", status.Message, want)
		}
	default:
		t.Fatalf("Confirm() sent no status message")
	}
}

// barKeys is what the keybinding bar is displaying, read out of the cells the menu renders into.
func barKeys(menu *Menu) []string {
	var displayed []string
	for row := range menu.Content.GetRowCount() {
		for col := 0; col < menu.Content.GetColumnCount(); col += 2 {
			if cell := menu.Content.GetCell(row, col); cell != nil && cell.Text != "" {
				displayed = append(displayed, cell.Text)
			}
		}
	}

	return displayed
}

// hasKey reports whether the bar is displaying the given key, whatever colour tag it carries.
func hasKey(menu *Menu, key string) bool {
	for _, displayed := range barKeys(menu) {
		if strings.Contains(displayed, key) {
			return true
		}
	}

	return false
}

// assertAnswerBar checks the bar is carrying the answer to a standing question and nothing else.
func assertAnswerBar(tb testing.TB, menu *Menu) {
	tb.Helper()

	displayed := barKeys(menu)
	if len(displayed) != 2 || !hasKey(menu, "<Y>") || !hasKey(menu, "<N/Esc>") {
		tb.Errorf("bar = %v, want <Y> and <N/Esc> alone", displayed)
	}
}

// While a question stands, none of what the page underneath advertises works, so the bar carries
// the answer instead — and goes back to that page's own bindings however the question is
// answered.
func TestConfirmPutsTheAnswerOnTheKeybindingBar(t *testing.T) {
	tests := map[string]*tcell.EventKey{
		"answered yes": keyRune('Y'),
		"answered no":  keyRune('N'),
		"abandoned":    tcell.NewEventKey(tcell.KeyEsc, 0, tcell.ModNone),
	}

	for name, event := range tests {
		t.Run(name, func(t *testing.T) {
			defer drainStatus()

			app := newConfirmApp(t, "")
			menu := app.Layout.Menu
			menu.SetMenu(TopicsPageMenu)
			standing := menu.Content.GetRowCount()
			if standing == 0 {
				t.Fatalf("the bar is empty before the question, nothing to replace")
			}

			app.Confirm("delete it?", func() {})
			assertAnswerBar(t, menu)

			// A page rebuilt by a background refresh must not put its own bindings back under the
			// question.
			menu.SetMenu(TopicsPageMenu)
			assertAnswerBar(t, menu)

			app.answer(event)
			if got := menu.Content.GetRowCount(); got != standing {
				t.Errorf("bar rows after the answer = %d, want %d", got, standing)
			}
		})
	}
}

// A menu set while the question stands is the one that comes back: the page underneath may have
// been rebuilt into a different one by the time the answer arrives.
func TestConfirmRestoresTheMenuSetWhilePinned(t *testing.T) {
	defer drainStatus()

	app := newConfirmApp(t, "")
	menu := app.Layout.Menu
	menu.SetMenu(TopicsPageMenu)

	app.Confirm("delete it?", func() {})
	menu.SetMenu(ConsumerGroupsPageMenu)
	app.answer(keyRune('N'))

	menu.SetMenu(ConsumerGroupsPageMenu)
	want := menu.Content.GetRowCount()

	app.Confirm("delete it?", func() {})
	menu.SetMenu(ConsumerGroupsPageMenu)
	app.answer(keyRune('N'))

	if got := menu.Content.GetRowCount(); got != want {
		t.Errorf("bar rows = %d, want %d", got, want)
	}
}
