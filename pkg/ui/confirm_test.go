// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"testing"

	"github.com/gdamore/tcell/v2"

	"github.com/uraniumdawn/karat/pkg/config"
)

// newConfirmApp builds the least App a confirmation needs: a mode, and no UI.
func newConfirmApp(mode config.Mode) *App {
	cfg := &config.Config{}
	cfg.SetMode(mode)
	return &App{Config: cfg}
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
		{name: "y confirms", event: keyRune('y'), wantYes: true},
		{name: "uppercase Y confirms", event: keyRune('Y'), wantYes: true},
		{name: "n abandons", event: keyRune('n')},
		{name: "uppercase N abandons", event: keyRune('N')},
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

			app := newConfirmApp("")
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

	app := newConfirmApp("")
	yes := 0
	app.Confirm("delete it?", func() { yes++ })

	app.answer(keyRune('y'))
	if consumed := app.answer(keyRune('y')); consumed {
		t.Errorf("answer() = true with no question pending, want the keypress passed through")
	}
	if yes != 1 {
		t.Errorf("onYes ran %d times, want 1", yes)
	}
}

func TestAnswerPassesKeysThroughWithNoQuestion(t *testing.T) {
	app := newConfirmApp("")
	if app.answer(keyRune('y')) {
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

			app := newConfirmApp(m.mode)
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
			if got := app.answer(keyRune('y')); got != m.wantPending {
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

	if app := newConfirmApp(config.ReadOnly); app.Allowed() {
		t.Errorf("Allowed() = true in read-only mode")
	}

	app := newConfirmApp(config.Confirm)
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

	app := &App{Config: &config.Config{}}
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

	app := newConfirmApp("")
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
