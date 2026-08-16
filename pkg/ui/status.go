// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// loadingMarker is appended to a table column header while that column's values are still
// being fetched in the background (topic sizes, consumer group lags).
const loadingMarker = "…"

// activeMarker is placed in the "Active" column of the clusters, Connect and Schema
// Registry tables against the entry currently selected in the session.
const activeMarker = "✓"

// activeMarkerFor returns activeMarker when active, and an empty cell otherwise.
func activeMarkerFor(active bool) string {
	if active {
		return activeMarker
	}
	return ""
}

// Status represents a status message with optional time-to-live
type Status struct {
	Message string
	TTL     time.Duration // 0 means infinite (no auto-clear)
	Spinner bool          // true to show spinner animation
	// Prompt marks a confirmation question. It is the one message shown while a confirmation
	// is pending, since everything else would paint over the question being asked.
	Prompt bool
}

var (
	StatusLineCh  = make(chan Status, 10)
	SpinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
)

// statusInterval is how often the status line is repainted: the spinner's frame rate, and the
// delay a message can wait before it is shown.
const statusInterval = 100 * time.Millisecond

// statusInbox holds the messages that have arrived but are not on screen yet. It is what keeps
// draining StatusLineCh independent of the UI goroutine — see RunStatusLineHandler.
type statusInbox struct {
	mu      sync.Mutex
	pending []Status
}

// put records a message. It never blocks on anything but the lock.
func (b *statusInbox) put(status Status) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending = append(b.pending, status)
}

// take returns the messages that arrived since the last call, oldest first.
func (b *statusInbox) take() []Status {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.pending) == 0 {
		return nil
	}
	taken := b.pending
	b.pending = nil
	return taken
}

// noteTTL is how long a message the user is meant to read stands before clearing itself.
const noteTTL = 10 * time.Second

// doneTTL is how long the confirmation of a finished operation stands. It is shorter than
// noteTTL because there is nothing to read but the fact — but not so short that a glance away
// from the status line misses it.
const doneTTL = 3 * time.Second

// sendStatus is the one way onto the status line. It is unexported so that a call site cannot
// invent a fourth timing: the four helpers below are the whole vocabulary, and each says what
// the message *is* rather than how long it happens to last. The mechanics being the public
// API is how a finished consume ended up in the in-flight tier, standing on the status line
// long after the page it belonged to had been closed.
func sendStatus(status Status) {
	StatusLineCh <- status
}

// SendStatusProgress reports an operation still in flight: spinner, and no TTL because the
// operation's own outcome is what replaces it.
func SendStatusProgress(message string) {
	sendStatus(Status{Message: message, TTL: 0, Spinner: true})
}

// SendStatusDone confirms an operation that finished as asked. It clears itself shortly after,
// having nothing to say beyond that it happened.
func SendStatusDone(message string) {
	sendStatus(Status{Message: message, TTL: doneTTL, Spinner: false})
}

// SendStatusNote reports something the user is meant to read but that is not a failure — a
// request that was a no-op, an empty result. It stands long enough to be read, then clears.
func SendStatusNote(message string) {
	sendStatus(Status{Message: message, TTL: noteTTL, Spinner: false})
}

// SendStatusError reports a failure. It carries no TTL, so a reason does not expire before it
// has been read: it stands until something replaces it.
func SendStatusError(message string) {
	sendStatus(Status{Message: message, TTL: 0, Spinner: false})
}

// SendStatusPrompt sends a confirmation question: it never auto-clears, shows no spinner, and
// is the only message displayed until it is answered.
func SendStatusPrompt(message string) {
	sendStatus(Status{Message: message, TTL: 0, Spinner: false, Prompt: true})
}

// ClearStatus clears the status line.
func ClearStatus() {
	sendStatus(Status{Message: "", TTL: 0, Spinner: false})
}

// RunStatusLineHandler paints the status line: messages as they arrive, the spinner animation,
// and the expiry of a message that carries a TTL.
//
// It runs as two goroutines on purpose. Draining StatusLineCh must never wait on the UI
// goroutine, because that goroutine is itself a sender — every key handler reports through the
// status line, and every page runs ClearStatus at the end of the queued update that builds it.
// tview's QueueUpdate does not return until the UI goroutine has run the update, so a single
// goroutine that both drained the channel and queued updates would stop draining while it
// waited, and a full channel would then leave the UI goroutine blocked in a send that only it
// could unblock: the whole application freezes with no way out. The drain loop below therefore
// touches nothing but the inbox, and only the painter parks in QueueUpdate.
func (app *App) RunStatusLineHandler(ctx context.Context, in chan Status) {
	inbox := &statusInbox{}

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("shutting down status line handler")
				return
			case status := <-in:
				inbox.put(status)
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(statusInterval)
		defer ticker.Stop()

		var currentStatus string
		var spinnerIdx int
		var spinnerActive bool
		// errorStanding is set while the message on display is a failure. ClearStatus is
		// ignored while it is, so a page build finishing after a call failed cannot sweep
		// the reason away before it has been read.
		// expiry is when the message on display clears itself; zero means it stands until
		// something replaces it.
		var expiry time.Time

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				arrived := inbox.take()
				expired := currentStatus != "" && !expiry.IsZero() && now.After(expiry)
				animating := spinnerActive && currentStatus != ""
				if len(arrived) == 0 && !expired && !animating {
					continue
				}

				app.QueueUpdateDraw(func() {
					for _, status := range arrived {
						// A pending confirmation owns the status line: a background
						// fetch reporting progress must not paint over the question
						// being asked.
						if app.confirmPending() && !status.Prompt {
							log.Debug().Str("status", status.Message).
								Msg("status suppressed while a confirmation is pending")
							continue
						}

						currentStatus = status.Message
						spinnerActive = status.Message != "" && status.Spinner
						expiry = time.Time{}
						if status.Message != "" && status.TTL > 0 {
							expiry = now.Add(status.TTL)
						}
						expired = false
					}

					// A question outlives the TTL of the message it displaced.
					if expired && !app.confirmPending() {
						currentStatus = ""
						spinnerActive = false
						expiry = time.Time{}
					}

					switch {
					case currentStatus == "":
						app.Layout.StatusLine.SetText("")
					case spinnerActive:
						spinnerIdx = (spinnerIdx + 1) % len(SpinnerFrames)
						app.Layout.StatusLine.SetText(
							fmt.Sprintf("%s %s", SpinnerFrames[spinnerIdx], currentStatus),
						)
					default:
						app.Layout.StatusLine.SetText(currentStatus)
					}
				})
			}
		}
	}()
}
