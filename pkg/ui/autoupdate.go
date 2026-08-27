// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	autoUpdateIntervals = []time.Duration{
		0, 1 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second, 60 * time.Second,
	}
	autoUpdateLabels = []string{"", "1s", "5s", "10s", "30s", "60s"}
)

// autoUpdateMarker opens the title indicator, and is what the countdown painter looks for to
// find the segment it may rewrite.
const autoUpdateMarker = "↺"

// countdownPoll is how often the countdown is recomputed. A second would be enough for what is
// displayed, but a poll free-running against the refresh ticker lands its last frame short of
// the refresh and shows the same count twice. A quarter of a second bounds that error; the
// painter still only queues an update when the displayed text changes.
const countdownPoll = 250 * time.Millisecond

type autoUpdateEntry struct {
	cancel      context.CancelFunc // nil when no ticker is running
	intervalIdx int
	refreshFn   func()
	nextRefresh time.Time // zero when no ticker is running
}

// EnterAutoUpdateMode puts the given page into auto-update mode.
// All normal shortcuts are blocked; Tab cycles intervals, Esc exits.
func (app *App) EnterAutoUpdateMode(pageKey string, fn func()) {
	app.autoUpdateMu.Lock()
	if entry, ok := app.autoUpdate[pageKey]; ok {
		if entry.cancel != nil {
			entry.cancel()
		}
		delete(app.autoUpdate, pageKey)
	}
	app.autoUpdate[pageKey] = &autoUpdateEntry{cancel: nil, intervalIdx: 0, refreshFn: fn}
	app.autoUpdateMode = true
	app.autoUpdatePageKey = pageKey
	app.autoUpdateMu.Unlock()

	app.Layout.Menu.SetMenu(AutoUpdateModePageMenu)
}

// ExitAutoUpdateMode stops the ticker, exits mode, and restores the normal menu.
// It triggers a final refresh so the title indicator is cleared.
func (app *App) ExitAutoUpdateMode() {
	app.autoUpdateMu.Lock()
	var fn func()
	if entry, ok := app.autoUpdate[app.autoUpdatePageKey]; ok {
		if entry.cancel != nil {
			entry.cancel()
		}
		fn = entry.refreshFn
		delete(app.autoUpdate, app.autoUpdatePageKey)
	}
	pageKey := app.autoUpdatePageKey
	app.autoUpdateMode = false
	app.autoUpdatePageKey = ""
	app.autoUpdateMu.Unlock()

	if menu, ok := app.Layout.PagesRegistry.PageMenuMap[pageKey]; ok {
		app.Layout.Menu.SetMenu(menu)
	}
	if fn != nil {
		fn()
	}
}

// CycleIntervalForCurrentPage advances the auto-update interval for the active page.
// off → 1s → 5s → 10s → 30s → 60s → off. Transitioning to a non-zero interval triggers
// an immediate refresh before starting the ticker.
func (app *App) CycleIntervalForCurrentPage() {
	app.autoUpdateMu.Lock()
	entry, ok := app.autoUpdate[app.autoUpdatePageKey]
	if !ok {
		app.autoUpdateMu.Unlock()
		return
	}
	if entry.cancel != nil {
		entry.cancel()
		entry.cancel = nil
	}
	nextIdx := (entry.intervalIdx + 1) % len(autoUpdateIntervals)
	entry.intervalIdx = nextIdx
	if nextIdx == 0 {
		entry.nextRefresh = time.Time{}
		fn := entry.refreshFn
		app.autoUpdateMu.Unlock()
		fn()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	entry.cancel = cancel
	interval := autoUpdateIntervals[nextIdx]
	fn := entry.refreshFn
	pageKey := app.autoUpdatePageKey
	// Set here rather than in the goroutine below: the fn() that follows publishes a rebuild
	// which reads the label back on the UI goroutine, and a deadline still zero by then would
	// paint a count computed from nothing.
	entry.nextRefresh = time.Now().Add(interval)
	app.autoUpdateMu.Unlock()

	SendStatusProgress("auto-update")
	fn()

	go func() {
		refresh := time.NewTicker(interval)
		defer refresh.Stop()

		// A one-second interval has nothing to count down. A nil channel never fires, which
		// leaves the select below with no countdown case at all.
		var countdown <-chan time.Time
		if interval > time.Second {
			poll := time.NewTicker(countdownPoll)
			defer poll.Stop()
			countdown = poll.C
		}

		var shown string
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-refresh.C:
				// The ticker's own timestamp, so the deadline on display cannot drift from the
				// one the ticker keeps.
				app.autoUpdateMu.Lock()
				entry.nextRefresh = now.Add(interval)
				app.autoUpdateMu.Unlock()
				fn()
			case now := <-countdown:
				segment := app.autoUpdateSegment(pageKey, now)
				if segment == "" || segment == shown {
					continue
				}
				shown = segment
				app.setPageTitleSegment(pageKey, segment)
			}
		}
	}()
}

// setPageTitleSegment paints segment into the auto-update indicator of the page it belongs to.
//
// No lock is held while it waits. QueueUpdateDraw does not return until the UI goroutine has run
// the closure, and the UI goroutine takes autoUpdateMu on every page build and in the key
// handler: holding it across the call lets each goroutine wait on the other, and auto-update
// mode swallows every key that could break the freeze. status.go documents the same trap.
//
// The page is looked up inside the closure because each refresh replaces the primitive under the
// same name, so a pointer taken out here would repaint a page that has left the screen.
func (app *App) setPageTitleSegment(pageKey, segment string) {
	app.QueueUpdateDraw(func() {
		page, ok := app.Layout.PagesRegistry.UI.Pages.GetPage(pageKey).(titledPrimitive)
		if !ok {
			return
		}
		if title, replaced := replaceAutoUpdateSegment(page.GetTitle(), segment); replaced {
			page.SetTitle(title)
		}
	})
}

// replaceAutoUpdateSegment rewrites the bracketed auto-update indicator in title to segment,
// leaving the page name before it and the last-refresh timestamp after it as they were. It
// reports false, and title unchanged, when there is no indicator to rewrite: a page rebuilt
// after the mode was left has none, and must not have a countdown painted back into it.
//
// The marker is searched for from the right, and so is the bracket that opens its group: a
// consumer group or transactional id may contain the marker itself, and a title may carry a
// bracketed group of its own before the indicator.
func replaceAutoUpdateSegment(title, segment string) (string, bool) {
	marker := strings.LastIndex(title, autoUpdateMarker)
	if marker < 0 {
		return title, false
	}
	open := strings.LastIndex(title[:marker], "[")
	if open < 0 {
		return title, false
	}
	closing := strings.Index(title[marker:], "]")
	if closing < 0 {
		return title, false
	}
	return title[:open+1] + segment + title[marker+closing:], true
}

// GetAutoUpdateLabel returns the display label for a page's auto-update state: "" when the page
// is not auto-updating, "↺1s" for an interval with nothing to count down, and "↺5s 3s" - the
// interval, then whole seconds until the next refresh - for the rest.
func (app *App) GetAutoUpdateLabel(pageKey string) string {
	return app.autoUpdateSegment(pageKey, time.Now())
}

// autoUpdateSegment is GetAutoUpdateLabel as of now. The countdown painter passes the timestamp
// its ticker woke on, and the tests pass a fixed one.
//
// The count is padded to the width of the interval's own digits: tview centres a title, and the
// mode badge anchors itself to where the centred title starts, so a count narrowing from two
// digits to one would shift both by a column.
func (app *App) autoUpdateSegment(pageKey string, now time.Time) string {
	app.autoUpdateMu.Lock()
	defer app.autoUpdateMu.Unlock()

	if !app.autoUpdateMode || pageKey != app.autoUpdatePageKey {
		return ""
	}
	entry, ok := app.autoUpdate[pageKey]
	if !ok || entry.intervalIdx == 0 {
		return ""
	}

	label := autoUpdateMarker + autoUpdateLabels[entry.intervalIdx]
	interval := autoUpdateIntervals[entry.intervalIdx]
	if interval <= time.Second || entry.nextRefresh.IsZero() {
		return label
	}

	width := len(strconv.Itoa(int(interval / time.Second)))
	return fmt.Sprintf("%s %*ds", label, width, countdownSeconds(entry.nextRefresh.Sub(now)))
}

// countdownSeconds is the whole number of seconds a countdown of remaining shows. It rounds up
// and never reaches zero, so that a just-started interval reads as its own length, the last
// second before a refresh reads as 1s, and the window between the refresh ticker firing and the
// deadline moving on reads as 1s rather than as zero or a negative number.
func countdownSeconds(remaining time.Duration) int {
	if remaining <= 0 {
		return 1
	}
	return int((remaining + time.Second - 1) / time.Second)
}

// StopAllAutoUpdates cancels all running auto-update tickers and resets mode state.
func (app *App) StopAllAutoUpdates() {
	app.autoUpdateMu.Lock()
	defer app.autoUpdateMu.Unlock()
	for key, entry := range app.autoUpdate {
		if entry.cancel != nil {
			entry.cancel()
		}
		delete(app.autoUpdate, key)
	}
	app.autoUpdateMode = false
	app.autoUpdatePageKey = ""
}
