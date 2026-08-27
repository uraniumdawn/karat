// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/config"
)

func TestCountdownSeconds(t *testing.T) {
	tests := map[string]struct {
		remaining time.Duration
		want      int
	}{
		"a whole interval":     {5 * time.Second, 5},
		"on a second boundary": {4 * time.Second, 4},
		"just past a boundary": {4001 * time.Millisecond, 5},
		"just short of one":    {4999 * time.Millisecond, 5},
		"the last instant":     {1, 1},
		"due now":              {0, 1},
		"past due":             {-3 * time.Millisecond, 1},
		"the longest interval": {60 * time.Second, 60},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := countdownSeconds(tt.remaining); got != tt.want {
				t.Errorf("countdownSeconds(%v) = %d, want %d", tt.remaining, got, tt.want)
			}
		})
	}
}

func TestReplaceAutoUpdateSegment(t *testing.T) {
	tests := map[string]struct {
		title   string
		segment string
		want    string
		wantOK  bool
	}{
		"a page as it was opened": {
			title:   " topic:orders [↺5s] [2026-08-27T12:04:11] ",
			segment: "↺5s 3s",
			want:    " topic:orders [↺5s 3s] [2026-08-27T12:04:11] ",
			wantOK:  true,
		},
		"a title already counting": {
			title:   " topic:orders [↺5s 4s] [2026-08-27T12:04:11] ",
			segment: "↺5s 3s",
			want:    " topic:orders [↺5s 3s] [2026-08-27T12:04:11] ",
			wantOK:  true,
		},
		"a bracketed group before the indicator": {
			title:   " connectors:[12] [↺10s] [2026-08-27T12:04:11] ",
			segment: "↺10s  9s",
			want:    " connectors:[12] [↺10s  9s] [2026-08-27T12:04:11] ",
			wantOK:  true,
		},
		"before the timestamp is appended": {
			title:   " topic:orders [↺5s]",
			segment: "↺5s 3s",
			want:    " topic:orders [↺5s 3s]",
			wantOK:  true,
		},
		"the marker inside the resource name": {
			title:   " cgroup:svc-↺-1 [↺5s] [2026-08-27T12:04:11] ",
			segment: "↺5s 3s",
			want:    " cgroup:svc-↺-1 [↺5s 3s] [2026-08-27T12:04:11] ",
			wantOK:  true,
		},
		"no indicator to rewrite": {
			title:   " topic:orders [2026-08-27T12:04:11] ",
			segment: "↺5s 3s",
			want:    " topic:orders [2026-08-27T12:04:11] ",
		},
		"a marker outside any group": {
			title:   " ↺ orders ",
			segment: "↺5s 3s",
			want:    " ↺ orders ",
		},
		"an unterminated group": {
			title:   " topic:orders [↺5s",
			segment: "↺5s 3s",
			want:    " topic:orders [↺5s",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := replaceAutoUpdateSegment(tt.title, tt.segment)
			if got != tt.want {
				t.Errorf("title = %q, want %q", got, tt.want)
			}
			if ok != tt.wantOK {
				t.Errorf("replaced = %v, want %v", ok, tt.wantOK)
			}
		})
	}
}

func TestAutoUpdateSegment(t *testing.T) {
	const pageKey = "local:topic:orders"
	now := time.Date(2026, 8, 27, 12, 4, 11, 0, time.UTC)

	tests := map[string]struct {
		mode        bool
		key         string
		intervalIdx int
		remaining   time.Duration
		want        string
	}{
		"mode is off": {mode: false, key: pageKey, intervalIdx: 2, remaining: 3 * time.Second},
		"another page is in mode": {
			mode:        true,
			key:         "local:topic:other",
			intervalIdx: 2,
			remaining:   3 * time.Second,
		},
		"in mode, no interval": {mode: true, key: pageKey, intervalIdx: 0},
		"one second, no count": {
			mode:        true,
			key:         pageKey,
			intervalIdx: 1,
			remaining:   time.Second,
			want:        "↺1s",
		},
		"five seconds": {
			mode:        true,
			key:         pageKey,
			intervalIdx: 2,
			remaining:   3200 * time.Millisecond,
			want:        "↺5s 4s",
		},
		"ten seconds, two digits": {
			mode:        true,
			key:         pageKey,
			intervalIdx: 3,
			remaining:   9500 * time.Millisecond,
			want:        "↺10s 10s",
		},
		"ten seconds, padded": {
			mode:        true,
			key:         pageKey,
			intervalIdx: 3,
			remaining:   8500 * time.Millisecond,
			want:        "↺10s  9s",
		},
		"no deadline yet": {mode: true, key: pageKey, intervalIdx: 2, want: "↺5s"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			entry := &autoUpdateEntry{intervalIdx: tt.intervalIdx}
			if tt.remaining != 0 {
				entry.nextRefresh = now.Add(tt.remaining)
			}
			app := &App{
				autoUpdate:        map[string]*autoUpdateEntry{pageKey: entry},
				autoUpdateMode:    tt.mode,
				autoUpdatePageKey: tt.key,
			}

			if got := app.autoUpdateSegment(pageKey, now); got != tt.want {
				t.Errorf("autoUpdateSegment() = %q, want %q", got, tt.want)
			}
		})
	}
}

// The countdown has to reach the title of the page on screen, not just compute correctly: the
// painter runs on its own goroutine and gets there through QueueUpdateDraw and a rewrite of one
// bracketed segment. This drives the real ticker against a simulation screen.
func TestCountdownRepaintsTheTitle(t *testing.T) {
	const pageKey = "local:topic:orders"
	const timestamp = " [2026-08-27T12:04:11] "

	app := newCountdownApp(t)
	showPage(app, pageKey, " topic:orders ", TopicDecriptionPageMenu)
	startEventLoop(t, app)

	var refreshes atomic.Int32
	app.autoUpdate = map[string]*autoUpdateEntry{
		pageKey: {intervalIdx: 1, refreshFn: func() { refreshes.Add(1) }},
	}
	app.autoUpdateMode = true
	app.autoUpdatePageKey = pageKey

	defer drainStatus()

	// QueueUpdateDraw does not return until the event loop has run the closure, so it doubles as
	// the wait for the loop to be up and as the race-free way to read a title back.
	title := func() string {
		var title string
		app.QueueUpdateDraw(func() {
			if page, ok := app.Layout.PagesRegistry.UI.Pages.GetPage(pageKey).(titledPrimitive); ok {
				title = page.GetTitle()
			}
		})
		return title
	}
	title()

	app.CycleIntervalForCurrentPage()
	if got := app.GetAutoUpdateLabel(pageKey); got != "↺5s 5s" {
		t.Fatalf("label right after cycling = %q, want %q", got, "↺5s 5s")
	}

	// What a page build leaves behind: the label of the moment, then the timestamp.
	app.QueueUpdateDraw(func() {
		page, ok := app.Layout.PagesRegistry.UI.Pages.GetPage(pageKey).(titledPrimitive)
		if !ok {
			return
		}
		page.SetTitle(" topic:orders [" + app.GetAutoUpdateLabel(pageKey) + "]" + timestamp)
	})

	for _, want := range []string{" topic:orders [↺5s 4s]" + timestamp, " topic:orders [↺5s 3s]" + timestamp} {
		deadline := time.Now().Add(2 * time.Second)
		for title() != want {
			if time.Now().After(deadline) {
				t.Fatalf("title = %q, want %q", title(), want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	if n := refreshes.Load(); n != 1 {
		t.Errorf("refreshes = %d, want 1: the countdown must not refresh the page", n)
	}
}

// newCountdownApp builds the least application a countdown needs, on a simulation screen, with
// its event loop not yet running: pages have to be in place before it starts, since adding one
// races with the draw.
//
// It does not use newBadgeApp: that one finalises the simulation screen itself, and Stop
// finalises it too, which the simulation screen panics on the second time.
func newCountdownApp(tb testing.TB) *App {
	tb.Helper()

	styles, borders := tview.Styles, tview.Borders
	tb.Cleanup(func() { tview.Styles, tview.Borders = styles, borders })

	colors, err := config.LoadColorConfig("")
	if err != nil {
		tb.Fatalf("LoadColorConfig() error = %v", err)
	}

	cfg := &config.Config{}
	cfg.SetMode(config.ReadOnly)

	app := &App{
		Application:    tview.NewApplication(),
		Config:         cfg,
		Colors:         colors,
		CurrentFilters: make(map[string]string),
	}
	app.ApplyColors()
	app.Layout = NewLayout(NewPagesRegistry(app.Colors), app.Colors, cfg, true, true)

	screen := tcell.NewSimulationScreen("UTF-8")
	app.SetScreen(screen)
	screen.SetSize(120, 40)
	app.SetRoot(app.Layout.Content, true)

	return app
}

// startEventLoop runs the application, so that QueueUpdateDraw runs its closure rather than
// parking forever. The first QueueUpdateDraw of a test doubles as the wait for the loop to be up.
func startEventLoop(tb testing.TB, app *App) {
	tb.Helper()

	go func() { _ = app.Application.Run() }()
	tb.Cleanup(app.Stop)
}
