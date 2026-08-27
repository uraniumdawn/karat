// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"reflect"
	"sort"
	"testing"

	"github.com/patrickmn/go-cache"
	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/config"
)

// TestPlanOpenedPages verifies the ordering rule behind h/l navigation: a newly
// opened page is inserted immediately after the current page, and re-opening an
// existing page moves it to that same position.
func TestPlanOpenedPages(t *testing.T) {
	tests := []struct {
		name      string
		names     []string
		current   string
		open      string
		wantOrder []string
		wantIndex int
	}{
		{
			name:      "rule: new page inserted after current, tail shifts right",
			names:     []string{"a", "b", "c", "d", "e"},
			current:   "d", // index 3
			open:      "new",
			wantOrder: []string{"a", "b", "c", "d", "new", "e"},
			wantIndex: 4,
		},
		{
			name:      "first page ever",
			names:     []string{},
			current:   "",
			open:      "a",
			wantOrder: []string{"a"},
			wantIndex: 0,
		},
		{
			name:      "open while on last page appends at end",
			names:     []string{"a", "b"},
			current:   "b",
			open:      "c",
			wantOrder: []string{"a", "b", "c"},
			wantIndex: 2,
		},
		{
			// Edge case 1: re-open a page located BEFORE the current one.
			name:      "reopen page before current moves it after current",
			names:     []string{"a", "b", "c", "d", "e"},
			current:   "d", // index 3
			open:      "b", // existing at index 1
			wantOrder: []string{"a", "c", "d", "b", "e"},
			wantIndex: 3,
		},
		{
			name:      "reopen page after current moves it after current",
			names:     []string{"a", "b", "c", "d", "e"},
			current:   "b", // index 1
			open:      "d", // existing at index 3
			wantOrder: []string{"a", "b", "d", "c", "e"},
			wantIndex: 2,
		},
		{
			// Edge case 2: re-opening the current page itself (e.g. a forced
			// refresh). It swaps places with its right-hand neighbour. This
			// documents current behaviour rather than endorsing it.
			name:      "reopen current page swaps with right neighbour",
			names:     []string{"a", "b", "c", "d", "e"},
			current:   "c", // index 2
			open:      "c",
			wantOrder: []string{"a", "b", "d", "c", "e"},
			wantIndex: 3,
		},
		{
			// Edge case 3: front page is not persistent (a modal is in front),
			// so it is not found in the order and the page is appended at the end.
			name:      "current not in list appends at end",
			names:     []string{"a", "b", "c"},
			current:   "modal",
			open:      "new",
			wantOrder: []string{"a", "b", "c", "new"},
			wantIndex: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOrder, gotIndex := planOpenedPages(tt.names, tt.current, tt.open)
			if !reflect.DeepEqual(gotOrder, tt.wantOrder) {
				t.Errorf("order = %v, want %v", gotOrder, tt.wantOrder)
			}
			if gotIndex != tt.wantIndex {
				t.Errorf("index = %d, want %d", gotIndex, tt.wantIndex)
			}
			if gotOrder[gotIndex] != tt.open {
				t.Errorf(
					"order[%d] = %q, want the opened page %q",
					gotIndex,
					gotOrder[gotIndex],
					tt.open,
				)
			}
		})
	}
}

// TestPlanOpenedPages_BackwardReturnsToOpener asserts the core h-navigation
// guarantee: after opening a brand-new page, the row directly before it is the
// page you opened it from, so pressing 'h' returns there.
func TestPlanOpenedPages_BackwardReturnsToOpener(t *testing.T) {
	names := []string{"a", "b", "c", "d", "e"}
	current := "d"

	order, index := planOpenedPages(names, current, "new")
	if index == 0 {
		t.Fatalf("new page landed at index 0; nothing to step back to")
	}
	if got := order[index-1]; got != current {
		t.Errorf("page before the new one = %q, want the opener %q", got, current)
	}
}

// newRegistryApp builds the least application the pages registry needs: a config, colors and
// a layout. It restores the tview package-level state ApplyColors and NewLayout write.
func newRegistryApp(tb testing.TB) *App {
	tb.Helper()

	// Anything that reaches Config.Save() writes to the real ~/.config/karat/config.yaml
	// otherwise. No test has business touching the file the user runs karat on.
	tb.Setenv(config.KaratEnvConfigDir, tb.TempDir())

	styles, borders := tview.Styles, tview.Borders
	tb.Cleanup(func() { tview.Styles, tview.Borders = styles, borders })

	colors, err := config.LoadColorConfig("")
	if err != nil {
		tb.Fatalf("LoadColorConfig() error = %v", err)
	}

	cfg := &config.Config{}
	app := &App{
		Application:    tview.NewApplication(),
		Config:         cfg,
		Colors:         colors,
		Cache:          cache.New(Expiration, Expiration),
		CurrentFilters: make(map[string]string),
	}

	app.ApplyColors()
	app.Layout = NewLayout(NewPagesRegistry(app.Colors), app.Colors, cfg, true, true)

	return app
}

// TestTransientPageStaysOutOfTheRegistry covers what a confirmation page must not do: appear
// among the opened pages, be reachable with h/l, or outlive its own dismissal.
func TestTransientPageStaysOutOfTheRegistry(t *testing.T) {
	app := newRegistryApp(t)
	registry := app.Layout.PagesRegistry

	app.AddToPagesRegistry("Topics", tview.NewTable(), TopicsPageMenu, false)
	app.AddToPagesRegistry("Topic", tview.NewTextView(), TopicDecriptionPageMenu, false)
	app.addTransientPage(TopicConfirm, tview.NewTextView())

	if got, _ := registry.UI.Pages.GetFrontPage(); got != TopicConfirm {
		t.Fatalf("front page = %q, want the confirmation page %q", got, TopicConfirm)
	}
	if want := []string{"Topics", "Topic"}; !reflect.DeepEqual(registry.openedPageNames(), want) {
		t.Errorf("opened pages = %v, want %v", registry.openedPageNames(), want)
	}
	if registry.IsPersistentPage(TopicConfirm) {
		t.Error("the confirmation page is listed as a persistent page")
	}

	// h and l have no row to step from, so they leave the confirmation page in front.
	app.Backward()
	app.Forward()
	if got, _ := registry.UI.Pages.GetFrontPage(); got != TopicConfirm {
		t.Errorf("after h/l the front page = %q, want %q", got, TopicConfirm)
	}

	app.removeTransientPage(TopicConfirm)

	if got, _ := registry.UI.Pages.GetFrontPage(); got != "Topic" {
		t.Errorf("after dismissal the front page = %q, want the page it opened from %q", got, "Topic")
	}
	if registry.UI.Pages.HasPage(TopicConfirm) {
		t.Error("the confirmation page survived its dismissal")
	}
	if _, found := registry.PageMenuMap[TopicConfirm]; found {
		t.Error("the confirmation page kept its menu entry")
	}
	if _, found := app.Cache.Get(TopicConfirm); found {
		t.Error("the confirmation page was cached")
	}
}

// TestTransientPageDoesNotDisturbNavigation asserts h/l keep working over the pages that were
// open before a confirmation page came and went.
func TestTransientPageDoesNotDisturbNavigation(t *testing.T) {
	app := newRegistryApp(t)
	registry := app.Layout.PagesRegistry

	app.AddToPagesRegistry("Topics", tview.NewTable(), TopicsPageMenu, false)
	app.AddToPagesRegistry("Topic", tview.NewTextView(), TopicDecriptionPageMenu, false)
	app.addTransientPage(TopicConfirm, tview.NewTextView())
	app.removeTransientPage(TopicConfirm)

	app.Backward()
	if got, _ := registry.UI.Pages.GetFrontPage(); got != "Topics" {
		t.Errorf("h from %q = %q, want %q", "Topic", got, "Topics")
	}

	app.Forward()
	if got, _ := registry.UI.Pages.GetFrontPage(); got != "Topic" {
		t.Errorf("l from %q = %q, want %q", "Topics", got, "Topic")
	}
}

func TestIndexOfString(t *testing.T) {
	s := []string{"a", "b", "c"}
	if got := indexOfString(s, "b"); got != 1 {
		t.Errorf("indexOfString(b) = %d, want 1", got)
	}
	if got := indexOfString(s, "x"); got != -1 {
		t.Errorf("indexOfString(x) = %d, want -1", got)
	}
	if got := indexOfString(nil, "a"); got != -1 {
		t.Errorf("indexOfString(nil) = %d, want -1", got)
	}
}

// TestOpenedPagesStartsOnTheCurrentPage covers the modal's own selection: it must land on the
// page it was opened from, so that closing it with Esc — which switches to whatever is
// highlighted — leaves the user where they were rather than on the first page in the list.
func TestOpenedPagesStartsOnTheCurrentPage(t *testing.T) {
	app := newRegistryApp(t)
	registry := app.Layout.PagesRegistry

	app.AddToPagesRegistry(Clusters, tview.NewTable(), ClustersPageMenu, false)
	app.AddToPagesRegistry("Topics", tview.NewTable(), TopicsPageMenu, false)

	// The modal is a page of its own, as app.Run() registers it — once it is in front, it is
	// what GetFrontPage() answers with.
	registry.UI.Pages.AddPage(OpenedPages, registry.UI.Main, true, false)

	app.ShowModalPage(OpenedPages)

	row, _ := registry.UI.FilteredPages.GetSelection()
	cell := registry.UI.FilteredPages.GetCell(row, 1)
	if cell == nil {
		t.Fatalf("row %d of the opened-pages list holds no page", row)
	}
	if cell.Text != "Topics" {
		t.Errorf("the opened-pages list starts on %q, want the current page %q", cell.Text, "Topics")
	}
}

// A confirmation page has no way back once the user switches off it: it is not in the
// opened-pages list, so nothing can navigate to it again, and its pending action goes with it.
// The keys that would switch pages are refused while it stands.
func TestConfirmationPageBlocksPageSwitching(t *testing.T) {
	app := newRegistryApp(t)
	registry := app.Layout.PagesRegistry

	app.AddToPagesRegistry("Topics", tview.NewTable(), TopicsPageMenu, false)

	if app.confirmationInFront() {
		t.Fatal("a list page is reported as a confirmation")
	}

	app.addTransientPage(TopicConfirm, tview.NewTextView())

	if !registry.IsTransientPage(TopicConfirm) {
		t.Error("the confirmation page is not marked transient")
	}
	if !app.confirmationInFront() {
		t.Error("a confirmation page in front is not reported as one")
	}

	app.removeTransientPage(TopicConfirm)

	if registry.IsTransientPage(TopicConfirm) {
		t.Error("the confirmation page stayed marked transient after its dismissal")
	}
	if app.confirmationInFront() {
		t.Error("the page returned to is reported as a confirmation")
	}
}

// A deleted topic must take its own pages with it: they are not reachable from the cluster any
// more, but they stay in the opened-pages list and still open, showing what it looked like.
func TestRemovePagesForDropsTheResourcePages(t *testing.T) {
	app := newRegistryApp(t)
	registry := app.Layout.PagesRegistry

	for _, page := range []string{
		"local:topics",
		"local:topic:orders",
		"local:producers:orders",
		"local:consume output:orders",
		"local:topic:orders-retry",
		"other:topic:orders",
	} {
		app.AddToPagesRegistry(page, tview.NewTable(), TopicsPageMenu, false)
	}

	app.RemovePagesFor("local", "orders")

	want := []string{"local:topics", "local:topic:orders-retry", "other:topic:orders"}
	got := registry.openedPageNames()
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("opened pages = %v, want %v", got, want)
	}

	for _, gone := range []string{"local:topic:orders", "local:producers:orders"} {
		if registry.UI.Pages.HasPage(gone) {
			t.Errorf("%q is still a page", gone)
		}
		if _, cached := app.Cache.Get(gone); cached {
			t.Errorf("%q is still cached", gone)
		}
	}
}

// Removing pages the user is not looking at must not move them: only a front page that is
// itself removed forces a switch.
func TestRemovePagesForLeavesTheCurrentPageAlone(t *testing.T) {
	app := newRegistryApp(t)
	registry := app.Layout.PagesRegistry

	app.AddToPagesRegistry("local:topic:orders", tview.NewTable(), TopicsPageMenu, false)
	app.AddToPagesRegistry("local:topics", tview.NewTable(), TopicsPageMenu, false)

	app.RemovePagesFor("local", "orders")

	if got, _ := registry.UI.Pages.GetFrontPage(); got != "local:topics" {
		t.Errorf("front page = %q, want the page the user was on", got)
	}
}

// A filter can leave the selection past the last match. Esc and Enter in the opened-pages
// modal act on the selected row, so an out-of-range index makes them do nothing at all.
func TestRebuildFilteredPagesKeepsTheSelectionInRange(t *testing.T) {
	app := newRegistryApp(t)
	registry := app.Layout.PagesRegistry

	for _, page := range []string{"local:topics", "local:acls", "local:subjects"} {
		app.AddToPagesRegistry(page, tview.NewTable(), TopicsPageMenu, false)
	}

	registry.RebuildFilteredPages("")
	registry.UI.FilteredPages.Select(2, 0)

	registry.RebuildFilteredPages("subj")

	row, _ := registry.UI.FilteredPages.GetSelection()
	if rows := registry.UI.FilteredPages.GetRowCount(); row < 0 || row >= rows {
		t.Fatalf("selection is row %d of %d rows", row, rows)
	}
	if cell := registry.UI.FilteredPages.GetCell(row, 1); cell == nil || cell.Text != "local:subjects" {
		t.Errorf("selected %v, want the only match", cell)
	}
}

// An in-range selection is the user's, and must survive a rebuild untouched.
func TestRebuildFilteredPagesKeepsAValidSelection(t *testing.T) {
	app := newRegistryApp(t)
	registry := app.Layout.PagesRegistry

	for _, page := range []string{"local:topics", "local:acls", "local:subjects"} {
		app.AddToPagesRegistry(page, tview.NewTable(), TopicsPageMenu, false)
	}

	registry.RebuildFilteredPages("")
	registry.UI.FilteredPages.Select(1, 0)

	registry.RebuildFilteredPages("")

	if row, _ := registry.UI.FilteredPages.GetSelection(); row != 1 {
		t.Errorf("selection moved to row %d, want it left at 1", row)
	}
}
