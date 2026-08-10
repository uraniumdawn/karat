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
	"github.com/uraniumdawn/karat/pkg/util"
)

// newBadgeApp builds the least application the badge needs — a mode, a layout and one bordered
// page — drawn once onto a simulation screen of the given size.
//
// It restores the tview package-level state NewLayout and ApplyColors write, so a test cannot
// leak borders or colors into the next one.
func newBadgeApp(
	tb testing.TB,
	mode config.Mode,
	width, height int,
) (*App, tcell.SimulationScreen) {
	tb.Helper()

	styles, borders := tview.Styles, tview.Borders
	tb.Cleanup(func() { tview.Styles, tview.Borders = styles, borders })

	colors, err := config.LoadColorConfig("")
	if err != nil {
		tb.Fatalf("LoadColorConfig() error = %v", err)
	}

	cfg := &config.Config{}
	cfg.SetMode(mode)

	app := &App{
		Application:    tview.NewApplication(),
		Config:         cfg,
		Colors:         colors,
		CurrentFilters: make(map[string]string),
	}

	app.ApplyColors()
	registry := NewPagesRegistry(app.Colors)
	app.Layout = NewLayout(registry, app.Colors, cfg, true, true)

	screen := tcell.NewSimulationScreen("UTF-8")
	tb.Cleanup(screen.Fini)

	// SetScreen initialises the screen, which resets it to its default size — so the size this
	// test wants has to be set after, not before.
	app.SetScreen(screen)
	screen.SetSize(width, height)

	app.SetAfterDrawFunc(app.drawModeBadge)
	app.SetRoot(app.Layout.Content, true)

	return app, screen
}

// showPage adds a bordered page under the given menu and brings it to the front.
//
// It goes straight to Pages: AddToPagesRegistry needs the cache and appends a timestamp to the
// title, neither of which the badge cares about. The title alignment is left at tview's default
// — centred — because that is what karat's pages use, and it is what keeps the title clear of
// the badge now that the badge is painted at the left.
func showPage(app *App, name, title, menu string) {
	page := tview.NewTable()
	page.SetBorder(true).SetTitle(title)

	registry := app.Layout.PagesRegistry
	registry.PageMenuMap[name] = menu
	registry.UI.Pages.AddAndSwitchToPage(name, page, true)
	app.ForceDraw()
}

// screenRow returns row y of the screen as text. A cell nothing was drawn into holds no runes
// at all, which reads as a blank.
func screenRow(t *testing.T, screen tcell.SimulationScreen, y int) string {
	t.Helper()

	cells, width, height := screen.GetContents()
	if y < 0 || y >= height {
		t.Fatalf("row %d is outside a screen of %d rows", y, height)
	}

	var row strings.Builder
	for x := range width {
		cell := cells[y*width+x]
		if len(cell.Runes) == 0 {
			row.WriteRune(' ')
			continue
		}
		row.WriteRune(cell.Runes[0])
	}
	return row.String()
}

// screenFg returns the foreground color of one cell.
func screenFg(t *testing.T, screen tcell.SimulationScreen, x, y int) tcell.Color {
	t.Helper()

	cells, width, _ := screen.GetContents()
	fg, _, _ := cells[y*width+x].Style.Decompose()
	return fg
}

// The mode and the title are one label: the badge is painted immediately to the left of the
// title, not stranded at the end of the line.
func TestModeBadgeSitsBesideTheTitle(t *testing.T) {
	app, screen := newBadgeApp(t, config.Yolo, 80, 24)
	showPage(app, "local:topics", " Topics ", TopicsPageMenu)

	row := screenRow(t, screen, headerHeight)
	if want := " [yolo] Topics "; !strings.Contains(row, want) {
		t.Errorf("content border row = %q, want it to contain %q", row, want)
	}
	// Beside the title, not in the corner.
	if strings.HasPrefix(row, string(tview.Borders.TopLeft)+" [yolo]") {
		t.Errorf("content border row = %q, want the badge away from the corner", row)
	}
}

func TestModeBadgeTextPerMode(t *testing.T) {
	for _, mode := range []config.Mode{config.ReadOnly, config.Confirm, config.Yolo} {
		t.Run(string(mode), func(t *testing.T) {
			app, screen := newBadgeApp(t, mode, 80, 24)
			showPage(app, "local:topics", " Topics ", TopicsPageMenu)

			row := screenRow(t, screen, headerHeight)
			want := " [" + string(mode) + "] Topics "
			if !strings.Contains(row, want) {
				t.Errorf("content border row = %q, want it to contain %q", row, want)
			}
		})
	}
}

// The default mode is named too: an empty corner would be indistinguishable from a badge that
// simply did not draw.
func TestModeBadgeNamesTheDefaultMode(t *testing.T) {
	app, screen := newBadgeApp(t, config.Confirm, 80, 24)
	showPage(app, "local:topics", " Topics ", TopicsPageMenu)

	if row := screenRow(t, screen, headerHeight); !strings.Contains(row, " [confirm] ") {
		t.Errorf("content border row = %q, want the default mode named", row)
	}
}

// The badge is the same on every page: there is one mode, and no page scope to resolve.
func TestModeBadgeIsTheSameOnEveryPage(t *testing.T) {
	app, screen := newBadgeApp(t, config.Yolo, 80, 24)

	for _, page := range []struct{ name, title, menu string }{
		{"local:topics", " Topics ", TopicsPageMenu},
		{"sr:subjects", " Subjects ", SubjectsPageMenu},
		{"connect:connectors", " Connectors ", ConnectorsPageMenu},
		{Clusters, " Clusters ", ClustersPageMenu},
	} {
		showPage(app, page.name, page.title, page.menu)
		if row := screenRow(t, screen, headerHeight); !strings.Contains(row, " [yolo] ") {
			t.Errorf("on %s the row = %q, want the one mode", page.name, row)
		}
	}
}

// Only the badge text carries the mode's color; the border it sits in keeps the style file's.
func TestModeBadgeColorsOnlyItself(t *testing.T) {
	app, screen := newBadgeApp(t, config.Yolo, 80, 24)
	showPage(app, "local:topics", " Topics ", TopicsPageMenu)

	pages := app.Layout.PagesRegistry.UI.Pages
	x0, _, width, _ := pages.GetRect()
	_, front := pages.GetFrontPage()
	badge := tview.TaggedStringWidth(modeBadgeText(config.Yolo))
	start, titled := titleStart(front, x0, width)
	if !titled {
		t.Fatalf("the test page has no title to sit beside")
	}
	at := start - badge

	// The badge's own cells, its leading pad aside.
	for x := at + 1; x < at+badge; x++ {
		if got := screenFg(t, screen, x, headerHeight); got != tcell.ColorRed {
			t.Fatalf("badge cell %d color = %v, want red", x, got)
		}
	}

	wantBorder := tcell.GetColor(app.Colors.Karat.Border)
	if got := screenFg(t, screen, x0, headerHeight); got != wantBorder {
		t.Errorf("corner color = %v, want the border's %v", got, wantBorder)
	}
}

// Only yolo is coloured, and no style file can change that.
func TestModeColorIsRedOnlyForYolo(t *testing.T) {
	for _, mode := range []config.Mode{config.ReadOnly, config.Confirm, config.Mode("")} {
		if got := modeColor(mode); got != tcell.ColorDefault {
			t.Errorf("modeColor(%q) = %v, want the default colour", mode, got)
		}
	}
	if got := modeColor(config.Yolo); got != tcell.ColorRed {
		t.Errorf("modeColor(yolo) = %v, want red", got)
	}
}

// A wide title on a narrow terminal reaches the badge's cells. The badge paints last, so it
// would eat the beginning of the title — the resource name — and leave the timestamp. The title
// wins that fight.
func TestModeBadgeStaysOffWhenItWouldEatTheTitle(t *testing.T) {
	app, screen := newBadgeApp(t, config.Yolo, 44, 24)
	title := " topics:[3] [2026-08-14T10:15:50] "
	showPage(app, "local:topics", title, TopicsPageMenu)

	row := screenRow(t, screen, headerHeight)
	if strings.Contains(row, "[yolo]") {
		t.Errorf("content border row = %q, want no badge over the title", row)
	}
	if !strings.Contains(row, strings.TrimSpace(title)) {
		t.Errorf("content border row = %q, want the whole title %q", row, title)
	}
}

// A terminal too narrow for both keeps the title: a half-eaten badge tells the user nothing.
func TestModeBadgeHiddenOnNarrowTerminal(t *testing.T) {
	app, screen := newBadgeApp(t, config.Yolo, 20, 24)
	showPage(app, "local:topics", " Topics ", TopicsPageMenu)

	row := screenRow(t, screen, headerHeight)
	// Guards the test itself: the screen must really be the narrow one that was asked for.
	if got := len([]rune(row)); got != 20 {
		t.Fatalf("screen width = %d, want 20", got)
	}
	if strings.Contains(row, "yolo") {
		t.Errorf("content border row = %q, want no badge on a narrow terminal", row)
	}
}

// The badge paints last, so a modal on top of a page cannot cover it.
func TestModeBadgeSurvivesAModal(t *testing.T) {
	app, screen := newBadgeApp(t, config.Yolo, 80, 24)
	showPage(app, "local:topics", " Topics ", TopicsPageMenu)

	pages := app.Layout.PagesRegistry.UI.Pages
	modal := tview.NewBox()
	modal.SetBorder(true).SetTitle(" Find By ")
	pages.AddPage(FindBy, modal, true, true)
	pages.SendToFront(FindBy)
	app.ForceDraw()

	if row := screenRow(t, screen, headerHeight); !strings.Contains(row, " [yolo] Find By ") {
		t.Errorf("content border row = %q, want the badge on top of the modal", row)
	}
}

// A page with no title has nothing to sit beside; the mode is still worth showing.
func TestModeBadgeOnAnUntitledPage(t *testing.T) {
	app, screen := newBadgeApp(t, config.Yolo, 80, 24)
	showPage(app, "untitled", "", TopicsPageMenu)

	row := screenRow(t, screen, headerHeight)
	if want := string(tview.Borders.TopLeft) + " [yolo]"; !strings.HasPrefix(row, want) {
		t.Errorf("content border row = %q, want it to start with %q", row, want)
	}
}

// tview.Print reads square brackets as style tags, so an unescaped badge would print as nothing
// at all. What must survive is the printed width: the mode, its brackets and the two spaces.
func TestModeBadgeTextEscapesItsBrackets(t *testing.T) {
	for _, mode := range []config.Mode{config.ReadOnly, config.Confirm, config.Yolo} {
		text := modeBadgeText(mode)
		if want := len(" [" + string(mode) + "]"); tview.TaggedStringWidth(text) != want {
			t.Errorf(
				"modeBadgeText(%q) prints %d columns, want %d — the brackets were eaten",
				mode,
				tview.TaggedStringWidth(text),
				want,
			)
		}
	}
}

// A modal leaves the page's border and title on screen, so the badge stays beside that title
// instead of retreating into the corner.
func TestModeBadgeIgnoresModals(t *testing.T) {
	app, screen := newBadgeApp(t, config.ReadOnly, 80, 24)
	showPage(app, "local:topics", " Topics ", TopicsPageMenu)

	before := screenRow(t, screen, headerHeight)

	dialog := tview.NewTextView()
	dialog.SetBorder(true).SetTitle(" Parameters ")
	app.Layout.PagesRegistry.UI.Pages.AddPage("consume:params", util.NewModal(dialog), true, true)
	app.ForceDraw()

	if after := screenRow(t, screen, headerHeight); after != before {
		t.Errorf("border row with a modal = %q, want it unchanged from %q", after, before)
	}
}
