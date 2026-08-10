// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"strconv"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/sahilm/fuzzy"

	"github.com/uraniumdawn/karat/pkg/config"
	"github.com/uraniumdawn/karat/pkg/util"
)

// PagesRegistry manages the application's pages and page-menu mappings.
// The OpenedPages table serves as the single source of truth for page order.
type PagesRegistry struct {
	UI              *UI
	PageMenuMap     map[string]string
	SearchablePages []string
	// transientReturn maps a transient page to the page that was in front when it opened.
	// A transient page is not in OpenedPages, so there is nothing else to return to.
	transientReturn map[string]string
}

// UI contains the main UI components including pages and opened pages table.
type UI struct {
	Pages         *tview.Pages
	OpenedPages   *tview.Table      // authoritative data registry, never displayed directly
	FilteredPages *tview.Table      // display-only table shown in the modal
	SearchInput   *tview.InputField // search input inside the opened-pages modal
	Main          tview.Primitive
}

// Expiration is the default cache expiration time.
const Expiration = time.Minute * 5

// NewPagesRegistry creates a new pages registry.
func NewPagesRegistry(colors *config.ColorConfig) *PagesRegistry {
	// Data registry — source of truth for page order; never shown directly in the modal.
	openedPages := tview.NewTable()
	openedPages.SetSelectable(true, false)

	// Display table shown inside the modal; rebuilt from openedPages on each open.
	filteredPages := tview.NewTable()
	filteredPages.SetSelectable(true, false).SetBorderPadding(0, 0, 1, 0)

	searchInput := tview.NewInputField()
	searchInput.SetLabel(" / ")
	searchInput.SetLabelColor(tcell.GetColor(colors.Karat.Search.FgColor))
	searchInput.SetFieldTextColor(tcell.GetColor(colors.Karat.Search.FgColor))
	searchInput.SetFieldBackgroundColor(tcell.GetColor(colors.Karat.Background))
	searchInput.SetBackgroundColor(tcell.GetColor(colors.Karat.Background))

	container := tview.NewFlex().SetDirection(tview.FlexRow)
	container.SetBorder(true).
		SetTitle(" Opened pages ")
	container.AddItem(filteredPages, 0, 1, true)
	container.AddItem(searchInput, 1, 0, false)

	pages := tview.NewPages()

	registry := &PagesRegistry{
		UI: &UI{
			Pages:         pages,
			OpenedPages:   openedPages,
			FilteredPages: filteredPages,
			SearchInput:   searchInput,
			Main:          util.NewModal(container),
		},
		PageMenuMap:     make(map[string]string),
		SearchablePages: []string{},
		transientReturn: make(map[string]string),
	}

	searchInput.SetChangedFunc(func(text string) {
		registry.RebuildFilteredPages(text)
	})

	registry.SetupPageMenus()

	return registry
}

// RebuildFilteredPages repopulates FilteredPages from OpenedPages applying an optional fuzzy filter.
func (pr *PagesRegistry) RebuildFilteredPages(filter string) {
	dataTable := pr.UI.OpenedPages
	displayTable := pr.UI.FilteredPages

	rowCount := dataTable.GetRowCount()
	names := make([]string, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		if cell := dataTable.GetCell(i, 1); cell != nil {
			names = append(names, cell.Text)
		}
	}

	displayTable.Clear()
	if filter == "" {
		for i, name := range names {
			displayTable.SetCell(i, 0, tview.NewTableCell(strconv.Itoa(i)))
			displayTable.SetCell(i, 1, tview.NewTableCell(name))
		}
	} else {
		matches := fuzzy.Find(filter, names)
		for i, match := range matches {
			displayTable.SetCell(i, 0, tview.NewTableCell(strconv.Itoa(i)))
			displayTable.SetCell(i, 1, tview.NewTableCell(match.Str))
		}
	}
	// Only ever put the cursor back in range, never move it otherwise: SetSelectionChangedFunc
	// switches the page underneath, so a gratuitous Select(0, 0) would override what the user
	// picked. A filter, though, can leave the index past the last match, and Esc/Enter read the
	// selection to decide where to go — out of range means they do nothing at all.
	if rows := displayTable.GetRowCount(); rows > 0 {
		if row, _ := displayTable.GetSelection(); row < 0 || row >= rows {
			displayTable.Select(0, 0)
		}
	}
}

func (pr *PagesRegistry) SetupPageMenus() {
	pr.PageMenuMap[Clusters] = ClustersPageMenu
	pr.PageMenuMap[SchemaRegistries] = SchemaRegistriesPageMenu
	pr.PageMenuMap[Resources] = ResourcesPageMenu
	pr.PageMenuMap[OpenedPages] = OpenedPagesMenu
	pr.PageMenuMap[CopyConsumerGroup] = CopyConsumerGroupPageMenu
	pr.PageMenuMap[ConnectorActions] = ConnectorActionsPageMenu
	pr.PageMenuMap[TaskActions] = TaskActionsPageMenu
	pr.PageMenuMap[ConnectorOffsets] = ConnectorOffsetsPageMenu
	pr.PageMenuMap[CopyConnectorOffsets] = CopyConnectorOffsetsPageMenu
	pr.PageMenuMap[CliTemplates] = CliTemplatesPageMenu
	pr.PageMenuMap[FindBy] = FindByPageMenu
	pr.PageMenuMap[FindSchemaByID] = FindSchemaByIDPageMenu
	pr.PageMenuMap[ConsumeOutput] = ConsumeOutputPageMenu
	pr.PageMenuMap[ConsumeParams] = ConsumeParamsPageMenu
	pr.PageMenuMap[ConsumeHelp] = ConsumeHelpPageMenu
	pr.PageMenuMap[ConsumeHistory] = ConsumeHistoryPageMenu
	pr.PageMenuMap[ClusterConfig] = ClusterConfigPageMenu
}

func (app *App) CheckInCache(name string, onAbsent func()) {
	_, found := app.Cache.Get(name)
	if found {
		app.SwitchToPage(name)
	} else {
		onAbsent()
	}
}

func (app *App) AddToPagesRegistry(
	name string,
	component tview.Primitive,
	menu string,
	searchable bool,
) {
	registry := app.Layout.PagesRegistry
	registry.PageMenuMap[name] = menu

	currentPage, _ := registry.UI.Pages.GetFrontPage()
	names := registry.openedPageNames()

	// A re-opened page must be removed from Pages before it can be re-added below.
	if indexOfString(names, name) >= 0 {
		registry.UI.Pages.RemovePage(name)
	}

	order, selectedRow := planOpenedPages(names, currentPage, name)
	registry.rebuildOpenedPages(order, selectedRow)

	if searchable && !registry.isPageSearchable(name) {
		registry.SearchablePages = append(registry.SearchablePages, name)
	}

	app.Cache.Set(name, name, Expiration)

	type titledPrimitive interface {
		GetTitle() string
		SetTitle(string) *tview.Box
	}
	if t, ok := component.(titledPrimitive); ok {
		ts := time.Now().Format("2006-01-02T15:04:05")
		t.SetTitle(strings.TrimRight(t.GetTitle(), " ") + " [" + ts + "] ")
	}

	menuToSet := menu
	if app.autoUpdateMode && name == app.autoUpdatePageKey {
		menuToSet = AutoUpdateModePageMenu
	}
	app.Layout.Menu.SetMenu(menuToSet)
	registry.UI.Pages.AddAndSwitchToPage(name, component, true)
}

// AddTransientPage shows a page that stands outside the opened-pages registry: a
// confirmation the user either applies or abandons, and which is gone either way. It is
// never listed in the opened-pages modal, <h>/<l> never iterate into it, and it is not
// cached — reopening it means going through the action that produces it again.
//
// RemoveTransientPage is the only way back: it returns to the page that was in front here.
func (app *App) AddTransientPage(name string, component tview.Primitive, menu string) {
	registry := app.Layout.PagesRegistry
	registry.PageMenuMap[name] = menu

	// Recorded even when there is nothing to return to, so that the entry itself is what marks
	// the page as transient.
	previous, _ := registry.UI.Pages.GetFrontPage()
	if previous == name {
		previous = ""
	}
	registry.transientReturn[name] = previous

	app.Layout.Menu.SetMenu(menu)
	registry.UI.Pages.AddAndSwitchToPage(name, component, true)
}

// IsTransientPage reports whether name is a confirmation page put up by AddTransientPage.
func (pr *PagesRegistry) IsTransientPage(name string) bool {
	_, ok := pr.transientReturn[name]
	return ok
}

// RemoveTransientPage removes a page added by AddTransientPage and switches back to the
// page that was in front when it opened.
func (app *App) RemoveTransientPage(name string) {
	registry := app.Layout.PagesRegistry

	registry.UI.Pages.RemovePage(name)
	delete(registry.PageMenuMap, name)

	previous, ok := registry.transientReturn[name]
	delete(registry.transientReturn, name)
	if ok && previous != "" {
		app.SwitchToPage(previous)
	}
}

// findPageInTable returns the row index of a page in the opened pages table, or -1 if not found.
func (pr *PagesRegistry) findPageInTable(name string) int {
	for i := 0; i < pr.UI.OpenedPages.GetRowCount(); i++ {
		cell := pr.UI.OpenedPages.GetCell(i, 1)
		if cell != nil && cell.Text == name {
			return i
		}
	}
	return -1
}

// openedPageNames returns the page names in their current registry order.
func (pr *PagesRegistry) openedPageNames() []string {
	n := pr.UI.OpenedPages.GetRowCount()
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if cell := pr.UI.OpenedPages.GetCell(i, 1); cell != nil {
			names = append(names, cell.Text)
		}
	}
	return names
}

// rebuildOpenedPages repopulates the OpenedPages table from the given ordered page
// names, renumbering the display-index column, and selects selectedRow.
func (pr *PagesRegistry) rebuildOpenedPages(order []string, selectedRow int) {
	pr.UI.OpenedPages.Clear()
	for i, name := range order {
		pr.UI.OpenedPages.SetCell(i, 0, tview.NewTableCell(strconv.Itoa(i)))
		pr.UI.OpenedPages.SetCell(i, 1, tview.NewTableCell(name))
	}
	if selectedRow >= 0 && selectedRow < len(order) {
		pr.UI.OpenedPages.Select(selectedRow, 0)
	}
}

// planOpenedPages computes the opened-pages order after a page named `name` is
// opened while `current` is the front page. It returns the reordered page names
// and the index at which `name` is placed.
//
// A newly opened page is inserted immediately after the current page, so pressing
// 'h' (Backward) returns to the page that opened it. Re-opening an existing page
// removes it first, then places it after the current page. If `current` is not among
// `names` (e.g. a modal is in front), the page is appended at the end.
func planOpenedPages(names []string, current, name string) ([]string, int) {
	existingRow := indexOfString(names, name)
	currentRow := indexOfString(names, current)
	if currentRow < 0 {
		currentRow = len(names) - 1
	}

	remaining := names
	if existingRow >= 0 {
		remaining = make([]string, 0, len(names)-1)
		remaining = append(remaining, names[:existingRow]...)
		remaining = append(remaining, names[existingRow+1:]...)
		if existingRow < currentRow {
			currentRow--
		}
	}

	insertRow := currentRow + 1
	if insertRow > len(remaining) {
		insertRow = len(remaining)
	}

	order := make([]string, 0, len(remaining)+1)
	order = append(order, remaining[:insertRow]...)
	order = append(order, name)
	order = append(order, remaining[insertRow:]...)
	return order, insertRow
}

// indexOfString returns the index of target in s, or -1 if absent.
func indexOfString(s []string, target string) int {
	for i, v := range s {
		if v == target {
			return i
		}
	}
	return -1
}

// IsPersistentPage reports whether a page name is a persistent (non-modal) page
// tracked in the opened-pages table.
func (pr *PagesRegistry) IsPersistentPage(name string) bool {
	return pr.findPageInTable(name) >= 0
}

// isPageSearchable checks if a page is in the searchable pages list.
func (pr *PagesRegistry) isPageSearchable(name string) bool {
	for _, p := range pr.SearchablePages {
		if p == name {
			return true
		}
	}
	return false
}

func (app *App) Forward() {
	registry := app.Layout.PagesRegistry
	currentPage, _ := registry.UI.Pages.GetFrontPage()
	currentRow := registry.findPageInTable(currentPage)

	if currentRow >= 0 && currentRow < registry.UI.OpenedPages.GetRowCount()-1 {
		nextCell := registry.UI.OpenedPages.GetCell(currentRow+1, 1)
		if nextCell != nil {
			nextPage := nextCell.Text
			if menu, ok := registry.PageMenuMap[nextPage]; ok {
				app.Layout.Menu.SetMenu(menu)
				registry.UI.Pages.SwitchToPage(nextPage)
			}
		}
	}
}

func (app *App) Backward() {
	registry := app.Layout.PagesRegistry
	currentPage, _ := registry.UI.Pages.GetFrontPage()
	currentRow := registry.findPageInTable(currentPage)

	if currentRow > 0 {
		prevCell := registry.UI.OpenedPages.GetCell(currentRow-1, 1)
		if prevCell != nil {
			prevPage := prevCell.Text
			if menu, ok := registry.PageMenuMap[prevPage]; ok {
				app.Layout.Menu.SetMenu(menu)
				registry.UI.Pages.SwitchToPage(prevPage)
			}
		}
	}
}

func (app *App) SwitchToPage(name string) {
	if menu, ok := app.Layout.PagesRegistry.PageMenuMap[name]; ok {
		app.Layout.Menu.SetMenu(menu)
		app.Layout.PagesRegistry.UI.Pages.SwitchToPage(name)
	}
}

func (app *App) ShowModalPage(pageName string) {
	registry := app.Layout.PagesRegistry
	menu, ok := registry.PageMenuMap[pageName]
	if !ok {
		return
	}

	// Read before the modal goes on top: from there on the front page is the modal itself.
	currentPage, _ := registry.UI.Pages.GetFrontPage()

	app.Layout.Menu.SetMenu(menu)
	registry.UI.Pages.ShowPage(pageName)
	registry.UI.Pages.SendToFront(pageName)

	if pageName != OpenedPages {
		return
	}

	// The opened-pages list starts on the page the user came from, so closing it with Esc
	// leaves them where they were.
	registry.RebuildFilteredPages("")
	for i := range registry.UI.FilteredPages.GetRowCount() {
		cell := registry.UI.FilteredPages.GetCell(i, 1)
		if cell != nil && cell.Text == currentPage {
			registry.UI.FilteredPages.Select(i, 0)
			break
		}
	}
}

func (app *App) HideModalPage(pageName string) {
	registry := app.Layout.PagesRegistry
	registry.UI.Pages.HidePage(pageName)

	currentPage, _ := registry.UI.Pages.GetFrontPage()
	if menu, ok := registry.PageMenuMap[currentPage]; ok {
		app.Layout.Menu.SetMenu(menu)
	}
}

func (app *App) IsCurrentPageSearchable() bool {
	currentPage, _ := app.Layout.PagesRegistry.UI.Pages.GetFrontPage()

	for _, searchablePage := range app.Layout.PagesRegistry.SearchablePages {
		if currentPage == searchablePage {
			return true
		}
	}
	return false
}

func (app *App) RemoveFromPagesRegistry(name string) {
	tableRow := app.removePage(name)

	registry := app.Layout.PagesRegistry
	if registry.UI.OpenedPages.GetRowCount() > 0 {
		targetRow := tableRow
		if targetRow >= registry.UI.OpenedPages.GetRowCount() {
			targetRow = registry.UI.OpenedPages.GetRowCount() - 1
		}
		cell := registry.UI.OpenedPages.GetCell(targetRow, 1)
		if cell != nil {
			app.SwitchToPage(cell.Text)
		}
	}
}

// removePage takes a page out of the registry, the cache and the searchable list, and returns
// the row it held. It does not move the user anywhere: that is the caller's decision, and a
// cleanup of pages nobody is looking at must not.
func (app *App) removePage(name string) int {
	registry := app.Layout.PagesRegistry

	registry.UI.Pages.RemovePage(name)
	delete(registry.PageMenuMap, name)

	tableRow := registry.findPageInTable(name)
	if tableRow >= 0 {
		registry.UI.OpenedPages.RemoveRow(tableRow)
		for i := tableRow; i < registry.UI.OpenedPages.GetRowCount(); i++ {
			registry.UI.OpenedPages.SetCell(i, 0, tview.NewTableCell(strconv.Itoa(i)))
		}
	}

	for i, p := range registry.SearchablePages {
		if p == name {
			registry.SearchablePages = append(
				registry.SearchablePages[:i],
				registry.SearchablePages[i+1:]...)
			break
		}
	}

	app.Cache.Delete(name)

	return tableRow
}

// RemovePagesFor drops every opened page that names the given resource on the given cluster:
// what a deleted topic, consumer group or connector leaves behind. Such a page is not
// reachable from the cluster any more, but it stays in the opened-pages list and still opens,
// showing what the resource looked like before it was deleted.
//
// Page keys are colon-joined, so a whole segment has to match: deleting "orders" must not take
// "orders-retry" with it.
func (app *App) RemovePagesFor(cluster, resource string) {
	prefix := cluster + ":"
	front, _ := app.Layout.PagesRegistry.UI.Pages.GetFrontPage()
	frontRemoved := false

	for _, name := range app.Layout.PagesRegistry.openedPageNames() {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		for _, segment := range strings.Split(name, ":") {
			if segment != resource {
				continue
			}
			app.removePage(name)
			if name == front {
				frontRemoved = true
			}
			break
		}
	}

	// Only when the page the user is on is one of the removed ones is there anywhere to go.
	if frontRemoved {
		if names := app.Layout.PagesRegistry.openedPageNames(); len(names) > 0 {
			app.SwitchToPage(names[len(names)-1])
		}
	}
}
