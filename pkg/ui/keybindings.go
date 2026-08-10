// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// IsKey reports whether the event is a printable rune key matching r.
func IsKey(event *tcell.EventKey, r rune) bool {
	return event.Key() == tcell.KeyRune && event.Rune() == r
}

// IsCtrlEnter reports whether the event is Enter with the Ctrl modifier held.
func IsCtrlEnter(event *tcell.EventKey) bool {
	return event.Key() == tcell.KeyEnter && event.Modifiers()&tcell.ModCtrl != 0
}

func (app *App) OpenPagesKeyHandler(filteredTable *tview.Table) {
	registry := app.Layout.PagesRegistry
	searchInput := registry.UI.SearchInput

	searchInput.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEsc:
			searchInput.SetText("")
			app.SetFocus(filteredTable)
			return nil
		case tcell.KeyEnter:
			app.SetFocus(filteredTable)
			return nil
		}
		return event
	})

	filteredTable.SetSelectionChangedFunc(func(row, column int) {
		// Skip while the user is typing — Select(0,0) from RebuildFilteredPages would
		// otherwise trigger ShowPage/SendToFront and steal focus from the search input.
		if app.GetFocus() == searchInput {
			return
		}
		if row >= 0 && row < filteredTable.GetRowCount() {
			cell := filteredTable.GetCell(row, 1)
			if cell != nil {
				pageName := cell.Text
				if _, ok := registry.PageMenuMap[pageName]; ok {
					registry.UI.Pages.SwitchToPage(pageName)
					registry.UI.Pages.ShowPage(OpenedPages)
					registry.UI.Pages.SendToFront(OpenedPages)
					// The opened-pages modal owns the bottom bar while it is on top;
					// keep its menu regardless of which page is highlighted underneath.
					app.Layout.Menu.SetMenu(OpenedPagesMenu)
				}
			}
		}
	})

	filteredTable.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Key() == tcell.KeyEnter {
			row, _ := filteredTable.GetSelection()
			if row >= 0 && row < filteredTable.GetRowCount() {
				cell := filteredTable.GetCell(row, 1)
				if cell != nil {
					app.SwitchToPage(cell.Text)
				}
			}
			searchInput.SetText("")
			app.HideModalPage(OpenedPages)
			return nil
		}

		if IsKey(event, 'x') {
			row, _ := filteredTable.GetSelection()
			if row >= 0 && row < filteredTable.GetRowCount() {
				cell := filteredTable.GetCell(row, 1)
				if cell != nil {
					pageName := cell.Text

					if pageName == Clusters {
						SendStatusWithDefaultTTL("Clusters page cannot be deleted")
						return nil
					}

					app.RemoveFromPagesRegistry(pageName)
					registry.RebuildFilteredPages(searchInput.GetText())

					targetRow := row
					if row > 0 {
						targetRow = row - 1
					}
					if targetRow >= filteredTable.GetRowCount() {
						targetRow = filteredTable.GetRowCount() - 1
					}
					if filteredTable.GetRowCount() > 0 {
						filteredTable.Select(targetRow, 0)
					}
				}
			}
			return nil
		}

		if IsKey(event, '/') {
			app.SetFocus(searchInput)
			return nil
		}

		return event
	})
}

// confirmationInFront reports whether a confirmation page is the page on screen. It is false
// before there is a layout to ask, which is the state the application starts in.
func (app *App) confirmationInFront() bool {
	if app.Layout == nil || app.Layout.PagesRegistry == nil {
		return false
	}

	front, _ := app.Layout.PagesRegistry.UI.Pages.GetFrontPage()

	return app.Layout.PagesRegistry.IsTransientPage(front)
}

func (app *App) MainOperationKeyHandler() {
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// <Ctrl-C> is tview's only built-in binding, and it fires only when this capture hands
		// the event back untouched. Nothing swallowed below is worth being unable to quit for:
		// a standing question and auto-update mode both consume every key otherwise.
		if event.Key() == tcell.KeyCtrlC {
			return event
		}

		// A standing question is answered before anything else looks at the key: while it
		// stands, no page, modal or search field reacts to any of them.
		if app.answer(event) {
			return nil
		}

		if app.autoUpdateMode {
			switch event.Key() {
			case tcell.KeyTab:
				app.CycleIntervalForCurrentPage()
			case tcell.KeyEsc:
				app.ExitAutoUpdateMode()
			}
			return nil
		}

		// A confirmation page is answered or abandoned on its own terms. Switching away from it
		// leaves it hidden with its pending action stranded, and nothing brings it back, so the
		// keys that would navigate off it do nothing while it stands. <h>/<l> are covered by
		// IsPersistentPage below, for the same reason.
		if event.Key() == tcell.KeyCtrlP || IsKey(event, ':') {
			if app.confirmationInFront() {
				SendStatusWithDefaultTTL("finish the open confirmation first")
				return nil
			}
		}

		if IsKey(event, ':') {
			if !app.IsSearchInFocus() && !app.IsInputFieldInFocus() {
				currentPage, _ := app.Layout.PagesRegistry.UI.Pages.GetFrontPage()
				if currentPage != Resources {
					app.ShowModalPage(Resources)
					return nil
				}
				// modal already open: fall through so table's SetInputCapture receives ':' and focuses
				// search
			}
		}

		// A modal is a page of its own, so the front page being a list page is what says the
		// keypress belongs to the list rather than to something on top of it.
		if event.Key() == tcell.KeyEsc && !app.IsSearchInFocus() && !app.IsInputFieldInFocus() {
			if app.clearFilter() {
				return nil
			}
		}

		if IsKey(event, '/') {
			currentPage, _ := app.Layout.PagesRegistry.UI.Pages.GetFrontPage()
			for _, searchablePage := range app.Layout.PagesRegistry.SearchablePages {
				if currentPage == searchablePage {
					if _, ok := app.Layout.Search[currentPage]; ok {
						app.Layout.ShowInlineSearch(currentPage)
						app.SetFocus(app.Layout.Search[currentPage])
						SendStatusWithDefaultTTL("")
						return nil
					}
				}
			}
		}

		if event.Key() == tcell.KeyCtrlP && !app.IsSearchInFocus() {
			app.ShowModalPage(OpenedPages)
		}

		if event.Key() == tcell.KeyRune && event.Rune() == 'h' && !app.IsSearchInFocus() {
			currentPage, _ := app.Layout.PagesRegistry.UI.Pages.GetFrontPage()
			if app.Layout.PagesRegistry.IsPersistentPage(currentPage) {
				app.Backward()
				return nil
			}
		}

		if event.Key() == tcell.KeyRune && event.Rune() == 'l' && !app.IsSearchInFocus() {
			currentPage, _ := app.Layout.PagesRegistry.UI.Pages.GetFrontPage()
			if app.Layout.PagesRegistry.IsPersistentPage(currentPage) {
				app.Forward()
				return nil
			}
		}

		return event
	})
}

func (app *App) SearchKeyHandler(input *tview.InputField) {
	input.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEnter {
			app.Layout.HideInlineSearch()
			app.SetFocus(app.Layout.PagesRegistry.UI.Pages)
		}

		if event.Key() == tcell.KeyEsc {
			// Esc abandons the search: the text goes with it, which restores the full list
			// through the change hook. <Enter> is what keeps a filter and leaves the field.
			input.SetText("")
			app.Layout.HideInlineSearch()
			app.SetFocus(app.Layout.PagesRegistry.UI.Pages)
			return nil
		}

		return event
	})
}

// clearFilter drops the filter on the page in front and reports whether there was one. The
// search bar is hidden by then and only the page title still says "/text", so Esc on the list
// is the way back to the whole list.
func (app *App) clearFilter() bool {
	page, _ := app.Layout.PagesRegistry.UI.Pages.GetFrontPage()
	if app.CurrentFilters[page] == "" {
		return false
	}

	search, ok := app.Layout.Search[page]
	if !ok {
		return false
	}

	search.SetText("")
	app.Layout.HideInlineSearch()
	app.SetFocus(app.Layout.PagesRegistry.UI.Pages)

	return true
}
