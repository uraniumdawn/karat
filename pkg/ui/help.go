// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/util"
)

// globalKeys lists the bindings that work on every page, in the order the help modal shows
// them. They are kept out of the per-page menus on purpose: the bottom bar has room for what
// changes from page to page, not for what never does.
var globalKeys = []string{
	"res",
	"opened",
	"search",
	"clear_filter",
	"b/f",
	"hlscroll",
	"help",
}

// ShowHelp opens the keybinding reference: the bindings that work everywhere, followed by
// the ones the page in front adds. Both sections are read from the same tables the bottom
// menu bar renders, so a binding cannot be documented here and missing there.
func (app *App) ShowHelp() {
	registry := app.Layout.PagesRegistry
	front, _ := registry.UI.Pages.GetFrontPage()

	view := tview.NewTextView().
		SetDynamicColors(true).
		SetText(app.helpText(front)).
		SetScrollable(true)
	view.SetBorder(false).SetBorderPadding(0, 0, 1, 1)

	closeHelp := func() {
		registry.UI.Pages.RemovePage(Help)
		delete(registry.PageMenuMap, Help)
		currentPage, _ := registry.UI.Pages.GetFrontPage()
		if menu, ok := registry.PageMenuMap[currentPage]; ok {
			app.Layout.Menu.SetMenu(menu)
		}
	}

	view.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || IsKey(event, '?') {
			closeHelp()
			return nil
		}
		if IsKey(event, ':') {
			// The resource menu would land behind the help page instead of replacing it.
			return nil
		}
		return event
	})

	container := tview.NewFlex().SetDirection(tview.FlexRow).AddItem(view, 0, 1, true)
	container.SetBorder(true).SetTitle(" Keys ")

	registry.PageMenuMap[Help] = HelpPageMenu
	app.Layout.Menu.SetMenu(HelpPageMenu)
	registry.UI.Pages.AddPage(Help, util.NewModal(container), true, true)
}

// helpText renders the help body for the page named front.
func (app *App) helpText(front string) string {
	labelColor := app.Colors.Karat.Label.FgColor
	keyColor := app.Colors.Karat.Keybinding.Key

	var b strings.Builder
	fmt.Fprintf(&b, "[%s]Global[-]\n", labelColor)
	writeKeyRows(&b, globalKeys, keyColor)

	if pageKeys := app.pageHelpKeys(front); len(pageKeys) > 0 {
		fmt.Fprintf(&b, "\n[%s]%s[-]\n", labelColor, front)
		writeKeyRows(&b, pageKeys, keyColor)
	}

	return b.String()
}

// pageHelpKeys returns the menu-bar bindings of the page named front, minus the ones the
// global section already lists.
func (app *App) pageHelpKeys(front string) []string {
	menu, ok := app.Layout.PagesRegistry.PageMenuMap[front]
	if !ok {
		return nil
	}
	bindings, ok := (*app.Layout.Menu.Map)[menu]
	if !ok {
		return nil
	}

	global := make(map[string]struct{}, len(globalKeys))
	for _, id := range globalKeys {
		global[id] = struct{}{}
	}

	pageKeys := make([]string, 0, len(*bindings))
	for _, id := range *bindings {
		if _, isGlobal := global[id]; isGlobal {
			continue
		}
		pageKeys = append(pageKeys, id)
	}

	return pageKeys
}

// writeKeyRows writes one "<key>  description" line per known binding id, with the key
// column padded to the widest key in the group.
func writeKeyRows(b *strings.Builder, ids []string, keyColor string) {
	width := 0
	for _, id := range ids {
		if pair, ok := keys[id]; ok {
			if n := len([]rune(pair.Key)); n > width {
				width = n
			}
		}
	}

	for _, id := range ids {
		pair, ok := keys[id]
		if !ok {
			continue
		}
		padding := strings.Repeat(" ", width-len([]rune(pair.Key)))
		fmt.Fprintf(b, "  [%s]%s[-]%s  %s\n", keyColor, pair.Key, padding, pair.Value)
	}
}
