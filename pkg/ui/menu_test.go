// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"strings"
	"testing"

	"github.com/uraniumdawn/karat/pkg/config"
)

// newTestMenu builds a Menu with every optional column enabled, so that the page menus
// under test are the full ones.
func newTestMenu(t *testing.T) *Menu {
	t.Helper()

	colors := &config.ColorConfig{}
	cfg := &config.Config{}

	return NewMenu(colors, cfg)
}

// TestMenuBindingsAreDefined guards the bottom bar against silently dropping a key: a page
// menu that names a binding no entry defines renders as a gap nobody notices.
func TestMenuBindingsAreDefined(t *testing.T) {
	menu := newTestMenu(t)

	for page, bindings := range *menu.Map {
		for _, id := range *bindings {
			if _, ok := keys[id]; !ok {
				t.Errorf("%s references undefined binding %q", page, id)
			}
		}
	}

	for _, id := range globalKeys {
		if _, ok := keys[id]; !ok {
			t.Errorf("globalKeys references undefined binding %q", id)
		}
	}
}

// TestNoOrphanBindings is the other half of the same guard: an entry no page and no global
// list claims is a binding that was removed from the handlers but left in the table, and it
// misleads whoever reads it next.
func TestNoOrphanBindings(t *testing.T) {
	menu := newTestMenu(t)

	used := map[string]struct{}{}
	for _, bindings := range *menu.Map {
		for _, id := range *bindings {
			used[id] = struct{}{}
		}
	}
	for _, id := range globalKeys {
		used[id] = struct{}{}
	}

	for id := range keys {
		if _, ok := used[id]; !ok {
			t.Errorf("binding %q is defined but never shown", id)
		}
	}
}

// TestNoKeyMeansTwoThingsOnOnePage catches a menu that advertises the same key twice with
// different meanings: on Clusters, <Enter> selects the cluster and only <d> describes it, so
// the shared "<Enter,d> Details" entry does not belong there.
func TestNoKeyMeansTwoThingsOnOnePage(t *testing.T) {
	menu := newTestMenu(t)

	for page, bindings := range *menu.Map {
		seen := map[string]string{}
		for _, id := range *bindings {
			pair, ok := keys[id]
			if !ok {
				continue
			}
			for _, key := range strings.Split(strings.Trim(pair.Key, "<>"), ",") {
				key = strings.TrimSpace(key)
				if other, clash := seen[key]; clash && other != pair.Value {
					t.Errorf("%s: <%s> means both %q and %q", page, key, other, pair.Value)
				}
				seen[key] = pair.Value
			}
		}
	}
}

// TestHelpTextSections verifies the help body: the global bindings always, the current
// page's own bindings under its own heading, and no binding listed twice.
func TestHelpTextSections(t *testing.T) {
	app := &App{
		Layout: &Layout{
			Menu:          newTestMenu(t),
			PagesRegistry: &PagesRegistry{PageMenuMap: map[string]string{Topics: TopicsPageMenu}},
		},
		Colors: &config.ColorConfig{},
	}

	text := app.helpText(Topics)

	if !strings.Contains(text, "Global") {
		t.Error("help text has no Global section")
	}
	if !strings.Contains(text, Topics) {
		t.Errorf("help text has no %s section", Topics)
	}
	// <.> is a Topics binding, </> is a global one: each belongs to exactly one section.
	if got := strings.Count(text, keys["extra_actions"].Key); got != 1 {
		t.Errorf("extra actions key appears %d times, want 1", got)
	}
	if got := strings.Count(text, keys["search"].Key); got != 1 {
		t.Errorf("search key appears %d times, want 1", got)
	}
}

// TestHelpTextUnknownPage covers a page with no menu of its own — a transient confirmation,
// say: the global section still has to be there.
func TestHelpTextUnknownPage(t *testing.T) {
	app := &App{
		Layout: &Layout{
			Menu:          newTestMenu(t),
			PagesRegistry: &PagesRegistry{PageMenuMap: map[string]string{}},
		},
		Colors: &config.ColorConfig{},
	}

	text := app.helpText("nowhere")

	if !strings.Contains(text, keys["help"].Key) {
		t.Error("global section missing for a page with no menu")
	}
	if strings.Contains(text, "nowhere") {
		t.Error("a page with no menu must not get a section heading")
	}
}
