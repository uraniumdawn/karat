// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"testing"
)

// Extra Actions is for what a shortcut should not carry: read-only navigation, an operation
// compound enough to want reading before it runs, and one that destroys data. A simple create,
// delete or edit belongs on <n>, <C-d> or <e> and must not also be offered here — one act, one
// route.
func TestExtraActionsHoldsOnlyWhatHasNoShortcut(t *testing.T) {
	want := map[string][]string{
		TopicDescriptionExtraActions: {"Producers", "Consumer groups"},
		ConsumerGroupsExtraActions:   {"Find by topic", "Clone consumer group"},
		TopicsExtraActions: {
			"Consume",
			"CLI commands",
			"Consumer groups",
			"Clone topic",
			"Recreate topic",
		},
		SubjectsExtraActions: {"Find schema by ID", "Clone subject"},
	}

	if len(extraActionsRegistry) != len(want) {
		t.Errorf("registry has %d kinds, want %d", len(extraActionsRegistry), len(want))
	}

	for kind, names := range want {
		actions, ok := extraActionsRegistry[kind]
		if !ok {
			t.Errorf("kind %q is missing from the registry", kind)
			continue
		}

		if len(actions) != len(names) {
			t.Errorf("kind %q has %d actions, want %d", kind, len(actions), len(names))
			continue
		}
		for i, name := range names {
			if actions[i].Name != name {
				t.Errorf("kind %q action %d = %q, want %q", kind, i, actions[i].Name, name)
			}
		}
	}

	for kind := range extraActionsRegistry {
		if _, ok := want[kind]; !ok {
			t.Errorf("kind %q is in the registry but not expected", kind)
		}
	}
}

// A kind with no actions leaves <.> pressable and silent, so the page must drop the key rather
// than keep an entry that opens nothing.
func TestNoEmptyExtraActionKinds(t *testing.T) {
	for kind, actions := range extraActionsRegistry {
		if len(actions) == 0 {
			t.Errorf("kind %q has no actions", kind)
		}
	}
}
