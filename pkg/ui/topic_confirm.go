// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// topicChange is what an edited topic document would do to an existing topic: the config
// entries whose value differs from the topic's current overrides, the ones dropped from
// the document (which reset to the cluster default), and the new partition count.
type topicChange struct {
	Name              string
	CurrentPartitions int
	NewPartitions     int

	// Set holds the overrides to write — only those that are new or whose value changed,
	// so an untouched document produces an empty change.
	Set map[string]string

	// Removed holds the overrides to reset to the cluster default, sorted.
	Removed []string
}

// topicChanges diffs an edited document's configs against the topic's current overrides.
func topicChanges(
	name string,
	currentPartitions, newPartitions int,
	current, edited map[string]string,
) topicChange {
	set := make(map[string]string)
	for key, value := range edited {
		if old, ok := current[key]; !ok || old != value {
			set[key] = value
		}
	}

	return topicChange{
		Name:              name,
		CurrentPartitions: currentPartitions,
		NewPartitions:     newPartitions,
		Set:               set,
		Removed:           removedConfigKeys(current, edited),
	}
}

// empty reports whether the change would do nothing at all.
func (c topicChange) empty() bool {
	return len(c.Set) == 0 && len(c.Removed) == 0 && c.NewPartitions <= c.CurrentPartitions
}

// renderTopicChanges describes the change for the confirmation page.
func renderTopicChanges(c topicChange) string {
	var b strings.Builder

	if c.NewPartitions > c.CurrentPartitions {
		fmt.Fprintf(&b, "partitions  %d -> %d\n", c.CurrentPartitions, c.NewPartitions)
	}

	if len(c.Set) > 0 || len(c.Removed) > 0 {
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString("configs\n")
		for _, key := range slices.Sorted(maps.Keys(c.Set)) {
			fmt.Fprintf(&b, "  %s = %s\n", key, c.Set[key])
		}
		for _, key := range c.Removed {
			fmt.Fprintf(&b, "  %s (reset to cluster default)\n", key)
		}
	}

	return b.String()
}

// renderNewTopic describes a topic about to be created for the confirmation page.
func renderNewTopic(params TopicParams) string {
	var b strings.Builder

	fmt.Fprintf(&b, "name                %s\n", params.TopicName)
	fmt.Fprintf(&b, "replication factor  %d\n", params.ReplicationFactor)
	fmt.Fprintf(&b, "partitions          %d\n", params.Partitions)

	if len(params.Config) > 0 {
		b.WriteString("\nconfigs\n")
		for _, key := range slices.Sorted(maps.Keys(params.Config)) {
			fmt.Fprintf(&b, "  %s = %s\n", key, params.Config[key])
		}
	}

	return b.String()
}

// CreateTopicConfirm shows what the edited document would create and creates it on
// Ctrl+Enter. Must be called from the UI goroutine.
func (app *App) CreateTopicConfirm(params TopicParams) {
	app.topicConfirmPage(
		fmt.Sprintf(" Confirm Create Topic: %s ", params.TopicName),
		renderNewTopic(params),
		func() {
			app.CreateTopicResultHandler(
				params.TopicName,
				params.Partitions,
				params.ReplicationFactor,
				params.Config,
			)
		},
	)
}

// UpdateTopicConfirm shows what the edited document would change and applies it on
// Ctrl+Enter. A document that changes nothing opens no page at all. Must be called from
// the UI goroutine.
func (app *App) UpdateTopicConfirm(change topicChange) {
	if change.empty() {
		SendStatusNote("no changes detected")
		return
	}

	app.topicConfirmPage(
		fmt.Sprintf(" Confirm Topic Update: %s ", change.Name),
		renderTopicChanges(change),
		func() {
			// No refresh here: the update is asynchronous, and the handler publishes a
			// forced one once the cluster has actually been changed.
			app.UpdateTopicResultHandler(
				change.Name,
				change.CurrentPartitions,
				change.NewPartitions,
				change.Set,
				change.Removed,
			)
		},
	)
}

// topicConfirmPage is the confirmation page both topic document flows end on: apply is
// deliberate, and nothing has touched the cluster until it happens. The read-only check is
// repeated here because the cluster can be switched while the editor holds the terminal.
func (app *App) topicConfirmPage(title, body string, apply func()) {
	messageText := tview.NewTextView().
		SetText(body).
		SetTextAlign(tview.AlignLeft).
		SetDynamicColors(false)

	messageText.SetBorder(true).
		SetTitle(title).
		SetBorderPadding(0, 0, 1, 1)

	messageText.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			// Re-checked at apply time: the mode can have been toggled while the page stood
			// open. The page itself is the confirmation, so nothing more is asked.
			if !app.Allowed() {
				return nil
			}
			apply()
			app.RemoveTransientPage(TopicConfirm)
			return nil
		}

		if event.Key() == tcell.KeyEsc {
			app.RemoveTransientPage(TopicConfirm)
			return nil
		}

		return event
	})

	ClearStatus()
	app.AddTransientPage(TopicConfirm, messageText, TopicConfirmPageMenu)
}
