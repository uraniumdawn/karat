// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"
	"github.com/sahilm/fuzzy"

	"github.com/uraniumdawn/karat/pkg/client"
	"github.com/uraniumdawn/karat/pkg/util"
)

const (
	// GetCgroupsEventType is the event type for fetching consumer groups.
	GetCgroupsEventType EventType = "cgroups:get"
	// GetCgroupEventType is the event type for fetching a specific consumer group.
	GetCgroupEventType EventType = "cgroup:get"
	// DeleteCgroupEventType is the event type for deleting a consumer group.
	DeleteCgroupEventType EventType = "cgroup:delete"
	// FindCgroupsByTopicEventType is the event type for finding consumer groups by topic.
	FindCgroupsByTopicEventType EventType = "cgroups:find-by-topic"
)

// CgroupsChannel is the channel for consumer group events.
var CgroupsChannel = NewEventChannel()

// RunCgroupsEventHandler processes consumer group events from the channel.
func (app *App) RunCgroupsEventHandler(ctx context.Context, in *EventChannel) {
	in.Run(ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("shutting down cgroups event handler")
				return
			case event := <-in.C:
				switch event.Type {
				case GetCgroupsEventType:
					pageName := util.BuildPageKey(app.Selected.Cluster.Name, ConsumerGroups)
					force := event.Payload.Force
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.QueueUpdateDraw(func() { app.SwitchToPage(pageName) })
					} else {
						app.ConsumerGroups(force)
					}

				case GetCgroupEventType:
					consumerGroup := event.Payload.Data.(string)
					force := event.Payload.Force
					pageName := util.BuildPageKey(
						app.Selected.Cluster.Name,
						ConsumerGroups,
						consumerGroup,
					)
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.QueueUpdateDraw(func() { app.SwitchToPage(pageName) })
					} else {
						app.ConsumerGroup(consumerGroup)
					}

				case DeleteCgroupEventType:
					groupName := event.Payload.Data.(string)
					app.QueueUpdateDraw(func() {
						app.DeleteConsumerGroup(groupName)
					})

				case FindCgroupsByTopicEventType:
					topicName := event.Payload.Data.(string)
					force := event.Payload.Force
					pageName := util.BuildPageKey(
						app.Selected.Cluster.Name,
						ConsumerGroups,
						"topic",
						topicName,
					)
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.QueueUpdateDraw(func() { app.SwitchToPage(pageName) })
					} else {
						app.ConsumerGroupsByTopic(topicName, force)
					}
				}
			}
		}
	}()
}

// ConsumerGroups fetches and displays the list of consumer groups.
func (app *App) ConsumerGroups(force bool) {
	resultCh := make(chan *client.ConsumerGroupsResult)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("getting consumer groups")
	c.ConsumerGroups(resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case groups := <-resultCh:
				app.QueueUpdateDraw(func() {
					pageKey := util.BuildPageKey(app.Selected.Cluster.Name, ConsumerGroups)
					lags := app.consumerGroupLags(pageKey, force)
					table := app.NewGroupsTable(groups, lags)
					title := util.BuildTitle(
						ConsumerGroups,
						"["+strconv.Itoa(len(groups.Valid))+"]",
					)
					app.setupGroupsTable(table, groups, lags, pageKey, title, func() {
						Publish(CgroupsChannel, GetCgroupsEventType, Payload{nil, true})
					})
					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to list consumer groups")
				SendStatusError(
					fmt.Sprintf("[red]failed to list consumer groups: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while listing consumer groups")
				SendStatusError("[red]timeout while listing consumer groups")
				return
			}
		}
	}()
}

// setupGroupsTable wires sorting, search, key bindings, and page registration for a consumer groups table.
// lags carries the Lag column state; its map is filled asynchronously below when the column is
// marked loading.
func (app *App) setupGroupsTable(
	table *tview.Table,
	groups *client.ConsumerGroupsResult,
	lags lagColumn,
	pageKey string,
	title string,
	onRefresh func(),
) {
	table.SetTitle(title)
	app.AddToPagesRegistry(pageKey, table, ConsumerGroupsPageMenu, true)

	// A refresh builds a new table: without this the cursor lands on the first row, and the
	// next key acts on a group the user did not pick.
	app.RestoreSelection(pageKey, table, afterHeaderRow)
	app.TrackSelection(pageKey, table, afterHeaderRow)

	sortCol := 0
	sortDesc := false
	labelColor := tcell.GetColor(app.Colors.Karat.Label.FgColor)

	table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyCtrlU {
			onRefresh()
		}

		// <Enter> opens the row under the cursor, the same as <d>.
		if IsKey(event, 'd') || event.Key() == tcell.KeyEnter {
			groupName, ok := selectedName(table, afterHeaderRow)
			if !ok {
				return nil
			}
			Publish(CgroupsChannel, GetCgroupEventType, Payload{groupName, false})
		}

		if IsKey(event, '.') {
			groupName, ok := selectedName(table, afterHeaderRow)
			if !ok {
				return nil
			}
			app.ShowExtraActions(ConsumerGroupsExtraActions, groupName)
		}

		if event.Key() == tcell.KeyCtrlD {
			groupName, ok := selectedName(table, afterHeaderRow)
			if !ok {
				return nil
			}

			for _, g := range groups.Valid {
				if g.GroupID == groupName {
					if g.State != kafka.ConsumerGroupStateEmpty {
						SendStatusError(fmt.Sprintf(
							"[red]cannot delete: consumer group state is %s, must be Empty",
							g.State,
						))
						return event
					}
					break
				}
			}

			Publish(CgroupsChannel, DeleteCgroupEventType, Payload{groupName, false})
		}

		if IsKey(event, '1') {
			if sortCol == 0 {
				sortDesc = !sortDesc
			} else {
				sortCol = 0
				sortDesc = false
			}
			sortGroupsTable(table, groups.Valid, lags, sortCol, sortDesc, labelColor)
			table.ScrollToBeginning()
			app.RestoreSelection(pageKey, table, afterHeaderRow)
			return event
		}

		if IsKey(event, '2') {
			if sortCol == 1 {
				sortDesc = !sortDesc
			} else {
				sortCol = 1
				sortDesc = false
			}
			sortGroupsTable(table, groups.Valid, lags, sortCol, sortDesc, labelColor)
			table.ScrollToBeginning()
			app.RestoreSelection(pageKey, table, afterHeaderRow)
			return event
		}

		// Sorting by Lag only exists while the column does.
		if IsKey(event, '3') && lags.enabled {
			if sortCol == 2 {
				sortDesc = !sortDesc
			} else {
				sortCol = 2
				sortDesc = true
			}
			sortGroupsTable(table, groups.Valid, lags, sortCol, sortDesc, labelColor)
			table.ScrollToBeginning()
			app.RestoreSelection(pageKey, table, afterHeaderRow)
			return event
		}

		return event
	})

	app.AssignSearch(func(text string) {
		filterConsumerGroupsTable(table, groups.Valid, lags, text, labelColor)
		util.SetSearchableTableTitle(table, title, text)
		table.ScrollToBeginning()
		// The rows the cursor pointed at are gone; without this the selection is left past
		// the end of the filtered table and every row action reads an empty cell.
		app.RestoreSelection(pageKey, table, afterHeaderRow)
	})

	// redraw rebuilds the rows in place, keeping the active filter or sort.
	redraw := func() {
		filterText := app.CurrentFilters[pageKey]
		if filterText != "" {
			filterConsumerGroupsTable(table, groups.Valid, lags, filterText, labelColor)
		} else {
			sortGroupsTable(table, groups.Valid, lags, sortCol, sortDesc, labelColor)
		}
		app.RestoreSelection(pageKey, table, afterHeaderRow)
	}

	// Fill the Lag column asynchronously. ConsumerGroupTotalLags issues one
	// ListConsumerGroupOffsets per group plus a single ListOffsets for all partitions.
	// Results are cached (5-min TTL); Ctrl+U forces a fresh fetch. The header carries
	// loadingMarker until this settles.
	if lags.loading {
		names := make([]string, 0, len(groups.Valid))
		for _, g := range groups.Valid {
			names = append(names, g.GroupID)
		}

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())
			defer cancel()

			fetched, err := app.GetCurrentKafkaClient().ConsumerGroupTotalLags(ctx, names)
			if err != nil {
				log.Debug().Err(err).Msg("failed to get consumer group lags")
				app.QueueUpdateDraw(func() {
					lags.loading = false
					redraw()
				})
				return
			}

			app.QueueUpdateDraw(func() {
				lags.loading = false
				maps.Copy(lags.lags, fetched)
				app.Cache.Set(pageKey+":lags", lags.lags, 5*time.Minute)
				redraw()
			})
		}()
	}
}

// consumerGroupLags returns the Lag column state for a consumer-groups page. On a cache hit
// (column enabled and not forced) the cached lags are reused as-is; otherwise the column is
// marked loading, which both marks the header and tells setupGroupsTable to fetch the lags
// asynchronously.
func (app *App) consumerGroupLags(pageKey string, force bool) lagColumn {
	lags := lagColumn{
		enabled: app.Config.ConsumerGroupLagEnabled(),
		lags:    map[string]int64{},
	}
	if !lags.enabled {
		return lags
	}
	if !force {
		if cachedVal, ok := app.Cache.Get(pageKey + ":lags"); ok {
			if m, ok := cachedVal.(map[string]int64); ok {
				lags.lags = m
				return lags
			}
		}
	}
	lags.loading = true
	return lags
}

// ConsumerGroupsByTopic fetches and displays consumer groups that have committed offsets for the given topic.
func (app *App) ConsumerGroupsByTopic(topicName string, force bool) {
	resultCh := make(chan *client.ConsumerGroupsResult)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("finding consumer groups by topic")
	c.ConsumerGroupsByTopic(topicName, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case groups := <-resultCh:
				app.QueueUpdateDraw(func() {
					pageKey := util.BuildPageKey(
						app.Selected.Cluster.Name,
						ConsumerGroups,
						"topic",
						topicName,
					)
					lags := app.consumerGroupLags(pageKey, force)
					table := app.NewGroupsTable(groups, lags)
					title := fmt.Sprintf(
						" consumer groups [%s][%d] ",
						topicName,
						len(groups.Valid),
					)
					app.setupGroupsTable(table, groups, lags, pageKey, title, func() {
						Publish(
							CgroupsChannel,
							FindCgroupsByTopicEventType,
							Payload{topicName, true},
						)
					})
					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to find consumer groups by topic")
				SendStatusError(
					fmt.Sprintf("[red]failed to find consumer groups by topic: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while finding consumer groups by topic")
				SendStatusError("[red]timeout while finding consumer groups by topic")
				return
			}
		}
	}()
}

// FindConsumerGroupsByTopicModal creates and registers the "find by topic" modal.
func (app *App) FindConsumerGroupsByTopicModal() {
	input := tview.NewInputField().
		SetFieldWidth(40).
		SetFieldStyle(
			tcell.StyleDefault.
				Foreground(tcell.GetColor(app.Colors.Karat.Foreground)).
				Background(tcell.GetColor(app.Colors.Karat.Background)),
		).
		SetPlaceholderStyle(
			tcell.StyleDefault.Background(tcell.GetColor(app.Colors.Karat.Background)),
		).
		SetPlaceholder("topic name").
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder))

	input.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			topicName := strings.TrimSpace(input.GetText())
			if topicName == "" {
				SendStatusError("[red]topic name cannot be empty")
				return nil
			}
			app.HideModalPage(FindBy)
			Publish(CgroupsChannel, FindCgroupsByTopicEventType, Payload{topicName, false})
			return nil
		}
		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(FindBy)
			return nil
		}
		return event
	})

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(input, 1, 0, true)
	flex.SetTitle(" Find Consumer Group By Topic ")
	flex.SetBorder(true)
	flex.SetBorderPadding(0, 0, 1, 0)

	modal := util.NewResourceModal(flex, 3)
	app.Layout.PagesRegistry.UI.Pages.AddPage(FindBy, modal, true, false)
}

// ConsumerGroup fetches and displays details for a specific consumer group.
func (app *App) ConsumerGroup(name string) {
	resultCh := make(chan *client.DescribeConsumerGroupResult)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	pageKey := util.BuildPageKey(app.Selected.Cluster.Name, ConsumerGroup, name)
	autoRefreshing := app.GetAutoUpdateLabel(pageKey) != ""
	if !autoRefreshing {
		SendStatusProgress("getting consumer group description")
	}
	c.DescribeConsumerGroup(name, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case description := <-resultCh:
				app.QueueUpdateDraw(func() {
					description.SetPrevLagByTopic(app.cgroupPrevLag[name])
					app.cgroupPrevLag[name] = description.GetLagByTopic()
					title := util.BuildTitle(ConsumerGroup, name)
					if label := app.GetAutoUpdateLabel(pageKey); label != "" {
						title = title + "[" + label + "]"
					}
					desc := app.NewDescription(title)
					desc.SetText(description.String())
					desc.SetInputCapture(
						app.WithHScroll(desc, func(event *tcell.EventKey) *tcell.EventKey {
							if event.Key() == tcell.KeyCtrlU {
								Publish(
									CgroupsChannel,
									GetCgroupEventType,
									Payload{name, true},
								)
							}
							if IsKey(event, 'a') {
								app.EnterAutoUpdateMode(pageKey, func() {
									Publish(
										CgroupsChannel,
										GetCgroupEventType,
										Payload{name, true},
									)
								})
								return nil
							}
							// Resetting offsets opens the group's offsets as an editable
							// document,
							// which is what <e> means everywhere else. <o> is left to mean
							// "show me the offsets", as it does on a connector.
							if IsKey(event, 'e') {
								if !app.offsetsEditable(description) {
									return event
								}
								app.resetOffsets(name, description, offsetsByTopic)
								return nil
							}
							if IsKey(event, 'E') {
								if !app.offsetsEditable(description) {
									return event
								}
								app.resetOffsets(name, description, offsetsByPartition)
								return nil
							}

							return event
						}),
					)
					app.AddToPagesRegistry(pageKey, desc, ConsumerGroupDescribePageMenu, false)
					if !autoRefreshing {
						ClearStatus()
					}
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to describe consumer group")
				SendStatusError(
					fmt.Sprintf("[red]failed to describe consumer group: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while describing consumer group")
				SendStatusError("[red]timeout while describing consumer group")
				return
			}
		}
	}()
}

// offsetsEditable reports whether the group's committed offsets can be altered right now,
// sending the reason to the status line when they cannot. Kafka rejects an offset commit
// for a group that still has members, and a read-only cluster forbids it outright.
func (app *App) offsetsEditable(description *client.DescribeConsumerGroupResult) bool {
	if !app.Allowed() {
		return false
	}

	for _, d := range description.ConsumerGroupDescriptions {
		if len(d.Members) > 0 {
			SendStatusError(
				"[red]cannot reset offsets: consumer group has active members",
			)
			return false
		}
	}

	return true
}

// WithTopicPartitionCounts fetches the cluster's topic partition counts and hands them to
// fn on the UI goroutine. The offset-reset flow needs them: the partitions of a topic the
// group has never consumed can only come from cluster metadata.
func (app *App) WithTopicPartitionCounts(fn func(counts map[string]int)) {
	resultCh := make(chan map[string]int)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("fetching cluster topics")
	c.TopicPartitionCounts(resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		defer cancel()

		select {
		case counts := <-resultCh:
			app.QueueUpdateDraw(func() {
				ClearStatus()
				fn(counts)
			})
		case err := <-errorCh:
			log.Error().Err(err).Msg("failed to fetch cluster topics")
			SendStatusError(
				fmt.Sprintf("[red]failed to fetch cluster topics: %s", err.Error()),
			)
		case <-ctx.Done():
			log.Error().Msg("timeout while fetching cluster topics")
			SendStatusError("[red]timeout while fetching cluster topics")
		}
	}()
}

// resetOffsets opens the offset-reset flow for a group: the offsets document in the editor,
// at the requested granularity. The cluster's topic partition counts are fetched first — the
// document takes the partitions of a topic the group has never consumed from them. The caller
// has already checked that the offsets are editable at all.
func (app *App) resetOffsets(
	name string,
	description *client.DescribeConsumerGroupResult,
	granularity offsetsGranularity,
) {
	committed := description.GetCommittedOffsets()

	app.WithTopicPartitionCounts(func(counts map[string]int) {
		app.editConsumerGroupOffsets(name, committed, counts, granularity)
	})
}

// offsetsGranularity selects which of the two offsets documents the editor is handed. Both
// parse the same way — a topic may always be narrowed to a partition mapping by hand — so
// this only decides what the buffer starts as.
type offsetsGranularity int

const (
	// offsetsByTopic renders one value per topic, covering all of its partitions.
	offsetsByTopic offsetsGranularity = iota
	// offsetsByPartition renders every committed partition with its own offset.
	offsetsByPartition
)

// editConsumerGroupOffsets hands the group's committed offsets to the editor as a YAML
// document, resolves what comes back against the cluster, and opens a confirmation page
// with the resulting per-partition changes. Nothing is committed until that page is
// confirmed. Must be called from the UI goroutine.
func (app *App) editConsumerGroupOffsets(
	group string,
	committed []client.CommittedOffset,
	partitionCounts map[string]int,
	granularity offsetsGranularity,
) {
	var (
		buf []byte
		err error
	)
	if granularity == offsetsByTopic {
		buf, err = renderConsumerGroupTopicOffsetsDocument(group, committed)
	} else {
		buf, err = renderConsumerGroupOffsetsDocument(group, committed)
	}
	if err != nil {
		log.Error().Err(err).Msg("failed to render consumer group offsets document")
		SendStatusError("[red]failed to render offsets document")
		return
	}

	edited, ok := app.OpenInEditor("cgroup-offsets-*.yaml", buf)
	if !ok {
		return
	}

	targets, err := parseConsumerGroupOffsetsDocument(edited, group, committed, partitionCounts)
	if err != nil {
		SendStatusError(fmt.Sprintf("[red]%s", err.Error()))
		return
	}
	if len(targets) == 0 {
		SendStatusNote("nothing to change")
		return
	}

	app.ResolveConsumerGroupOffsets(group, committed, targets)
}

// ResolveConsumerGroupOffsets turns the edited targets into concrete offsets — watermarks
// and timestamps are looked up on the cluster — and hands them to the confirmation page.
func (app *App) ResolveConsumerGroupOffsets(
	group string,
	committed []client.CommittedOffset,
	targets map[client.TopicPartition]client.OffsetTarget,
) {
	resultCh := make(chan map[client.TopicPartition]client.ResolvedOffset)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("resolving target offsets")
	c.ResolveOffsetTargets(targets, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		defer cancel()

		select {
		case resolved := <-resultCh:
			app.QueueUpdateDraw(func() {
				app.ConsumerGroupOffsetsConfirm(group, committed, targets, resolved)
			})
		case err := <-errorCh:
			log.Error().Err(err).Msg("failed to resolve target offsets")
			SendStatusError(
				fmt.Sprintf("[red]failed to resolve target offsets: %s", err.Error()),
			)
		case <-ctx.Done():
			log.Error().Msg("timeout while resolving target offsets")
			SendStatusError("[red]timeout while resolving target offsets")
		}
	}()
}

// ConsumerGroupOffsetsConfirm shows the pending per-partition changes and commits them once the
// question in the status line is answered. Targets that fall outside their partition's log are refused here rather than
// sent: the broker accepts an out-of-range commit and the consumer then silently overrides
// it via auto.offset.reset. It reports whether the confirmation page opened — a refusal
// leaves the caller's UI as it was, with the reason on the status line.
func (app *App) ConsumerGroupOffsetsConfirm(
	group string,
	committed []client.CommittedOffset,
	targets map[client.TopicPartition]client.OffsetTarget,
	resolved map[client.TopicPartition]client.ResolvedOffset,
) bool {
	changes := offsetChanges(committed, targets, resolved)
	if len(changes) == 0 {
		SendStatusNote("no changes detected")
		return false
	}
	if invalid := outOfRangeChanges(changes); len(invalid) > 0 {
		SendStatusError(fmt.Sprintf("[red]%s", outOfRangeMessage(invalid, resolved)))
		return false
	}

	app.ConfirmPage(
		OffsetsConfirm,
		newConfirmView(fmt.Sprintf(" Confirm Offsets: %s ", group), renderOffsetChanges(group, changes)),
		fmt.Sprintf("commit these offsets for consumer group '%s'?", group),
		func() { app.SetConsumerGroupOffsetsResultHandler(group, changes) },
	)

	return true
}

// SetConsumerGroupOffsetsResultHandler commits the confirmed changes and refreshes the
// consumer group page.
func (app *App) SetConsumerGroupOffsetsResultHandler(group string, changes []offsetChange) {
	offsets := make(map[client.TopicPartition]int64, len(changes))
	for _, change := range changes {
		offsets[change.TopicPartition] = change.To
	}

	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("committing consumer group offsets")
	c.SetConsumerGroupOffsets(group, offsets, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		defer cancel()

		select {
		case <-resultCh:
			SendStatusDone(fmt.Sprintf(
				"offsets for '%s' committed (%d partition%s)",
				group,
				len(changes),
				pluralSuffix(len(changes)),
			))
			Publish(CgroupsChannel, GetCgroupEventType, Payload{group, true})
		case err := <-errorCh:
			log.Error().Err(err).Msg("failed to commit consumer group offsets")
			SendStatusError(
				fmt.Sprintf("[red]failed to commit offsets: %s", err.Error()),
			)
		case <-ctx.Done():
			log.Error().Msg("timeout while committing consumer group offsets")
			SendStatusError("[red]timeout while committing offsets")
		}
	}()
}

// parseTimestamp parses s as "2006-01-02T15:04:05.000", falling back to RFC3339.
func parseTimestamp(s string) (time.Time, error) {
	if t, err := time.Parse("2006-01-02T15:04:05.000", s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

// CopyConsumerGroupModal creates and registers the "copy consumer group" modal.
// A single input field accepts the target group name; Ctrl+Enter submits, Enter unfocuses, Esc closes.
func (app *App) CopyConsumerGroupModal(groupName string) {
	foregroundColor := tcell.GetColor(app.Colors.Karat.Foreground)
	backgroundColor := tcell.GetColor(app.Colors.Karat.Background)

	input := tview.NewInputField().
		SetFieldWidth(0).
		SetPlaceholder("new consumer group name").
		SetFieldStyle(tcell.StyleDefault.Foreground(foregroundColor).Background(backgroundColor)).
		SetPlaceholderStyle(tcell.StyleDefault.Background(backgroundColor)).
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder))

	mainFlex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(input, 1, 0, true)
	mainFlex.SetTitle(fmt.Sprintf(" Copy: %s ", groupName))
	mainFlex.SetBorder(true)
	mainFlex.SetBorderPadding(0, 0, 1, 0)

	submit := func() {
		targetGroup := strings.TrimSpace(input.GetText())
		if targetGroup == "" {
			SendStatusError("[red]group name cannot be empty")
			return
		}
		app.HideModalPage(CopyConsumerGroup)
		app.CopyConsumerGroupOffsetsBatchResultHandler(groupName, targetGroup)
	}

	input.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch {
		case IsCtrlEnter(event):
			submit()
			return nil
		case event.Key() == tcell.KeyEnter:
			app.SetFocus(mainFlex)
			return nil
		case event.Key() == tcell.KeyEsc:
			app.HideModalPage(CopyConsumerGroup)
			return nil
		}
		return event
	})

	mainFlex.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch {
		case IsCtrlEnter(event):
			submit()
			return nil
		case event.Key() == tcell.KeyEnter:
			app.SetFocus(input)
			return nil
		case event.Key() == tcell.KeyEsc:
			app.HideModalPage(CopyConsumerGroup)
			return nil
		}
		return event
	})

	// height: 1 border top + 1 content row + 1 border bottom = 3
	modal := util.NewResourceModal(mainFlex, 3)
	app.Layout.PagesRegistry.UI.Pages.AddPage(CopyConsumerGroup, modal, true, false)
}

// CopyConsumerGroupOffsetsBatchResultHandler copies committed offsets from sourceGroup to
// targetGroup and shows the result status.
func (app *App) CopyConsumerGroupOffsetsBatchResultHandler(sourceGroup, targetGroup string) {
	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress(fmt.Sprintf("copying offsets to '%s'", targetGroup))
	c.CopyConsumerGroupOffsets(sourceGroup, targetGroup, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatusDone(fmt.Sprintf("offsets copied to '%s'", targetGroup))
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to copy consumer group offsets")
				SendStatusError(
					fmt.Sprintf("[red]failed to copy offsets: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while copying consumer group offsets")
				SendStatusError("[red]timeout while copying consumer group offsets")
				return
			}
		}
	}()
}

// lagColumn is the state of the optional Consumer Groups "Lag" column: whether the feature
// is enabled (karat.features.consumer_group_lag) and the per-group total lags, filled
// asynchronously. When disabled the column is not rendered at all and no offset lookups are
// issued. The lags map is shared by reference with the fetch goroutine.
type lagColumn struct {
	enabled bool

	// loading is true while the background fetch is in flight, which marks the header. It
	// is only ever read and written from the UI goroutine.
	loading bool

	lags map[string]int64
}

// header returns the Lag column label, marked while lags are still being fetched.
func (c lagColumn) header() string {
	if c.loading {
		return "Lag" + loadingMarker
	}
	return "Lag"
}

// value returns a group's known total lag, or 0 when it has not been reported.
func (c lagColumn) value(name string) int64 {
	return c.lags[name]
}

// text returns the Lag cell text for a group. It shows "-" when the lag has not been fetched
// yet (group absent from lags), otherwise the total lag as a plain integer.
func (c lagColumn) text(name string) string {
	if v, ok := c.lags[name]; ok {
		return strconv.FormatInt(v, 10)
	}
	return "-"
}

// addGroupsTableHeader adds a fixed header row (row 0) with label-coloured cells.
func addGroupsTableHeader(table *tview.Table, labelColor tcell.Color, lags lagColumn) {
	headers := []string{"Name", "State"}
	if lags.enabled {
		headers = append(headers, lags.header())
	}
	util.SetTableHeaders(table, labelColor, headers...)
}

// sortGroupsTable rebuilds the table sorted by col (0=Name, 1=State, 2=Lag).
// State and Lag tiebreak by Name ascending. Adds ↑/↓ indicator to the active header cell.
func sortGroupsTable(
	table *tview.Table,
	listing []kafka.ConsumerGroupListing,
	lags lagColumn,
	col int,
	desc bool,
	labelColor tcell.Color,
) {
	entries := make([]kafka.ConsumerGroupListing, len(listing))
	copy(entries, listing)

	sort.Slice(entries, func(i, j int) bool {
		switch col {
		case 1:
			si, sj := entries[i].State.String(), entries[j].State.String()
			if si != sj {
				if desc {
					return si > sj
				}
				return si < sj
			}
			return entries[i].GroupID < entries[j].GroupID
		case 2:
			li, lj := lags.value(entries[i].GroupID), lags.value(entries[j].GroupID)
			if li != lj {
				if desc {
					return li > lj
				}
				return li < lj
			}
			return entries[i].GroupID < entries[j].GroupID
		default:
			if desc {
				return entries[i].GroupID > entries[j].GroupID
			}
			return entries[i].GroupID < entries[j].GroupID
		}
	})

	table.Clear()
	addGroupsTableHeader(table, labelColor, lags)

	indicator := "[↑]"
	if desc {
		indicator = "[↓]"
	}
	switch col {
	case 0:
		table.GetCell(0, 0).SetText("Name" + indicator)
	case 1:
		table.GetCell(0, 1).SetText("State" + indicator)
	case 2:
		if lags.enabled {
			table.GetCell(0, 2).SetText(lags.header() + indicator)
		}
	}

	for i, r := range entries {
		table.SetCell(i+1, 0, tview.NewTableCell(r.GroupID))
		table.SetCell(i+1, 1, tview.NewTableCell(r.State.String()))
		if lags.enabled {
			table.SetCell(i+1, 2, tview.NewTableCell(lags.text(r.GroupID)))
		}
	}
}

// NewGroupsTable creates a table displaying consumer groups.
func (app *App) NewGroupsTable(groups *client.ConsumerGroupsResult, lags lagColumn) *tview.Table {
	table := tview.NewTable()
	table.SetSelectable(true, false).
		SetBorder(true).
		SetBorderPadding(0, 0, 1, 0)
	table.SetSelectedStyle(
		tcell.StyleDefault.Foreground(
			tcell.GetColor(app.Colors.Karat.Selection.FgColor),
		).Background(
			tcell.GetColor(app.Colors.Karat.Selection.BgColor),
		),
	)
	table.SetFixed(1, 0)

	labelColor := tcell.GetColor(app.Colors.Karat.Label.FgColor)
	sortGroupsTable(table, groups.Valid, lags, 0, false, labelColor)

	return table
}

func filterConsumerGroupsTable(
	table *tview.Table,
	groupListing []kafka.ConsumerGroupListing,
	lags lagColumn,
	filter string,
	labelColor tcell.Color,
) {
	table.Clear()
	addGroupsTableHeader(table, labelColor, lags)

	var groups []string
	for _, g := range groupListing {
		groups = append(groups, g.GroupID)
	}

	if filter == "" {
		// Show all consumer groups sorted alphabetically when filter is empty
		sort.Strings(groups)
		row := 1
		for _, groupID := range groups {
			// Find the matching group in groupListing
			for _, g := range groupListing {
				if g.GroupID == groupID {
					table.SetCell(row, 0, tview.NewTableCell(g.GroupID))
					table.SetCell(row, 1, tview.NewTableCell(g.State.String()))
					if lags.enabled {
						table.SetCell(row, 2, tview.NewTableCell(lags.text(g.GroupID)))
					}
					row++
					break
				}
			}
		}
		return
	}

	matches := fuzzy.Find(filter, groups)

	row := 1
	for _, match := range matches {
		table.SetCell(row, 0, tview.NewTableCell(match.Str))
		table.SetCell(
			row,
			1,
			tview.NewTableCell(groupListing[match.Index].State.String()),
		)
		if lags.enabled {
			table.SetCell(row, 2, tview.NewTableCell(lags.text(match.Str)))
		}
		row++
	}
}

// DeleteConsumerGroup asks in the status line before deleting a consumer group.
func (app *App) DeleteConsumerGroup(groupName string) {
	app.Modify(
		fmt.Sprintf("delete consumer group '%s'?", groupName),
		func() { app.DeleteConsumerGroupResultHandler(groupName) },
	)
}

// DeleteConsumerGroupResultHandler performs the consumer group deletion.
func (app *App) DeleteConsumerGroupResultHandler(name string) {
	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("deleting consumer group")
	c.DeleteConsumerGroup(name, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatusDone(fmt.Sprintf("consumer group '%s' has been deleted", name))
				cluster := app.Selected.Cluster.Name
				app.QueueUpdateDraw(func() { app.RemovePagesFor(cluster, name) })
				Publish(CgroupsChannel, GetCgroupsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to delete consumer group")
				SendStatusError(
					fmt.Sprintf("[red]failed to delete consumer group: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while deleting consumer group")
				SendStatusError("[red]timeout while deleting consumer group")
				return
			}
		}
	}()
}
