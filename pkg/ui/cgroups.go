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
	// ResetCgroupOffsetEventType is the event type for resetting consumer group offsets.
	ResetCgroupOffsetEventType EventType = "cgroup:reset-offset"
	// DeleteCgroupEventType is the event type for deleting a consumer group.
	DeleteCgroupEventType EventType = "cgroup:delete"
	// FindCgroupsByTopicEventType is the event type for finding consumer groups by topic.
	FindCgroupsByTopicEventType EventType = "cgroups:find-by-topic"
)

// CgroupsChannel is the channel for consumer group events.
var CgroupsChannel = make(chan Event)

// ResetOffsetPayload contains data for resetting consumer group offsets.
type ResetOffsetPayload struct {
	GroupName   string
	Description *client.DescribeConsumerGroupResult
}

// RunCgroupsEventHandler processes consumer group events from the channel.
func (app *App) RunCgroupsEventHandler(ctx context.Context, in chan Event) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("shutting down cgroups event handler")
				return
			case event := <-in:
				switch event.Type {
				case GetCgroupsEventType:
					pageName := util.BuildPageKey(app.Selected.Cluster.Name, ConsumerGroups)
					force := event.Payload.Force
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.SwitchToPage(pageName)
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
						app.SwitchToPage(pageName)
					} else {
						app.ConsumerGroup(consumerGroup)
					}

				case ResetCgroupOffsetEventType:
					payload := event.Payload.Data.(ResetOffsetPayload)
					app.QueueUpdateDraw(func() {
						app.ResetConsumerGroupOffsetModal(
							payload.GroupName,
							payload.Description,
						)
						app.ShowModalPage(ResetOffset)
					})

				case DeleteCgroupEventType:
					groupName := event.Payload.Data.(string)
					app.QueueUpdateDraw(func() {
						app.DeleteConsumerGroup(groupName)
						app.ShowModalPage(DeleteConsumerGroup)
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
						app.SwitchToPage(pageName)
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
	SendStatusInfinite("getting consumer groups")
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
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to list consumer groups: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while to list consumer groups")
				SendStatusWithDefaultTTL("[red]timeout while to list consumer groups")
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

	sortCol := 0
	sortDesc := false
	labelColor := tcell.GetColor(app.Colors.Karat.Label.FgColor)

	table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyCtrlU {
			onRefresh()
		}

		if IsKey(event, 'd') {
			row, _ := table.GetSelection()
			groupName := table.GetCell(row, 0).Text
			Publish(CgroupsChannel, GetCgroupEventType, Payload{groupName, false})
		}

		if IsKey(event, '.') {
			row, _ := table.GetSelection()
			groupName := table.GetCell(row, 0).Text
			app.ShowExtraActions(ConsumerGroupsExtraActions, groupName)
		}

		if event.Key() == tcell.KeyCtrlD {
			if app.IsCurrentClusterReadOnly() {
				SendStatusWithDefaultTTL("[red]cluster is in read-only mode")
				return event
			}
			row, _ := table.GetSelection()
			groupName := table.GetCell(row, 0).Text

			for _, g := range groups.Valid {
				if g.GroupID == groupName {
					if g.State != kafka.ConsumerGroupStateEmpty {
						SendStatusWithDefaultTTL(fmt.Sprintf(
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
			return event
		}

		return event
	})

	app.AssignSearch(func(text string) {
		filterConsumerGroupsTable(table, groups.Valid, lags, text, labelColor)
		util.SetSearchableTableTitle(table, title, text)
		table.ScrollToBeginning()
	})

	// redraw rebuilds the rows in place, keeping the active filter or sort.
	redraw := func() {
		filterText := app.CurrentFilters[pageKey]
		if filterText != "" {
			filterConsumerGroupsTable(table, groups.Valid, lags, filterText, labelColor)
		} else {
			sortGroupsTable(table, groups.Valid, lags, sortCol, sortDesc, labelColor)
		}
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
	SendStatusInfinite("finding consumer groups by topic")
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
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to find consumer groups by topic: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while finding consumer groups by topic")
				SendStatusWithDefaultTTL("[red]timeout while finding consumer groups by topic")
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
				SendStatusWithDefaultTTL("[red]topic name cannot be empty")
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
		SendStatusInfinite("getting consumer group description")
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
							if IsKey(event, 'g') {
								app.EnterAutoUpdateMode(pageKey, func() {
									Publish(
										CgroupsChannel,
										GetCgroupEventType,
										Payload{name, true},
									)
								})
								return nil
							}
							if IsKey(event, 'o') {
								if app.IsCurrentClusterReadOnly() {
									SendStatusWithDefaultTTL(
										"[red]cluster is in read-only mode",
									)
									return event
								}
								for _, d := range description.ConsumerGroupDescriptions {
									if len(d.Members) > 0 {
										SendStatusWithDefaultTTL(
											"[red]cannot reset offsets: consumer group has active members",
										)
										return event
									}
								}
								Publish(
									CgroupsChannel,
									ResetCgroupOffsetEventType,
									Payload{
										Data: ResetOffsetPayload{
											GroupName:   name,
											Description: description,
										},
										Force: false,
									},
								)
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
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to describe consumer group: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while describing consumer group")
				SendStatusWithDefaultTTL("[red]timeout while describing consumer group")
				return
			}
		}
	}()
}

// ResetConsumerGroupOffsetModal opens the reset offset modal for the given consumer group.
func (app *App) ResetConsumerGroupOffsetModal(
	groupName string,
	description *client.DescribeConsumerGroupResult,
	extraTopics ...string,
) {
	allTopics := description.GetTopicNames()
	// Make a copy so we can add new topics to it; extraTopics carries user-added entries.
	allTopicsCopy := make([]string, len(allTopics), len(allTopics)+len(extraTopics))
	copy(allTopicsCopy, allTopics)
	allTopics = append(allTopicsCopy, extraTopics...)
	topicStrategies := make(map[string]client.TopicStrategy, len(allTopics)+1)
	invalidValues := make(map[string]bool)
	for _, t := range allTopics {
		topicStrategies[t] = client.TopicStrategy{}
	}

	// ── Color & style shortcuts ───────────────────────────────────────────
	labelColor := tcell.GetColor(app.Colors.Karat.Label.FgColor)
	foregroundColor := tcell.GetColor(app.Colors.Karat.Foreground)
	selectedStyle := tcell.StyleDefault.
		Foreground(tcell.GetColor(app.Colors.Karat.Selection.FgColor)).
		Background(tcell.GetColor(app.Colors.Karat.Selection.BgColor))

	isValueStrategy := func(strategy string) bool {
		return strategy == "to-timestamp" || strategy == "to-offset"
	}

	table, valueInputs, valueColumnFlex, container := app.newOffsetBatchTable(
		allTopics,
		labelColor,
		selectedStyle,
	)

	// innerPages hosts the batch table as the base layer and the strategy picker as an overlay.
	innerPages := tview.NewPages()
	innerPages.AddPage("batch", container, true, true)

	formatTimestampMs := func(ms int64) string {
		return time.UnixMilli(ms).Format("2006-01-02T15:04:05.000")
	}

	placeholderTextColor := tcell.GetColor(app.Colors.Karat.Placeholder)

	setInputPlaceholder := func(input *tview.InputField, strategy string) {
		input.SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			))
		switch strategy {
		case "to-timestamp":
			input.SetPlaceholder("eg: 2025-02-23T00:00:00.000")
			input.SetPlaceholderTextColor(placeholderTextColor)
		case "to-offset":
			input.SetPlaceholder("19 digits")
			input.SetPlaceholderTextColor(placeholderTextColor)
		default:
			input.SetPlaceholder("")
		}
	}

	// setInputValueText sets the input text and resets the color for the given strategy & value.
	// For value strategies with 0 committed value, preserves text but clears invalid red.
	// For non-value strategies, clears the input entirely.
	setInputValueText := func(input *tview.InputField, strategy string, value int64) {
		if strategy == "to-timestamp" && value > 0 {
			input.SetText(formatTimestampMs(value))
			input.SetFieldTextColor(foregroundColor)
		} else if strategy == "to-offset" && value > 0 {
			input.SetText(fmt.Sprintf("%d", value))
			input.SetFieldTextColor(foregroundColor)
		} else if !isValueStrategy(strategy) {
			input.SetText("")
		} else {
			input.SetFieldTextColor(foregroundColor)
		}
	}

	// applyStrategyToAll applies the given strategy to all topics.
	applyStrategyToAll := func(strategy string, value int64) {
		for _, topic := range allTopics {
			newTs := client.TopicStrategy{Strategy: strategy}
			switch strategy {
			case "to-timestamp":
				newTs.TimestampMs = value
			case "to-offset":
				newTs.OffsetValue = value
			}
			topicStrategies[topic] = newTs
			row := topicToRow(table, topic)
			if row > 0 {
				table.GetCell(row, 1).SetText(strategy)
				if input, ok := valueInputs[row]; ok {
					setInputPlaceholder(input, strategy)
					setInputValueText(input, strategy, value)
				}
				delete(invalidValues, topic)
			}
		}
	}

	// syncValueCell updates the value input for a specific row from the strategy map.
	syncValueCell := func(row int) {
		if row <= 0 || row >= table.GetRowCount() {
			return
		}
		topic := table.GetCell(row, 0).Text
		input, ok := valueInputs[row]
		if !ok {
			return
		}

		// Skip syncing for special rows.
		if topic == allTopicsRowName || topic == newTopicRowName {
			input.SetText("")
			return
		}

		ts := topicStrategies[topic]

		if ts.Strategy == "" {
			input.SetText("")
			return
		}

		setInputPlaceholder(input, ts.Strategy)

		switch {
		case ts.Strategy == "to-timestamp" && ts.TimestampMs > 0:
			input.SetText(formatTimestampMs(ts.TimestampMs))
			input.SetFieldTextColor(foregroundColor)
		case ts.Strategy == "to-offset" && ts.OffsetValue > 0:
			input.SetText(fmt.Sprintf("%d", ts.OffsetValue))
			input.SetFieldTextColor(foregroundColor)
		case ts.Strategy == "to-timestamp":
			// When TimestampMs == 0, leave the input as-is to preserve any uncommitted user input.
		case ts.Strategy == "to-offset":
			// When OffsetValue == 0, leave the input as-is to preserve any uncommitted user input.
		default:
			input.SetText("")
		}

		// setInputPlaceholder resets SetFieldStyle to normal colors; re-apply red if still invalid.
		if invalidValues[topic] {
			input.SetFieldTextColor(tcell.ColorRed)
		}
	}

	// applyStrategy sets the chosen strategy directly on the given topic row.
	applyStrategy := func(topic string, row int, strategy string) {
		if topic == allTopicsRowName {
			currentValue := int64(0)
			if len(allTopics) > 0 {
				currentTs := topicStrategies[allTopics[0]]
				switch currentTs.Strategy {
				case "to-timestamp":
					currentValue = currentTs.TimestampMs
				case "to-offset":
					currentValue = currentTs.OffsetValue
				}
			}
			nextValue := currentValue
			if !isValueStrategy(strategy) {
				nextValue = 0
			}
			applyStrategyToAll(strategy, nextValue)
			return
		}

		ts := topicStrategies[topic]
		newTs := client.TopicStrategy{
			Strategy:    strategy,
			TimestampMs: ts.TimestampMs,
			OffsetValue: ts.OffsetValue,
		}
		if !isValueStrategy(strategy) {
			newTs.TimestampMs = 0
			newTs.OffsetValue = 0
		}
		topicStrategies[topic] = newTs
		delete(invalidValues, topic)

		table.GetCell(row, 1).SetText(strategy)
		if input, ok := valueInputs[row]; ok {
			setInputPlaceholder(input, strategy)
			value := int64(0)
			switch strategy {
			case "to-timestamp":
				value = ts.TimestampMs
			case "to-offset":
				value = ts.OffsetValue
			}
			setInputValueText(input, strategy, value)
		}
	}

	// commitValueInput parses and commits the value from a row's input field.
	commitValueInput := func(row int) bool {
		if row <= 0 || row >= table.GetRowCount() {
			return false
		}
		topic := table.GetCell(row, 0).Text
		input, ok := valueInputs[row]
		if !ok {
			return false
		}

		checkTopic := topic
		if topic == allTopicsRowName && len(allTopics) > 0 {
			checkTopic = allTopics[0]
		}
		currentTs, hasStrategy := topicStrategies[checkTopic]

		valStr := input.GetText()
		if valStr == "" || !hasStrategy || currentTs.Strategy == "" {
			delete(invalidValues, topic)
			return true
		}

		switch currentTs.Strategy {
		case "to-timestamp":
			t, err := parseTimestamp(valStr)
			if err != nil {
				if topic != allTopicsRowName {
					input.SetFieldTextColor(tcell.ColorRed)
					invalidValues[topic] = true
				}
				return false
			}
			tsMs := t.UnixMilli()
			if topic == allTopicsRowName {
				applyStrategyToAll("to-timestamp", tsMs)
				return true
			}
			topicStrategies[topic] = client.TopicStrategy{
				Strategy: "to-timestamp", TimestampMs: tsMs,
			}
			delete(invalidValues, topic)
			input.SetFieldTextColor(foregroundColor)

		case "to-offset":
			offsetVal, err := strconv.ParseInt(valStr, 10, 64)
			if err != nil || offsetVal < 0 {
				if topic != allTopicsRowName {
					input.SetFieldTextColor(tcell.ColorRed)
					invalidValues[topic] = true
				}
				return false
			}
			if topic == allTopicsRowName {
				applyStrategyToAll("to-offset", offsetVal)
				return true
			}
			topicStrategies[topic] = client.TopicStrategy{
				Strategy: "to-offset", OffsetValue: offsetVal,
			}
			delete(invalidValues, topic)
			input.SetFieldTextColor(foregroundColor)
		}
		return true
	}

	// openStrategyPicker overlays a small strategy selection table over innerPages.
	openStrategyPicker := func(topic string, row int) {
		pickerLabels := []string{" __clear", " to-earliest", " to-latest", " to-timestamp", " to-offset"}
		pickerValues := []string{"", "to-earliest", "to-latest", "to-timestamp", "to-offset"}

		currentStrategy := ""
		if topic == allTopicsRowName {
			if len(allTopics) > 0 {
				currentStrategy = topicStrategies[allTopics[0]].Strategy
			}
		} else {
			currentStrategy = topicStrategies[topic].Strategy
		}

		pickerTable := tview.NewTable()
		pickerTable.SetBorder(true)
		pickerTable.SetTitle(" Strategy ")
		pickerTable.SetSelectable(true, false)
		pickerTable.SetSelectedStyle(selectedStyle)

		initialRow := 0
		for i, label := range pickerLabels {
			pickerTable.SetCell(i, 0, tview.NewTableCell(label).SetSelectable(true))
			if pickerValues[i] == currentStrategy {
				initialRow = i
			}
		}
		pickerTable.Select(initialRow, 0)

		closePicker := func() {
			innerPages.RemovePage("picker")
			app.SetFocus(table)
		}

		pickerTable.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
			switch event.Key() {
			case tcell.KeyEsc:
				closePicker()
				return nil
			case tcell.KeyEnter:
				selectedIdx, _ := pickerTable.GetSelection()
				strategy := pickerValues[selectedIdx]
				applyStrategy(topic, row, strategy)
				closePicker()
				if strategy == "to-timestamp" || strategy == "to-offset" {
					if input, ok := valueInputs[row]; ok {
						app.SetFocus(input)
					}
				}
				return nil
			}
			return event
		})

		const pickerH = 7  // 5 strategy rows + 2 border lines
		const pickerW = 20 // wide enough for all strategy labels + borders
		pickerWrapper := tview.NewFlex().SetDirection(tview.FlexColumn).
			AddItem(nil, 0, 1, false).
			AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
				AddItem(nil, 0, 0, false).
				AddItem(pickerTable, pickerH, 0, true).
				AddItem(nil, 0, 1, false),
				pickerW, 0, true).
			AddItem(nil, 1, 0, false)

		innerPages.AddPage("picker", pickerWrapper, true, true)
		app.SetFocus(pickerTable)
	}

	table.SetSelectionChangedFunc(func(row, _ int) {
		syncValueCell(row)
	})

	table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		row, _ := table.GetSelection()
		switch {
		case event.Key() == tcell.KeyEsc:
			app.HideModalPage(ResetOffset)

		case event.Key() == tcell.KeyEnter:
			if row > 0 {
				topic := table.GetCell(row, 0).Text
				if topic == newTopicRowName {
					if input, ok := valueInputs[row]; ok {
						app.SetFocus(input)
					}
				} else {
					openStrategyPicker(topic, row)
				}
			}
			return nil

		case event.Key() == tcell.KeyTab:
			if row > 0 {
				topic := table.GetCell(row, 0).Text
				if topic == newTopicRowName {
					if input, ok := valueInputs[row]; ok {
						app.SetFocus(input)
					}
				}
			}

		case IsKey(event, 'e'):
			if row > 0 {
				topic := table.GetCell(row, 0).Text
				if topic != newTopicRowName {
					checkTopic := topic
					if topic == allTopicsRowName && len(allTopics) > 0 {
						checkTopic = allTopics[0]
					}
					if ts, ok := topicStrategies[checkTopic]; ok &&
						(ts.Strategy == "to-timestamp" || ts.Strategy == "to-offset") {
						if input, ok := valueInputs[row]; ok {
							app.SetFocus(input)
						}
					}
				}
			}

		case event.Key() == tcell.KeyCtrlB:
			// Commit all value inputs before validation.
			for r := range valueInputs {
				commitValueInput(r)
			}

			hasStrategy := false
			for _, ts := range topicStrategies {
				if ts.Strategy != "" {
					hasStrategy = true
					break
				}
			}
			if !hasStrategy {
				SendStatusWithDefaultTTL("[red]at least one topic must have a strategy")
				return event
			}
			if len(invalidValues) > 0 {
				topics := make([]string, 0, len(invalidValues))
				for t := range invalidValues {
					topics = append(topics, t)
				}
				sort.Strings(topics)
				SendStatusWithDefaultTTL(fmt.Sprintf("[red]invalid value for topics: %s",
					strings.Join(topics, ", ")))
				return event
			}
			app.ResetConsumerGroupOffsetBatchResultHandler(groupName, topicStrategies)
			app.HideModalPage(ResetOffset)
		}
		return event
	})

	// ── Value input field handlers ───────────────────────────────────────
	wireValueInput := func(row int, input *tview.InputField) {
		input.SetDoneFunc(func(_ tcell.Key) {
			commitValueInput(row)
			app.SetFocus(table)
		})
		input.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
			switch {
			case event.Key() == tcell.KeyEsc:
				commitValueInput(row)
				app.SetFocus(table)
				return nil
			case event.Key() == tcell.KeyEnter:
				commitValueInput(row)
				app.SetFocus(table)
				return nil
			case event.Key() == tcell.KeyTab || event.Key() == tcell.KeyBacktab:
				commitValueInput(row)
				nextRow := row + 1
				if event.Key() == tcell.KeyBacktab {
					nextRow = row - 1
				}
				if nextRow >= 1 && nextRow < table.GetRowCount() {
					if nextInput, ok := valueInputs[nextRow]; ok {
						app.SetFocus(nextInput)
						return nil
					}
				}
				app.SetFocus(table)
				return nil
			}
			return event
		})
	}

	newTopicInputHandler := func(input *tview.InputField) {
		input.SetDoneFunc(func(key tcell.Key) {
			if key == tcell.KeyEnter {
				newTopicName := strings.TrimSpace(input.GetText())
				if newTopicName == "" {
					SendStatusWithDefaultTTL("[red]topic name cannot be empty")
					return
				}
				for _, t := range allTopics {
					if t == newTopicName {
						SendStatusWithDefaultTTL("[red]topic already exists in the list")
						return
					}
				}
				// + new topic row is always at len(allTopics)+2 before the append.
				insertRow := len(allTopics) + 2
				allTopics = append(allTopics, newTopicName)
				topicStrategies[newTopicName] = client.TopicStrategy{}

				// Insert a new table row before "+ new topic", shifting it down.
				table.InsertRow(insertRow)
				table.SetCell(insertRow, 0, tview.NewTableCell(newTopicName).SetSelectable(true))
				table.SetCell(insertRow, 1, tview.NewTableCell("").SetSelectable(false))

				// Create a value InputField for the new topic row (no placeholder by default).
				newInput := tview.NewInputField().
					SetFieldWidth(30).
					SetFieldStyle(
						tcell.StyleDefault.Foreground(
							tcell.GetColor(app.Colors.Karat.Foreground),
						).Background(
							tcell.GetColor(app.Colors.Karat.Background),
						)).
					SetPlaceholderStyle(
						tcell.StyleDefault.Background(
							tcell.GetColor(app.Colors.Karat.Background),
						))

				// Slide the "+ new topic" input down in the map and register the new input.
				valueInputs[insertRow+1] = valueInputs[insertRow]
				valueInputs[insertRow] = newInput

				// Reorder the value column flex.
				valueColumnFlex.RemoveItem(input)
				valueColumnFlex.AddItem(newInput, 1, 0, false)
				valueColumnFlex.AddItem(input, 1, 0, false)

				wireValueInput(insertRow, newInput)

				input.SetText("")
			}
			app.SetFocus(table)
		})
	}

	for row, input := range valueInputs {
		if row == len(allTopics)+2 { // new topic row
			newTopicInputHandler(input)
			continue
		}

		wireValueInput(row, input)
	}

	// ── Layout ────────────────────────────────────────────────────────────
	mainFlex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(innerPages, 0, 1, true)
	mainFlex.SetTitle(fmt.Sprintf(" Reset Offsets: %s ", groupName))
	mainFlex.SetBorder(true)

	modal := util.NewTopicModal(mainFlex)
	app.Layout.PagesRegistry.UI.Pages.AddPage(ResetOffset, modal, true, false)
}

// parseTimestamp parses s as "2006-01-02T15:04:05.000", falling back to RFC3339.
func parseTimestamp(s string) (time.Time, error) {
	if t, err := time.Parse("2006-01-02T15:04:05.000", s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

// allTopicsRowName is the special row name that applies strategy to all topics.
const allTopicsRowName = "__all topics"

// newTopicRowName is the special row name for adding a new topic.
const newTopicRowName = "+ new topic"

func (app *App) newOffsetBatchTable(
	topics []string,
	labelColor tcell.Color,
	selectedStyle tcell.Style,
) (*tview.Table, map[int]*tview.InputField, *tview.Flex, *tview.Flex) {
	mkHeader := func(text string) *tview.TableCell {
		return tview.NewTableCell(text).SetSelectable(false).SetTextColor(labelColor)
	}

	table := tview.NewTable()
	table.SetBorder(false)
	table.SetBorderPadding(0, 0, 1, 0)
	table.SetSelectable(true, false)
	table.SetFixed(1, 0)
	table.SetSelectedStyle(selectedStyle)
	table.SetCell(0, 0, mkHeader("Topic"))
	table.SetCell(0, 1, mkHeader("Strategy"))

	valueInputs := make(map[int]*tview.InputField)
	valueColumnFlex := tview.NewFlex().SetDirection(tview.FlexRow)

	// Header for value column.
	valueHeaderLabel := tview.NewTableCell("Value").
		SetTextColor(labelColor).
		SetSelectable(false).
		SetAlign(tview.AlignLeft)
	valueHeaderTable := tview.NewTable()
	valueHeaderTable.SetCell(0, 0, valueHeaderLabel)
	valueColumnFlex.AddItem(valueHeaderTable, 1, 0, false)

	// "__all topics" row.
	row := 1
	table.SetCell(row, 0, tview.NewTableCell(allTopicsRowName).SetSelectable(true))
	table.SetCell(row, 1, tview.NewTableCell("").SetSelectable(false))

	valueInput := tview.NewInputField().
		SetFieldWidth(30).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetPlaceholderStyle(
			tcell.StyleDefault.Background(
				tcell.GetColor(app.Colors.Karat.Background),
			))
	valueInputs[row] = valueInput
	valueColumnFlex.AddItem(valueInput, 1, 0, false)

	for i, topic := range topics {
		row = i + 2
		table.SetCell(row, 0, tview.NewTableCell(topic).SetSelectable(true))
		table.SetCell(row, 1, tview.NewTableCell("").SetSelectable(false))

		valueInput := tview.NewInputField().
			SetFieldWidth(30).
			SetFieldStyle(
				tcell.StyleDefault.Foreground(
					tcell.GetColor(app.Colors.Karat.Foreground),
				).Background(
					tcell.GetColor(app.Colors.Karat.Background),
				)).
			SetPlaceholderStyle(
				tcell.StyleDefault.Background(
					tcell.GetColor(app.Colors.Karat.Background),
				))
		valueInputs[row] = valueInput
		valueColumnFlex.AddItem(valueInput, 1, 0, false)
	}

	// "+ new topic" row for adding custom topics
	newTopicRow := len(topics) + 2
	table.SetCell(newTopicRow, 0, tview.NewTableCell(newTopicRowName).SetSelectable(true))
	table.SetCell(newTopicRow, 1, tview.NewTableCell("").SetSelectable(false))

	newTopicInput := tview.NewInputField().
		SetFieldWidth(30).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder))
	valueInputs[newTopicRow] = newTopicInput
	valueColumnFlex.AddItem(newTopicInput, 1, 0, false)

	container := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(table, 0, 2, true).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(valueColumnFlex, 0, 1, false)

	return table, valueInputs, valueColumnFlex, container
}

// topicToRow returns the table row index for a given topic name.
func topicToRow(table *tview.Table, topic string) int {
	for row := 0; row < table.GetRowCount(); row++ {
		if cell := table.GetCell(row, 0); cell != nil && cell.Text == topic {
			return row
		}
	}
	return -1
}

// ResetConsumerGroupOffsetBatchResultHandler performs batch offset reset and shows the result status.
func (app *App) ResetConsumerGroupOffsetBatchResultHandler(
	group string,
	topicStrategies map[string]client.TopicStrategy,
) {
	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusInfinite("resetting consumer group offsets")
	c.BatchResetConsumerGroupOffsets(group, topicStrategies, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				topicCount := len(topicStrategies)
				msg := fmt.Sprintf("offsets for '%s' have been reset", group)
				if topicCount == 1 {
					for topic := range topicStrategies {
						if topic != "" {
							msg = fmt.Sprintf(
								"offsets for '%s' have been reset [%s]",
								group,
								topic,
							)
						}
						break
					}
				} else if topicCount > 1 {
					msg = fmt.Sprintf(
						"offsets for '%s' have been reset [%d topics]",
						group,
						topicCount,
					)
				}
				SendStatus(msg, 2*time.Second, false)
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to reset consumer group offsets")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to reset offsets: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while resetting consumer group offsets")
				SendStatusWithDefaultTTL("[red]timeout while resetting consumer group offsets")
				return
			}
		}
	}()
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
			SendStatusWithDefaultTTL("[red]group name cannot be empty")
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
	SendStatusInfinite(fmt.Sprintf("copying offsets to '%s'", targetGroup))
	c.CopyConsumerGroupOffsets(sourceGroup, targetGroup, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatus(fmt.Sprintf("offsets copied to '%s'", targetGroup), 2*time.Second, false)
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to copy consumer group offsets")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to copy offsets: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while copying consumer group offsets")
				SendStatusWithDefaultTTL("[red]timeout while copying consumer group offsets")
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

// DeleteConsumerGroup shows a confirmation modal for deleting a consumer group.
func (app *App) DeleteConsumerGroup(groupName string) {
	messageText := tview.NewTextView().
		SetText(fmt.Sprintf("Consumer group [red::b]%s[-::-] will be deleted. Confirm?", groupName)).
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	messageText.SetBorder(true).
		SetTitle(" Confirm Deletion ").
		SetBorderPadding(0, 0, 1, 1)

	messageText.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			app.DeleteConsumerGroupResultHandler(groupName)
			app.HideModalPage(DeleteConsumerGroup)
			Publish(CgroupsChannel, GetCgroupsEventType, Payload{nil, true})
		}

		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(DeleteConsumerGroup)
		}

		return event
	})

	modal := util.NewConfirmationModal(messageText)
	app.Layout.PagesRegistry.UI.Pages.AddPage(DeleteConsumerGroup, modal, true, true)
	app.Layout.PagesRegistry.UI.Pages.ShowPage(DeleteConsumerGroup)
}

// DeleteConsumerGroupResultHandler performs the consumer group deletion.
func (app *App) DeleteConsumerGroupResultHandler(name string) {
	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusInfinite("deleting consumer group")
	c.DeleteConsumerGroup(name, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatus(
					fmt.Sprintf("consumer group '%s' has been deleted", name),
					2*time.Second,
					false,
				)
				Publish(CgroupsChannel, GetCgroupsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to delete consumer group")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to delete consumer group: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while deleting consumer group")
				SendStatusWithDefaultTTL("[red]timeout while deleting consumer group")
				return
			}
		}
	}()
}
