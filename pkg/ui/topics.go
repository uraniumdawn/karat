// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"
	"github.com/sahilm/fuzzy"

	"github.com/uraniumdawn/karat/pkg/client"
	"github.com/uraniumdawn/karat/pkg/franz"
	"github.com/uraniumdawn/karat/pkg/util"
)

const (
	GetTopicsEventType         EventType = "topics:get"
	GetTopicEventType          EventType = "topic:get"
	GetTopicProducersEventType EventType = "topic:producers"
	CreateTopicEventType       EventType = "topic:create"
	DeleteTopicEventType       EventType = "topic:delete"
	EditTopicEventType         EventType = "topic:edit"
	CliTemplatesEventType      EventType = "topic:cli-templates"
)

var TopicsChannel = NewEventChannel()

func (app *App) RunTopicsEventHandler(ctx context.Context, in *EventChannel) {
	in.Run(ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("shutting down topics event handler")
				return
			case event := <-in.C:
				switch event.Type {
				case GetTopicsEventType:
					pageName := util.BuildPageKey(app.Selected.Cluster.Name, Topics)
					force := event.Payload.Force
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.QueueUpdateDraw(func() { app.SwitchToPage(pageName) })
					} else {
						app.Topics(force)
					}

				case GetTopicEventType:
					topicName := event.Payload.Data.(string)
					force := event.Payload.Force
					pageName := util.BuildPageKey(app.Selected.Cluster.Name, Topics, topicName)
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.QueueUpdateDraw(func() { app.SwitchToPage(pageName) })
					} else {
						app.Topic(topicName)
					}

				case GetTopicProducersEventType:
					topicName := event.Payload.Data.(string)
					force := event.Payload.Force
					pageName := util.BuildPageKey(app.Selected.Cluster.Name, Producers, topicName)
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.QueueUpdateDraw(func() { app.SwitchToPage(pageName) })
					} else {
						app.TopicProducers(topicName)
					}

				case CreateTopicEventType:
					app.QueueUpdateDraw(func() {
						app.CreateTopicDocument()
					})

				case DeleteTopicEventType:
					topicName := event.Payload.Data.(string)
					app.QueueUpdateDraw(func() {
						app.DeleteTopic(topicName)
					})

				case EditTopicEventType:
					topicName := event.Payload.Data.(string)
					app.QueueUpdateDraw(func() {
						app.UpdateTopic(topicName)
					})

				case CliTemplatesEventType:
					topicName := event.Payload.Data.(string)
					app.QueueUpdateDraw(func() {
						app.CliTemplates(topicName)
					})
				}
			}
		}
	}()
}

type TopicParams struct {
	TopicName         string
	ReplicationFactor int
	Partitions        int
	Config            map[string]string
}

func (app *App) Topics(force bool) {
	resultCh := make(chan *client.TopicsResult)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("getting topics")
	c.Topics(resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case topics := <-resultCh:
				app.QueueUpdateDraw(func() {
					pageKey := util.BuildPageKey(app.Selected.Cluster.Name, Topics)

					// size carries the Size column state: whether the feature is enabled
					// (karat.features.topic_size) and the per-topic sizes map, filled
					// asynchronously (see below) and shared by reference with the table
					// builders and the fetch goroutine.
					size := sizeColumn{
						enabled: app.Config.TopicSizeEnabled(),
						sizes:   map[string]franz.TopicLogDirSummary{},
					}
					sizesCacheKey := pageKey + ":sizes"
					sizesCached := false
					if size.enabled && !force {
						if cached, ok := app.Cache.Get(sizesCacheKey); ok {
							if m, ok := cached.(map[string]franz.TopicLogDirSummary); ok {
								size.sizes = m
								sizesCached = true
							}
						}
					}

					// Sizes need franz-go; without it the column stays "-" and nothing is
					// fetched, so the header must not claim to be loading.
					fc := app.GetCurrentFranzClient()
					size.loading = size.enabled && !sizesCached && fc != nil

					table := app.NewTopicsTable(topics, size)
					title := util.BuildTitle(Topics,
						"["+strconv.Itoa(len(topics.Result))+"]")
					app.AddToPagesRegistry(pageKey, table, TopicsPageMenu, true)

					// A refresh builds a new table: without this the cursor lands on the
					// first row, and the next key acts on a topic the user did not pick.
					app.RestoreSelection(pageKey, table, afterHeaderRow)
					app.TrackSelection(pageKey, table, afterHeaderRow)

					sortCol := 0
					sortDesc := false
					hideInternal := app.HideInternalTopics
					labelColor := tcell.GetColor(app.Colors.Karat.Label.FgColor)

					updateTitle := func(filterText string) {
						t := title
						if hideInternal {
							t = strings.TrimRight(t, " ") + "[grey] hide-internal[-] "
						}
						util.SetSearchableTableTitle(table, t, filterText)
					}
					updateTitle("")

					table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
						if event.Key() == tcell.KeyCtrlU {
							Publish(TopicsChannel, GetTopicsEventType, Payload{nil, true})
						}
						// <Enter> opens the row under the cursor, the same as <d>.
						if IsKey(event, 'd') || event.Key() == tcell.KeyEnter {
							topicName, ok := selectedName(table, afterHeaderRow)
							if !ok {
								return nil
							}
							Publish(
								TopicsChannel,
								GetTopicEventType,
								Payload{topicName, false},
							)
						}

						if IsKey(event, 'n') {
							if !app.Allowed() {
								return event
							}
							app.CreateTopicDocument()
						}

						if event.Key() == tcell.KeyCtrlD {
							topicName, ok := selectedName(table, afterHeaderRow)
							if !ok {
								return nil
							}
							app.DeleteTopic(topicName)
						}

						if IsKey(event, 'e') {
							if !app.Allowed() {
								return event
							}
							topicName, ok := selectedName(table, afterHeaderRow)
							if !ok {
								return nil
							}
							app.UpdateTopic(topicName)
						}

						if IsKey(event, 'c') {
							topicName, ok := selectedName(table, afterHeaderRow)
							if !ok {
								return nil
							}
							app.ConsumeWithDefaultParams(topicName)
						}

						if IsKey(event, '.') {
							topicName, ok := selectedName(table, afterHeaderRow)
							if !ok {
								return nil
							}
							app.ShowExtraActions(TopicsExtraActions, topicName)
						}

						if IsKey(event, '1') {
							if sortCol == 0 {
								sortDesc = !sortDesc
							} else {
								sortCol = 0
								sortDesc = false
							}
							sortTopicsTable(
								table,
								topics.Result,
								size,
								sortCol,
								sortDesc,
								labelColor,
								hideInternal,
								app.InternalTopicPatterns,
							)
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
							sortTopicsTable(
								table,
								topics.Result,
								size,
								sortCol,
								sortDesc,
								labelColor,
								hideInternal,
								app.InternalTopicPatterns,
							)
							table.ScrollToBeginning()
							app.RestoreSelection(pageKey, table, afterHeaderRow)
							return event
						}

						// Sorting by Size only exists while the column does.
						if IsKey(event, '3') && size.enabled {
							if sortCol == 3 {
								sortDesc = !sortDesc
							} else {
								sortCol = 3
								sortDesc = true
							}
							sortTopicsTable(
								table,
								topics.Result,
								size,
								sortCol,
								sortDesc,
								labelColor,
								hideInternal,
								app.InternalTopicPatterns,
							)
							table.ScrollToBeginning()
							app.RestoreSelection(pageKey, table, afterHeaderRow)
							return event
						}

						if IsKey(event, 'i') {
							hideInternal = !hideInternal
							app.HideInternalTopics = hideInternal
							filterText := app.CurrentFilters[pageKey]
							if filterText != "" {
								filterTopicsTable(
									table,
									topics.Result,
									size,
									filterText,
									labelColor,
									hideInternal,
									app.InternalTopicPatterns,
								)
							} else {
								sortTopicsTable(
									table,
									topics.Result,
									size,
									sortCol,
									sortDesc,
									labelColor,
									hideInternal,
									app.InternalTopicPatterns,
								)
							}
							updateTitle(filterText)
							table.ScrollToBeginning()
							app.RestoreSelection(pageKey, table, afterHeaderRow)
							return event
						}

						return event
					})

					app.AssignSearch(func(text string) {
						filterTopicsTable(
							table,
							topics.Result,
							size,
							text,
							labelColor,
							hideInternal,
							app.InternalTopicPatterns,
						)
						updateTitle(text)
						table.ScrollToBeginning()
						// The rows the cursor pointed at are gone; without this the selection
						// is left past the end of the filtered table and every row action
						// reads an empty cell.
						app.RestoreSelection(pageKey, table, afterHeaderRow)
					})

					// redraw rebuilds the rows in place, keeping the active filter or sort.
					redraw := func() {
						filterText := app.CurrentFilters[pageKey]
						if filterText != "" {
							filterTopicsTable(
								table,
								topics.Result,
								size,
								filterText,
								labelColor,
								hideInternal,
								app.InternalTopicPatterns,
							)
						} else {
							sortTopicsTable(
								table,
								topics.Result,
								size,
								sortCol,
								sortDesc,
								labelColor,
								hideInternal,
								app.InternalTopicPatterns,
							)
						}
						app.RestoreSelection(pageKey, table, afterHeaderRow)
					}

					// Fill the Size column asynchronously. AllTopicsLogDirSizes issues a single
					// sharded DescribeLogDirs (one request per broker, not per topic), so broker
					// load scales with broker count. Results are cached (5-min TTL); Ctrl+U forces
					// a fresh fetch. The header carries loadingMarker until this settles.
					if size.loading {
						go func() {
							fetchCtx, fetchCancel := context.WithTimeout(
								context.Background(),
								app.Config.GetAPICallTimeout(),
							)
							defer fetchCancel()

							fetched, err := fc.AllTopicsLogDirSizes(fetchCtx)
							if err != nil {
								log.Debug().
									Err(err).
									Msg("failed to get topic sizes")
								app.QueueUpdateDraw(func() {
									size.loading = false
									redraw()
								})
								return
							}

							app.QueueUpdateDraw(func() {
								size.loading = false
								maps.Copy(size.sizes, fetched)
								app.Cache.Set(
									sizesCacheKey,
									size.sizes,
									5*time.Minute,
								)
								redraw()
							})
						}()
					}

					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to list topics")
				SendStatusError(fmt.Sprintf("[red]failed to list topics: %s", err.Error()))
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while listing topics")
				SendStatusError("[red]timeout while listing topics")
				return
			}
		}
	}()
}

func (app *App) Topic(name string) {
	resultCh := make(chan *client.TopicResult)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	fc := app.GetCurrentFranzClient()
	pageKey := util.BuildPageKey(app.Selected.Cluster.Name, Topic, name)
	autoRefreshing := app.GetAutoUpdateLabel(pageKey) != ""
	if !autoRefreshing {
		SendStatusProgress("getting topic description")
	}
	c.DescribeTopic(name, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	type topicSizeResult struct {
		size franz.TopicLogDirSummary
		ok   bool
	}
	sizeCh := make(chan topicSizeResult, 1)
	if fc != nil && app.Config.TopicSizeEnabled() {
		go func() {
			size, err := fc.TopicLogDirSize(ctx, name)
			if err != nil {
				log.Debug().
					Err(err).
					Str("topic", name).
					Msg("failed to get actual topic size")
				sizeCh <- topicSizeResult{}
				return
			}
			sizeCh <- topicSizeResult{size: size, ok: true}
		}()
	} else {
		sizeCh <- topicSizeResult{}
	}

	go func() {
		for {
			select {
			case description := <-resultCh:
				select {
				case sr := <-sizeCh:
					if sr.ok {
						description.SetActualSize(sr.size.TotalSizeBytes, sr.size.Hint)
					}
				case <-ctx.Done():
				}
				app.QueueUpdateDraw(func() {
					title := util.BuildTitle(Topic, name)
					if label := app.GetAutoUpdateLabel(pageKey); label != "" {
						title = title + "[" + label + "]"
					}
					desc := app.NewDescription(title)
					desc.SetText(description.String())
					desc.SetInputCapture(
						app.WithHScroll(desc, func(event *tcell.EventKey) *tcell.EventKey {
							if event.Key() == tcell.KeyCtrlU {
								Publish(
									TopicsChannel,
									GetTopicEventType,
									Payload{name, true},
								)
							}
							if IsKey(event, 'a') {
								app.EnterAutoUpdateMode(pageKey, func() {
									Publish(
										TopicsChannel,
										GetTopicEventType,
										Payload{name, true},
									)
								})
								return nil
							}
							if IsKey(event, '.') {
								app.ShowExtraActions(TopicDescriptionExtraActions, name)
								return nil
							}
							return event
						}),
					)
					app.AddToPagesRegistry(pageKey, desc, TopicDecriptionPageMenu, false)
					if !autoRefreshing {
						ClearStatus()
					}
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to describe topic")
				SendStatusError(fmt.Sprintf("[red]failed to describe topic: %s", err.Error()))
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while describing topic")
				SendStatusError("[red]timeout while describing topic")
				return
			}
		}
	}()
}

// TopicProducers fetches and displays the active producers (and any open transactions)
// for each partition of the given topic, via the franz-go client.
func (app *App) TopicProducers(name string) {
	fc := app.GetCurrentFranzClient()
	if fc == nil {
		SendStatusError("[red]producers view requires franz-go connectivity for this cluster")
		return
	}

	pageKey := util.BuildPageKey(app.Selected.Cluster.Name, Producers, name)
	autoRefreshing := app.GetAutoUpdateLabel(pageKey) != ""
	if !autoRefreshing {
		SendStatusProgress("getting topic producers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		defer cancel()
		result, err := fc.DescribeTopicProducers(ctx, name)
		if err != nil {
			log.Error().Err(err).Str("topic", name).Msg("failed to describe topic producers")
			SendStatusError(
				fmt.Sprintf("[red]failed to describe topic producers: %s", err.Error()),
			)
			return
		}
		app.QueueUpdateDraw(func() {
			title := util.BuildTitle(Producers, name)
			if label := app.GetAutoUpdateLabel(pageKey); label != "" {
				title = title + "[" + label + "]"
			}
			desc := app.NewDescription(title)
			desc.SetText(result.String())
			desc.SetInputCapture(
				app.WithHScroll(desc, func(event *tcell.EventKey) *tcell.EventKey {
					if event.Key() == tcell.KeyCtrlU {
						Publish(
							TopicsChannel,
							GetTopicProducersEventType,
							Payload{name, true},
						)
					}
					if IsKey(event, 'a') {
						app.EnterAutoUpdateMode(pageKey, func() {
							Publish(
								TopicsChannel,
								GetTopicProducersEventType,
								Payload{name, true},
							)
						})
						return nil
					}
					return event
				}),
			)
			app.AddToPagesRegistry(pageKey, desc, TopicProducersPageMenu, false)
			if !autoRefreshing {
				ClearStatus()
			}
		})
	}()
}

func (app *App) CreateTopicResultHandler(
	name string,
	numPartitions int,
	replicationFactor int,
	config map[string]string,
) {
	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("creating topic")
	c.CreateTopic(name, numPartitions, replicationFactor, config, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatusDone(fmt.Sprintf("topic '%s' has been created", name))
				Publish(TopicsChannel, GetTopicsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to create topic")
				SendStatusError(fmt.Sprintf("[red]%s", err.Error()))
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while creating topics")
				SendStatusError("[red]timeout while creating topics")
				return
			}
		}
	}()
}

// topicParamsFromDescription derives the partition count, replication factor, and
// non-default, non-read-only config entries from a described topic — the settings needed
// to reproduce it via CreateTopic.
func topicParamsFromDescription(
	result *client.TopicResult,
) (partitions, replicationFactor int, config map[string]string) {
	config = make(map[string]string)
	if len(result.TopicDescriptions) > 0 {
		desc := result.TopicDescriptions[0]
		partitions = len(desc.Partitions)
		if len(desc.Partitions) > 0 {
			replicationFactor = len(desc.Partitions[0].Replicas)
		}
	}

	for _, configResult := range result.Config {
		for _, entry := range configResult.Config {
			if !entry.IsDefault && !entry.IsReadOnly {
				config[entry.Name] = entry.Value
			}
		}
	}

	return partitions, replicationFactor, config
}

// topicConfigsFromDescription splits a described topic's writable configuration into the
// overrides explicitly set on the topic and the settings still at their cluster default.
// Sensitive entries are skipped, since DescribeConfigs returns them with an empty value
// and writing that back would clear them.
func topicConfigsFromDescription(
	result *client.TopicResult,
) (overrides, defaults map[string]string) {
	overrides = make(map[string]string)
	defaults = make(map[string]string)

	for _, configResult := range result.Config {
		for _, entry := range configResult.Config {
			if entry.IsReadOnly || entry.IsSensitive {
				continue
			}
			if entry.IsDefault {
				defaults[entry.Name] = entry.Value
			} else {
				overrides[entry.Name] = entry.Value
			}
		}
	}

	return overrides, defaults
}

// CloneTopic fetches the source topic's description and opens its definition in the
// editor as the starting point for a new topic.
func (app *App) CloneTopic(sourceTopic string) {
	resultCh := make(chan *client.TopicResult)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("fetching topic configuration")
	c.DescribeTopic(sourceTopic, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case topicResult := <-resultCh:
				app.QueueUpdateDraw(func() {
					ClearStatus()
					app.CloneTopicDocument(sourceTopic, topicResult)
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to fetch topic config")
				SendStatusError(
					fmt.Sprintf("[red]failed to fetch topic config: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while fetching topic config")
				SendStatusError("[red]timeout while fetching topic config")
				return
			}
		}
	}()
}

func (app *App) UpdateTopic(topicName string) {
	resultCh := make(chan *client.TopicResult)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("fetching topic configuration")
	c.DescribeTopic(topicName, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case topicResult := <-resultCh:
				app.QueueUpdateDraw(func() {
					ClearStatus()
					app.EditTopicDocument(topicName, topicResult)
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to fetch topic config")
				SendStatusError(
					fmt.Sprintf("[red]failed to fetch topic config: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while fetching topic config")
				SendStatusError("[red]timeout while fetching topic config")
				return
			}
		}
	}()
}

// updatedTopicConfigMessage describes what an applied config update actually did, so a
// change that only resets overrides is not reported as a plain update.
func updatedTopicConfigMessage(name string, removed int) string {
	if removed == 0 {
		return fmt.Sprintf("topic '%s' config has been updated", name)
	}
	return fmt.Sprintf("topic '%s' config has been updated (%d reset to default)", name, removed)
}

// UpdateTopicResultHandler applies the edited topic settings: config is set, removed is
// reset to the cluster default, and the partition count is grown when newPartitions
// exceeds currentPartitions.
func (app *App) UpdateTopicResultHandler(
	name string,
	currentPartitions int,
	newPartitions int,
	config map[string]string,
	removed []string,
) {
	c := app.GetCurrentKafkaClient()

	// The configuration and the partition count are two calls, either of which can be the
	// only one to run. The list is refreshed once the last of them is done, and only when
	// something can have changed — a refusal leaves the cluster as it was, and refreshing
	// then would wipe the reason off the status line. Refreshing from the caller instead
	// would race: both calls are asynchronous, and the list would be refetched while the
	// cluster still holds the old topic.
	updateDone := app.topicUpdateBarrier(
		len(config) > 0 || len(removed) > 0,
		newPartitions > currentPartitions,
	)

	if len(config) > 0 || len(removed) > 0 {
		resultCh := make(chan bool)
		errorCh := make(chan error)
		SendStatusProgress("updating topic configuration")
		c.UpdateTopicConfig(name, config, removed, resultCh, errorCh)
		ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

		go func() {
			for {
				select {
				case <-resultCh:
					SendStatusDone(updatedTopicConfigMessage(name, len(removed)))
					updateDone(true)
					cancel()
					return
				case err := <-errorCh:
					log.Error().Err(err).Msg("failed to update topic configuration")
					SendStatusError(
						fmt.Sprintf(
							"[red]failed to update topic configuration: %s",
							err.Error(),
						),
					)
					updateDone(false)
					cancel()
					return
				case <-ctx.Done():
					log.Error().Msg("timeout while updating topic config")
					SendStatusError("[red]timeout while updating topic config")
					updateDone(true)
					return
				}
			}
		}()
	}

	if newPartitions > currentPartitions {
		resultCh := make(chan bool)
		errorCh := make(chan error)
		SendStatusProgress("increasing partition count")
		c.IncreasePartitions(name, newPartitions, resultCh, errorCh)
		ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

		go func() {
			for {
				select {
				case <-resultCh:
					SendStatusDone(fmt.Sprintf(
						"topic '%s' partitions increased to %d",
						name,
						newPartitions,
					))
					updateDone(true)
					cancel()
					return
				case err := <-errorCh:
					log.Error().Err(err).Msg("failed to increase partition count")
					SendStatusError(
						fmt.Sprintf("[red]failed to increase partition count: %s", err.Error()),
					)
					updateDone(false)
					cancel()
					return
				case <-ctx.Done():
					log.Error().Msg("timeout while increasing partition count")
					SendStatusError("[red]timeout while increasing partition count")
					updateDone(true)
					return
				}
			}
		}()
	}
}

// topicUpdateBarrier returns the function every leg of a topic update calls when it is done,
// passing whether the cluster can have changed: true for a leg that succeeded or timed out —
// a timeout says nothing about what the broker did with the request — and false for one the
// broker refused.
//
// The last leg to report refreshes the topics list, so a two-leg update refreshes once and a
// one-leg update refreshes as soon as its own leg is over. An update where every leg was
// refused refreshes nothing: there is nothing new to show, and the refresh would replace the
// refusal on the status line with its own progress message.
//
// It is called from the goroutines watching each leg, hence the mutex.
func (app *App) topicUpdateBarrier(legs ...bool) func(changed bool) {
	pending := 0
	for _, running := range legs {
		if running {
			pending++
		}
	}

	var (
		mu      sync.Mutex
		changed bool
	)
	return func(legChanged bool) {
		mu.Lock()
		pending--
		changed = changed || legChanged
		last, refresh := pending <= 0, changed
		mu.Unlock()

		if last && refresh {
			Publish(TopicsChannel, GetTopicsEventType, Payload{nil, true})
		}
	}
}

// DeleteTopic asks in the status line before deleting the topic.
func (app *App) DeleteTopic(topicName string) {
	app.Modify(
		fmt.Sprintf("delete topic '%s'?", topicName),
		func() { app.DeleteTopicResultHandler(topicName) },
	)
}

func (app *App) DeleteTopicResultHandler(name string) {
	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("deleting topic")
	c.DeleteTopic(name, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatusDone(fmt.Sprintf("topic '%s' has been deleted", name))
				// The topic's own pages — description, producers, consume output — describe
				// something that no longer exists, so they go with it.
				cluster := app.Selected.Cluster.Name
				app.QueueUpdateDraw(func() { app.RemovePagesFor(cluster, name) })
				Publish(TopicsChannel, GetTopicsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to delete topic")
				SendStatusError(fmt.Sprintf("[red]failed to delete topic: %s", err.Error()))
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while deleting topic")
				SendStatusError("[red]timeout while deleting topic")
				return
			}
		}
	}()
}

// recreateUITimeout bounds the UI-side wait for a recreate to complete. It must exceed the
// client's worst case (delete + waiting for the deletion to propagate + create) so a
// slow-but-successful recreate is not reported as a spurious timeout.
const recreateUITimeout = 3 * time.Minute

// RecreateTopic fetches the source topic's configuration and asks before deleting the topic
// and re-creating it empty with the same name, partition count, replication factor, and
// config. All existing messages are lost.
func (app *App) RecreateTopic(sourceTopic string) {
	resultCh := make(chan *client.TopicResult)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("fetching topic configuration")
	c.DescribeTopic(sourceTopic, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case topicResult := <-resultCh:
				partitionCount, replicationFactor, sourceConfig := topicParamsFromDescription(
					topicResult,
				)

				app.QueueUpdateDraw(func() {
					// Cleared before the question is asked: the spinner reporting the
					// fetch would otherwise stand in the status line beside it.
					ClearStatus()
					app.ConfirmRecreateTopic(
						sourceTopic,
						partitionCount,
						replicationFactor,
						sourceConfig,
					)
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to fetch topic config")
				SendStatusError(
					fmt.Sprintf("[red]failed to fetch topic config: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while fetching topic config")
				SendStatusError("[red]timeout while fetching topic config")
				return
			}
		}
	}()
}

// ConfirmRecreateTopic warns in the status line that recreating the topic deletes all of its
// data, then re-creates it with the captured settings on confirm.
func (app *App) ConfirmRecreateTopic(
	topicName string,
	partitions int,
	replicationFactor int,
	config map[string]string,
) {
	app.Modify(fmt.Sprintf("recreate topic '%s' empty, losing all its data?", topicName),
		func() {
			app.RecreateTopicResultHandler(topicName, partitions, replicationFactor, config)
		},
	)
}

// RecreateTopicResultHandler drives the delete-then-create sequence and refreshes the
// topics list on success.
func (app *App) RecreateTopicResultHandler(
	name string,
	numPartitions int,
	replicationFactor int,
	config map[string]string,
) {
	// Buffered so the client goroutine never blocks sending its single result even if the
	// UI-side wait below has already timed out.
	resultCh := make(chan bool, 1)
	errorCh := make(chan error, 1)

	c := app.GetCurrentKafkaClient()
	SendStatusProgress("recreating topic")
	c.RecreateTopic(name, numPartitions, replicationFactor, config, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), recreateUITimeout)

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatusDone(fmt.Sprintf("topic '%s' has been recreated", name))
				Publish(TopicsChannel, GetTopicsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to recreate topic")
				SendStatusError(fmt.Sprintf("[red]failed to recreate topic: %s", err.Error()))
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while recreating topic")
				SendStatusError("[red]timeout while recreating topic")
				return
			}
		}
	}()
}

func (app *App) NewTopicsTable(
	topics *client.TopicsResult,
	size sizeColumn,
) *tview.Table {
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
	sortTopicsTable(
		table,
		topics.Result,
		size,
		0,
		false,
		labelColor,
		app.HideInternalTopics,
		app.InternalTopicPatterns,
	)

	return table
}

// topicDocumentSession renders the topic as a YAML document, hands it to the editor and
// parses what comes back. ok is false when the editor was aborted or the document was
// rejected — the reason is already on the status line by then.
func (app *App) topicDocumentSession(
	header string,
	name string,
	replicationFactor int,
	partitions int,
	configs map[string]string,
	defaults map[string]string,
) (topicDocument, map[string]string, bool) {
	buf, err := renderTopicDocument(header, name, replicationFactor, partitions, configs, defaults)
	if err != nil {
		log.Error().Err(err).Msg("failed to render topic document")
		SendStatusError("[red]failed to render topic document")
		return topicDocument{}, nil, false
	}

	edited, ok := app.OpenInEditor("topic-*.yaml", buf)
	if !ok {
		return topicDocument{}, nil, false
	}

	doc, edits, _, err := parseTopicDocument(edited)
	if err != nil {
		SendStatusError(fmt.Sprintf("[red]%s", err.Error()))
		return topicDocument{}, nil, false
	}

	return doc, edits, true
}

// What a new topic starts at in the editor. One partition and one replica are what a
// single-broker cluster takes, and are the smallest values that pass validation — a document
// starting at zero has to be edited on both lines before it can be submitted at all.
const (
	defaultNewTopicPartitions        = 1
	defaultNewTopicReplicationFactor = 1
)

// CreateTopicDocument opens Create Topic: an empty topic document goes to the editor and
// what comes back is shown for confirmation before anything is created.
func (app *App) CreateTopicDocument() {
	doc, configs, ok := app.topicDocumentSession(
		createTopicDocumentHeader,
		"",
		defaultNewTopicReplicationFactor,
		defaultNewTopicPartitions,
		nil,
		nil,
	)
	if !ok {
		return
	}

	params := TopicParams{
		TopicName:         doc.Name,
		ReplicationFactor: doc.ReplicationFactor,
		Partitions:        doc.Partitions,
		Config:            configs,
	}
	if err := params.validate(); err != nil {
		SendStatusError(fmt.Sprintf("[red]%s", err.Error()))
		return
	}

	app.CreateTopicConfirm(params)
}

// EditTopicDocument opens Edit Topic: the described topic goes to the editor and the diff
// of what comes back is shown for confirmation before anything is applied.
func (app *App) EditTopicDocument(topicName string, topicResult *client.TopicResult) {
	partitionCount, replicationFactor := topicShape(topicResult)
	currentConfig, defaultConfig := topicConfigsFromDescription(topicResult)

	doc, configs, ok := app.topicDocumentSession(
		editTopicDocumentHeader,
		topicName,
		replicationFactor,
		partitionCount,
		currentConfig,
		defaultConfig,
	)
	if !ok {
		return
	}
	if err := validateTopicDocumentEdit(doc, topicName, replicationFactor, partitionCount); err != nil {
		SendStatusError(fmt.Sprintf("[red]%s", err.Error()))
		return
	}

	app.UpdateTopicConfirm(
		topicChanges(topicName, partitionCount, doc.Partitions, currentConfig, configs),
	)
}

// CloneTopicDocument opens Clone Topic: the source topic's definition goes to the editor
// under its own name, and the topic the edited document names is shown for confirmation
// before anything is created. The document is a create, so an unchanged name would only
// fail against the broker as "already exists" and is rejected here instead.
func (app *App) CloneTopicDocument(sourceTopic string, topicResult *client.TopicResult) {
	partitionCount, replicationFactor := topicShape(topicResult)
	sourceConfig, defaultConfig := topicConfigsFromDescription(topicResult)

	doc, configs, ok := app.topicDocumentSession(
		cloneTopicDocumentHeader(sourceTopic),
		sourceTopic,
		replicationFactor,
		partitionCount,
		sourceConfig,
		defaultConfig,
	)
	if !ok {
		return
	}

	params := TopicParams{
		TopicName:         doc.Name,
		ReplicationFactor: doc.ReplicationFactor,
		Partitions:        doc.Partitions,
		Config:            configs,
	}
	if err := params.validate(); err != nil {
		SendStatusError(fmt.Sprintf("[red]%s", err.Error()))
		return
	}
	if err := validateCloneName(params.TopicName, sourceTopic); err != nil {
		SendStatusError(fmt.Sprintf("[red]%s", err.Error()))
		return
	}

	app.CreateTopicConfirm(params)
}

// topicShape returns a described topic's partition count and replication factor, both 0
// when the description carries no partitions.
func topicShape(result *client.TopicResult) (partitions, replicationFactor int) {
	if len(result.TopicDescriptions) == 0 {
		return 0, 0
	}

	desc := result.TopicDescriptions[0]
	partitions = len(desc.Partitions)
	if partitions > 0 {
		replicationFactor = len(desc.Partitions[0].Replicas)
	}

	return partitions, replicationFactor
}

// sizeColumn is the state of the optional Topics "Size" column: whether the feature is
// enabled (karat.features.topic_size) and the per-topic on-disk sizes, filled
// asynchronously. When disabled the column is not rendered at all and no DescribeLogDirs
// request is issued. The sizes map is shared by reference with the fetch goroutine.
type sizeColumn struct {
	enabled bool

	// loading is true while the background fetch is in flight, which marks the header. It
	// is only ever read and written from the UI goroutine.
	loading bool

	sizes map[string]franz.TopicLogDirSummary
}

// header returns the Size column label, marked while sizes are still being fetched.
func (c sizeColumn) header() string {
	if c.loading {
		return "Size" + loadingMarker
	}
	return "Size"
}

// bytes returns a topic's known on-disk size, or 0 when it has not been reported.
func (c sizeColumn) bytes(name string) int64 {
	return c.sizes[name].TotalSizeBytes
}

// text returns the Size cell text for a topic. It shows "-" when the size has not been
// fetched yet (topic absent from sizes), and prefixes "~" when the reported size is a known
// undercount (some replicas did not report their log dir).
func (c sizeColumn) text(name string) string {
	s, ok := c.sizes[name]
	if !ok {
		return "-"
	}
	text := util.FormatBytes(s.TotalSizeBytes)
	if s.Hint != "" {
		text = "~" + text
	}
	return text
}

// addTopicsTableHeader adds a fixed header row (row 0) with label-coloured cells.
func addTopicsTableHeader(table *tview.Table, labelColor tcell.Color, size sizeColumn) {
	headers := []string{"Name", "Partitions", "Replication"}
	if size.enabled {
		headers = append(headers, size.header())
	}
	util.SetTableHeaders(table, labelColor, headers...)
}

func populateTable(table *tview.Table, row int, t string, partitions, replicas int, size sizeColumn) {
	table.SetCell(row, 0, tview.NewTableCell(t))
	table.SetCell(row, 1, tview.NewTableCell(strconv.Itoa(partitions)))
	table.SetCell(row, 2, tview.NewTableCell(strconv.Itoa(replicas)))
	if size.enabled {
		table.SetCell(row, 3, tview.NewTableCell(size.text(t)))
	}
}

// isInternalTopic reports whether name matches one of the configured internal
// topic patterns (karat.ui.internal_topic_patterns).
func isInternalTopic(name string, patterns []*regexp.Regexp) bool {
	for _, re := range patterns {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

// sortTopicsTable rebuilds the table sorted by col (0=Name, 1=Partitions, 3=Size).
// Partitions and Size tiebreak by Name ascending. Adds ↑/↓ indicator to the active header cell.
// If hideInternal is true, internal topics (see isInternalTopic) are omitted.
func sortTopicsTable(
	table *tview.Table,
	metadata map[string]*kafka.TopicMetadata,
	size sizeColumn,
	col int,
	desc bool,
	labelColor tcell.Color,
	hideInternal bool,
	internalPatterns []*regexp.Regexp,
) {
	type entry struct {
		name       string
		partitions int
		replicas   int
		size       int64
	}

	entries := make([]entry, 0, len(metadata))
	for name, meta := range metadata {
		if hideInternal && isInternalTopic(name, internalPatterns) {
			continue
		}
		p := len(meta.Partitions)
		r := 0
		if p > 0 {
			r = len(meta.Partitions[0].Replicas)
		}
		entries = append(entries, entry{name, p, r, size.bytes(name)})
	}

	sort.Slice(entries, func(i, j int) bool {
		switch col {
		case 1:
			if entries[i].partitions != entries[j].partitions {
				if desc {
					return entries[i].partitions > entries[j].partitions
				}
				return entries[i].partitions < entries[j].partitions
			}
			return entries[i].name < entries[j].name
		case 3:
			if entries[i].size != entries[j].size {
				if desc {
					return entries[i].size > entries[j].size
				}
				return entries[i].size < entries[j].size
			}
			return entries[i].name < entries[j].name
		default:
			if desc {
				return entries[i].name > entries[j].name
			}
			return entries[i].name < entries[j].name
		}
	})

	table.Clear()
	addTopicsTableHeader(table, labelColor, size)

	indicator := "[↑]"
	if desc {
		indicator = "[↓]"
	}
	switch col {
	case 0:
		table.GetCell(0, 0).SetText("Name" + indicator)
	case 1:
		table.GetCell(0, 1).SetText("Partitions" + indicator)
	case 3:
		if size.enabled {
			table.GetCell(0, 3).SetText(size.header() + indicator)
		}
	}

	for i, e := range entries {
		populateTable(table, i+1, e.name, e.partitions, e.replicas, size)
	}
}

func filterTopicsTable(
	table *tview.Table,
	metadata map[string]*kafka.TopicMetadata,
	size sizeColumn,
	filter string,
	labelColor tcell.Color,
	hideInternal bool,
	internalPatterns []*regexp.Regexp,
) {
	table.Clear()
	addTopicsTableHeader(table, labelColor, size)

	var topics []string
	for topicName := range metadata {
		if hideInternal && isInternalTopic(topicName, internalPatterns) {
			continue
		}
		topics = append(topics, topicName)
	}

	if filter == "" {
		// Sort topics in ascending order if the filter is empty
		sort.Strings(topics)
		for i, topicName := range topics {
			meta := metadata[topicName]
			partitions := len(meta.Partitions)
			replicas := 0
			if len(meta.Partitions) > 0 {
				replicas = len(meta.Partitions[0].Replicas)
			}

			populateTable(table, i+1, topicName, partitions, replicas, size)
		}
		return
	}

	matches := fuzzy.Find(filter, topics)

	for i, match := range matches {
		topicName := match.Str
		meta := metadata[topicName]
		partitions := len(meta.Partitions)
		replicas := 0
		if len(meta.Partitions) > 0 {
			replicas = len(meta.Partitions[0].Replicas)
		}

		populateTable(table, i+1, topicName, partitions, replicas, size)
	}
}

func (tp *TopicParams) validate() error {
	if strings.TrimSpace(tp.TopicName) == "" {
		return fmt.Errorf("topic name cannot be empty")
	}
	if tp.ReplicationFactor <= 0 {
		return fmt.Errorf("replication factor must be greater than 0")
	}
	if tp.Partitions <= 0 {
		return fmt.Errorf("partitions must be greater than 0")
	}
	return nil
}
