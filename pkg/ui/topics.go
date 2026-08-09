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

var TopicsChannel = make(chan Event)

func (app *App) RunTopicsEventHandler(ctx context.Context, in chan Event) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("shutting down topics event handler")
				return
			case event := <-in:
				switch event.Type {
				case GetTopicsEventType:
					pageName := util.BuildPageKey(app.Selected.Cluster.Name, Topics)
					force := event.Payload.Force
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.SwitchToPage(pageName)
					} else {
						app.Topics(force)
					}

				case GetTopicEventType:
					topicName := event.Payload.Data.(string)
					force := event.Payload.Force
					pageName := util.BuildPageKey(app.Selected.Cluster.Name, Topics, topicName)
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.SwitchToPage(pageName)
					} else {
						app.Topic(topicName)
					}

				case GetTopicProducersEventType:
					topicName := event.Payload.Data.(string)
					force := event.Payload.Force
					pageName := util.BuildPageKey(app.Selected.Cluster.Name, Producers, topicName)
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.SwitchToPage(pageName)
					} else {
						app.TopicProducers(topicName)
					}

				case CreateTopicEventType:
					app.QueueUpdateDraw(func() {
						app.CreateTopic()
						app.ShowModalPage(CreateTopic)
					})

				case DeleteTopicEventType:
					topicName := event.Payload.Data.(string)
					app.QueueUpdateDraw(func() {
						app.DeleteTopic(topicName)
						app.ShowModalPage(DeleteTopic)
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
	SendStatusInfinite("getting topics")
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
						if IsKey(event, 'd') {
							row, _ := table.GetSelection()
							topicName := table.GetCell(row, 0).Text
							Publish(
								TopicsChannel,
								GetTopicEventType,
								Payload{topicName, false},
							)
						}

						if IsKey(event, 'n') {
							if app.IsCurrentClusterReadOnly() {
								SendStatusWithDefaultTTL(
									"[red]cluster is in read-only mode",
								)
								return event
							}
							app.CreateTopic()
							app.ShowModalPage(CreateTopic)
						}

						if event.Key() == tcell.KeyCtrlD {
							if app.IsCurrentClusterReadOnly() {
								SendStatusWithDefaultTTL(
									"[red]cluster is in read-only mode",
								)
								return event
							}
							row, _ := table.GetSelection()
							topicName := table.GetCell(row, 0).Text
							app.DeleteTopic(topicName)
							app.ShowModalPage(DeleteTopic)
						}

						if IsKey(event, 'e') {
							if app.IsCurrentClusterReadOnly() {
								SendStatusWithDefaultTTL(
									"[red]cluster is in read-only mode",
								)
								return event
							}
							row, _ := table.GetSelection()
							topicName := table.GetCell(row, 0).Text
							app.UpdateTopic(topicName)
						}

						if IsKey(event, 'c') {
							row, _ := table.GetSelection()
							topicName := table.GetCell(row, 0).Text
							app.ConsumeWithLastParams(topicName)
						}

						if IsKey(event, '.') {
							row, _ := table.GetSelection()
							topicName := table.GetCell(row, 0).Text
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
				SendStatusWithDefaultTTL(fmt.Sprintf("[red]failed to list topics: %s", err.Error()))
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while to list topics")
				SendStatusWithDefaultTTL("[red]timeout while to list topics")
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
		SendStatusInfinite("getting topic description")
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
							if IsKey(event, 'g') {
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
				SendStatusWithDefaultTTL(fmt.Sprintf("[red]failed to describe topic: %s", err.Error()))
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while describing topic")
				SendStatusWithDefaultTTL("[red]timeout while describing topic")
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
		SendStatusWithDefaultTTL("[red]producers view requires franz-go connectivity for this cluster")
		return
	}

	pageKey := util.BuildPageKey(app.Selected.Cluster.Name, Producers, name)
	autoRefreshing := app.GetAutoUpdateLabel(pageKey) != ""
	if !autoRefreshing {
		SendStatusInfinite("getting topic producers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		defer cancel()
		result, err := fc.DescribeTopicProducers(ctx, name)
		if err != nil {
			log.Error().Err(err).Str("topic", name).Msg("failed to describe topic producers")
			SendStatusWithDefaultTTL(
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
					if IsKey(event, 'g') {
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

func (app *App) CreateTopic() {
	params := &TopicParams{
		TopicName:         "",
		ReplicationFactor: -1,
		Partitions:        -1,
		Config:            make(map[string]string),
	}
	width := 40

	topicName := tview.NewInputField().
		SetFieldWidth(width).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder))

	replicationFactor := tview.NewInputField().
		SetFieldWidth(width).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder))
	replicationFactor.SetAcceptanceFunc(tview.InputFieldInteger)

	partitions := tview.NewInputField().
		SetFieldWidth(width).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder))
	partitions.SetAcceptanceFunc(tview.InputFieldInteger)

	// Text area for optional properties (multi-line)
	configTextArea := tview.NewTextArea().
		SetPlaceholder(`Enter properties (one per line):
cleanup.policy=delete
retention.ms=604800000`).
		SetPlaceholderStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Placeholder),
			))

	selection := tview.NewTable()
	selection.SetCell(
		0,
		0,
		tview.NewTableCell("Name:").
			SetAlign(tview.AlignRight).
			SetTextColor(tcell.GetColor(app.Colors.Karat.Label.FgColor)),
	)
	selection.SetCell(
		1,
		0,
		tview.NewTableCell("Replication factor:").
			SetAlign(tview.AlignRight).
			SetTextColor(tcell.GetColor(app.Colors.Karat.Label.FgColor)),
	)
	selection.SetCell(
		2,
		0,
		tview.NewTableCell("Partitions:").
			SetAlign(tview.AlignRight).
			SetTextColor(tcell.GetColor(app.Colors.Karat.Label.FgColor)),
	)
	selection.SetCell(
		3,
		0,
		tview.NewTableCell("Configs (optional):").
			SetAlign(tview.AlignRight).
			SetTextColor(tcell.GetColor(app.Colors.Karat.Label.FgColor)),
	)
	selection.SetSelectable(true, false)
	selection.SetBorderPadding(0, 0, 1, 0)
	selection.SetSelectedStyle(
		tcell.StyleDefault.Foreground(
			tcell.GetColor(app.Colors.Karat.Selection.FgColor),
		).Background(
			tcell.GetColor(app.Colors.Karat.Selection.BgColor),
		),
	)

	f := tview.NewFlex()
	f.SetDirection(tview.FlexColumn)
	f.AddItem(selection, 20, 0, true)
	f.AddItem(tview.NewBox(), 3, 0, false)

	inputs := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(topicName, 1, 0, false).
		AddItem(replicationFactor, 1, 0, false).
		AddItem(partitions, 1, 0, false).
		AddItem(configTextArea, 0, 1, false)

	f.AddItem(inputs, 40, 0, false).
		AddItem(tview.NewBox(), 0, 1, false)

	topicName.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			params.TopicName = topicName.GetText()
			app.SetFocus(selection)
			app.Layout.Menu.SetMenu(CreateTopicPageMenu)
		}
		return event
	})

	replicationFactor.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			params.ReplicationFactor, _ = strconv.Atoi(replicationFactor.GetText())
			app.SetFocus(selection)
			app.Layout.Menu.SetMenu(CreateTopicPageMenu)
		}
		return event
	})

	partitions.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			params.Partitions, _ = strconv.Atoi(partitions.GetText())
			app.SetFocus(selection)
			app.Layout.Menu.SetMenu(CreateTopicPageMenu)
		}
		return event
	})

	configTextArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			propertiesText := configTextArea.GetText()
			params.Config = parseConfig(propertiesText)
			app.SetFocus(selection)
			app.Layout.Menu.SetMenu(CreateTopicPageMenu)
			return nil
		}
		return event
	})

	inputFields := []*tview.InputField{topicName, replicationFactor, partitions}
	selection.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		row, _ := selection.GetSelection()

		if IsKey(event, 'e') {
			if row < len(inputFields) {
				app.SetFocus(inputFields[row])
				app.Layout.Menu.SetMenu(CreateTopicInputMenu)
			} else if row == 3 {
				app.SetFocus(configTextArea)
				app.Layout.Menu.SetMenu(CreateTopicInputMenu)
			}
		}

		if IsCtrlEnter(event) {
			params.TopicName = topicName.GetText()
			params.ReplicationFactor, _ = strconv.Atoi(replicationFactor.GetText())
			params.Partitions, _ = strconv.Atoi(partitions.GetText())
			params.Config = parseConfig(configTextArea.GetText())

			if err := params.validate(); err != nil {
				SendStatusWithDefaultTTL(fmt.Sprintf("[red]%s", err.Error()))
				return event
			}

			app.CreateTopicResultHandler(
				params.TopicName,
				params.Partitions,
				params.ReplicationFactor,
				params.Config,
			)
			app.HideModalPage(CreateTopic)
		}

		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(CreateTopic)
		}

		return event
	})

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(f, 0, 1, true)
	flex.SetTitle(" Create Topic ")
	flex.SetBorder(true)

	modal := util.NewTopicModal(flex)
	app.Layout.PagesRegistry.UI.Pages.AddPage(CreateTopic, modal, true, true)
	app.Layout.PagesRegistry.UI.Pages.ShowPage(CreateTopic)
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
	SendStatusInfinite("creating topic")
	c.CreateTopic(name, numPartitions, replicationFactor, config, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatus(fmt.Sprintf("topic '%s' has been created", name), 2*time.Second, false)
				Publish(TopicsChannel, GetTopicsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to create topic")
				SendStatusWithDefaultTTL(fmt.Sprintf("[red]failed to create topic: %s", err.Error()))
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while creating topics")
				SendStatusWithDefaultTTL("[red]timeout while creating topics")
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

// CloneTopic fetches the source topic's description and opens a modal to create
// a new topic with the same configuration.
func (app *App) CloneTopic(sourceTopic string) {
	resultCh := make(chan *client.TopicResult)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusInfinite("fetching topic configuration")
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
					app.NewCloneTopicModal(
						sourceTopic,
						partitionCount,
						replicationFactor,
						sourceConfig,
					)
					app.ShowModalPage(CloneTopic)
					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to fetch topic config")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to fetch topic config: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while fetching topic config")
				SendStatusWithDefaultTTL("[red]timeout while fetching topic config")
				return
			}
		}
	}()
}

// NewCloneTopicModal builds a "Clone Topic" modal pre-filled with the source topic's
// partition count, replication factor, and non-default configuration entries.
func (app *App) NewCloneTopicModal(
	sourceTopic string,
	srcPartitions int,
	srcReplicationFactor int,
	srcConfig map[string]string,
) {
	params := &TopicParams{
		TopicName:         sourceTopic,
		ReplicationFactor: srcReplicationFactor,
		Partitions:        srcPartitions,
		Config:            srcConfig,
	}
	width := 40

	topicName := tview.NewInputField().
		SetFieldWidth(width).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder)).
		SetText(sourceTopic)

	replicationFactor := tview.NewInputField().
		SetFieldWidth(width).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder)).
		SetText(fmt.Sprintf("%d", srcReplicationFactor))
	replicationFactor.SetAcceptanceFunc(tview.InputFieldInteger)

	partitions := tview.NewInputField().
		SetFieldWidth(width).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder)).
		SetText(fmt.Sprintf("%d", srcPartitions))
	partitions.SetAcceptanceFunc(tview.InputFieldInteger)

	configTextArea := tview.NewTextArea()
	var configLines []string
	for key, value := range srcConfig {
		configLines = append(configLines, fmt.Sprintf("%s=%s", key, value))
	}
	if len(configLines) > 0 {
		sort.Strings(configLines)
		configTextArea.SetText(strings.Join(configLines, "\n"), false)
	} else {
		configTextArea.SetPlaceholder(`Enter properties (one per line):
cleanup.policy=delete
retention.ms=604800000`).
			SetPlaceholderStyle(
				tcell.StyleDefault.Foreground(
					tcell.GetColor(app.Colors.Karat.Placeholder),
				))
	}

	selection := tview.NewTable()
	selection.SetCell(
		0,
		0,
		tview.NewTableCell("Name:").
			SetAlign(tview.AlignRight).
			SetTextColor(tcell.GetColor(app.Colors.Karat.Label.FgColor)),
	)
	selection.SetCell(
		1,
		0,
		tview.NewTableCell("Replication factor:").
			SetAlign(tview.AlignRight).
			SetTextColor(tcell.GetColor(app.Colors.Karat.Label.FgColor)),
	)
	selection.SetCell(
		2,
		0,
		tview.NewTableCell("Partitions:").
			SetAlign(tview.AlignRight).
			SetTextColor(tcell.GetColor(app.Colors.Karat.Label.FgColor)),
	)
	selection.SetCell(
		3,
		0,
		tview.NewTableCell("Configs (optional):").
			SetAlign(tview.AlignRight).
			SetTextColor(tcell.GetColor(app.Colors.Karat.Label.FgColor)),
	)
	selection.SetSelectable(true, false)
	selection.SetBorderPadding(0, 0, 1, 0)
	selection.SetSelectedStyle(
		tcell.StyleDefault.Foreground(
			tcell.GetColor(app.Colors.Karat.Selection.FgColor),
		).Background(
			tcell.GetColor(app.Colors.Karat.Selection.BgColor),
		),
	)

	f := tview.NewFlex()
	f.SetDirection(tview.FlexColumn)
	f.AddItem(selection, 20, 0, true)
	f.AddItem(tview.NewBox(), 3, 0, false)

	inputs := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(topicName, 1, 0, false).
		AddItem(replicationFactor, 1, 0, false).
		AddItem(partitions, 1, 0, false).
		AddItem(configTextArea, 0, 1, false)

	f.AddItem(inputs, 40, 0, false).
		AddItem(tview.NewBox(), 0, 1, false)

	topicName.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			params.TopicName = topicName.GetText()
			app.SetFocus(selection)
			app.Layout.Menu.SetMenu(CloneTopicPageMenu)
		}
		return event
	})

	replicationFactor.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			params.ReplicationFactor, _ = strconv.Atoi(replicationFactor.GetText())
			app.SetFocus(selection)
			app.Layout.Menu.SetMenu(CloneTopicPageMenu)
		}
		return event
	})

	partitions.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			params.Partitions, _ = strconv.Atoi(partitions.GetText())
			app.SetFocus(selection)
			app.Layout.Menu.SetMenu(CloneTopicPageMenu)
		}
		return event
	})

	configTextArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			propertiesText := configTextArea.GetText()
			params.Config = parseConfig(propertiesText)
			app.SetFocus(selection)
			app.Layout.Menu.SetMenu(CloneTopicPageMenu)
			return nil
		}
		return event
	})

	inputFields := []*tview.InputField{topicName, replicationFactor, partitions}
	selection.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		row, _ := selection.GetSelection()

		if IsKey(event, 'e') {
			if row < len(inputFields) {
				app.SetFocus(inputFields[row])
				app.Layout.Menu.SetMenu(CloneTopicInputMenu)
			} else if row == 3 {
				app.SetFocus(configTextArea)
				app.Layout.Menu.SetMenu(CloneTopicInputMenu)
			}
		}

		if IsCtrlEnter(event) {
			params.TopicName = topicName.GetText()
			params.ReplicationFactor, _ = strconv.Atoi(replicationFactor.GetText())
			params.Partitions, _ = strconv.Atoi(partitions.GetText())
			params.Config = parseConfig(configTextArea.GetText())

			if err := params.validate(); err != nil {
				SendStatusWithDefaultTTL(fmt.Sprintf("[red]%s", err.Error()))
				return event
			}

			app.CreateTopicResultHandler(
				params.TopicName,
				params.Partitions,
				params.ReplicationFactor,
				params.Config,
			)
			app.HideModalPage(CloneTopic)
		}

		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(CloneTopic)
		}

		return event
	})

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(f, 0, 1, true)
	flex.SetTitle(fmt.Sprintf(" Clone Topic (from %s) ", sourceTopic))
	flex.SetBorder(true)

	modal := util.NewTopicModal(flex)
	app.Layout.PagesRegistry.UI.Pages.AddPage(CloneTopic, modal, true, true)
	app.Layout.PagesRegistry.UI.Pages.ShowPage(CloneTopic)
	app.SetFocus(topicName)
	app.Layout.Menu.SetMenu(CloneTopicInputMenu)
}

func (app *App) UpdateTopic(topicName string) {
	resultCh := make(chan *client.TopicResult)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusInfinite("fetching topic configuration")
	c.DescribeTopic(topicName, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case topicResult := <-resultCh:
				app.QueueUpdateDraw(func() {
					app.NewUpdateTopicModal(topicName, topicResult)
					app.ShowModalPage(EditTopic)
					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to fetch topic config")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to fetch topic config: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while fetching topic config")
				SendStatusWithDefaultTTL("[red]timeout while fetching topic config")
				return
			}
		}
	}()
}

func (app *App) UpdateTopicResultHandler(
	name string,
	currentPartitions int,
	newPartitions int,
	config map[string]string,
) {
	c := app.GetCurrentKafkaClient()

	if len(config) > 0 {
		resultCh := make(chan bool)
		errorCh := make(chan error)
		SendStatusInfinite("updating topic configuration")
		c.UpdateTopicConfig(name, config, resultCh, errorCh)
		ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

		go func() {
			for {
				select {
				case <-resultCh:
					SendStatus(
						fmt.Sprintf("topic '%s' config has been updated", name),
						2*time.Second,
						false,
					)
					cancel()
					return
				case err := <-errorCh:
					log.Error().Err(err).Msg("failed to update topic configuration")
					SendStatusWithDefaultTTL(
						fmt.Sprintf(
							"[red]failed to update topic configuration: %s",
							err.Error(),
						),
					)
					cancel()
					return
				case <-ctx.Done():
					log.Error().Msg("timeout while updating topic config")
					SendStatusWithDefaultTTL("[red]timeout while updating topic config")
					return
				}
			}
		}()
	}

	if newPartitions > currentPartitions {
		resultCh := make(chan bool)
		errorCh := make(chan error)
		SendStatusInfinite("increasing partition count")
		c.IncreasePartitions(name, newPartitions, resultCh, errorCh)
		ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

		go func() {
			for {
				select {
				case <-resultCh:
					SendStatus(
						fmt.Sprintf(
							"topic '%s' partitions increased to %d",
							name,
							newPartitions,
						),
						2*time.Second,
						false,
					)
					cancel()
					return
				case err := <-errorCh:
					log.Error().Err(err).Msg("failed to increase partition count")
					SendStatusWithDefaultTTL(
						fmt.Sprintf("[red]failed to increase partition count: %s", err.Error()),
					)
					cancel()
					return
				case <-ctx.Done():
					log.Error().Msg("timeout while increasing partition count")
					SendStatusWithDefaultTTL("[red]timeout while increasing partition count")
					return
				}
			}
		}()
	}
}

func (app *App) DeleteTopic(topicName string) {
	messageText := tview.NewTextView().
		SetText(fmt.Sprintf("Topic [red::b]%s[-::-] will be deleted. Confirm?", topicName)).
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	messageText.SetBorder(true).
		SetTitle(" Confirm Deletion ").
		SetBorderPadding(0, 0, 1, 1)

	messageText.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			app.DeleteTopicResultHandler(topicName)
			app.HideModalPage(DeleteTopic)
			Publish(TopicsChannel, GetTopicsEventType, Payload{nil, true})
		}

		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(DeleteTopic)
		}

		return event
	})

	modal := util.NewConfirmationModal(messageText)
	app.Layout.PagesRegistry.UI.Pages.AddPage(DeleteTopic, modal, true, true)
	app.Layout.PagesRegistry.UI.Pages.ShowPage(DeleteTopic)
}

func (app *App) DeleteTopicResultHandler(name string) {
	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusInfinite("deleting topic")
	c.DeleteTopic(name, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatus(fmt.Sprintf("topic '%s' has been deleted", name), 2*time.Second, false)
				Publish(TopicsChannel, GetTopicsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to delete topic")
				SendStatusWithDefaultTTL(fmt.Sprintf("[red]failed to delete topic: %s", err.Error()))
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while deleting topic")
				SendStatusWithDefaultTTL("[red]timeout while deleting topic")
				return
			}
		}
	}()
}

// recreateUITimeout bounds the UI-side wait for a recreate to complete. It must exceed the
// client's worst case (delete + waiting for the deletion to propagate + create) so a
// slow-but-successful recreate is not reported as a spurious timeout.
const recreateUITimeout = 3 * time.Minute

// RecreateTopic fetches the source topic's configuration and opens a confirmation modal to
// delete the topic and re-create it empty with the same name, partition count, replication
// factor, and config. All existing messages are lost.
func (app *App) RecreateTopic(sourceTopic string) {
	resultCh := make(chan *client.TopicResult)
	errorCh := make(chan error)

	c := app.GetCurrentKafkaClient()
	SendStatusInfinite("fetching topic configuration")
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
					app.NewRecreateTopicModal(
						sourceTopic,
						partitionCount,
						replicationFactor,
						sourceConfig,
					)
					app.ShowModalPage(RecreateTopic)
					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to fetch topic config")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to fetch topic config: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while fetching topic config")
				SendStatusWithDefaultTTL("[red]timeout while fetching topic config")
				return
			}
		}
	}()
}

// NewRecreateTopicModal builds a confirmation modal warning that recreating the topic
// deletes all of its data, then re-creates it with the captured settings on confirm.
func (app *App) NewRecreateTopicModal(
	topicName string,
	partitions int,
	replicationFactor int,
	config map[string]string,
) {
	messageText := tview.NewTextView().
		SetText(fmt.Sprintf(
			"Topic [red::b]%s[-::-] will be deleted and recreated empty (all data lost). Confirm?",
			topicName,
		)).
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	messageText.SetBorder(true).
		SetTitle(" Confirm Recreation ").
		SetBorderPadding(0, 0, 1, 1)

	messageText.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			app.RecreateTopicResultHandler(topicName, partitions, replicationFactor, config)
			app.HideModalPage(RecreateTopic)
		}

		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(RecreateTopic)
		}

		return event
	})

	modal := util.NewConfirmationModal(messageText)
	app.Layout.PagesRegistry.UI.Pages.AddPage(RecreateTopic, modal, true, true)
	app.Layout.PagesRegistry.UI.Pages.ShowPage(RecreateTopic)
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
	SendStatusInfinite("recreating topic")
	c.RecreateTopic(name, numPartitions, replicationFactor, config, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), recreateUITimeout)

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatus(fmt.Sprintf("topic '%s' has been recreated", name), 2*time.Second, false)
				Publish(TopicsChannel, GetTopicsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to recreate topic")
				SendStatusWithDefaultTTL(fmt.Sprintf("[red]failed to recreate topic: %s", err.Error()))
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while recreating topic")
				SendStatusWithDefaultTTL("[red]timeout while recreating topic")
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

func (app *App) NewUpdateTopicModal(topicName string, topicResult *client.TopicResult) {
	width := 40

	currentConfig := make(map[string]string)
	partitionCount := 0
	replicationFactor := 0

	if len(topicResult.TopicDescriptions) > 0 {
		desc := topicResult.TopicDescriptions[0]
		partitionCount = len(desc.Partitions)
		if len(desc.Partitions) > 0 {
			replicationFactor = len(desc.Partitions[0].Replicas)
		}
	}

	for _, configResult := range topicResult.Config {
		for _, entry := range configResult.Config {
			// Only include non-default, non-readonly configs
			if !entry.IsDefault && !entry.IsReadOnly {
				currentConfig[entry.Name] = entry.Value
			}
		}
	}

	topicNameField := tview.NewInputField().
		SetFieldWidth(width).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetText(topicName)
	topicNameField.SetDisabled(true)

	replicationFactorField := tview.NewInputField().
		SetFieldWidth(width).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetText(fmt.Sprintf("%d", replicationFactor))
	replicationFactorField.SetDisabled(true)

	partitionsField := tview.NewInputField().
		SetFieldWidth(width).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder)).
		SetText(fmt.Sprintf("%d", partitionCount))
	partitionsField.SetAcceptanceFunc(tview.InputFieldInteger)

	configTextArea := tview.NewTextArea()

	var configLines []string
	for key, value := range currentConfig {
		configLines = append(configLines, fmt.Sprintf("%s=%s", key, value))
	}
	if len(configLines) > 0 {
		configTextArea.SetText(strings.Join(configLines, "\n"), false)
	}

	selection := tview.NewTable()
	selection.SetCell(
		0,
		0,
		tview.NewTableCell("Name:").
			SetAlign(tview.AlignRight).
			SetSelectable(false).
			SetTextColor(tcell.GetColor(app.Colors.Karat.Label.FgColor)),
	)
	selection.SetCell(
		1,
		0,
		tview.NewTableCell("Replication factor:").
			SetAlign(tview.AlignRight).
			SetSelectable(false).
			SetTextColor(tcell.GetColor(app.Colors.Karat.Label.FgColor)),
	)
	selection.SetCell(
		2,
		0,
		tview.NewTableCell("Partitions:").
			SetAlign(tview.AlignRight).
			SetTextColor(tcell.GetColor(app.Colors.Karat.Label.FgColor)),
	)
	selection.SetCell(
		3,
		0,
		tview.NewTableCell("Configs:").
			SetAlign(tview.AlignRight).
			SetTextColor(tcell.GetColor(app.Colors.Karat.Label.FgColor)),
	)
	selection.SetSelectable(true, false)
	selection.SetBorderPadding(0, 0, 1, 0)
	selection.SetSelectedStyle(
		tcell.StyleDefault.Foreground(
			tcell.GetColor(app.Colors.Karat.Selection.FgColor),
		).Background(
			tcell.GetColor(app.Colors.Karat.Selection.BgColor),
		),
	)

	f := tview.NewFlex()
	f.SetDirection(tview.FlexColumn)
	f.AddItem(selection, 20, 0, true)
	f.AddItem(tview.NewBox(), 3, 0, false)

	inputs := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(topicNameField, 1, 0, false).
		AddItem(replicationFactorField, 1, 0, false).
		AddItem(partitionsField, 1, 0, false).
		AddItem(configTextArea, 0, 1, false)

	f.AddItem(inputs, 40, 0, false).
		AddItem(tview.NewBox(), 0, 1, false)

	var editedConfig map[string]string

	partitionsField.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			app.SetFocus(selection)
			app.Layout.Menu.SetMenu(EditTopicPageMenu)
		}

		if IsKey(event, 'e') {
			app.SetFocus(partitionsField)
			app.Layout.Menu.SetMenu(EditTopicInputMenu)
		}

		return event
	})

	configTextArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			propertiesText := configTextArea.GetText()
			editedConfig = parseConfig(propertiesText)
			app.SetFocus(selection)
			app.Layout.Menu.SetMenu(EditTopicPageMenu)
			return nil
		}

		if IsKey(event, 'e') {
			app.SetFocus(configTextArea)
			app.Layout.Menu.SetMenu(EditTopicInputMenu)
		}

		return event
	})

	selection.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		row, _ := selection.GetSelection()

		if IsKey(event, 'e') {
			switch row {
			case 2:
				app.SetFocus(partitionsField)
				app.Layout.Menu.SetMenu(EditTopicInputMenu)
			case 3:
				app.SetFocus(configTextArea)
				app.Layout.Menu.SetMenu(EditTopicInputMenu)
			}
		}

		if IsCtrlEnter(event) {
			propertiesText := configTextArea.GetText()
			editedConfig = parseConfig(propertiesText)

			newPartitions, err := strconv.Atoi(partitionsField.GetText())
			if err != nil || newPartitions <= 0 {
				SendStatusWithDefaultTTL("[red]partitions must be a positive integer")
				return event
			}
			if newPartitions < partitionCount {
				SendStatusWithDefaultTTL("[red]partition count cannot be decreased")
				return event
			}

			app.UpdateTopicResultHandler(topicName, partitionCount, newPartitions, editedConfig)
			app.HideModalPage(EditTopic)
			Publish(TopicsChannel, GetTopicsEventType, Payload{nil, false})
		}

		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(EditTopic)
		}

		return event
	})

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(f, 0, 1, true)
	flex.SetTitle(fmt.Sprintf(" Edit Topic: %s ", topicName))
	flex.SetBorder(true)

	modal := util.NewTopicModal(flex)
	app.Layout.PagesRegistry.UI.Pages.AddPage(EditTopic, modal, true, false)
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

func parseConfig(text string) map[string]string {
	properties := make(map[string]string)

	lines := strings.Split(text, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			if key != "" && value != "" {
				properties[key] = value
			}
		}
	}

	return properties
}
