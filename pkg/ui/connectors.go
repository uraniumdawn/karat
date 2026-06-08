// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"
	"github.com/sahilm/fuzzy"

	"github.com/uraniumdawn/karat/pkg/connect"
	"github.com/uraniumdawn/karat/pkg/util"
)

const (
	// GetConnectorsEventType is the event type for fetching connectors.
	GetConnectorsEventType EventType = "connectors:get"
	// GetConnectorEventType is the event type for fetching a single connector.
	GetConnectorEventType EventType = "connector:get"
	// DeleteConnectorEventType is the event type for deleting a connector.
	DeleteConnectorEventType EventType = "connector:delete"
	// DeleteConnectorOffsetsEventType is the event type for deleting a connector's offsets.
	DeleteConnectorOffsetsEventType EventType = "connector:offsets:delete"
)

// ConnectorsChannel is the channel for connector events.
var ConnectorsChannel = make(chan Event)

// RunConnectorsEventHandler processes connector events from the channel.
func (app *App) RunConnectorsEventHandler(ctx context.Context, in chan Event) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("shutting down connectors event handler")
				return
			case event := <-in:
				switch event.Type {
				case GetConnectorsEventType:
					pageName := util.BuildPageKey(app.Selected.Connect.Name, Connectors)
					force := event.Payload.Force
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.SwitchToPage(pageName)
					} else {
						app.Connectors()
					}

				case GetConnectorEventType:
					connectorName := event.Payload.Data.(string)
					force := event.Payload.Force
					pageName := util.BuildPageKey(
						app.Selected.Connect.Name,
						Connectors,
						connectorName,
					)
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.SwitchToPage(pageName)
					} else {
						app.ConnectorDetail(connectorName)
					}

				case DeleteConnectorEventType:
					connectorName := event.Payload.Data.(string)
					app.QueueUpdateDraw(func() {
						app.DeleteConnector(connectorName)
						app.ShowModalPage(DeleteConnector)
					})

				case DeleteConnectorOffsetsEventType:
					connectorName := event.Payload.Data.(string)
					app.QueueUpdateDraw(func() {
						app.DeleteConnectorOffsets(connectorName)
						app.ShowModalPage(DeleteConnectorOffsets)
					})
				}
			}
		}
	}()
}

// Connectors fetches and displays the list of Kafka connectors.
func (app *App) Connectors() {
	resultCh := make(chan []string)
	errorCh := make(chan error)

	c := app.GetCurrentConnectClient()
	pageKey := util.BuildPageKey(app.Selected.Connect.Name, Connectors)
	SendStatusInfinite("getting connectors...")
	c.ListConnectors(resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case connectorNames := <-resultCh:
				app.QueueUpdateDraw(func() {
					statuses := app.fetchConnectorStatuses(c, connectorNames)

					table := app.NewConnectorsTable(connectorNames, statuses)
					title := util.BuildTitle(Connectors,
						"["+strconv.Itoa(len(connectorNames))+"]")
					if label := app.GetAutoUpdateLabel(pageKey); label != "" {
						title = title + "[" + label + "]"
					}
					table.SetTitle(title)
					sortCol := 0
					sortDesc := false
					labelColor := tcell.GetColor(app.Colors.Karat.Label.FgColor)

					table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
						if event.Key() == tcell.KeyCtrlU {
							Publish(
								ConnectorsChannel,
								GetConnectorsEventType,
								Payload{nil, true},
							)
						}
						if IsKey(event, 'd') {
							row, _ := table.GetSelection()
							connectorName := table.GetCell(row, 0).Text
							Publish(
								ConnectorsChannel,
								GetConnectorEventType,
								Payload{connectorName, false},
							)
						}

						if IsKey(event, 'a') {
							if app.IsCurrentConnectReadOnly() {
								SendStatusWithDefaultTTL(
									"[red]cluster is in read-only mode",
								)
								return event
							}
							row, _ := table.GetSelection()
							connectorName := table.GetCell(row, 0).Text
							connectorState := table.GetCell(row, 1).Text
							app.ConnectorActionsModal(connectorName, connectorState)
							app.ShowModalPage(ConnectorActions)
						}

						if IsKey(event, 'e') {
							if app.IsCurrentConnectReadOnly() {
								SendStatusWithDefaultTTL(
									"[red]cluster is in read-only mode",
								)
								return event
							}
							row, _ := table.GetSelection()
							connectorName := table.GetCell(row, 0).Text
							app.EditConnectorConfig(connectorName)
						}

						if event.Key() == tcell.KeyCtrlD {
							if app.IsCurrentConnectReadOnly() {
								SendStatusWithDefaultTTL(
									"[red]cluster is in read-only mode",
								)
								return event
							}
							row, _ := table.GetSelection()
							connectorName := table.GetCell(row, 0).Text
							Publish(
								ConnectorsChannel,
								DeleteConnectorEventType,
								Payload{connectorName, false},
							)
						}

						if IsKey(event, '1') {
							if sortCol == 0 {
								sortDesc = !sortDesc
							} else {
								sortCol = 0
								sortDesc = false
							}
							sortConnectorsTable(
								table,
								connectorNames,
								statuses,
								sortCol,
								sortDesc,
								labelColor,
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
							sortConnectorsTable(
								table,
								connectorNames,
								statuses,
								sortCol,
								sortDesc,
								labelColor,
							)
							table.ScrollToBeginning()
							return event
						}

						if IsKey(event, '3') {
							if sortCol == 2 {
								sortDesc = !sortDesc
							} else {
								sortCol = 2
								sortDesc = false
							}
							sortConnectorsTable(
								table,
								connectorNames,
								statuses,
								sortCol,
								sortDesc,
								labelColor,
							)
							table.ScrollToBeginning()
							return event
						}

						return event
					})

					app.AddToPagesRegistry(pageKey, table, ConnectorsPageMenu, true)

					app.AssignSearch(func(text string) {
						filterConnectorsTable(table, connectorNames, statuses, text, labelColor)
						util.SetSearchableTableTitle(table, title, text)
						table.ScrollToBeginning()
					})

					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to list connectors")
				SendStatusWithDefaultTTL(fmt.Sprintf("[red]failed to list connectors: %s", err.Error()))
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while listing connectors")
				SendStatusWithDefaultTTL("[red]timeout while listing connectors")
				return
			}
		}
	}()
}

// fetchConnectorStatusesMaxConcurrency caps the number of in-flight status
// requests issued by fetchConnectorStatuses.
const fetchConnectorStatusesMaxConcurrency = 8

// fetchConnectorStatuses retrieves status for each connector and returns a map keyed by name.
// Requests are fanned out concurrently, bounded by fetchConnectorStatusesMaxConcurrency.
func (app *App) fetchConnectorStatuses(c *connect.Client, names []string) map[string]*connect.ConnectorStatus {
	type result struct {
		name   string
		status *connect.ConnectorStatus
	}

	sem := make(chan struct{}, fetchConnectorStatusesMaxConcurrency)
	resultsCh := make(chan result, len(names))
	var wg sync.WaitGroup

	for _, name := range names {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			statusCh := make(chan *connect.ConnectorStatus)
			errorCh := make(chan error)
			c.GetConnectorStatus(name, statusCh, errorCh)

			select {
			case status := <-statusCh:
				resultsCh <- result{name, status}
			case err := <-errorCh:
				log.Debug().Err(err).Str("connector", name).Msg("failed to get status")
			}
		}()
	}

	wg.Wait()
	close(resultsCh)

	statuses := make(map[string]*connect.ConnectorStatus, len(names))
	for r := range resultsCh {
		statuses[r.name] = r.status
	}
	return statuses
}

// ConnectorDetail fetches and displays detailed status for a connector.
func (app *App) ConnectorDetail(name string) {
	resultCh := make(chan *connect.ConnectorDetail)
	errorCh := make(chan error)

	c := app.GetCurrentConnectClient()
	pageKey := util.BuildPageKey(app.Selected.Connect.Name, Connectors, name)
	SendStatusInfinite(fmt.Sprintf("getting connector '%s' details...", name))
	c.DescribeConnector(name, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case detail := <-resultCh:
				app.QueueUpdateDraw(func() {
					title := util.BuildTitle(Connectors, name)
					if label := app.GetAutoUpdateLabel(pageKey); label != "" {
						title = title + "[" + label + "]"
					}

					isRunning := strings.ToUpper(detail.Status.Connector.State) == "RUNNING"
					isReadOnly := app.IsCurrentConnectReadOnly()
					descMenu := ConnectorDescriptionPageMenu
					if isRunning && !isReadOnly {
						descMenu = ConnectorDescriptionPageMenuRunning
					}

					desc := app.NewDescription(title)
					desc.SetText(detail.String())
					desc.SetInputCapture(
						app.WithHScroll(desc, func(event *tcell.EventKey) *tcell.EventKey {
							if event.Key() == tcell.KeyCtrlU {
								Publish(
									ConnectorsChannel,
									GetConnectorEventType,
									Payload{name, true},
								)
							}
							if IsKey(event, 'e') {
								app.EditConnectorConfig(name)
							}
							if isRunning && !isReadOnly && IsKey(event, 'a') {
								app.TaskActionsModal(detail)
								app.ShowModalPage(TaskActions)
							}
							if IsKey(event, 'o') {
								currentPage, _ := app.Layout.PagesRegistry.UI.Pages.GetFrontPage()
								if currentPage == ConnectorOffsets {
									app.HideModalPage(ConnectorOffsets)
								} else {
									app.ConnectorOffsetsModal(
										name,
										detail.Status.Connector.State,
									)
								}
							}
							return event
						}),
					)

					app.AddToPagesRegistry(pageKey, desc, descMenu, false)
					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to get connector details")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to get connector details: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while getting connector details")
				SendStatusWithDefaultTTL("[red]timeout while getting connector details")
				return
			}
		}
	}()
}

// addConnectorsTableHeader adds a fixed header row (row 0) with label-coloured cells.
func addConnectorsTableHeader(table *tview.Table, labelColor tcell.Color) {
	util.SetTableHeaders(table, labelColor, "Name", "State", "Type", "Tasks")
}

// sortConnectorsTable rebuilds the table sorted by col (0=Name, 1=State, 2=Type).
// State and Type tiebreak by Name ascending. Adds ↑/↓ indicator to the active header cell.
func sortConnectorsTable(
	table *tview.Table,
	connectorNames []string,
	statuses map[string]*connect.ConnectorStatus,
	col int,
	desc bool,
	labelColor tcell.Color,
) {
	type entry struct {
		name  string
		state string
		ctype string
		tasks string
	}

	entries := make([]entry, 0, len(connectorNames))
	for _, name := range connectorNames {
		if status, ok := statuses[name]; ok {
			entries = append(entries, entry{
				name:  name,
				state: status.Connector.State,
				ctype: strings.ToLower(status.Type),
				tasks: fmt.Sprintf("%d/%d", runningTasks(status.Tasks), len(status.Tasks)),
			})
		} else {
			entries = append(entries, entry{name: name, state: "unknown", ctype: "-", tasks: "-"})
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		switch col {
		case 1:
			if entries[i].state != entries[j].state {
				if desc {
					return entries[i].state > entries[j].state
				}
				return entries[i].state < entries[j].state
			}
			return entries[i].name < entries[j].name
		case 2:
			if entries[i].ctype != entries[j].ctype {
				if desc {
					return entries[i].ctype > entries[j].ctype
				}
				return entries[i].ctype < entries[j].ctype
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
	addConnectorsTableHeader(table, labelColor)

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
		table.GetCell(0, 2).SetText("Type" + indicator)
	}

	for i, e := range entries {
		table.SetCell(i+1, 0, tview.NewTableCell(e.name))
		table.SetCell(i+1, 1, tview.NewTableCell(e.state))
		table.SetCell(i+1, 2, tview.NewTableCell(e.ctype))
		table.SetCell(i+1, 3, tview.NewTableCell(e.tasks))
	}
}

// NewConnectorsTable creates a table displaying connectors with their status.
func (app *App) NewConnectorsTable(connectorNames []string, statuses map[string]*connect.ConnectorStatus) *tview.Table {
	table := tview.NewTable()
	table.SetSelectable(true, false).
		SetBorder(true).
		SetBorderPadding(0, 0, 1, 0)

	if app.Colors != nil {
		table.SetSelectedStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Selection.FgColor),
			).Background(
				tcell.GetColor(app.Colors.Karat.Selection.BgColor),
			),
		)
	}
	table.SetFixed(1, 0)

	labelColor := tcell.GetColor(app.Colors.Karat.Label.FgColor)
	sortConnectorsTable(table, connectorNames, statuses, 0, false, labelColor)

	return table
}

func filterConnectorsTable(
	table *tview.Table,
	connectorNames []string,
	statuses map[string]*connect.ConnectorStatus,
	filter string,
	labelColor tcell.Color,
) {
	table.Clear()
	addConnectorsTableHeader(table, labelColor)

	var names []string
	if filter == "" {
		names = make([]string, len(connectorNames))
		copy(names, connectorNames)
		sort.Strings(names)
	} else {
		matches := fuzzy.Find(filter, connectorNames)
		names = make([]string, len(matches))
		for i, match := range matches {
			names[i] = match.Str
		}
	}

	for i, name := range names {
		table.SetCell(i+1, 0, tview.NewTableCell(name))
		if status, ok := statuses[name]; ok {
			table.SetCell(i+1, 1, tview.NewTableCell(status.Connector.State))
			table.SetCell(i+1, 2, tview.NewTableCell(strings.ToLower(status.Type)))
			table.SetCell(
				i+1,
				3,
				tview.NewTableCell(fmt.Sprintf("%d/%d", runningTasks(status.Tasks), len(status.Tasks))),
			)
		} else {
			table.SetCell(i+1, 1, tview.NewTableCell("-"))
			table.SetCell(i+1, 2, tview.NewTableCell("-"))
			table.SetCell(i+1, 3, tview.NewTableCell("-"))
		}
	}
}

// runningTasks counts how many tasks are in RUNNING state.
func runningTasks(tasks []connect.TaskStateInfo) int {
	count := 0
	for _, t := range tasks {
		if strings.ToUpper(t.State) == "RUNNING" {
			count++
		}
	}
	return count
}

// EditConnectorConfig opens the connector config in an external editor for editing.
func (app *App) EditConnectorConfig(name string) {
	resultCh := make(chan map[string]interface{})
	errorCh := make(chan error)

	c := app.GetCurrentConnectClient()
	SendStatusInfinite("fetching connector config for edit...")
	c.GetConnectorConfig(name, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		defer cancel()

		select {
		case config := <-resultCh:
			app.QueueUpdateDraw(func() {
				app.openEditorForConfig(name, config)
			})
		case err := <-errorCh:
			log.Error().Err(err).Msg("failed to fetch connector config for edit")
			SendStatusWithDefaultTTL(
				fmt.Sprintf("[red]failed to fetch connector config: %s", err.Error()),
			)
		case <-ctx.Done():
			log.Error().Msg("timeout while fetching connector config for edit")
			SendStatusWithDefaultTTL("[red]timeout while fetching connector config")
		}
	}()
}

// openEditorForConfig creates a temp file, opens the editor, and handles the result.
func (app *App) openEditorForConfig(name string, config map[string]interface{}) {
	// Marshal to pretty JSON
	oldJSON, err := json.MarshalIndent(config, "", "    ")
	if err != nil {
		SendStatusWithDefaultTTL("[red]failed to marshal connector config")
		return
	}
	oldHash := sha256.Sum256(bytes.TrimRight(oldJSON, "\r\n\t "))

	// Create temp file
	tmpFile, err := os.CreateTemp("", "connector-config-*.json")
	if err != nil {
		log.Error().Err(err).Msg("failed to create temp file")
		SendStatusWithDefaultTTL("[red]failed to create temp file")
		return
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if _, err := tmpFile.Write(oldJSON); err != nil {
		log.Error().Err(err).Msg("failed to write temp file")
		SendStatusWithDefaultTTL("[red]failed to write temp file")
		return
	}
	if err := tmpFile.Close(); err != nil {
		log.Error().Err(err).Msg("failed to close temp file")
		SendStatusWithDefaultTTL("[red]failed to close temp file")
		return
	}

	// Open editor
	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = "vim"
	}

	// Parse editor command and arguments
	parts := strings.Fields(editor)
	cmd := exec.Command(parts[0], append(parts[1:], tmpPath)...)

	// os.Stdin/Stdout/Stderr are redirected to the log file by InitLogger.
	// Open /dev/tty directly so the editor gets the actual terminal.
	tty, ttyErr := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if ttyErr == nil {
		cmd.Stdin = tty
		cmd.Stdout = tty
		cmd.Stderr = tty
	} else {
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	// Temporarily stop the TUI so the editor can take over the terminal
	app.Suspend(func() {
		_ = cmd.Run()
		if tty != nil {
			_ = tty.Close()
		}
	})

	// Read edited content
	newContent, err := os.ReadFile(tmpPath)
	if err != nil {
		log.Error().Err(err).Msg("failed to read edited config")
		SendStatusWithDefaultTTL("[red]failed to read edited config")
		return
	}

	// Check if content changed (trim trailing whitespace so editor newline conventions don't matter)
	newHash := sha256.Sum256(bytes.TrimRight(newContent, "\r\n\t "))
	if bytes.Equal(oldHash[:], newHash[:]) {
		SendStatusWithDefaultTTL("[yellow]no changes detected")
		return
	}

	// Validate JSON
	var newConfig map[string]interface{}
	if err := json.Unmarshal(newContent, &newConfig); err != nil {
		SendStatusWithDefaultTTL(fmt.Sprintf("[red]invalid JSON: %s", err.Error()))
		return
	}

	// Show confirmation modal
	ClearStatus()
	app.ConnectorConfigConfirm(name, newConfig)
}

// ConnectorConfigConfirm shows a confirmation modal with the new connector config.
func (app *App) ConnectorConfigConfirm(name string, newConfig map[string]interface{}) {
	prettyJSON, err := json.MarshalIndent(newConfig, "", "    ")
	if err != nil {
		SendStatusWithDefaultTTL("[red]failed to format config for display")
		return
	}

	messageText := tview.NewTextView().
		SetText(string(prettyJSON)).
		SetTextAlign(tview.AlignLeft).
		SetDynamicColors(false)

	messageText.SetBorder(true).
		SetTitle(fmt.Sprintf(" Confirm Config Update: %s ", name)).
		SetBorderPadding(0, 0, 1, 1)

	messageText.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			if app.IsCurrentConnectReadOnly() {
				SendStatusWithDefaultTTL("[red]cluster is in read-only mode")
				return nil
			}
			app.UpdateConnectorConfig(name, newConfig)
			app.RemoveFromPagesRegistry(ConnectorConfigConfirm)
			return nil
		}

		if event.Key() == tcell.KeyEsc {
			app.RemoveFromPagesRegistry(ConnectorConfigConfirm)
			return nil
		}

		return event
	})

	app.AddToPagesRegistry(ConnectorConfigConfirm, messageText, ConnectorConfigEditPageMenu, false)
}

// UpdateConnectorConfig applies the updated connector config.
func (app *App) UpdateConnectorConfig(name string, config map[string]interface{}) {
	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentConnectClient()
	SendStatusInfinite("updating connector config...")
	c.UpdateConnectorConfig(name, config, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatus(
					fmt.Sprintf("connector '%s' config updated", name),
					2*time.Second,
					false,
				)
				Publish(ConnectorsChannel, GetConnectorEventType, Payload{name, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to update connector config")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to update connector config: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while updating connector config")
				SendStatusWithDefaultTTL("[red]timeout while updating connector config")
				return
			}
		}
	}()
}

// TaskActionsModal shows a modal table of connector tasks with per-task restart action.
func (app *App) TaskActionsModal(detail *connect.ConnectorDetail) {
	tasks := detail.Status.Tasks
	if len(tasks) == 0 {
		SendStatusWithDefaultTTL("[yellow]no tasks found for this connector")
		return
	}

	connectorName := detail.Status.Name
	actions := []string{"RESTART"}

	labelColor := tcell.GetColor(app.Colors.Karat.Label.FgColor)
	actionColor := tcell.GetColor(app.Colors.Karat.Foreground)

	taskTable := tview.NewTable()
	taskTable.SetSelectable(true, false)
	taskTable.SetFixed(1, 0)
	taskTable.SetBorderPadding(0, 0, 1, 0)
	taskTable.SetSelectedStyle(
		tcell.StyleDefault.Foreground(
			tcell.GetColor(app.Colors.Karat.Selection.FgColor),
		).Background(
			tcell.GetColor(app.Colors.Karat.Selection.BgColor),
		),
	)
	taskTable.SetCell(0, 0, tview.NewTableCell("Task").SetSelectable(false).SetTextColor(labelColor))
	taskTable.SetCell(0, 1, tview.NewTableCell("State").SetSelectable(false).SetTextColor(labelColor))
	taskTable.SetCell(0, 2, tview.NewTableCell("Worker").SetSelectable(false).SetTextColor(labelColor))
	taskTable.SetCell(0, 3, tview.NewTableCell("Action").SetSelectable(false).SetTextColor(labelColor))

	for i, task := range tasks {
		stateColor := tcell.GetColor(app.Colors.Karat.Foreground)
		taskTable.SetCell(i+1, 0, tview.NewTableCell(strconv.Itoa(task.ID)))
		taskTable.SetCell(i+1, 1, tview.NewTableCell(task.State).SetTextColor(stateColor))
		taskTable.SetCell(i+1, 2, tview.NewTableCell(task.WorkerID))
		taskTable.SetCell(i+1, 3, tview.NewTableCell(""))
	}

	modalHeight := len(tasks) + 3 // header + task rows + top/bottom borders
	if modalHeight > 13 {
		modalHeight = 13
	}

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(taskTable, 0, 1, true)
	flex.SetTitle(fmt.Sprintf(" Task Actions: %s ", connectorName))
	flex.SetBorder(true)

	taskTable.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		row, _ := taskTable.GetSelection()
		switch {
		case event.Key() == tcell.KeyTab:
			if row > 0 && row <= len(tasks) {
				cell := taskTable.GetCell(row, 3)
				if cell.Text == "" {
					cell.SetText(actions[0]).SetTextColor(actionColor)
				} else {
					idx := 0
					for i, a := range actions {
						if a == cell.Text {
							idx = i
							break
						}
					}
					next := actions[(idx+1)%len(actions)]
					if next == actions[0] && len(actions) == 1 {
						cell.SetText("").SetTextColor(tcell.ColorDefault)
					} else {
						cell.SetText(next).SetTextColor(actionColor)
					}
				}
			}
		case IsCtrlEnter(event):
			if row > 0 && row <= len(tasks) {
				action := taskTable.GetCell(row, 3).Text
				if action == "" {
					SendStatusWithDefaultTTL(
						"[yellow]press Tab to set an action for the selected task",
					)
					return event
				}
				taskID := tasks[row-1].ID
				app.ExecuteTaskAction(connectorName, taskID, action)
				app.HideModalPage(TaskActions)
			}
		case event.Key() == tcell.KeyEsc:
			app.HideModalPage(TaskActions)
		}
		return event
	})

	modal := util.NewResourceModal(flex, modalHeight)
	app.Layout.PagesRegistry.UI.Pages.AddPage(TaskActions, modal, true, false)
}

// ConnectorOffsetsModal fetches and displays a modal table of a connector's
// current partition offsets (topic:partition -> current offset). state is the
// connector's current state (e.g. "RUNNING", "STOPPED"), used to gate offset deletion.
func (app *App) ConnectorOffsetsModal(name, state string) {
	app.fetchConnectorOffsets(name, func(offsets []connect.ConnectorOffset) {
		app.buildConnectorOffsetsModal(name, state, offsets)
	})
}

// fetchConnectorOffsets retrieves a connector's current offsets asynchronously and
// invokes onSuccess on the UI goroutine with the result. An empty result, an error,
// or a timeout is reported via the status bar instead of calling onSuccess.
func (app *App) fetchConnectorOffsets(name string, onSuccess func([]connect.ConnectorOffset)) {
	resultCh := make(chan []connect.ConnectorOffset)
	errorCh := make(chan error)

	c := app.GetCurrentConnectClient()
	SendStatusInfinite(fmt.Sprintf("getting connector '%s' offsets...", name))
	c.GetConnectorOffsets(name, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case offsets := <-resultCh:
				if len(offsets) == 0 {
					ClearStatus()
					SendStatusWithDefaultTTL("[yellow]no offsets found for this connector")
					cancel()
					return
				}
				app.QueueUpdateDraw(func() {
					onSuccess(offsets)
					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to get connector offsets")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to get connector offsets: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while getting connector offsets")
				SendStatusWithDefaultTTL("[red]timeout while getting connector offsets")
				return
			}
		}
	}()
}

// buildConnectorOffsetsModal builds and shows the connector offsets modal.
func (app *App) buildConnectorOffsetsModal(name, state string, offsets []connect.ConnectorOffset) {
	desc := app.NewDescription(fmt.Sprintf(" Offsets: %s ", name))
	desc.SetText(connect.FormatConnectorOffsets(offsets))
	desc.SetInputCapture(
		app.WithHScroll(desc, func(event *tcell.EventKey) *tcell.EventKey {
			if event.Key() == tcell.KeyEsc || IsKey(event, 'o') {
				app.HideModalPage(ConnectorOffsets)
				return nil
			}
			if event.Key() == tcell.KeyCtrlU {
				app.fetchConnectorOffsets(name, func(fresh []connect.ConnectorOffset) {
					offsets = fresh
					desc.SetText(connect.FormatConnectorOffsets(fresh))
				})
				return nil
			}
			if event.Key() == tcell.KeyCtrlD {
				if app.IsCurrentConnectReadOnly() {
					SendStatusWithDefaultTTL("[red]cluster is in read-only mode")
					return event
				}
				if !strings.EqualFold(state, "STOPPED") {
					SendStatusWithDefaultTTL("[red]connector must be stopped to delete its offsets")
					return event
				}
				Publish(ConnectorsChannel, DeleteConnectorOffsetsEventType, Payload{name, false})
				return nil
			}
			if IsKey(event, 'c') {
				if app.IsCurrentConnectReadOnly() {
					SendStatusWithDefaultTTL("[red]cluster is in read-only mode")
					return event
				}
				app.CopyConnectorOffsetsModal(name, offsets)
				app.ShowModalPage(CopyConnectorOffsets)
				return nil
			}
			return event
		}),
	)

	_, _, _, screenHeight := app.Layout.Content.GetRect()
	if screenHeight == 0 {
		screenHeight = 40 // default fallback
	}
	modalHeight := screenHeight / 2

	modal := util.NewResourceModal(desc, modalHeight)
	app.Layout.PagesRegistry.UI.Pages.AddPage(ConnectorOffsets, modal, true, false)
	app.ShowModalPage(ConnectorOffsets)
}

// ExecuteTaskAction restarts a specific connector task.
func (app *App) ExecuteTaskAction(connectorName string, taskID int, action string) {
	action = strings.ToLower(action)

	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentConnectClient()
	SendStatusInfinite(fmt.Sprintf("%sing task %d...", action, taskID))

	switch action {
	case "restart":
		c.RestartTask(connectorName, taskID, resultCh, errorCh)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		app.Config.GetAPICallTimeout()+5*time.Second,
	)

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatus(
					fmt.Sprintf("task %d restarted", taskID),
					2*time.Second,
					false,
				)
				Publish(ConnectorsChannel, GetConnectorEventType, Payload{connectorName, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msgf("failed to %s task %d", action, taskID)
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to %s task %d: %s", action, taskID, err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msgf("timeout while %sing task %d", action, taskID)
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]timeout while %sing task %d", action, taskID),
				)
				return
			}
		}
	}()
}

// ConnectorActionsModal shows a modal with available actions for a connector.
func (app *App) ConnectorActionsModal(connectorName, state string) {
	var actions []string
	switch strings.ToUpper(state) {
	case "RUNNING":
		actions = []string{"PAUSE", "STOP", "RESTART"}
	case "PAUSED":
		actions = []string{"RESUME"}
	case "STOPPED":
		actions = []string{"RESUME"}
	default:
		actions = []string{"RESTART"}
	}

	actionIdx := 0
	width := 40

	actionField := tview.NewInputField().
		SetFieldWidth(width).
		SetText(actions[0]).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			))
	actionField.SetDisabled(true)

	selection := tview.NewTable()
	selection.SetCell(0, 0, tview.NewTableCell("Action:").SetAlign(tview.AlignRight))
	selection.SetSelectable(true, false)
	selection.SetBorderPadding(0, 0, 1, 0)
	selection.SetSelectedStyle(
		tcell.StyleDefault.Foreground(
			tcell.GetColor(app.Colors.Karat.Selection.FgColor),
		).Background(
			tcell.GetColor(app.Colors.Karat.Selection.BgColor),
		),
	)

	f := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(selection, 12, 0, true).
		AddItem(tview.NewBox(), 1, 0, false)

	inputs := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(actionField, 1, 0, false).
		AddItem(tview.NewBox(), 0, 1, false)

	f.AddItem(inputs, 40, 0, false).
		AddItem(tview.NewBox(), 0, 1, false)

	selection.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyTab {
			actionIdx = (actionIdx + 1) % len(actions)
			actionField.SetText(actions[actionIdx])
		}

		if IsCtrlEnter(event) {
			app.ExecuteConnectorAction(connectorName, actions[actionIdx])
			app.HideModalPage(ConnectorActions)
		}

		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(ConnectorActions)
		}

		return event
	})

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(f, 0, 1, true)
	flex.SetTitle(fmt.Sprintf(" Actions: %s ", connectorName))
	flex.SetBorder(true)

	modal := util.NewConnectorActionModal(flex)
	app.Layout.PagesRegistry.UI.Pages.AddPage(ConnectorActions, modal, true, false)
}

// connectorActionVerbForms maps a connector action to its present participle and
// past tense forms for status messages (plain string concatenation mangles English
// spelling rules, e.g. "pause"+"ing" -> "pauseing").
var connectorActionVerbForms = map[string]struct{ ing, ed string }{
	"pause":   {"pausing", "paused"},
	"resume":  {"resuming", "resumed"},
	"restart": {"restarting", "restarted"},
	"stop":    {"stopping", "stopped"},
}

// ExecuteConnectorAction executes a connector action (pause, resume, restart, stop).
func (app *App) ExecuteConnectorAction(name, action string) {
	action = strings.ToLower(action)
	forms, ok := connectorActionVerbForms[action]
	if !ok {
		log.Error().Str("action", action).Msg("unsupported connector action")
		SendStatusWithDefaultTTL(fmt.Sprintf("[red]unsupported connector action: %s", action))
		return
	}

	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentConnectClient()
	SendStatusInfinite(fmt.Sprintf("%s connector...", forms.ing))

	switch action {
	case "pause":
		c.PauseConnector(name, resultCh, errorCh)
	case "resume":
		c.ResumeConnector(name, resultCh, errorCh)
	case "restart":
		c.RestartConnector(name, resultCh, errorCh)
	case "stop":
		c.StopConnector(name, resultCh, errorCh)
	}

	// UI timeout is slightly longer than the HTTP client timeout so that an
	// HTTP-level error always arrives on errorCh before ctx.Done() fires,
	// preventing the race where both become ready simultaneously.
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout()+5*time.Second)

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatus(
					fmt.Sprintf("connector '%s' %s", name, forms.ed),
					2*time.Second,
					false,
				)
				Publish(ConnectorsChannel, GetConnectorsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msgf("failed to %s connector", action)
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to %s connector: %s", action, err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msgf("timeout while %s connector", forms.ing)
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]timeout while %s connector", forms.ing),
				)
				return
			}
		}
	}()
}

// DeleteConnector shows a confirmation modal for deleting a connector.
func (app *App) DeleteConnector(connectorName string) {
	messageText := tview.NewTextView().
		SetText(fmt.Sprintf("Connector [red::b]%s[-::-] will be deleted. Confirm?", connectorName)).
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	messageText.SetBorder(true).
		SetTitle(" Confirm Deletion ").
		SetBorderPadding(0, 0, 1, 1)

	messageText.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			app.DeleteConnectorResultHandler(connectorName)
			app.HideModalPage(DeleteConnector)
			Publish(ConnectorsChannel, GetConnectorsEventType, Payload{nil, true})
		}

		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(DeleteConnector)
		}

		return event
	})

	modal := util.NewConfirmationModal(messageText)
	app.Layout.PagesRegistry.UI.Pages.AddPage(DeleteConnector, modal, true, true)
	app.Layout.Menu.SetMenu(DeleteConnectorPageMenu)
	app.Layout.PagesRegistry.UI.Pages.ShowPage(DeleteConnector)
}

// DeleteConnectorResultHandler performs the connector deletion.
func (app *App) DeleteConnectorResultHandler(name string) {
	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentConnectClient()
	SendStatusInfinite("deleting connector")
	c.DeleteConnector(name, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatus(
					fmt.Sprintf("connector '%s' has been deleted", name),
					2*time.Second,
					false,
				)
				Publish(ConnectorsChannel, GetConnectorsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to delete connector")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to delete connector: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while deleting connector")
				SendStatusWithDefaultTTL("[red]timeout while deleting connector")
				return
			}
		}
	}()
}

// DeleteConnectorOffsets shows a confirmation modal for resetting a connector's offsets.
func (app *App) DeleteConnectorOffsets(connectorName string) {
	messageText := tview.NewTextView().
		SetText(fmt.Sprintf("Offsets for connector [red::b]%s[-::-] will be deleted. Confirm?", connectorName)).
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	messageText.SetBorder(true).
		SetTitle(" Confirm Deletion ").
		SetBorderPadding(0, 0, 1, 1)

	messageText.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			app.DeleteConnectorOffsetsResultHandler(connectorName)
			app.HideModalPage(DeleteConnectorOffsets)
		}

		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(DeleteConnectorOffsets)
		}

		return event
	})

	modal := util.NewConfirmationModal(messageText)
	app.Layout.PagesRegistry.UI.Pages.AddPage(DeleteConnectorOffsets, modal, true, true)
	app.Layout.Menu.SetMenu(DeleteConnectorOffsetsPageMenu)
	app.Layout.PagesRegistry.UI.Pages.ShowPage(DeleteConnectorOffsets)
}

// DeleteConnectorOffsetsResultHandler performs the connector offsets reset.
func (app *App) DeleteConnectorOffsetsResultHandler(name string) {
	resultCh := make(chan bool)
	errorCh := make(chan error)

	c := app.GetCurrentConnectClient()
	SendStatusInfinite("deleting connector offsets")
	c.DeleteConnectorOffsets(name, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatus(
					fmt.Sprintf("offsets for connector '%s' have been deleted", name),
					2*time.Second,
					false,
				)
				app.QueueUpdateDraw(func() {
					app.HideModalPage(ConnectorOffsets)
				})
				Publish(ConnectorsChannel, GetConnectorEventType, Payload{name, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to delete connector offsets")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to delete connector offsets: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while deleting connector offsets")
				SendStatusWithDefaultTTL("[red]timeout while deleting connector offsets")
				return
			}
		}
	}()
}

// CopyConnectorOffsetsModal creates and registers the "copy connector offsets" modal.
// A single input field accepts the target connector name; Ctrl+Enter submits, Enter
// unfocuses, Esc closes.
func (app *App) CopyConnectorOffsetsModal(connectorName string, offsets []connect.ConnectorOffset) {
	foregroundColor := tcell.GetColor(app.Colors.Karat.Foreground)
	backgroundColor := tcell.GetColor(app.Colors.Karat.Background)

	input := tview.NewInputField().
		SetFieldWidth(0).
		SetPlaceholder("target connector name").
		SetFieldStyle(tcell.StyleDefault.Foreground(foregroundColor).Background(backgroundColor)).
		SetPlaceholderStyle(tcell.StyleDefault.Background(backgroundColor)).
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder))

	mainFlex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(input, 1, 0, true)
	mainFlex.SetTitle(fmt.Sprintf(" Copy Offsets: %s ", connectorName))
	mainFlex.SetBorder(true)
	mainFlex.SetBorderPadding(0, 0, 1, 0)

	submit := func() {
		targetConnector := strings.TrimSpace(input.GetText())
		if targetConnector == "" {
			SendStatusWithDefaultTTL("[red]connector name cannot be empty")
			return
		}
		if targetConnector == connectorName {
			SendStatusWithDefaultTTL("[red]target connector must be different")
			return
		}
		app.HideModalPage(CopyConnectorOffsets)
		app.CopyConnectorOffsetsResultHandler(targetConnector, offsets)
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
			app.HideModalPage(CopyConnectorOffsets)
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
			app.HideModalPage(CopyConnectorOffsets)
			return nil
		}
		return event
	})

	// height: 1 border top + 1 content row + 1 border bottom = 3
	modal := util.NewResourceModal(mainFlex, 3)
	app.Layout.PagesRegistry.UI.Pages.AddPage(CopyConnectorOffsets, modal, true, false)
}

// CopyConnectorOffsetsResultHandler verifies that the target connector exists, then
// applies the source connector's offsets to it and shows the result status.
func (app *App) CopyConnectorOffsetsResultHandler(targetName string, offsets []connect.ConnectorOffset) {
	c := app.GetCurrentConnectClient()

	namesCh := make(chan []string)
	errorCh := make(chan error)
	SendStatusInfinite(fmt.Sprintf("checking connector '%s'...", targetName))
	c.ListConnectors(namesCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		defer cancel()
		select {
		case names := <-namesCh:
			if !slices.Contains(names, targetName) {
				SendStatusWithDefaultTTL(fmt.Sprintf("[red]connector '%s' does not exist", targetName))
				return
			}
			app.setConnectorOffsets(c, targetName, offsets)
		case err := <-errorCh:
			log.Error().Err(err).Msg("failed to list connectors")
			SendStatusWithDefaultTTL(fmt.Sprintf("[red]failed to list connectors: %s", err.Error()))
		case <-ctx.Done():
			log.Error().Msg("timeout while listing connectors")
			SendStatusWithDefaultTTL("[red]timeout while listing connectors")
		}
	}()
}

// setConnectorOffsets applies offsets to the target connector and shows the result status.
func (app *App) setConnectorOffsets(c *connect.Client, targetName string, offsets []connect.ConnectorOffset) {
	resultCh := make(chan bool)
	errorCh := make(chan error)

	SendStatusInfinite(fmt.Sprintf("copying offsets to '%s'", targetName))
	c.SetConnectorOffsets(targetName, offsets, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())
	defer cancel()

	select {
	case <-resultCh:
		SendStatus(fmt.Sprintf("offsets copied to '%s'", targetName), 2*time.Second, false)
	case err := <-errorCh:
		log.Error().Err(err).Msg("failed to copy connector offsets")
		SendStatusWithDefaultTTL(
			fmt.Sprintf("[red]failed to copy connector offsets: %s", err.Error()),
		)
	case <-ctx.Done():
		log.Error().Msg("timeout while copying connector offsets")
		SendStatusWithDefaultTTL("[red]timeout while copying connector offsets")
	}
}
