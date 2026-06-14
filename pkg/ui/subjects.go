// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"
	"github.com/sahilm/fuzzy"

	"github.com/uraniumdawn/karat/pkg/schemaregistry"
	"github.com/uraniumdawn/karat/pkg/util"
)

const (
	// GetSubjectsEventType is the event type for fetching subjects.
	GetSubjectsEventType EventType = "subjects:get"
	// GetVersionsEventType is the event type for fetching versions.
	GetVersionsEventType EventType = "versions:get"
	// GetSchemaEventType is the event type for fetching a schema.
	GetSchemaEventType EventType = "schema:get"
)

// SubjectsChannel is the channel for subject events.
var SubjectsChannel = make(chan Event)

// SubjectVersionPair represents a subject and version pair.
type SubjectVersionPair struct {
	Subject string
	Version string
}

// RunSubjectsEventHandler processes subject events from the channel.
func (app *App) RunSubjectsEventHandler(ctx context.Context, in chan Event) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("shutting down subjects event handler")
				return
			case event := <-in:
				switch event.Type {
				case GetSubjectsEventType:
					pageName := util.BuildPageKey(app.Selected.SchemaRegistry.Name, Subjects)
					force := event.Payload.Force
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.SwitchToPage(pageName)
					} else {
						app.Subjects()
					}

				case GetVersionsEventType:
					subject := event.Payload.Data.(string)
					force := event.Payload.Force
					pageName := util.BuildPageKey(
						app.Selected.SchemaRegistry.Name,
						subject,
						"versions",
					)
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.SwitchToPage(pageName)
					} else {
						app.Versions(subject)
					}

				case GetSchemaEventType:
					sv := event.Payload.Data.(SubjectVersionPair)
					force := event.Payload.Force
					v, _ := strconv.Atoi(sv.Version)
					subject := sv.Subject
					pageName := util.BuildPageKey(
						app.Selected.SchemaRegistry.Name,
						subject,
						"version",
						sv.Version,
					)
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.SwitchToPage(pageName)
					} else {
						app.Schema(subject, v)
					}
				}
			}
		}
	}()
}

// Subjects fetches and displays the list of schema subjects.
func (app *App) Subjects() {
	resultCh := make(chan []string)
	errorCh := make(chan error)

	c := app.GetCurrentSchemaRegistryClient()
	SendStatusInfinite("getting subjects...")
	c.Subjects(resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case subjects := <-resultCh:
				app.QueueUpdateDraw(func() {
					pageKey := util.BuildPageKey(app.Selected.SchemaRegistry.Name, Subjects)
					table := app.NewSubjectsTable(subjects)
					title := util.BuildTitle(Subjects,
						"["+strconv.Itoa(len(subjects))+"]")
					table.SetTitle(title)
					table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
						if event.Key() == tcell.KeyCtrlU {
							Publish(
								SubjectsChannel,
								GetSubjectsEventType,
								Payload{nil, true},
							)
						}

						if event.Key() == tcell.KeyEnter {
							row, _ := table.GetSelection()
							subject := table.GetCell(row, 0).Text
							Publish(
								SubjectsChannel,
								GetVersionsEventType,
								Payload{subject, false},
							)
						}

						if IsKey(event, '.') {
							row, _ := table.GetSelection()
							subject := table.GetCell(row, 0).Text
							app.ShowExtraActions(SubjectsExtraActions, subject)
						}

						return event
					})

					app.AddToPagesRegistry(pageKey, table, SubjectsPageMenu, true)

					app.AssignSearch(func(text string) {
						filterSubjectsTable(table, subjects, text)
						util.SetSearchableTableTitle(table, title, text)
						table.ScrollToBeginning()
					})

					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to list subjects")
				SendStatusWithDefaultTTL(fmt.Sprintf("[red]failed to list subjects: %s", err.Error()))
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while to list subjects")
				SendStatusWithDefaultTTL("[red]timeout while to list subjects")
				return
			}
		}
	}()
}

// Versions fetches and displays the versions for a specific subject.
func (app *App) Versions(subject string) {
	resultCh := make(chan []int)
	errorCh := make(chan error)

	c := app.GetCurrentSchemaRegistryClient()
	SendStatusInfinite("getting subject's versions...")
	c.VersionsBySubject(subject, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case versions := <-resultCh:
				app.QueueUpdateDraw(func() {
					pageKey := util.BuildPageKey(
						app.Selected.SchemaRegistry.Name,
						subject,
						"versions",
					)
					table := app.NewVersionsTable(versions)
					title := util.BuildTitle(subject, "["+strconv.Itoa(len(versions))+"]")
					table.SetTitle(title)

					app.AddToPagesRegistry(pageKey, table, VersionsPageMenu, false)
					table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
						if event.Key() == tcell.KeyCtrlU {
							Publish(
								SubjectsChannel,
								GetVersionsEventType,
								Payload{subject, true},
							)
						}

						if IsKey(event, 'd') {
							row, _ := table.GetSelection()
							version := table.GetCell(row, 0).Text

							Publish(SubjectsChannel, GetSchemaEventType,
								Payload{SubjectVersionPair{subject, version}, false})
						}

						if IsKey(event, '.') {
							row, _ := table.GetSelection()
							version := table.GetCell(row, 0).Text
							app.ShowExtraActions(
								VersionsExtraActions,
								subject+"\x00"+version,
							)
						}

						return event
					})

					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to list subject's versions")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to list subject's versions: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while to list subject's versions")
				SendStatusWithDefaultTTL("[red]timeout while to list subject's versions")
				return
			}
		}
	}()
}

// Schema fetches and displays a specific schema version for a subject.
func (app *App) Schema(subject string, version int) {
	resultCh := make(chan schemaregistry.SchemaResult)
	errorCh := make(chan error)

	c := app.GetCurrentSchemaRegistryClient()
	SendStatusInfinite("getting schema")
	c.Schema(subject, version, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case result := <-resultCh:
				var formattedSchema string
				var pretty bytes.Buffer
				indentErr := json.Indent(&pretty, []byte(result.Metadata.Schema), "", "  ")
				if indentErr != nil {
					log.Error().Err(indentErr).Msg("failed to format schema")
					SendStatusWithDefaultTTL(
						fmt.Sprintf("[red]failed to format schema: %s", indentErr.Error()),
					)
					cancel()
					return
				}
				formattedSchema = pretty.String()

				karatFormatted, karatErr := util.FormatAvroSchemaKarat(result.Metadata.Schema)
				if karatErr != nil {
					log.Error().Err(karatErr).Msg("failed to format schema in Karat format")
				}

				app.QueueUpdateDraw(func() {
					v := strconv.Itoa(version)
					pageKey := util.BuildPageKey(
						app.Selected.SchemaRegistry.Name,
						subject,
						"version",
						v,
					)
					baseTitle := fmt.Sprintf(
						" %s [v:%s] [id:%d] ",
						subject, v, result.Metadata.ID,
					)
					desc := app.NewDescription(baseTitle)

					app.AddToPagesRegistry(pageKey, desc, SubjectDecriptionPageMenu, false)
					titleWithTS := strings.TrimRight(desc.GetTitle(), " ")

					setContent := func(format, content string) {
						desc.Clear()
						writer := tview.ANSIWriter(desc)
						_, err := writer.Write([]byte(content))
						if err != nil {
							log.Error().Err(err).Msg("failed to write formatted schema")
							SendStatusWithDefaultTTL(
								"[red]failed to write formatted schema",
							)
						}
						desc.SetTitle(fmt.Sprintf("%s (%s) ", titleWithTS, format))
					}

					desc.SetInputCapture(
						app.WithHScroll(desc, func(event *tcell.EventKey) *tcell.EventKey {
							if event.Key() == tcell.KeyCtrlU {
								Publish(SubjectsChannel, GetSchemaEventType,
									Payload{SubjectVersionPair{subject, v}, true})
								return event
							}
							if IsKey(event, '1') {
								if karatErr != nil {
									SendStatusWithDefaultTTL(
										fmt.Sprintf(
											"[red]failed to format schema: %s",
											karatErr.Error(),
										),
									)
									return nil
								}
								setContent("karat", karatFormatted)
								return nil
							}
							if IsKey(event, '2') {
								setContent("json", formattedSchema)
								return nil
							}
							return event
						}),
					)

					if karatErr == nil {
						setContent("karat", karatFormatted)
					} else {
						setContent("json", formattedSchema)
					}
					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to list subject's versions")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to list subject's versions: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while to list subject's versions")
				SendStatusWithDefaultTTL("[red]tmeout while to list subject's versions")
				return
			}
		}
	}()
}

// NewSubjectsTable creates a table displaying schema subjects.
func (app *App) NewSubjectsTable(subjects []string) *tview.Table {
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

	sort.Strings(subjects)
	for i, subject := range subjects {
		table.SetCell(i, 0, tview.NewTableCell(subject))
	}

	return table
}

// NewVersionsTable creates a table displaying schema versions.
func (app *App) NewVersionsTable(versions []int) *tview.Table {
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

	row := 0
	for _, version := range versions {
		table.SetCell(row, 0, tview.NewTableCell(strconv.Itoa(version)))
		row++
	}
	return table
}

// CloneSubjectResult holds the source subject's schema and compatibility level.
type CloneSubjectResult struct {
	Schema        sr.SchemaInfo
	Compatibility sr.Compatibility
}

// CloneSubject fetches the source subject's latest schema and config, then opens
// a modal to register the same schema under a new subject name.
func (app *App) CloneSubject(sourceSubject string) {
	schemaCh := make(chan schemaregistry.SchemaResult)
	configCh := make(chan sr.ServerConfig)
	errorCh := make(chan error, 2)

	c := app.GetCurrentSchemaRegistryClient()
	SendStatusInfinite("fetching subject schema")
	c.LatestSchema(sourceSubject, schemaCh, errorCh)
	c.SubjectConfig(sourceSubject, configCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		var result CloneSubjectResult
		received := 0
		for received < 2 {
			select {
			case schema := <-schemaCh:
				result.Schema = schema.Metadata.SchemaInfo
				received++
			case cfg := <-configCh:
				result.Compatibility = cfg.CompatibilityLevel
				received++
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to fetch subject info")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to fetch subject info: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while fetching subject info")
				SendStatusWithDefaultTTL("[red]timeout while fetching subject info")
				return
			}
		}

		app.QueueUpdateDraw(func() {
			app.NewCloneSubjectModal(sourceSubject, &result)
			app.ShowModalPage(CloneSubject)
			ClearStatus()
		})
		cancel()
	}()
}

// NewCloneSubjectModal builds a "Clone Subject" modal with the source subject's
// name pre-filled and its compatibility level displayed.
func (app *App) NewCloneSubjectModal(sourceSubject string, result *CloneSubjectResult) {
	width := 40
	subjectName := sourceSubject

	nameField := tview.NewInputField().
		SetFieldWidth(width).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder)).
		SetText(sourceSubject)

	compatField := tview.NewInputField().
		SetFieldWidth(width).
		SetFieldStyle(
			tcell.StyleDefault.Foreground(
				tcell.GetColor(app.Colors.Karat.Foreground),
			).Background(
				tcell.GetColor(app.Colors.Karat.Background),
			)).
		SetText(result.Compatibility.String())
	compatField.SetDisabled(true)

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
		tview.NewTableCell("Compatibility:").
			SetAlign(tview.AlignRight).
			SetSelectable(false).
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
	f.AddItem(selection, 16, 0, true)
	f.AddItem(tview.NewBox(), 3, 0, false)

	inputs := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(nameField, 1, 0, false).
		AddItem(compatField, 1, 0, false)

	f.AddItem(inputs, 40, 0, false).
		AddItem(tview.NewBox(), 0, 1, false)

	nameField.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			subjectName = nameField.GetText()
			app.SetFocus(selection)
			app.Layout.Menu.SetMenu(CloneSubjectPageMenu)
		}
		return event
	})

	selection.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		row, _ := selection.GetSelection()

		if IsKey(event, 'e') {
			if row == 0 {
				app.SetFocus(nameField)
				app.Layout.Menu.SetMenu(CloneSubjectInputMenu)
			}
		}

		if IsCtrlEnter(event) {
			subjectName = nameField.GetText()
			if strings.TrimSpace(subjectName) == "" {
				SendStatusWithDefaultTTL("[red]subject name cannot be empty")
				return event
			}
			app.CloneSubjectResultHandler(
				subjectName,
				result.Schema,
				result.Compatibility,
			)
			app.HideModalPage(CloneSubject)
		}

		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(CloneSubject)
		}

		return event
	})

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(f, 0, 1, true)
	flex.SetTitle(fmt.Sprintf(" Clone Subject (from %s) ", sourceSubject))
	flex.SetBorder(true)

	modal := util.NewResourceModal(flex, 4)
	app.Layout.PagesRegistry.UI.Pages.AddPage(CloneSubject, modal, true, true)
	app.Layout.PagesRegistry.UI.Pages.ShowPage(CloneSubject)
	app.SetFocus(nameField)
	app.Layout.Menu.SetMenu(CloneSubjectInputMenu)
}

// CloneSubjectResultHandler registers the schema under the new subject and copies
// the compatibility level from the source.
func (app *App) CloneSubjectResultHandler(
	subject string,
	schema sr.SchemaInfo,
	compatibility sr.Compatibility,
) {
	resultCh := make(chan int)
	errorCh := make(chan error)

	c := app.GetCurrentSchemaRegistryClient()
	SendStatusInfinite("registering schema")
	c.RegisterSchema(subject, schema, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				configResultCh := make(chan sr.ServerConfig)
				configErrorCh := make(chan error)
				c.UpdateSubjectConfig(
					subject,
					sr.ServerConfig{CompatibilityUpdate: compatibility},
					configResultCh,
					configErrorCh,
				)
				configCtx, configCancel := context.WithTimeout(
					context.Background(),
					app.Config.GetAPICallTimeout(),
				)
				select {
				case <-configResultCh:
				case err := <-configErrorCh:
					log.Error().Err(err).Msg("failed to set subject compatibility")
					SendStatusWithDefaultTTL(
						fmt.Sprintf(
							"[red]subject created but failed to set compatibility: %s",
							err.Error(),
						),
					)
				case <-configCtx.Done():
					log.Error().Msg("timeout while setting subject compatibility")
					SendStatusWithDefaultTTL(
						"[red]subject created but timeout setting compatibility",
					)
				}
				configCancel()

				SendStatus(
					fmt.Sprintf("subject '%s' has been created", subject),
					2*time.Second,
					false,
				)
				Publish(SubjectsChannel, GetSubjectsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to register schema")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to register schema: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while registering schema")
				SendStatusWithDefaultTTL("[red]timeout while registering schema")
				return
			}
		}
	}()
}

// DeleteSubjectConfirm shows a confirmation modal before deleting a subject.
func (app *App) DeleteSubjectConfirm(subject string) {
	messageText := tview.NewTextView().
		SetText(fmt.Sprintf(
			"Subject [red::b]%s[-::-] will be deleted. Confirm?",
			subject,
		)).
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	messageText.SetBorder(true).
		SetTitle(" Confirm Deletion ").
		SetBorderPadding(0, 0, 1, 1)

	messageText.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			app.DeleteSubjectResultHandler(subject)
			app.HideModalPage(DeleteSubject)
		}

		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(DeleteSubject)
		}

		return event
	})

	modal := util.NewConfirmationModal(messageText)
	app.Layout.PagesRegistry.UI.Pages.AddPage(DeleteSubject, modal, true, true)
	app.Layout.PagesRegistry.UI.Pages.ShowPage(DeleteSubject)
}

// DeleteSubjectResultHandler soft-deletes all versions of a subject.
func (app *App) DeleteSubjectResultHandler(subject string) {
	resultCh := make(chan []int)
	errorCh := make(chan error)

	c := app.GetCurrentSchemaRegistryClient()
	SendStatusInfinite("deleting subject")
	c.DeleteSubjectVersions(subject, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatus(
					fmt.Sprintf("subject '%s' has been deleted", subject),
					2*time.Second,
					false,
				)
				Publish(SubjectsChannel, GetSubjectsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to delete subject")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to delete subject: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while deleting subject")
				SendStatusWithDefaultTTL("[red]timeout while deleting subject")
				return
			}
		}
	}()
}

// DeleteSubjectVersionConfirm shows a confirmation modal before deleting a subject version.
func (app *App) DeleteSubjectVersionConfirm(subject string, version int) {
	messageText := tview.NewTextView().
		SetText(fmt.Sprintf(
			"Version [red::b]%d[-::-] of subject [red::b]%s[-::-] will be deleted. Confirm?",
			version,
			subject,
		)).
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	messageText.SetBorder(true).
		SetTitle(" Confirm Deletion ").
		SetBorderPadding(0, 0, 1, 1)

	messageText.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			app.DeleteSubjectVersionResultHandler(subject, version)
			app.HideModalPage(DeleteSubjectVersion)
		}

		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(DeleteSubjectVersion)
		}

		return event
	})

	modal := util.NewConfirmationModal(messageText)
	app.Layout.PagesRegistry.UI.Pages.AddPage(
		DeleteSubjectVersion,
		modal,
		true,
		true,
	)
	app.Layout.PagesRegistry.UI.Pages.ShowPage(DeleteSubjectVersion)
}

// DeleteSubjectVersionResultHandler soft-deletes a specific version of a subject.
func (app *App) DeleteSubjectVersionResultHandler(subject string, version int) {
	resultCh := make(chan int)
	errorCh := make(chan error)

	c := app.GetCurrentSchemaRegistryClient()
	SendStatusInfinite("deleting version")
	c.DeleteVersion(subject, version, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case <-resultCh:
				SendStatus(
					fmt.Sprintf(
						"version %d of subject '%s' has been deleted",
						version,
						subject,
					),
					2*time.Second,
					false,
				)
				Publish(SubjectsChannel, GetSubjectsEventType, Payload{nil, true})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to delete version")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to delete version: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while deleting version")
				SendStatusWithDefaultTTL("[red]timeout while deleting version")
				return
			}
		}
	}()
}

// FindSchemaByIDModal builds a modal with an input field for entering a schema ID.
func (app *App) FindSchemaByIDModal() {
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
		SetPlaceholder("schema ID").
		SetPlaceholderTextColor(tcell.GetColor(app.Colors.Karat.Placeholder)).
		SetAcceptanceFunc(tview.InputFieldInteger)

	input.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			idStr := strings.TrimSpace(input.GetText())
			if idStr == "" {
				SendStatusWithDefaultTTL("[red]schema ID cannot be empty")
				return nil
			}
			id, err := strconv.Atoi(idStr)
			if err != nil {
				SendStatusWithDefaultTTL("[red]invalid schema ID")
				return nil
			}
			app.HideModalPage(FindSchemaByID)
			app.SchemaByID(id)
			return nil
		}
		if event.Key() == tcell.KeyEsc {
			app.HideModalPage(FindSchemaByID)
			return nil
		}
		return event
	})

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(input, 1, 0, true)
	flex.SetTitle(" Find Schema By ID ")
	flex.SetBorder(true)
	flex.SetBorderPadding(0, 0, 1, 0)

	modal := util.NewResourceModal(flex, 3)
	app.Layout.PagesRegistry.UI.Pages.AddPage(FindSchemaByID, modal, true, false)
}

// SchemaByID fetches and displays a schema by its global ID.
func (app *App) SchemaByID(id int) {
	resultCh := make(chan sr.SchemaInfo)
	errorCh := make(chan error)

	c := app.GetCurrentSchemaRegistryClient()
	SendStatusInfinite("getting schema by ID...")
	c.SchemaByID(id, resultCh, errorCh)
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		for {
			select {
			case result := <-resultCh:
				var formattedSchema string
				var pretty bytes.Buffer
				indentErr := json.Indent(&pretty, []byte(result.Schema), "", "  ")
				if indentErr != nil {
					log.Error().Err(indentErr).Msg("failed to format schema")
					SendStatusWithDefaultTTL(
						fmt.Sprintf("[red]failed to format schema: %s", indentErr.Error()),
					)
					cancel()
					return
				}
				formattedSchema = pretty.String()

				karatFormatted, karatErr := util.FormatAvroSchemaKarat(result.Schema)
				if karatErr != nil {
					log.Error().Err(karatErr).Msg("failed to format schema in Karat format")
				}

				app.QueueUpdateDraw(func() {
					idStr := strconv.Itoa(id)
					pageKey := util.BuildPageKey(
						app.Selected.SchemaRegistry.Name,
						"schema-by-id",
						idStr,
					)
					baseTitle := strings.TrimRight(
						util.BuildTitle("Schema ID", idStr), " ",
					)
					desc := app.NewDescription(baseTitle)

					app.AddToPagesRegistry(
						pageKey, desc, SubjectDecriptionPageMenu, false,
					)
					titleWithTS := strings.TrimRight(desc.GetTitle(), " ")

					setContent := func(format, content string) {
						desc.Clear()
						writer := tview.ANSIWriter(desc)
						_, err := writer.Write([]byte(content))
						if err != nil {
							log.Error().Err(err).Msg("failed to write formatted schema")
							SendStatusWithDefaultTTL(
								"[red]failed to write formatted schema",
							)
						}
						desc.SetTitle(fmt.Sprintf("%s (%s) ", titleWithTS, format))
					}

					desc.SetInputCapture(
						app.WithHScroll(desc, func(event *tcell.EventKey) *tcell.EventKey {
							if IsKey(event, '1') {
								if karatErr != nil {
									SendStatusWithDefaultTTL(
										fmt.Sprintf(
											"[red]failed to format schema: %s",
											karatErr.Error(),
										),
									)
									return nil
								}
								setContent("karat", karatFormatted)
								return nil
							}
							if IsKey(event, '2') {
								setContent("json", formattedSchema)
								return nil
							}
							return event
						}),
					)

					if karatErr == nil {
						setContent("karat", karatFormatted)
					} else {
						setContent("json", formattedSchema)
					}
					ClearStatus()
				})
				cancel()
				return
			case err := <-errorCh:
				log.Error().Err(err).Msg("failed to get schema by ID")
				SendStatusWithDefaultTTL(
					fmt.Sprintf("[red]failed to get schema by ID: %s", err.Error()),
				)
				cancel()
				return
			case <-ctx.Done():
				log.Error().Msg("timeout while getting schema by ID")
				SendStatusWithDefaultTTL("[red]timeout while getting schema by ID")
				return
			}
		}
	}()
}

func filterSubjectsTable(table *tview.Table, subjects []string, filter string) {
	table.Clear()

	if filter == "" {
		// Show all subjects sorted alphabetically when filter is empty
		sort.Strings(subjects)
		row := 0
		for _, subject := range subjects {
			table.SetCell(row, 0, tview.NewTableCell(subject))
			row++
		}
		return
	}

	matches := fuzzy.Find(filter, subjects)

	row := 0
	for _, match := range matches {
		table.SetCell(row, 0, tview.NewTableCell(match.Str))
		row++
	}
}
