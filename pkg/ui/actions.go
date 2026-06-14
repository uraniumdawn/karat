// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"strconv"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/util"
)

// Extra actions registry keys, one per page kind that supports the "Extra Actions"
// modal (triggered by '>').
const (
	// TopicDescriptionExtraActions is the registry key for extra actions available
	// from the topic description page.
	TopicDescriptionExtraActions = "topic-description"
	// ConsumerGroupsExtraActions is the registry key for extra actions available
	// from the consumer groups list page.
	ConsumerGroupsExtraActions = "consumer-groups"
	// TopicsExtraActions is the registry key for extra actions available from the
	// topics list page.
	TopicsExtraActions = "topics"
	// SubjectsExtraActions is the registry key for extra actions available from the
	// subjects list page.
	SubjectsExtraActions = "subjects"
	// VersionsExtraActions is the registry key for extra actions available from the
	// subject versions page.
	VersionsExtraActions = "versions"
)

// ExtraAction describes a single entry in the "Extra Actions" modal.
type ExtraAction struct {
	// Name is the label shown in the modal.
	Name string
	// Run is invoked when the action is selected. ctx carries page-specific context,
	// e.g. the topic name for the topic description page.
	Run func(app *App, ctx string)
}

// extraActionsRegistry maps a page kind to the list of extra actions it offers.
var extraActionsRegistry = map[string][]ExtraAction{
	ConsumerGroupsExtraActions: {
		{
			Name: "Find by topic",
			Run: func(app *App, _ string) {
				app.FindConsumerGroupsByTopicModal()
				app.ShowModalPage(FindBy)
			},
		},
	},
	TopicDescriptionExtraActions: {
		{
			Name: "Producers",
			Run: func(app *App, ctx string) {
				Publish(TopicsChannel, GetTopicProducersEventType, Payload{ctx, false})
			},
		},
	},
	TopicsExtraActions: {
		{
			Name: "Consume",
			Run: func(app *App, ctx string) {
				app.ConsumeModal(ctx)
				app.ShowModalPage(ConsumeParams)
			},
		},
		{
			Name: "CLI commands",
			Run: func(app *App, ctx string) {
				app.CliTemplates(ctx)
			},
		},
		{
			Name: "Clone topic",
			Run: func(app *App, ctx string) {
				if app.IsCurrentClusterReadOnly() {
					SendStatusWithDefaultTTL("[red]cluster is in read-only mode")
					return
				}
				app.CloneTopic(ctx)
			},
		},
	},
	SubjectsExtraActions: {
		{
			Name: "Find schema by ID",
			Run: func(app *App, _ string) {
				app.FindSchemaByIDModal()
				app.ShowModalPage(FindSchemaByID)
			},
		},
		{
			Name: "Clone subject",
			Run: func(app *App, ctx string) {
				if app.IsCurrentSchemaRegistryReadOnly() {
					SendStatusWithDefaultTTL("[red]schema registry is in read-only mode")
					return
				}
				app.CloneSubject(ctx)
			},
		},
		{
			Name: "Delete subject",
			Run: func(app *App, ctx string) {
				if app.IsCurrentSchemaRegistryReadOnly() {
					SendStatusWithDefaultTTL("[red]schema registry is in read-only mode")
					return
				}
				app.DeleteSubjectConfirm(ctx)
				app.ShowModalPage(DeleteSubject)
			},
		},
	},
	VersionsExtraActions: {
		{
			Name: "Delete version",
			Run: func(app *App, ctx string) {
				if app.IsCurrentSchemaRegistryReadOnly() {
					SendStatusWithDefaultTTL("[red]schema registry is in read-only mode")
					return
				}
				parts := strings.SplitN(ctx, "\x00", 2)
				subject := parts[0]
				version, _ := strconv.Atoi(parts[1])
				app.DeleteSubjectVersionConfirm(subject, version)
				app.ShowModalPage(DeleteSubjectVersion)
			},
		},
	},
}

// ShowExtraActions opens the "Extra Actions" modal listing the actions registered for
// kind. ctx is page-specific context passed through to the selected action's Run func.
func (app *App) ShowExtraActions(kind, ctx string) {
	actions, ok := extraActionsRegistry[kind]
	if !ok || len(actions) == 0 {
		return
	}

	table := tview.NewTable()
	table.SetSelectable(true, false).SetBorderPadding(0, 0, 1, 0)
	for i, action := range actions {
		table.SetCell(i, 0, tview.NewTableCell(action.Name))
	}
	table.SetSelectedStyle(
		tcell.StyleDefault.Foreground(
			tcell.GetColor(app.Colors.Karat.Selection.FgColor),
		).Background(
			tcell.GetColor(app.Colors.Karat.Selection.BgColor),
		),
	)

	closeModal := func() {
		registry := app.Layout.PagesRegistry
		registry.UI.Pages.RemovePage(ExtraActions)
		currentPage, _ := registry.UI.Pages.GetFrontPage()
		if menu, ok := registry.PageMenuMap[currentPage]; ok {
			app.Layout.Menu.SetMenu(menu)
		}
	}

	table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEnter {
			row, _ := table.GetSelection()
			action := actions[row]
			closeModal()
			action.Run(app, ctx)
			return nil
		}

		if event.Key() == tcell.KeyEsc {
			closeModal()
			return nil
		}

		return event
	})

	container := tview.NewFlex().SetDirection(tview.FlexRow)
	container.SetBorder(true).SetTitle(" Extra actions ")
	container.AddItem(table, 0, 1, true)

	// +2 for border
	height := len(actions) + 2
	modal := util.NewResourceModal(container, height)

	registry := app.Layout.PagesRegistry
	registry.PageMenuMap[ExtraActions] = ExtraActionsPageMenu
	app.Layout.Menu.SetMenu(ExtraActionsPageMenu)
	registry.UI.Pages.AddPage(ExtraActions, modal, true, true)
}
