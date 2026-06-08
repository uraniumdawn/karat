// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"context"
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"

	"github.com/uraniumdawn/karat/pkg/util"
)

const (
	// GetSchemaRegistriesEventType is the event type for fetching schema registries.
	GetSchemaRegistriesEventType EventType = "srs:get"
)

// SchemaRegistriesChannel is the channel for schema registry events.
var SchemaRegistriesChannel = make(chan Event)

// RunSchemaRegistriesEventHandler processes schema registry events from the channel.
func (app *App) RunSchemaRegistriesEventHandler(ctx context.Context, in chan Event) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("shutting down schema-registries event handler")
				return
			case event := <-in:
				switch event.Type {
				case GetSchemaRegistriesEventType:
					app.QueueUpdateDraw(func() {
						sr := app.NewSchemaRegistriesTable()
						app.SchemaRegistriesTableInputHandler(sr)
						app.AddToPagesRegistry(
							SchemaRegistries,
							sr,
							SchemaRegistriesPageMenu,
							false,
						)
					})
				}
			}
		}
	}()
}

// addSchemaRegistriesTableHeader adds a fixed header row (row 0) with label-coloured cells.
func addSchemaRegistriesTableHeader(table *tview.Table, labelColor tcell.Color) {
	util.SetTableHeaders(table, labelColor, "Name", "URL", "Mode")
}

// NewSchemaRegistriesTable creates a table displaying schema registries.
func (app *App) NewSchemaRegistriesTable() *tview.Table {
	table := tview.NewTable()
	table.SetTitle(" Schema Registry URLs ")
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
	addSchemaRegistriesTableHeader(table, labelColor)

	// Iterate over the config slice to preserve order from config file
	row := 1
	for _, sr := range app.Config.Karat.SchemaRegistries {
		modeText := sr.Mode
		if modeText == "" {
			modeText = "regular"
		}
		table.
			SetCell(row, 0, tview.NewTableCell(sr.Name)).
			SetCell(row, 1, tview.NewTableCell(sr.SchemaRegistryURL)).
			SetCell(row, 2, tview.NewTableCell(modeText))
		row++
	}
	return table
}

// SchemaRegistriesTableInputHandler sets up input handling for the schema registries table.
func (app *App) SchemaRegistriesTableInputHandler(st *tview.Table) {
	st.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		row, _ := st.GetSelection()
		name := st.GetCell(row, 0).Text
		sr := app.SchemaRegistries[name]

		if event.Key() == tcell.KeyEnter {
			app.SelectSchemaRegistry(sr, true)
			ClearStatus()
		}

		if event.Key() == tcell.KeyTab {
			if sr == nil {
				return event
			}
			if sr.Mode == "read-only" {
				sr.Mode = ""
			} else {
				sr.Mode = "read-only"
			}
			modeText := sr.Mode
			if modeText == "" {
				modeText = "regular"
			}
			st.GetCell(row, 2).SetText(modeText)
			if app.isSchemaRegistrySelected(app.Selected) && app.Selected.SchemaRegistry.Name == name {
				app.Layout.SetSelected(
					app.Selected.Cluster,
					app.Selected.SchemaRegistry,
					app.Selected.Connect,
				)
			}
			if err := app.Config.Save(); err != nil {
				SendStatusWithDefaultTTL(fmt.Sprintf("[red]failed to save config: %s", err.Error()))
			}
			return nil
		}

		return event
	})
}
