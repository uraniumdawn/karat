// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"context"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"

	"github.com/uraniumdawn/karat/pkg/util"
)

const (
	// GetConnectEventType is the event type for fetching connect clusters.
	GetConnectEventType EventType = "connect:get"
)

// ConnectChannel is the channel for connect events.
var ConnectChannel = NewEventChannel()

// RunConnectEventHandler processes connect events from the channel.
func (app *App) RunConnectEventHandler(ctx context.Context, in *EventChannel) {
	in.Run(ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("shutting down connect event handler")
				return
			case event := <-in.C:
				switch event.Type {
				case GetConnectEventType:
					app.QueueUpdateDraw(func() {
						ct := app.NewConnectTable()
						app.ConnectTableInputHandler(ct)
						app.AddToPagesRegistry(Connect, ct, ConnectPageMenu, false)
					})
				}
			}
		}
	}()
}

// addConnectTableHeader adds a fixed header row (row 0) with label-coloured cells.
func addConnectTableHeader(table *tview.Table, labelColor tcell.Color) {
	util.SetTableHeaders(table, labelColor, "Name", "URL", "Active")
}

// NewConnectTable creates a table displaying connect clusters.
func (app *App) NewConnectTable() *tview.Table {
	table := tview.NewTable()
	table.SetTitle(" Connect Clusters ")
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
	addConnectTableHeader(table, labelColor)

	row := 1
	for _, connect := range app.Config.Karat.Connect {
		active := app.isConnectSelected(app.Selected) && app.Selected.Connect.Name == connect.Name
		table.
			SetCell(row, 0, tview.NewTableCell(connect.Name)).
			SetCell(row, 1, tview.NewTableCell(connect.URL)).
			SetCell(row, 2, tview.NewTableCell(activeMarkerFor(active)))
		row++
	}
	return table
}

// ConnectTableInputHandler sets up input handling for the connect table.
func (app *App) ConnectTableInputHandler(ct *tview.Table) {
	ct.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		row, _ := ct.GetSelection()
		connectName := ct.GetCell(row, 0).Text
		connect := app.Connects[connectName]

		if event.Key() == tcell.KeyEnter {
			if connect == nil {
				return event
			}
			app.SelectConnect(connect, true)
			util.SetColumnMarker(ct, 2, row, activeMarker)
			ClearStatus()
		}

		return event
	})
}
