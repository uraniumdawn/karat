// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"
	"github.com/sahilm/fuzzy"

	"github.com/uraniumdawn/karat/pkg/franz"
	"github.com/uraniumdawn/karat/pkg/util"
)

// GetACLsEventType is the event type for fetching ACLs.
const GetACLsEventType EventType = "acls:get"

// ACLsChannel is the channel for ACL events.
var ACLsChannel = make(chan Event)

// RunACLsEventHandler processes ACL events from the channel.
func (app *App) RunACLsEventHandler(ctx context.Context, in chan Event) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("shutting down acls event handler")
				return
			case event := <-in:
				switch event.Type {
				case GetACLsEventType:
					pageName := util.BuildPageKey(app.Selected.Cluster.Name, ACLs)
					force := event.Payload.Force
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.SwitchToPage(pageName)
					} else {
						app.ACLs()
					}
				}
			}
		}
	}()
}

// ACLs fetches and displays the list of ACLs, via the franz-go client.
func (app *App) ACLs() {
	fc := app.GetCurrentFranzClient()
	if fc == nil {
		SendStatusWithDefaultTTL("[red]ACLs view requires franz-go connectivity for this cluster")
		return
	}

	SendStatusInfinite("getting acls")
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		defer cancel()
		acls, err := fc.ListACLs(ctx)
		if err != nil {
			log.Error().Err(err).Msg("failed to list acls")
			SendStatusWithDefaultTTL(
				fmt.Sprintf("[red]failed to list acls: %s", err.Error()),
			)
			return
		}
		app.QueueUpdateDraw(func() {
			pageKey := util.BuildPageKey(app.Selected.Cluster.Name, ACLs)
			table := app.NewACLsTable(acls)
			title := util.BuildTitle(ACLs, "["+strconv.Itoa(len(acls))+"]")
			table.SetTitle(title)
			app.AddToPagesRegistry(pageKey, table, ACLsPageMenu, true)

			sortCol := 0
			sortDesc := false
			labelColor := tcell.GetColor(app.Colors.Karat.Label.FgColor)

			table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				if event.Key() == tcell.KeyCtrlU {
					Publish(ACLsChannel, GetACLsEventType, Payload{nil, true})
				}

				if IsKey(event, '1') {
					if sortCol == 0 {
						sortDesc = !sortDesc
					} else {
						sortCol = 0
						sortDesc = false
					}
					sortACLsTable(table, acls, sortCol, sortDesc, labelColor)
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
					sortACLsTable(table, acls, sortCol, sortDesc, labelColor)
					table.ScrollToBeginning()
					return event
				}

				return event
			})

			app.AssignSearch(func(text string) {
				filterACLsTable(table, acls, text, labelColor)
				util.SetSearchableTableTitle(table, title, text)
				table.ScrollToBeginning()
			})

			ClearStatus()
		})
	}()
}

// addACLsTableHeader adds a fixed header row (row 0) with label-coloured cells.
func addACLsTableHeader(table *tview.Table, labelColor tcell.Color) {
	util.SetTableHeaders(table, labelColor,
		"Principal", "Resource Type", "Resource Name", "Pattern", "Operation", "Permission", "Host")
}

func populateACLsTable(table *tview.Table, row int, a franz.ACL) {
	table.SetCell(row, 0, tview.NewTableCell(a.Principal))
	table.SetCell(row, 1, tview.NewTableCell(a.Type))
	table.SetCell(row, 2, tview.NewTableCell(a.Name))
	table.SetCell(row, 3, tview.NewTableCell(a.Pattern))
	table.SetCell(row, 4, tview.NewTableCell(a.Operation))
	table.SetCell(row, 5, tview.NewTableCell(a.Permission))
	table.SetCell(row, 6, tview.NewTableCell(a.Host))
}

// sortACLsTable rebuilds the table sorted by col (0=Principal, 1=Resource Type).
// Resource Type tiebreaks by Resource Name then Principal ascending. Adds ↑/↓
// indicator to the active header cell.
func sortACLsTable(
	table *tview.Table,
	acls []franz.ACL,
	col int,
	desc bool,
	labelColor tcell.Color,
) {
	entries := make([]franz.ACL, len(acls))
	copy(entries, acls)

	sort.Slice(entries, func(i, j int) bool {
		switch col {
		case 1:
			if entries[i].Type != entries[j].Type {
				if desc {
					return entries[i].Type > entries[j].Type
				}
				return entries[i].Type < entries[j].Type
			}
			if entries[i].Name != entries[j].Name {
				return entries[i].Name < entries[j].Name
			}
			return entries[i].Principal < entries[j].Principal
		default:
			if desc {
				return entries[i].Principal > entries[j].Principal
			}
			return entries[i].Principal < entries[j].Principal
		}
	})

	table.Clear()
	addACLsTableHeader(table, labelColor)

	indicator := "[↑]"
	if desc {
		indicator = "[↓]"
	}
	switch col {
	case 0:
		table.GetCell(0, 0).SetText("Principal" + indicator)
	case 1:
		table.GetCell(0, 1).SetText("Resource Type" + indicator)
	}

	for i, a := range entries {
		populateACLsTable(table, i+1, a)
	}
}

func filterACLsTable(
	table *tview.Table,
	acls []franz.ACL,
	filter string,
	labelColor tcell.Color,
) {
	table.Clear()
	addACLsTableHeader(table, labelColor)

	if filter == "" {
		entries := make([]franz.ACL, len(acls))
		copy(entries, acls)
		sort.Slice(entries, func(i, j int) bool { return entries[i].Principal < entries[j].Principal })
		for i, a := range entries {
			populateACLsTable(table, i+1, a)
		}
		return
	}

	terms := make([]string, len(acls))
	for i, a := range acls {
		terms[i] = fmt.Sprintf(
			"%s %s %s %s %s %s %s",
			a.Principal, a.Type, a.Name, a.Pattern, a.Operation, a.Permission, a.Host,
		)
	}

	matches := fuzzy.Find(filter, terms)
	for i, match := range matches {
		populateACLsTable(table, i+1, acls[match.Index])
	}
}

// NewACLsTable creates a table displaying ACLs.
func (app *App) NewACLsTable(acls []franz.ACL) *tview.Table {
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
	sortACLsTable(table, acls, 0, false, labelColor)

	return table
}
