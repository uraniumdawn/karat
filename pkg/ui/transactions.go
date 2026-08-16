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

const (
	// GetTransactionsEventType is the event type for fetching transactions.
	GetTransactionsEventType EventType = "transactions:get"
	// GetTransactionEventType is the event type for fetching a specific transaction.
	GetTransactionEventType EventType = "transaction:get"
)

// TransactionsChannel is the channel for transaction events.
var TransactionsChannel = make(chan Event)

// RunTransactionsEventHandler processes transaction events from the channel.
func (app *App) RunTransactionsEventHandler(ctx context.Context, in chan Event) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("shutting down transactions event handler")
				return
			case event := <-in:
				switch event.Type {
				case GetTransactionsEventType:
					pageName := util.BuildPageKey(app.Selected.Cluster.Name, Transactions)
					force := event.Payload.Force
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.SwitchToPage(pageName)
					} else {
						app.Transactions()
					}

				case GetTransactionEventType:
					txnID := event.Payload.Data.(string)
					force := event.Payload.Force
					pageName := util.BuildPageKey(app.Selected.Cluster.Name, Transaction, txnID)
					_, found := app.Cache.Get(pageName)
					if found && !force {
						app.SwitchToPage(pageName)
					} else {
						app.Transaction(txnID)
					}
				}
			}
		}
	}()
}

// Transactions fetches and displays the list of transactions, via the franz-go client.
func (app *App) Transactions() {
	fc := app.GetCurrentFranzClient()
	if fc == nil {
		SendStatusWithDefaultTTL("[red]transactions view requires franz-go connectivity for this cluster")
		return
	}

	SendStatusInfinite("getting transactions")
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		defer cancel()
		txns, err := fc.ListTransactions(ctx)
		if err != nil {
			log.Error().Err(err).Msg("failed to list transactions")
			SendStatusWithDefaultTTL(
				fmt.Sprintf("[red]%s", err.Error()),
			)
			return
		}
		app.QueueUpdateDraw(func() {
			pageKey := util.BuildPageKey(app.Selected.Cluster.Name, Transactions)
			table := app.NewTransactionsTable(txns)
			title := util.BuildTitle(Transactions, "["+strconv.Itoa(len(txns))+"]")
			table.SetTitle(title)
			app.AddToPagesRegistry(pageKey, table, TransactionsPageMenu, true)

			// A refresh builds a new table: without this the cursor lands on the first row,
			// and the next key acts on a transaction the user did not pick.
			app.RestoreSelection(pageKey, table, afterHeaderRow)
			app.TrackSelection(pageKey, table, afterHeaderRow)

			sortCol := 0
			sortDesc := false
			labelColor := tcell.GetColor(app.Colors.Karat.Label.FgColor)

			table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				if event.Key() == tcell.KeyCtrlU {
					Publish(TransactionsChannel, GetTransactionsEventType, Payload{nil, true})
				}

				// <Enter> opens the row under the cursor, the same as <d>.
				if IsKey(event, 'd') || event.Key() == tcell.KeyEnter {
					txnID, ok := selectedName(table, afterHeaderRow)
					if !ok {
						return nil
					}
					Publish(TransactionsChannel, GetTransactionEventType, Payload{txnID, false})
				}

				if IsKey(event, '1') {
					if sortCol == 0 {
						sortDesc = !sortDesc
					} else {
						sortCol = 0
						sortDesc = false
					}
					sortTransactionsTable(table, txns, sortCol, sortDesc, labelColor)
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
					sortTransactionsTable(table, txns, sortCol, sortDesc, labelColor)
					table.ScrollToBeginning()
					app.RestoreSelection(pageKey, table, afterHeaderRow)
					return event
				}

				return event
			})

			app.AssignSearch(func(text string) {
				filterTransactionsTable(table, txns, text, labelColor)
				util.SetSearchableTableTitle(table, title, text)
				table.ScrollToBeginning()
				// The rows the cursor pointed at are gone; without this the selection is left
				// past the end of the filtered table and every row action reads an empty cell.
				app.RestoreSelection(pageKey, table, afterHeaderRow)
			})

			ClearStatus()
		})
	}()
}

// Transaction fetches and displays the details of a single transaction, via the
// franz-go client.
func (app *App) Transaction(txnID string) {
	fc := app.GetCurrentFranzClient()
	if fc == nil {
		SendStatusWithDefaultTTL("[red]transactions view requires franz-go connectivity for this cluster")
		return
	}

	pageKey := util.BuildPageKey(app.Selected.Cluster.Name, Transaction, txnID)
	autoRefreshing := app.GetAutoUpdateLabel(pageKey) != ""
	if !autoRefreshing {
		SendStatusInfinite("getting transaction description")
	}
	ctx, cancel := context.WithTimeout(context.Background(), app.Config.GetAPICallTimeout())

	go func() {
		defer cancel()
		detail, err := fc.DescribeTransaction(ctx, txnID)
		if err != nil {
			log.Error().Err(err).Str("txn", txnID).Msg("failed to describe transaction")
			SendStatusWithDefaultTTL(
				fmt.Sprintf("[red]failed to describe transaction: %s", err.Error()),
			)
			return
		}
		app.QueueUpdateDraw(func() {
			title := util.BuildTitle(Transaction, txnID)
			if label := app.GetAutoUpdateLabel(pageKey); label != "" {
				title = title + "[" + label + "]"
			}
			desc := app.NewDescription(title)
			desc.SetText(detail.String())
			desc.SetInputCapture(
				app.WithHScroll(desc, func(event *tcell.EventKey) *tcell.EventKey {
					if event.Key() == tcell.KeyCtrlU {
						Publish(
							TransactionsChannel,
							GetTransactionEventType,
							Payload{txnID, true},
						)
					}
					if IsKey(event, 'a') {
						app.EnterAutoUpdateMode(pageKey, func() {
							Publish(
								TransactionsChannel,
								GetTransactionEventType,
								Payload{txnID, true},
							)
						})
						return nil
					}
					return event
				}),
			)
			app.AddToPagesRegistry(pageKey, desc, TransactionDescribePageMenu, false)
			if !autoRefreshing {
				ClearStatus()
			}
		})
	}()
}

// addTransactionsTableHeader adds a fixed header row (row 0) with label-coloured cells.
func addTransactionsTableHeader(table *tview.Table, labelColor tcell.Color) {
	util.SetTableHeaders(table, labelColor, "Transactional ID", "State", "Producer ID", "Coordinator")
}

func populateTransactionsTable(table *tview.Table, row int, t franz.Transaction) {
	table.SetCell(row, 0, tview.NewTableCell(t.TxnID))
	table.SetCell(row, 1, tview.NewTableCell(t.State))
	table.SetCell(row, 2, tview.NewTableCell(strconv.FormatInt(t.ProducerID, 10)))
	table.SetCell(row, 3, tview.NewTableCell(strconv.FormatInt(int64(t.Coordinator), 10)))
}

// sortTransactionsTable rebuilds the table sorted by col (0=Transactional ID, 1=State).
// State tiebreaks by Transactional ID ascending. Adds ↑/↓ indicator to the active header cell.
func sortTransactionsTable(
	table *tview.Table,
	txns []franz.Transaction,
	col int,
	desc bool,
	labelColor tcell.Color,
) {
	entries := make([]franz.Transaction, len(txns))
	copy(entries, txns)

	sort.Slice(entries, func(i, j int) bool {
		switch col {
		case 1:
			if entries[i].State != entries[j].State {
				if desc {
					return entries[i].State > entries[j].State
				}
				return entries[i].State < entries[j].State
			}
			return entries[i].TxnID < entries[j].TxnID
		default:
			if desc {
				return entries[i].TxnID > entries[j].TxnID
			}
			return entries[i].TxnID < entries[j].TxnID
		}
	})

	table.Clear()
	addTransactionsTableHeader(table, labelColor)

	indicator := "[↑]"
	if desc {
		indicator = "[↓]"
	}
	switch col {
	case 0:
		table.GetCell(0, 0).SetText("Transactional ID" + indicator)
	case 1:
		table.GetCell(0, 1).SetText("State" + indicator)
	}

	for i, t := range entries {
		populateTransactionsTable(table, i+1, t)
	}
}

func filterTransactionsTable(
	table *tview.Table,
	txns []franz.Transaction,
	filter string,
	labelColor tcell.Color,
) {
	table.Clear()
	addTransactionsTableHeader(table, labelColor)

	if filter == "" {
		entries := make([]franz.Transaction, len(txns))
		copy(entries, txns)
		sort.Slice(entries, func(i, j int) bool { return entries[i].TxnID < entries[j].TxnID })
		for i, t := range entries {
			populateTransactionsTable(table, i+1, t)
		}
		return
	}

	ids := make([]string, len(txns))
	for i, t := range txns {
		ids[i] = t.TxnID
	}

	matches := fuzzy.Find(filter, ids)
	for i, match := range matches {
		populateTransactionsTable(table, i+1, txns[match.Index])
	}
}

// NewTransactionsTable creates a table displaying transactions.
func (app *App) NewTransactionsTable(txns []franz.Transaction) *tview.Table {
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
	sortTransactionsTable(table, txns, 0, false, labelColor)

	return table
}
