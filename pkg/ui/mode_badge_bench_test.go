// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"strconv"
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/config"
)

// The badge is painted on every frame, so what it costs is worth knowing rather than assuming.
// Run all three together — the badge alone means little without the frame it is a share of:
//
//	go test ./pkg/ui -run '^$' -bench BenchmarkModeBadge -benchmem
const (
	// benchPages is how many pages a session realistically has open, which is what
	// GetFrontPage walks on every draw.
	benchPages = 20
	// benchRows and benchColumns size the table under the badge — the thing that actually
	// costs something to draw.
	benchRows    = 200
	benchColumns = 5
)

// newBenchApp is newBadgeApp with a realistic page count and a populated table in front.
func newBenchApp(b *testing.B) (*App, tcell.SimulationScreen) {
	b.Helper()

	app, screen := newBadgeApp(b, config.Yolo, 200, 60)

	registry := app.Layout.PagesRegistry
	for i := range benchPages {
		table := tview.NewTable()
		table.SetBorder(true).SetTitle(" Topics ")
		for row := range benchRows {
			for column := range benchColumns {
				table.SetCell(row, column, tview.NewTableCell("some-topic-"+strconv.Itoa(row)))
			}
		}

		name := "local:topics:" + strconv.Itoa(i)
		registry.PageMenuMap[name] = TopicsPageMenu
		registry.UI.Pages.AddAndSwitchToPage(name, table, true)
	}

	return app, screen
}

// BenchmarkModeBadgeAlone is the badge's own cost: a front-page lookup, a handful of map
// reads, one small string and a row of cells.
func BenchmarkModeBadgeAlone(b *testing.B) {
	app, screen := newBenchApp(b)

	b.ResetTimer()
	for range b.N {
		app.drawModeBadge(screen)
	}
}

// BenchmarkModeBadgeFullDraw and its counterpart below are the pair that matters: the
// difference between them is what installing the badge costs a frame.
func BenchmarkModeBadgeFullDraw(b *testing.B) {
	app, _ := newBenchApp(b)
	app.SetAfterDrawFunc(app.drawModeBadge)

	b.ResetTimer()
	for range b.N {
		app.ForceDraw()
	}
}

func BenchmarkModeBadgeFullDrawWithout(b *testing.B) {
	app, _ := newBenchApp(b)
	app.SetAfterDrawFunc(nil)

	b.ResetTimer()
	for range b.N {
		app.ForceDraw()
	}
}
