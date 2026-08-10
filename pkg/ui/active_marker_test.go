// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"testing"

	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/config"
)

// activeMarkerColumn is the index of the "Active" column in the clusters, Connect and
// Schema Registry tables.
const activeMarkerColumn = 2

// markersOf collects the Active column of every data row of table.
func markersOf(table *tview.Table) []string {
	markers := make([]string, 0, table.GetRowCount()-1)
	for row := 1; row < table.GetRowCount(); row++ {
		markers = append(markers, table.GetCell(row, activeMarkerColumn).Text)
	}
	return markers
}

func assertMarkers(t *testing.T, table *tview.Table, want []string) {
	t.Helper()
	got := markersOf(table)
	if len(got) != len(want) {
		t.Fatalf("markers = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d marker = %q, want %q", i+1, got[i], want[i])
		}
	}
	if header := table.GetCell(0, activeMarkerColumn).Text; header != "Active" {
		t.Errorf("header = %q, want %q", header, "Active")
	}
}

func newTestApp() *App {
	app := &App{Config: &config.Config{}, Colors: &config.ColorConfig{}}
	app.Config.Karat.Clusters = []*config.ClusterConfig{
		{Name: "local"},
		{Name: "staging"},
	}
	app.Config.Karat.SchemaRegistries = []*config.SchemaRegistryConfig{
		{Name: "sr-local"},
		{Name: "sr-staging"},
	}
	app.Config.Karat.Connect = []*config.ConnectConfig{
		{Name: "connect-local"},
		{Name: "connect-staging"},
	}
	return app
}

func TestClustersTableMarksSelectedCluster(t *testing.T) {
	app := newTestApp()

	assertMarkers(t, app.NewClustersTable(), []string{"", ""})

	app.Selected.Cluster = app.Config.Karat.Clusters[1]
	assertMarkers(t, app.NewClustersTable(), []string{"", activeMarker})
}

func TestSchemaRegistriesTableMarksSelectedRegistry(t *testing.T) {
	app := newTestApp()

	assertMarkers(t, app.NewSchemaRegistriesTable(), []string{"", ""})

	app.Selected.SchemaRegistry = app.Config.Karat.SchemaRegistries[0]
	assertMarkers(t, app.NewSchemaRegistriesTable(), []string{activeMarker, ""})
}

func TestConnectTableMarksSelectedConnect(t *testing.T) {
	app := newTestApp()

	assertMarkers(t, app.NewConnectTable(), []string{"", ""})

	app.Selected.Connect = app.Config.Karat.Connect[1]
	assertMarkers(t, app.NewConnectTable(), []string{"", activeMarker})
}
