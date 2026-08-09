// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"testing"

	"github.com/uraniumdawn/karat/pkg/config"
	"github.com/uraniumdawn/karat/pkg/schemaregistry"
)

func TestStripComments(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "drops comments and surrounding blank lines",
			in:   "-o 100\n\n# Flags\n#   -o    beginning\n",
			want: "-o 100",
		},
		{
			name: "drops indented comments",
			in:   "  # comment\n-o 100\n",
			want: "-o 100",
		},
		{
			name: "keeps multi-line parameters",
			in:   "-o 100\n-f '{\"Key\":\"%k\"}'\n\n# Flags\n",
			want: "-o 100\n-f '{\"Key\":\"%k\"}'",
		},
		{
			name: "all commented out yields empty params",
			in:   "# Flags\n#   -o    beginning\n",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripComments(tt.in); got != tt.want {
				t.Errorf("stripComments() = %q, want %q", got, tt.want)
			}
		})
	}
}

// testApp is the minimum an App needs to resolve consume parameters: a selected cluster
// and the schema registry maps.
func testApp() *App {
	app := &App{
		Colors:                &config.ColorConfig{},
		SchemaRegistries:      map[string]*config.SchemaRegistryConfig{},
		SchemaRegistryClients: map[string]*schemaregistry.Client{},
		History:               &config.History{},
	}
	app.Selected.Cluster = &config.ClusterConfig{
		Name:       "test",
		Properties: map[string]string{"bootstrap.servers": "localhost:9092"},
	}
	return app
}

func TestPrepareConsume(t *testing.T) {
	t.Run("valid params", func(t *testing.T) {
		prepared, ok := testApp().prepareConsume("orders", "-o 100 -p 3 -g my-group | error")
		if !ok {
			t.Fatal("prepareConsume() rejected valid params")
		}
		if prepared.params.Topic != "orders" {
			t.Errorf("topic = %q, want orders", prepared.params.Topic)
		}
		if prepared.params.MaxCount != 100 {
			t.Errorf("MaxCount = %d, want 100", prepared.params.MaxCount)
		}
		if len(prepared.params.Partitions) != 1 || prepared.params.Partitions[0] != 3 {
			t.Errorf("Partitions = %v, want [3]", prepared.params.Partitions)
		}
		if prepared.params.Group != "my-group" {
			t.Errorf("Group = %q, want my-group", prepared.params.Group)
		}
		if prepared.filter != "error" {
			t.Errorf("filter = %q, want error", prepared.filter)
		}
		if prepared.formatFn == nil {
			t.Error("formatFn is nil")
		}
	})

	t.Run("unparsable params are rejected", func(t *testing.T) {
		if _, ok := testApp().prepareConsume("orders", "-o"); ok {
			t.Error("prepareConsume() accepted a flag with no value")
		}
	})

	t.Run("avro without a schema registry is rejected", func(t *testing.T) {
		if _, ok := testApp().prepareConsume("orders", "-d avro"); ok {
			t.Error("prepareConsume() accepted -d avro without -r")
		}
	})

	t.Run("unknown schema registry is rejected", func(t *testing.T) {
		if _, ok := testApp().prepareConsume("orders", "-d avro -r absent"); ok {
			t.Error("prepareConsume() accepted an unconfigured schema registry")
		}
	})
}

// The editor buffer is params + commented reference; reading it back must yield the
// params unchanged, whatever the reference contains.
func TestEditorBufferRoundTrip(t *testing.T) {
	app := testApp()
	params := "-o 100 -r sr -f '{\"Key\":\"%k\"}'"

	buf := params + "\n\n" + commentOut(app.consumeReference(false)) + "\n"

	if got := stripComments(buf); got != params {
		t.Errorf("round trip = %q, want %q", got, params)
	}
}
