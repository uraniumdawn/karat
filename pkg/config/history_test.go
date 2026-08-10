// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package config

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func params(h *History) []string {
	out := make([]string, 0, len(h.Consume))
	for _, e := range h.Consume {
		out = append(out, e.Params)
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestAddConsume(t *testing.T) {
	t.Run("newest first", func(t *testing.T) {
		h := &History{}
		h.AddConsume("prod", "orders", "-o 100")
		h.AddConsume("prod", "orders", "-o beginning")

		if want := []string{"-o beginning", "-o 100"}; !equal(params(h), want) {
			t.Errorf("params = %v, want %v", params(h), want)
		}
	})

	t.Run("repeating params moves the entry to the front", func(t *testing.T) {
		h := &History{}
		h.AddConsume("prod", "orders", "-o 100")
		h.AddConsume("prod", "orders", "-o beginning")
		h.AddConsume("prod", "orders", "-o 100")

		if want := []string{"-o 100", "-o beginning"}; !equal(params(h), want) {
			t.Errorf("params = %v, want %v", params(h), want)
		}
		if h.Consume[0].At.IsZero() {
			t.Error("timestamp of the refreshed entry is zero")
		}
	})

	t.Run("same params on another topic is a separate entry", func(t *testing.T) {
		h := &History{}
		h.AddConsume("prod", "orders", "-o 100")
		h.AddConsume("prod", "payments", "-o 100")

		if len(h.Consume) != 2 {
			t.Fatalf("len = %d, want 2", len(h.Consume))
		}
		if h.Consume[0].Topic != "payments" {
			t.Errorf("newest topic = %q, want payments", h.Consume[0].Topic)
		}
	})

	t.Run("cap drops the oldest entries of that topic", func(t *testing.T) {
		h := &History{}
		for i := 0; i < maxTopicConsumeHistory+10; i++ {
			h.AddConsume("prod", "orders", "-o "+strconv.Itoa(i))
		}

		if len(h.Consume) != maxTopicConsumeHistory {
			t.Fatalf("len = %d, want %d", len(h.Consume), maxTopicConsumeHistory)
		}
		newest := "-o " + strconv.Itoa(maxTopicConsumeHistory+9)
		if h.Consume[0].Params != newest {
			t.Errorf("newest = %q, want %q", h.Consume[0].Params, newest)
		}
		oldestKept := "-o 10"
		if last := h.Consume[len(h.Consume)-1].Params; last != oldestKept {
			t.Errorf("oldest kept = %q, want %q", last, oldestKept)
		}
	})

	// The point of the per-topic cap: a topic consumed once keeps its parameters however
	// much other topics are consumed afterwards.
	t.Run("a busy topic does not evict a quiet one", func(t *testing.T) {
		h := &History{}
		h.AddConsume("prod", "orders", "-o 100 -p 3")
		for i := 0; i < maxTopicConsumeHistory+10; i++ {
			h.AddConsume("prod", "payments", "-o "+strconv.Itoa(i))
		}

		if got := h.LastConsume("prod", "orders"); got != "-o 100 -p 3" {
			t.Errorf("LastConsume(orders) = %q, want the entry it kept", got)
		}
	})
}

func TestLastConsume(t *testing.T) {
	h := &History{}
	h.AddConsume("prod", "orders", "-o 100")
	h.AddConsume("prod", "orders", "-o beginning")
	h.AddConsume("stage", "orders", "-o latest")

	tests := []struct {
		name           string
		cluster, topic string
		want           string
	}{
		{"most recent for the topic", "prod", "orders", "-o beginning"},
		{"same topic on another cluster", "stage", "orders", "-o latest"},
		{"unknown topic", "prod", "payments", ""},
		{"unknown cluster", "dev", "orders", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := h.LastConsume(tt.cluster, tt.topic); got != tt.want {
				t.Errorf("LastConsume(%q, %q) = %q, want %q", tt.cluster, tt.topic, got, tt.want)
			}
		})
	}
}

func TestConsumeFor(t *testing.T) {
	h := &History{}
	h.AddConsume("prod", "payments", "-o 1")
	h.AddConsume("prod", "orders", "-o 2")
	h.AddConsume("stage", "orders", "-o 3")
	h.AddConsume("prod", "payments", "-o 4")
	h.AddConsume("prod", "orders", "-o 5")

	got := h.ConsumeFor("prod", "orders")

	want := []string{"-o 5", "-o 2"} // only this cluster's entries for this topic, newest first
	gotParams := make([]string, 0, len(got))
	for _, e := range got {
		gotParams = append(gotParams, e.Params)
	}
	if !equal(gotParams, want) {
		t.Errorf("ConsumeFor = %v, want %v", gotParams, want)
	}
	for _, e := range got {
		if e.Cluster != "prod" {
			t.Errorf("entry from cluster %q leaked in", e.Cluster)
		}
		if e.Topic != "orders" {
			t.Errorf("entry from topic %q leaked in", e.Topic)
		}
	}
}

func TestHistoryRoundTrip(t *testing.T) {
	t.Setenv(KaratEnvConfigDir, t.TempDir())

	h := &History{}
	h.AddConsume("prod", "orders", "-o 100 -f '{\"Key\":\"%k\"}'")

	// First run: the config directory does not exist yet.
	if err := h.Save(); err != nil {
		t.Fatalf("Save() error: %v", err)
	}

	loaded := LoadHistory()
	if len(loaded.Consume) != 1 {
		t.Fatalf("len = %d, want 1", len(loaded.Consume))
	}
	if got := loaded.LastConsume("prod", "orders"); got != h.Consume[0].Params {
		t.Errorf("params = %q, want %q", got, h.Consume[0].Params)
	}
	if loaded.Consume[0].At.IsZero() {
		t.Error("timestamp did not survive the round trip")
	}
}

func TestLoadHistoryMissingAndMalformed(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		t.Setenv(KaratEnvConfigDir, t.TempDir())

		if h := LoadHistory(); len(h.Consume) != 0 {
			t.Errorf("len = %d, want 0", len(h.Consume))
		}
	})

	t.Run("malformed file", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv(KaratEnvConfigDir, dir)

		path := filepath.Join(dir, ".config", "karat")
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(
			filepath.Join(path, "history.yaml"),
			[]byte("consume: [oh: no"),
			0o644,
		); err != nil {
			t.Fatal(err)
		}

		if h := LoadHistory(); len(h.Consume) != 0 {
			t.Errorf("len = %d, want 0", len(h.Consume))
		}
	})
}

func TestGetHistoryPath(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(KaratEnvConfigDir, dir)

	got, err := GetHistoryPath()
	if err != nil {
		t.Fatalf("GetHistoryPath() error: %v", err)
	}
	if want := filepath.Join(dir, ".config", "karat", "history.yaml"); got != want {
		t.Errorf("GetHistoryPath() = %q, want %q", got, want)
	}

	cfg, err := GetConfigPath()
	if err != nil {
		t.Fatalf("GetConfigPath() error: %v", err)
	}
	if want := filepath.Join(dir, ".config", "karat", "config.yaml"); cfg != want {
		t.Errorf("GetConfigPath() = %q, want %q", cfg, want)
	}
}
