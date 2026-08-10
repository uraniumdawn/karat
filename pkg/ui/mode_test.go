// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/uraniumdawn/karat/pkg/config"
)

// newCycleApp builds an App holding the given mode, with the config file redirected into a
// temp directory so cycleMode's Save() cannot touch the real one.
func newCycleApp(t *testing.T, mode config.Mode) (*App, string) {
	t.Helper()

	dir := t.TempDir()
	t.Setenv(config.KaratEnvConfigDir, dir)

	// Config.Save writes the file but does not create its directory.
	karatDir := filepath.Join(dir, ".config", "karat")
	if err := os.MkdirAll(karatDir, 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{}
	cfg.SetMode(mode)

	return &App{Config: cfg}, filepath.Join(karatDir, "config.yaml")
}

// <Tab> cycles and saves, asking nothing — the badge is what reports where you landed.
func TestCycleMode(t *testing.T) {
	tests := []struct {
		from config.Mode
		to   config.Mode
	}{
		{from: config.ReadOnly, to: config.Confirm},
		{from: config.Confirm, to: config.Yolo},
		{from: config.Yolo, to: config.ReadOnly},
		// A mode the loader would never produce still has to lead somewhere valid.
		{from: config.Mode(""), to: config.Yolo},
	}

	for _, tt := range tests {
		t.Run(string(tt.from)+"->"+string(tt.to), func(t *testing.T) {
			defer drainStatus()

			app, path := newCycleApp(t, tt.from)
			app.cycleMode()

			if app.confirmPending() {
				t.Errorf("cycling raised a question, want it silent")
			}
			if got := app.Config.Mode(); got != tt.to {
				t.Errorf("mode = %q, want %q", got, tt.to)
			}
			assertConfigMode(t, path, string(tt.to))
		})
	}
}

func assertConfigMode(t *testing.T, path, want string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading saved config: %v", err)
	}
	if got := "mode: " + want; !strings.Contains(string(data), got) {
		t.Errorf("saved config does not contain %q:\n%s", got, data)
	}
}
