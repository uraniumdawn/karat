// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNextMode(t *testing.T) {
	tests := []struct {
		from Mode
		want Mode
	}{
		{from: ReadOnly, want: Confirm},
		{from: Confirm, want: Yolo},
		{from: Yolo, want: ReadOnly},
		{from: Mode("read_only"), want: Yolo}, // unknown counts as confirm
		{from: Mode(""), want: Yolo},
	}

	for _, tt := range tests {
		if got := NextMode(tt.from); got != tt.want {
			t.Errorf("NextMode(%q) = %q, want %q", tt.from, got, tt.want)
		}
	}
}

func TestNextModeCyclesThroughEveryMode(t *testing.T) {
	seen := map[Mode]bool{}
	mode := ReadOnly
	for range len(modeCycle) {
		seen[mode] = true
		mode = NextMode(mode)
	}

	for _, want := range []Mode{ReadOnly, Confirm, Yolo} {
		if !seen[want] {
			t.Errorf("cycling never reached %q", want)
		}
	}
	if mode != ReadOnly {
		t.Errorf("cycling ended at %q, want to be back at %q", mode, ReadOnly)
	}
}

func TestModeAndSetMode(t *testing.T) {
	cfg := &Config{}
	cfg.SetMode(Yolo)

	if got := cfg.Mode(); got != Yolo {
		t.Errorf("Mode() = %q, want %q", got, Yolo)
	}
}

// The default lives in default_config.yaml and nowhere else — a user config that says nothing
// about the mode gets it from the merge, the same way it gets api.timeout.
func TestModeDefaultsFromTheDefaultConfig(t *testing.T) {
	cfg, err := mergeAppConfig([]byte("karat:\n  clusters: []\n"))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}
	if got := cfg.Mode(); got != Confirm {
		t.Errorf("Mode() = %q, want the built-in default %q", got, Confirm)
	}

	defaults, err := loadDefaultAppConfig()
	if err != nil {
		t.Fatalf("loadDefaultAppConfig() error = %v", err)
	}
	if defaults.Karat.Mode != Confirm {
		t.Errorf("default_config.yaml says mode %q, want %q", defaults.Karat.Mode, Confirm)
	}
}

// A mode nobody recognises is not worth guessing at, and must not leave the application running
// in something that is neither refused nor asked about.
func TestInvalidModeFallsBackToTheDefault(t *testing.T) {
	cfg, err := mergeAppConfig([]byte("karat:\n  mode: nonsense\n"))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}
	if got := cfg.Mode(); got != Confirm {
		t.Errorf("Mode() = %q, want %q", got, Confirm)
	}
}

func TestModeFromUserConfig(t *testing.T) {
	for _, want := range []Mode{ReadOnly, Confirm, Yolo} {
		cfg, err := mergeAppConfig([]byte("karat:\n  mode: " + string(want) + "\n"))
		if err != nil {
			t.Fatalf("mergeAppConfig() error = %v", err)
		}
		if got := cfg.Mode(); got != want {
			t.Errorf("Mode() = %q, want %q", got, want)
		}
	}
}

// karat 0.3.0 wrote the mode on the connection entry. That key is no longer read, and must not
// survive the next write of the file.
func TestPerEntryModeIsIgnored(t *testing.T) {
	cfg, err := mergeAppConfig([]byte(`
karat:
  clusters:
    - name: prod
      properties:
        bootstrap.servers: localhost:9092
      mode: read-only
`))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}

	if got := cfg.Mode(); got != Confirm {
		t.Errorf("Mode() = %q, want the default %q", got, Confirm)
	}

	dir := t.TempDir()
	t.Setenv(KaratEnvConfigDir, dir)
	if err := os.MkdirAll(filepath.Join(dir, ".config", "karat"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.SaveIfChanged(); err != nil {
		t.Fatalf("SaveIfChanged() error = %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, ".config", "karat", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "read-only") {
		t.Errorf("the per-entry mode survived the rewrite:\n%s", data)
	}
	if !strings.Contains(string(data), "mode: confirm") {
		t.Errorf("the rewritten config does not name the mode:\n%s", data)
	}
}

// The file is kept equal to the configuration karat is running, and nothing is written when it
// already says the same thing.
func TestSaveIfChanged(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(KaratEnvConfigDir, dir)

	karatDir := filepath.Join(dir, ".config", "karat")
	if err := os.MkdirAll(karatDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(karatDir, "config.yaml")

	cfg := &Config{}
	cfg.SetMode(ReadOnly)

	written, err := cfg.SaveIfChanged()
	if err != nil {
		t.Fatalf("SaveIfChanged() error = %v", err)
	}
	if !written {
		t.Errorf("SaveIfChanged() = false on a file that does not exist yet")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading saved config: %v", err)
	}
	if !strings.Contains(string(data), "mode: read-only") {
		t.Errorf("saved config does not name the mode:\n%s", data)
	}

	// Same configuration, same file: nothing to do on the next startup.
	written, err = cfg.SaveIfChanged()
	if err != nil {
		t.Fatalf("SaveIfChanged() error = %v", err)
	}
	if written {
		t.Errorf("SaveIfChanged() = true with the file already up to date")
	}
}
