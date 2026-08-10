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

// writeStyle puts a style file at path, creating the directory it lives in.
func writeStyle(t *testing.T, path, border string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	body := "karat:\n  border: \"" + border + "\"\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A style file is copied next to config.yaml, so naming it is enough: a relative karat.style
// is read from the config directory, not from wherever karat happens to be launched.
func TestLoadColorConfigResolvesRelativeToTheConfigDir(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(KaratEnvConfigDir, dir)

	writeStyle(t, filepath.Join(dir, ".config", "karat", "mine.yaml"), "#123456")

	cfg, err := LoadColorConfig("mine.yaml")
	if err != nil {
		t.Fatalf("LoadColorConfig() error = %v", err)
	}
	if cfg.Karat.Border != "#123456" {
		t.Errorf("border = %q, want the style file's #123456", cfg.Karat.Border)
	}
}

// An absolute path is taken as written, wherever it points.
func TestLoadColorConfigTakesAnAbsolutePath(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(KaratEnvConfigDir, dir)

	elsewhere := filepath.Join(t.TempDir(), "theme.yaml")
	writeStyle(t, elsewhere, "#abcdef")

	cfg, err := LoadColorConfig(elsewhere)
	if err != nil {
		t.Fatalf("LoadColorConfig() error = %v", err)
	}
	if cfg.Karat.Border != "#abcdef" {
		t.Errorf("border = %q, want #abcdef", cfg.Karat.Border)
	}
}

// Only the fields the style file sets are overridden; the rest stay at the built-in defaults.
func TestLoadColorConfigKeepsDefaultsForUnsetFields(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(KaratEnvConfigDir, dir)
	writeStyle(t, filepath.Join(dir, ".config", "karat", "mine.yaml"), "#123456")

	defaults, err := LoadColorConfig("")
	if err != nil {
		t.Fatalf("LoadColorConfig(\"\") error = %v", err)
	}

	cfg, err := LoadColorConfig("mine.yaml")
	if err != nil {
		t.Fatalf("LoadColorConfig() error = %v", err)
	}

	if cfg.Karat.Background != defaults.Karat.Background {
		t.Errorf("background = %q, want the default %q",
			cfg.Karat.Background, defaults.Karat.Background)
	}
}

// A style that is not there stops karat, naming the path it looked at — resolved, so the
// message points at the file that is actually missing.
func TestLoadColorConfigReportsAMissingFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(KaratEnvConfigDir, dir)

	_, err := LoadColorConfig("nope.yaml")
	if err == nil {
		t.Fatal("LoadColorConfig() accepted a style file that does not exist")
	}

	want := filepath.Join(dir, ".config", "karat", "nope.yaml")
	if !strings.Contains(err.Error(), want) {
		t.Errorf("error = %q, want it to name %q", err, want)
	}
}
