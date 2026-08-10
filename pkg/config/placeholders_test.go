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

// configWithSecrets is a user config in the shape the README documents: connection secrets
// named by environment variable rather than written down.
const configWithSecrets = `
karat:
  clusters:
    - name: prod
      properties:
        bootstrap.servers: ${KAFKA_BOOTSTRAP}
        sasl.password: ${KAFKA_PASSWORD}
        sasl.username: karat
  schema-registries:
    - name: prod-sr
      schema.registry.url: http://sr:8081
      schema.registry.sasl.password: ${SR_PASSWORD}
  connect:
    - name: prod-connect
      url: http://connect:8083
      password: ${CONNECT_PASSWORD}
`

// saveInTempConfigDir points the config path at a temporary directory and writes cfg there,
// returning what landed on disk.
func saveInTempConfigDir(t *testing.T, cfg *Config) string {
	t.Helper()

	dir := t.TempDir()
	t.Setenv(KaratEnvConfigDir, dir)
	karatDir := filepath.Join(dir, ".config", "karat")
	if err := os.MkdirAll(karatDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := cfg.Save(); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(karatDir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// The write-back must never turn a ${VAR} reference into the secret it stands for: the file is
// the only place the reference was written down, and it is world-readable prose otherwise.
func TestSaveKeepsEnvironmentReferences(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP", "broker:9092")
	t.Setenv("KAFKA_PASSWORD", "s3cr3t")
	t.Setenv("SR_PASSWORD", "sr-s3cr3t")
	t.Setenv("CONNECT_PASSWORD", "connect-s3cr3t")

	cfg, err := mergeAppConfig([]byte(configWithSecrets))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}
	// The running configuration does hold the expanded values — that is the point of expanding.
	if got := cfg.Karat.Clusters[0].Properties["sasl.password"]; got != "s3cr3t" {
		t.Fatalf("running config has password %q, want the expanded value", got)
	}

	saved := saveInTempConfigDir(t, cfg)

	for _, secret := range []string{"s3cr3t", "sr-s3cr3t", "connect-s3cr3t", "broker:9092"} {
		if strings.Contains(saved, secret) {
			t.Errorf("the expanded value %q was written to config.yaml:\n%s", secret, saved)
		}
	}
	for _, ref := range []string{
		"${KAFKA_BOOTSTRAP}",
		"${KAFKA_PASSWORD}",
		"${SR_PASSWORD}",
		"${CONNECT_PASSWORD}",
	} {
		if !strings.Contains(saved, ref) {
			t.Errorf("the reference %s did not survive the write-back:\n%s", ref, saved)
		}
	}
	// Values the user wrote out stay as they are.
	if !strings.Contains(saved, "sasl.username: karat") {
		t.Errorf("a plain value did not survive the write-back:\n%s", saved)
	}
}

// An unset variable expands to nothing. Writing that back would replace the reference with an
// empty string, and the next launch would connect with an empty password instead of failing.
func TestSaveKeepsReferencesToUnsetVariables(t *testing.T) {
	t.Setenv("KAFKA_PASSWORD", "")
	if err := os.Unsetenv("KAFKA_PASSWORD"); err != nil {
		t.Fatal(err)
	}

	cfg, err := mergeAppConfig([]byte(configWithSecrets))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}
	if got := cfg.Karat.Clusters[0].Properties["sasl.password"]; got != "" {
		t.Fatalf("unset variable expanded to %q, want empty", got)
	}

	if saved := saveInTempConfigDir(t, cfg); !strings.Contains(saved, "${KAFKA_PASSWORD}") {
		t.Errorf("the reference to an unset variable was replaced by nothing:\n%s", saved)
	}
}

// Only expansion is undone. What karat itself changed while running — the mode, the selected
// connection — is still written, including over a value that came from a variable.
func TestSaveKeepsRuntimeChanges(t *testing.T) {
	t.Setenv("KARAT_MODE", "yolo")

	cfg, err := mergeAppConfig([]byte("karat:\n  mode: ${KARAT_MODE}\n  clusters:\n    - name: prod\n"))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}
	if got := cfg.Mode(); got != Yolo {
		t.Fatalf("Mode() = %q, want the expanded %q", got, Yolo)
	}

	cfg.SetMode(ReadOnly)
	cfg.Karat.Clusters[0].Selected = true

	saved := saveInTempConfigDir(t, cfg)
	if !strings.Contains(saved, "mode: read-only") {
		t.Errorf("the mode karat is running in was not written:\n%s", saved)
	}
	if !strings.Contains(saved, "selected: true") {
		t.Errorf("the selected cluster was not written:\n%s", saved)
	}
}

// Nothing to restore for a config that never came through the loader.
func TestSaveWithNoLiteralTree(t *testing.T) {
	cfg := &Config{}
	cfg.SetMode(Yolo)

	if saved := saveInTempConfigDir(t, cfg); !strings.Contains(saved, "mode: yolo") {
		t.Errorf("saved config does not name the mode:\n%s", saved)
	}
}

// The file holds every cluster's credentials, so karat must not create it readable by anyone
// else. An existing file keeps whatever mode the user gave it.
func TestSaveCreatesThePrivateConfigFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(KaratEnvConfigDir, dir)
	karatDir := filepath.Join(dir, ".config", "karat")
	if err := os.MkdirAll(karatDir, 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := &Config{}
	if err := cfg.Save(); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	info, err := os.Stat(filepath.Join(karatDir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != configFileMode {
		t.Errorf("config.yaml created with mode %o, want %o", got, configFileMode)
	}
}

// A struct field carries omitempty, so a reference that expands to nothing leaves no key at all
// in the marshalled tree — unlike a map entry, which keeps its key with an empty value. The
// reference has to survive that too, or a session started without the variable exported strips
// the passwords out of the user's config file.
func TestSaveKeepsUnsetReferencesInOmitemptyFields(t *testing.T) {
	for _, name := range []string{"SR_PASSWORD", "CONNECT_PASSWORD"} {
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}

	cfg, err := mergeAppConfig([]byte(configWithSecrets))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}

	saved := saveInTempConfigDir(t, cfg)
	for _, want := range []string{"${SR_PASSWORD}", "${CONNECT_PASSWORD}"} {
		if !strings.Contains(saved, want) {
			t.Errorf("%s was dropped from the saved config:\n%s", want, saved)
		}
	}
}

// A reference in a field that is not a string decodes to that field's type, so the value the
// marshaller hands back is a number, not the text the user wrote. It is still their reference.
func TestSaveKeepsReferencesInNonStringFields(t *testing.T) {
	t.Setenv("KARAT_TIMEOUT", "45")

	cfg, err := mergeAppConfig([]byte("karat:\n  api:\n    timeout: ${KARAT_TIMEOUT}\n"))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}
	if got := cfg.Karat.API.Timeout; got != 45 {
		t.Fatalf("Timeout = %d, want the expanded 45", got)
	}

	saved := saveInTempConfigDir(t, cfg)
	if !strings.Contains(saved, "${KARAT_TIMEOUT}") {
		t.Errorf("the reference was written out as the value it stood for:\n%s", saved)
	}
	if strings.Contains(saved, "timeout: 45") {
		t.Errorf("the expanded value was written alongside the reference:\n%s", saved)
	}
}
