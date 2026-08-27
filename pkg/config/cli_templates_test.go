// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package config

import (
	"strings"
	"testing"
)

const configWithCliTemplates = `
karat:
  clusters:
    - name: prod
      properties:
        bootstrap.servers: ${KAFKA_BOOTSTRAP}
        sasl.password: ${KAFKA_PASSWORD}
  cli_templates:
    - kcat -C -b $KAFKA_BOOTSTRAP -t {{topic}}
    - kcat -C -b ${KAFKA_BOOTSTRAP} -r ${SR_URL} -t {{topic}} | jq '.[] | $value'
`

func TestCliTemplatesKeepEnvironmentReferences(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP", "broker:9092")
	t.Setenv("KAFKA_PASSWORD", "s3cr3t")
	t.Setenv("SR_URL", "http://sr:8081")

	cfg, err := mergeAppConfig([]byte(configWithCliTemplates))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}

	want := []string{
		"kcat -C -b $KAFKA_BOOTSTRAP -t {{topic}}",
		"kcat -C -b ${KAFKA_BOOTSTRAP} -r ${SR_URL} -t {{topic}} | jq '.[] | $value'",
	}
	got := cfg.Karat.CliTemplates
	if len(got) != len(want) {
		t.Fatalf("cli_templates = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("cli_templates[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// Everything that is a value for karat rather than text for a shell still gets expanded.
	if bootstrap := cfg.Karat.Clusters[0].GetBootstrapServers(); bootstrap != "broker:9092" {
		t.Errorf("bootstrap.servers = %q, want %q", bootstrap, "broker:9092")
	}
	if password := cfg.Karat.Clusters[0].Properties["sasl.password"]; password != "s3cr3t" {
		t.Errorf("sasl.password = %q, want %q", password, "s3cr3t")
	}
}

// An unset variable must not silently empty the command: the reference stays, and the shell
// reports the failure when the command runs.
func TestCliTemplatesKeepReferencesToUnsetVariables(t *testing.T) {
	cfg, err := mergeAppConfig(
		[]byte("karat:\n  cli_templates:\n    - kcat -b ${MISSING_BOOTSTRAP} -t {{topic}}\n"),
	)
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}

	want := "kcat -b ${MISSING_BOOTSTRAP} -t {{topic}}"
	if len(cfg.Karat.CliTemplates) != 1 || cfg.Karat.CliTemplates[0] != want {
		t.Errorf("cli_templates = %v, want [%q]", cfg.Karat.CliTemplates, want)
	}
}

func TestSaveKeepsEnvironmentReferencesInCliTemplates(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP", "broker:9092")
	t.Setenv("KAFKA_PASSWORD", "s3cr3t")
	t.Setenv("SR_URL", "http://sr:8081")

	cfg, err := mergeAppConfig([]byte(configWithCliTemplates))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}

	data := saveInTempConfigDir(t, cfg)
	if !strings.Contains(data, "kcat -C -b $KAFKA_BOOTSTRAP -t {{topic}}") {
		t.Errorf("written config lost the bare reference:\n%s", data)
	}
	if strings.Contains(data, "broker:9092 -t {{topic}}") {
		t.Errorf("written config expanded a cli template:\n%s", data)
	}
}

func TestLiteralCliTemplatesRejectsAnUnusableTree(t *testing.T) {
	for name, literal := range map[string]any{
		"nil":              nil,
		"not a map":        "karat",
		"no karat key":     map[string]any{"other": map[string]any{}},
		"templates as map": map[string]any{"karat": map[string]any{"cli_templates": map[string]any{}}},
		"non-string entry": map[string]any{"karat": map[string]any{"cli_templates": []any{42}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := literalCliTemplates(literal); ok {
				t.Errorf("literalCliTemplates(%v) reported success", literal)
			}
		})
	}
}
