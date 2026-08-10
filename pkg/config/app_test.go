// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestFeaturesDefaultToEnabled(t *testing.T) {
	cfg, err := mergeAppConfig([]byte("karat:\n  clusters: []\n"))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}

	if !cfg.TopicSizeEnabled() {
		t.Error("topic size should be enabled when the user config says nothing about it")
	}
	if !cfg.ConsumerGroupLagEnabled() {
		t.Error("consumer group lag should be enabled when the user config says nothing about it")
	}
}

// The defaults come from default_config.yaml, not from the nil fallback in featureEnabled.
func TestFeatureDefaultsComeFromDefaultConfig(t *testing.T) {
	defaults, err := loadDefaultAppConfig()
	if err != nil {
		t.Fatalf("loadDefaultAppConfig() error = %v", err)
	}

	if defaults.Karat.Features.TopicSize == nil {
		t.Error("default_config.yaml should set karat.features.topic_size")
	}
	if defaults.Karat.Features.ConsumerGroupLag == nil {
		t.Error("default_config.yaml should set karat.features.consumer_group_lag")
	}
}

// karat.editor is the only place the editor command line comes from, so the defaults must
// carry a usable one, and a user entry must win over it.
func TestEditorDefaultsToVim(t *testing.T) {
	cfg, err := mergeAppConfig([]byte("karat:\n  clusters: []\n"))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}
	if got := cfg.Editor(); got != "vim" {
		t.Errorf("Editor() = %q, want %q by default", got, "vim")
	}

	cfg, err = mergeAppConfig([]byte("karat:\n  editor: \"code --wait\"\n"))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}
	if got := cfg.Editor(); got != "code --wait" {
		t.Errorf("Editor() = %q, want %q", got, "code --wait")
	}
}

// A feature flag is only useful if switching it off survives the merge with the defaults.
func TestFeaturesCanBeDisabledIndividually(t *testing.T) {
	tests := []struct {
		name         string
		userConfig   string
		wantSize     bool
		wantGroupLag bool
	}{
		{
			name:         "topic size off",
			userConfig:   "karat:\n  features:\n    topic_size: false\n",
			wantSize:     false,
			wantGroupLag: true,
		},
		{
			name:         "consumer group lag off",
			userConfig:   "karat:\n  features:\n    consumer_group_lag: false\n",
			wantSize:     true,
			wantGroupLag: false,
		},
		{
			name:         "both off",
			userConfig:   "karat:\n  features:\n    topic_size: false\n    consumer_group_lag: false\n",
			wantSize:     false,
			wantGroupLag: false,
		},
		{
			name:         "both explicitly on",
			userConfig:   "karat:\n  features:\n    topic_size: true\n    consumer_group_lag: true\n",
			wantSize:     true,
			wantGroupLag: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := mergeAppConfig([]byte(tt.userConfig))
			if err != nil {
				t.Fatalf("mergeAppConfig() error = %v", err)
			}

			if got := cfg.TopicSizeEnabled(); got != tt.wantSize {
				t.Errorf("TopicSizeEnabled() = %v, want %v", got, tt.wantSize)
			}
			if got := cfg.ConsumerGroupLagEnabled(); got != tt.wantGroupLag {
				t.Errorf("ConsumerGroupLagEnabled() = %v, want %v", got, tt.wantGroupLag)
			}
		})
	}
}

// Disabling a feature must not disturb the rest of the defaults.
func TestFeaturesMergeKeepsAPIDefaults(t *testing.T) {
	cfg, err := mergeAppConfig([]byte("karat:\n  features:\n    topic_size: false\n"))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}

	if cfg.GetAPICallTimeout() <= 0 {
		t.Errorf("GetAPICallTimeout() = %v, want the built-in default", cfg.GetAPICallTimeout())
	}
	if cfg.GetMaxConcurrency() <= 0 {
		t.Errorf("GetMaxConcurrency() = %d, want the built-in default", cfg.GetMaxConcurrency())
	}
	if len(cfg.Karat.UI.InternalTopicPatterns) == 0 {
		t.Error("internal_topic_patterns defaults should survive the merge")
	}
}

// Config.Save rewrites the user's config file (e.g. when a cluster is selected in the UI),
// so a disabled feature must survive marshalling and being merged back in.
func TestDisabledFeatureSurvivesSaveRoundTrip(t *testing.T) {
	cfg, err := mergeAppConfig([]byte("karat:\n  features:\n    consumer_group_lag: false\n"))
	if err != nil {
		t.Fatalf("mergeAppConfig() error = %v", err)
	}

	saved, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("yaml.Marshal() error = %v", err)
	}
	if !strings.Contains(string(saved), "consumer_group_lag: false") {
		t.Fatalf("saved config does not keep the flag:\n%s", saved)
	}

	reloaded, err := mergeAppConfig(saved)
	if err != nil {
		t.Fatalf("mergeAppConfig(saved) error = %v", err)
	}
	if reloaded.ConsumerGroupLagEnabled() {
		t.Error("consumer group lag should stay disabled after a save/reload round trip")
	}
	if !reloaded.TopicSizeEnabled() {
		t.Error("topic size should stay enabled after a save/reload round trip")
	}
}

// A first run has no config file. Refusing to start would leave the user with nothing to edit;
// karat comes up on the built-in defaults and writes them out instead.
func TestLoadAppConfigWithoutAFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(KaratEnvConfigDir, dir)

	cfg, err := LoadAppConfig()
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v, want a default config", err)
	}
	if cfg.Mode() != Confirm {
		t.Errorf("Mode() = %q, want the default %q", cfg.Mode(), Confirm)
	}
	if got := cfg.GetAPICallTimeout(); got == 0 {
		t.Error("the API timeout was left at zero")
	}

	written, err := os.ReadFile(filepath.Join(dir, ".config", "karat", "config.yaml"))
	if err != nil {
		t.Fatalf("no config file was written: %v", err)
	}
	if !strings.Contains(string(written), "mode: confirm") {
		t.Errorf("the written config does not carry the defaults:\n%s", written)
	}
}
