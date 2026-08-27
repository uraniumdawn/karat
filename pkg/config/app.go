// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

// Package config provides configuration management for the karat application.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

// configFileMode is what karat creates config.yaml with. It holds the credentials for every
// cluster the user connects to, so it is theirs to read and nobody else's.
const configFileMode = 0o600

// configDirMode is what karat creates the config directory with, when a first run has to.
const configDirMode = 0o755

// JqConfig holds settings for piping output through jq.
type JqConfig struct {
	Enable  bool     `yaml:"enable,omitempty"`
	Command []string `yaml:"command,omitempty"`
}

// Config is the root application configuration.
type Config struct {
	// literal is the same configuration merged from the user's file without expanding
	// environment variables: the record of what the user actually typed. It is what keeps a
	// write-back from replacing every ${VAR} reference with the value it stands for. Nil for a
	// Config that did not come through the loader, in which case there is nothing to restore.
	literal any

	Karat struct {
		// Mode is how much karat may change what it points at. It comes first so it is the
		// first thing the config file says.
		Mode Mode `yaml:"mode,omitempty"`

		Clusters         []*ClusterConfig        `yaml:"clusters"`
		SchemaRegistries []*SchemaRegistryConfig `yaml:"schema-registries"`
		CliTemplates     []string                `yaml:"cli_templates,omitempty"`
		Connect          []*ConnectConfig        `yaml:"connect,omitempty"`
		API              ApiConfig               `yaml:"api,omitempty"`
		UI               UIConfig                `yaml:"ui,omitempty"`
		Features         FeaturesConfig          `yaml:"features,omitempty"`
		Style            string                  `yaml:"style,omitempty"`
		Editor           string                  `yaml:"editor,omitempty"`
	} `yaml:"karat"`
}

// FeaturesConfig toggles optional features. Defaults live in default_config.yaml; set a key
// in your own config to override one.
//
// Flags are *bool so that "absent" is distinguishable from an explicit value, which is what
// lets a user's false override a true default and vice versa.
type FeaturesConfig struct {
	// TopicSize controls the Topics list Size column and the topic description's actual
	// size, both fed by franz-go DescribeLogDirs.
	TopicSize *bool `yaml:"topic_size,omitempty"`

	// ConsumerGroupLag controls the Consumer Groups list Lag column, fed by one
	// ListConsumerGroupOffsets per group plus a single ListOffsets.
	ConsumerGroupLag *bool `yaml:"consumer_group_lag,omitempty"`
}

// TopicSizeEnabled reports whether the topic size feature is enabled (default: true).
func (c *Config) TopicSizeEnabled() bool {
	return featureEnabled(c.Karat.Features.TopicSize)
}

// ConsumerGroupLagEnabled reports whether the consumer group lag feature is enabled
// (default: true).
func (c *Config) ConsumerGroupLagEnabled() bool {
	return featureEnabled(c.Karat.Features.ConsumerGroupLag)
}

// featureEnabled resolves an optional feature flag that is on unless it is explicitly
// turned off. A nil flag means neither default_config.yaml nor the user config set it.
func featureEnabled(flag *bool) bool {
	return flag == nil || *flag
}

// Editor returns the editor command line used by every editor-backed view (karat.editor).
// default_config.yaml sets it to vim, so it is empty only when the user config clears it,
// and the caller falls back to vim in that case.
func (c *Config) Editor() string {
	return c.Karat.Editor
}

// ApiConfig holds settings controlling Kafka Admin API calls.
type ApiConfig struct {
	Timeout        int `yaml:"timeout"`
	MaxConcurrency int `yaml:"max_concurrency,omitempty"`
}

// UIConfig holds settings controlling UI behavior.
type UIConfig struct {
	// InternalTopicPatterns are regular expressions matched against topic names to
	// classify them as internal when hiding internal topics. Defaults are provided
	// in default_config.yaml and can be overridden by the user's config.
	InternalTopicPatterns []string `yaml:"internal_topic_patterns,omitempty"`
}

// ClusterConfig holds Kafka cluster connection properties.
type ClusterConfig struct {
	Name       string            `yaml:"name"`
	Properties map[string]string `yaml:"properties"`
	Selected   bool              `yaml:"selected,omitempty"`
}

// GetBootstrapServers returns the cluster's bootstrap.servers property, or "" if unset.
func (c *ClusterConfig) GetBootstrapServers() string {
	if bootstrap, ok := c.Properties["bootstrap.servers"]; ok {
		return bootstrap
	}
	return ""
}

// GetAPICallTimeout returns the API call timeout duration.
func (c *Config) GetAPICallTimeout() time.Duration {
	return time.Duration(c.Karat.API.Timeout) * time.Second
}

// GetMaxConcurrency returns the maximum number of concurrent Kafka API calls.
func (c *Config) GetMaxConcurrency() int {
	return c.Karat.API.MaxConcurrency
}

// SchemaRegistryConfig holds Schema Registry connection properties.
type SchemaRegistryConfig struct {
	Name                   string `yaml:"name"`
	SchemaRegistryURL      string `yaml:"schema.registry.url"`
	SchemaRegistryUsername string `yaml:"schema.registry.sasl.username,omitempty"`
	SchemaRegistryPassword string `yaml:"schema.registry.sasl.password,omitempty"`
	Selected               bool   `yaml:"selected,omitempty"`
}

// ConnectConfig holds Kafka Connect connection properties.
type ConnectConfig struct {
	Name     string `yaml:"name"`
	URL      string `yaml:"url"`
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
	Selected bool   `yaml:"selected,omitempty"`
}

// LoadAppConfig loads the application configuration by merging built-in defaults
// with the user config file. User values take precedence.
func LoadAppConfig() (*Config, error) {
	configPath, err := GetConfigPath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(configPath)
	if errors.Is(err, fs.ErrNotExist) {
		// A first run has nothing to read. Start on the built-in defaults and write them out,
		// so the file the user is meant to edit exists and says what karat is running.
		log.Info().Str("path", configPath).Msg("no config file, starting with the defaults")

		cfg, mergeErr := mergeAppConfig(nil)
		if mergeErr != nil {
			return nil, mergeErr
		}
		if writeErr := cfg.Save(); writeErr != nil {
			log.Warn().Err(writeErr).Msg("cannot write the default config file")
		}

		return cfg, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	return mergeAppConfig(data)
}

// mergeAppConfig merges a raw user config (with environment variables expanded) on top of the
// built-in defaults and validates the result. See mergeYAMLInto for the merge rule.
func mergeAppConfig(userConfig []byte) (*Config, error) {
	defaults, err := loadDefaultAppConfig()
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err := mergeYAMLInto(defaultConfigData, []byte(os.ExpandEnv(string(userConfig))), cfg); err != nil {
		return nil, err
	}

	validateAPIConfig(&cfg.Karat.API, defaults.Karat.API)
	validateMode(cfg, defaults.Karat.Mode)

	// The same merge again, unexpanded, so a later write-back can put the user's ${VAR}
	// references back where the running config only has the values. A file karat cannot parse
	// twice is not worth failing the launch over: without it the write-back is skipped.
	literal, err := literalTree(userConfig)
	if err != nil {
		log.Warn().Err(err).Msg("cannot read the config literally, it will not be written back")
	}
	cfg.literal = literal

	// CLI templates are the one part of the config that is text for a shell, not a value for
	// karat, so they keep the ${VAR} references the user wrote: sh expands them when the
	// command runs, and until then the command stays the portable one the user can read, yank
	// and paste elsewhere. Expanding them here would also swallow a $ that was never a
	// variable, such as a jq filter's $name.
	if templates, ok := literalCliTemplates(literal); ok {
		cfg.Karat.CliTemplates = templates
	}

	return cfg, nil
}

// literalCliTemplates returns karat.cli_templates as the user wrote them, and reports whether
// the literal tree held them in that shape. A tree karat could not read leaves the expanded
// templates in place: a command with the values in it still runs.
func literalCliTemplates(literal any) ([]string, bool) {
	root, ok := literal.(map[string]any)
	if !ok {
		return nil, false
	}
	karat, ok := root["karat"].(map[string]any)
	if !ok {
		return nil, false
	}
	entries, ok := karat["cli_templates"].([]any)
	if !ok {
		return nil, false
	}

	templates := make([]string, 0, len(entries))
	for _, entry := range entries {
		text, ok := entry.(string)
		if !ok {
			return nil, false
		}
		templates = append(templates, text)
	}
	return templates, true
}

// literalTree merges the user config into the defaults without expanding environment
// variables, and returns it as a plain YAML tree rather than a Config: the point is to keep the
// text the user wrote, which decoding into typed fields would not preserve any better but a
// tree keeps free of the struct's own defaults.
func literalTree(userConfig []byte) (any, error) {
	var tree any
	if err := mergeYAMLInto(defaultConfigData, userConfig, &tree); err != nil {
		return nil, err
	}
	return tree, nil
}

// validateAPIConfig resets any invalid (<=0) API fields to their defaults and logs a warning.
func validateAPIConfig(cfg *ApiConfig, def ApiConfig) {
	if cfg.Timeout <= 0 {
		log.Warn().Int("value", cfg.Timeout).Int("default", def.Timeout).
			Msg("invalid api.timeout, using default")
		cfg.Timeout = def.Timeout
	}
	if cfg.MaxConcurrency <= 0 {
		log.Warn().Int("value", cfg.MaxConcurrency).Int("default", def.MaxConcurrency).
			Msg("invalid api.max_concurrency, using default")
		cfg.MaxConcurrency = def.MaxConcurrency
	}
}

// validateMode resets an unrecognised mode to the built-in default and logs a warning. An
// absent one never reaches here: default_config.yaml supplies it through the merge.
func validateMode(cfg *Config, def Mode) {
	if !cfg.Karat.Mode.valid() {
		log.Warn().Str("value", string(cfg.Karat.Mode)).Str("default", string(def)).
			Msg("invalid mode, using default")
		cfg.Karat.Mode = def
	}
}

func loadDefaultAppConfig() (*Config, error) {
	cfg := &Config{}
	if err := yaml.Unmarshal(defaultConfigData, cfg); err != nil {
		return nil, fmt.Errorf("error unmarshalling default_config.yaml: %w", err)
	}
	return cfg, nil
}

// SaveIfChanged writes the configuration back when the file no longer says the same thing,
// and reports whether it wrote.
//
// It is what keeps config.yaml equal to the configuration karat is actually running: defaults
// merged in, every connection's mode named. Comments and hand-formatting do not survive the
// marshal, which is the price of the file being the whole picture rather than only the part
// the user typed.
func (c *Config) SaveIfChanged() (bool, error) {
	configPath, err := GetConfigPath()
	if err != nil {
		return false, err
	}

	data, err := c.marshal()
	if err != nil {
		return false, err
	}

	if current, err := os.ReadFile(configPath); err == nil && bytes.Equal(current, data) {
		return false, nil
	}

	if err := os.WriteFile(configPath, data, configFileMode); err != nil {
		log.Error().Err(err).Msg("failed to write config")
		return false, err
	}
	return true, nil
}

// Save writes the current configuration back to the config file.
func (c *Config) Save() error {
	configPath, err := GetConfigPath()
	if err != nil {
		return err
	}

	data, err := c.marshal()
	if err != nil {
		return err
	}

	// The directory is missing on a first run, when this call is what creates the file.
	if err := os.MkdirAll(filepath.Dir(configPath), configDirMode); err != nil {
		log.Error().Err(err).Msg("failed to create the config directory")
		return err
	}

	if err := os.WriteFile(configPath, data, configFileMode); err != nil {
		log.Error().Err(err).Msg("failed to write config")
		return err
	}

	return nil
}

// marshal renders the configuration the way it belongs in config.yaml: as karat is running it,
// except that every value the user wrote as an environment-variable reference stays a
// reference. See restorePlaceholders for why that matters.
func (c *Config) marshal() ([]byte, error) {
	data, err := yaml.Marshal(c)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal config")
		return nil, err
	}
	if c.literal == nil {
		return data, nil
	}

	var tree any
	if err := yaml.Unmarshal(data, &tree); err != nil {
		log.Error().Err(err).Msg("failed to re-read the marshalled config")
		return nil, err
	}

	data, err = yaml.Marshal(restorePlaceholders(tree, c.literal))
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal config")
		return nil, err
	}
	return data, nil
}

// restorePlaceholders walks the configuration about to be written alongside the file as the
// user wrote it, and returns it with every value that differs only because of environment
// variable expansion put back the way they wrote it.
//
// LoadAppConfig expands ${VAR} before decoding, so the running configuration holds the secrets
// themselves, not the references to them. Writing that back would copy every password into
// config.yaml in plain text and destroy the reference — unrecoverably, since the file is the
// only place it was written down. A value karat changed while running does not match the
// expansion of what the user typed, so it is kept as changed; only pure expansions are undone.
func restorePlaceholders(current, literal any) any {
	switch literal := literal.(type) {
	case map[string]any:
		currentMap, ok := current.(map[string]any)
		if !ok {
			return current
		}
		for key, value := range currentMap {
			if literalValue, ok := literal[key]; ok {
				currentMap[key] = restorePlaceholders(value, literalValue)
			}
		}
		// A reference to a variable that is not set expands to nothing, which leaves the field
		// at its zero value, which omitempty drops from the marshalled tree altogether. Put it
		// back: config.yaml is the only place that reference is written down, and a session
		// started without the variable exported would otherwise erase it.
		for key, literalValue := range literal {
			if _, ok := currentMap[key]; ok {
				continue
			}
			if text, ok := literalValue.(string); ok && isEnvRef(text) && os.ExpandEnv(text) == "" {
				currentMap[key] = text
			}
		}
		return currentMap

	case []any:
		currentList, ok := current.([]any)
		if !ok {
			return current
		}
		// Entries line up by position: both trees come from the same document, and a sequence
		// is replaced wholesale by the merge rather than merged element by element.
		for i := range min(len(currentList), len(literal)) {
			currentList[i] = restorePlaceholders(currentList[i], literal[i])
		}
		return currentList

	case string:
		if !isEnvRef(literal) {
			return current
		}
		if value, ok := current.(string); ok {
			if value != literal && os.ExpandEnv(literal) == value {
				return literal
			}
			return current
		}
		// A reference in a field that is not a string — a timeout, a feature flag — decodes to
		// that field's type, so what comes back from the marshaller is a number or a bool.
		// Compare it as text, or the reference is written out as the value it stood for.
		if os.ExpandEnv(literal) == fmt.Sprint(current) {
			return literal
		}
		return current

	default:
		return current
	}
}

// isEnvRef reports whether the user wrote this value as an environment variable reference.
func isEnvRef(value string) bool {
	return strings.Contains(value, "${")
}
