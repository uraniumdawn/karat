// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

// Package config provides configuration management for the karat application.
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

// JqConfig holds settings for piping output through jq.
type JqConfig struct {
	Enable  bool     `yaml:"enable,omitempty"`
	Command []string `yaml:"command,omitempty"`
}

// Config is the root application configuration.
type Config struct {
	Karat struct {
		Clusters         []*ClusterConfig        `yaml:"clusters"`
		SchemaRegistries []*SchemaRegistryConfig `yaml:"schema-registries"`
		CliTemplates     []string                `yaml:"cli_templates,omitempty"`
		Connect          []*ConnectConfig        `yaml:"connect,omitempty"`
		API              ApiConfig               `yaml:"api,omitempty"`
		UI               UIConfig                `yaml:"ui,omitempty"`
		Features         FeaturesConfig          `yaml:"features,omitempty"`
		Style            string                  `yaml:"style,omitempty"`
	} `yaml:"karat"`
}

// FeaturesConfig toggles optional features that each cost extra Kafka API calls beyond the
// data a page already needs. Defaults live in default_config.yaml; set a key to false in your
// own config to drop both the derived column and its background call.
//
// Flags are *bool so that "absent" is distinguishable from "set to false": see
// optionalBoolTransformer, which is what lets an explicit false override a true default.
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

// featureEnabled resolves an optional feature flag. A nil flag means neither
// default_config.yaml nor the user config set it, which falls back to enabled.
func featureEnabled(flag *bool) bool {
	return flag == nil || *flag
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
	Mode       string            `yaml:"mode,omitempty"`
}

// IsReadOnly returns true when the cluster mode is set to "read-only".
func (c *ClusterConfig) IsReadOnly() bool {
	return c.Mode == "read-only"
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
	Mode                   string `yaml:"mode,omitempty"`
}

// IsReadOnly returns true when the Schema Registry mode is set to "read-only".
func (c *SchemaRegistryConfig) IsReadOnly() bool {
	return c.Mode == "read-only"
}

// ConnectConfig holds Kafka Connect connection properties.
type ConnectConfig struct {
	Name     string `yaml:"name"`
	URL      string `yaml:"url"`
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
	Selected bool   `yaml:"selected,omitempty"`
	Mode     string `yaml:"mode,omitempty"`
}

// IsReadOnly returns true when the Connect cluster mode is set to "read-only".
func (c *ConnectConfig) IsReadOnly() bool {
	return c.Mode == "read-only"
}

// LoadAppConfig loads the application configuration by merging built-in defaults
// with the user config file. User values take precedence.
func LoadAppConfig() (*Config, error) {
	configPath, err := GetConfigPath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(configPath)
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

	return cfg, nil
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

func loadDefaultAppConfig() (*Config, error) {
	cfg := &Config{}
	if err := yaml.Unmarshal(defaultConfigData, cfg); err != nil {
		return nil, fmt.Errorf("error unmarshalling default_config.yaml: %w", err)
	}
	return cfg, nil
}

// Save writes the current configuration back to the config file.
func (c *Config) Save() error {
	configPath, err := GetConfigPath()
	if err != nil {
		return err
	}

	data, err := yaml.Marshal(c)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal config")
		return err
	}

	err = os.WriteFile(configPath, data, 0o644)
	if err != nil {
		log.Error().Err(err).Msg("failed to write config")
		return err
	}

	return nil
}
