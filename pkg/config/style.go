// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

type ColorConfig struct {
	Karat struct {
		Cluster struct {
			FgColor string `yaml:"fgColor"`
			BgColor string `yaml:"bgColor"`
		} `yaml:"cluster"`
		Status struct {
			FgColor string `yaml:"fgColor"`
			BgColor string `yaml:"bgColor"`
		} `yaml:"status"`
		Label struct {
			FgColor string `yaml:"fgColor"`
			BgColor string `yaml:"bgColor"`
		} `yaml:"label"`
		Keybinding struct {
			Key   string `yaml:"key"`
			Value string `yaml:"value"`
		} `yaml:"keybinding"`
		Search struct {
			FgColor string `yaml:"fgColor"`
			BgColor string `yaml:"bgColor"`
		} `yaml:"search"`
		Selection struct {
			FgColor string `yaml:"fgColor"`
			BgColor string `yaml:"bgColor"`
		} `yaml:"selection"`
		Placeholder string `yaml:"placeholder"`
		Title       string `yaml:"title"`
		Border      string `yaml:"border"`
		Background  string `yaml:"background"`
		Foreground  string `yaml:"foreground"`
	} `yaml:"karat" `
}

func loadDefaultColorConfig() (*ColorConfig, error) {
	defaultConfig := &ColorConfig{}
	if err := yaml.Unmarshal(defaultStyleData, defaultConfig); err != nil {
		log.Error().Err(err).Msg("error unmarshalling default style.yaml")
		return nil, err
	}
	return defaultConfig, nil
}

// LoadColorConfig loads color configuration.
// Loads default_style.yaml as the base. If stylePath is set (karat.style in config.yaml)
// that file is loaded and merged on top, overriding any fields it defines and keeping the
// defaults for the rest (see mergeYAMLInto).
// Relative stylePath is resolved against the config directory.
func LoadColorConfig(stylePath string) (*ColorConfig, error) {
	if stylePath == "" {
		return loadDefaultColorConfig()
	}

	stylePath, err := resolveStylePath(stylePath)
	if err != nil {
		return nil, err
	}

	override, err := readStyleFile(stylePath)
	if err != nil {
		return nil, err
	}

	cfg := &ColorConfig{}
	if err := mergeYAMLInto(defaultStyleData, override, cfg); err != nil {
		log.Error().Err(err).Str("path", stylePath).Msg("error merging style file")
		return nil, err
	}

	return cfg, nil
}

// resolveStylePath turns karat.style into a path on disk: "~/" is expanded, an absolute path is
// taken as written, and anything else is read from the config directory — the same place the
// style file is copied to, so naming it is enough.
func resolveStylePath(stylePath string) (string, error) {
	if strings.HasPrefix(stylePath, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("cannot resolve ~: %w", err)
		}

		return filepath.Join(home, stylePath[2:]), nil
	}

	if filepath.IsAbs(stylePath) {
		return stylePath, nil
	}

	dir, err := configDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, stylePath), nil
}

func readStyleFile(path string) ([]byte, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("style file not found: %s", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("error reading style file")
		return nil, err
	}
	return data, nil
}
