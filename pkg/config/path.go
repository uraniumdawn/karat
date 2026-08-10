// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package config

import (
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
)

const (
	// KaratEnvConfigDir is the environment variable name for custom config directory.
	KaratEnvConfigDir = "KARAT_CONFIG_DIR"
)

func isEnvSet(env string) bool {
	return os.Getenv(env) != ""
}

// configDir returns the directory holding the application's files: <KARAT_CONFIG_DIR or
// $HOME>/.config/karat.
func configDir() (string, error) {
	var base string
	switch {
	case isEnvSet(KaratEnvConfigDir):
		base = os.Getenv(KaratEnvConfigDir)
	default:
		homeDir, err := os.UserHomeDir()
		if err != nil {
			log.Fatal().Err(err).Msg("error getting home directory")
			return "", err
		}
		base = homeDir
	}
	return filepath.Join(base, ".config", "karat"), nil
}

// GetConfigPath returns the path to the application configuration file.
func GetConfigPath() (string, error) {
	dir, err := configDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "config.yaml"), nil
}

// GetLogPath returns the path to the application log file. It sits beside the configuration,
// so a session pointed at another KARAT_CONFIG_DIR keeps its own log instead of writing into
// the one the default instance is using.
func GetLogPath() (string, error) {
	dir, err := configDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "karat.log"), nil
}

// GetHistoryPath returns the path to the file holding the application's usage history.
func GetHistoryPath() (string, error) {
	dir, err := configDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "history.yaml"), nil
}
