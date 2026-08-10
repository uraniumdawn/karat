// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package config

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

const (
	// maxTopicConsumeHistory caps the remembered consume parameter strings per cluster and
	// topic. The cap is per topic so that consuming other topics can never evict a topic's
	// own history, which is also where its default parameters come from.
	maxTopicConsumeHistory = 30

	// maxConsumeHistory caps the file as a whole. It only ever bites when many topics have
	// been consumed on many clusters, since every topic is capped on its own first.
	maxConsumeHistory = 300
)

// ConsumeEntry is one remembered consume parameter string, scoped to the cluster and topic
// it was used on.
type ConsumeEntry struct {
	Cluster string    `yaml:"cluster"`
	Topic   string    `yaml:"topic"`
	Params  string    `yaml:"params"`
	At      time.Time `yaml:"at"`
}

// History is the persisted usage history, stored next to config.yaml. It is best-effort
// state: it never carries anything the application cannot run without.
type History struct {
	// Consume holds the consume parameter strings, newest first.
	Consume []ConsumeEntry `yaml:"consume,omitempty"`
}

// LoadHistory reads the history file. A missing, unreadable or malformed file yields an
// empty history — losing it must never keep the application from starting.
func LoadHistory() *History {
	path, err := GetHistoryPath()
	if err != nil {
		return &History{}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Warn().Err(err).Str("path", path).Msg("cannot read history file")
		}
		return &History{}
	}

	h := &History{}
	if err := yaml.Unmarshal(data, h); err != nil {
		log.Warn().Err(err).Str("path", path).Msg("cannot parse history file, starting empty")
		return &History{}
	}

	return h
}

// Save writes the history back to disk, creating the config directory when this is the
// first run.
func (h *History) Save() error {
	path, err := GetHistoryPath()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	data, err := yaml.Marshal(h)
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0o644)
}

// AddConsume records params as the newest entry for the given cluster and topic. Repeating
// parameters that are already known moves them to the front instead of duplicating them.
// The topic keeps at most maxTopicConsumeHistory entries, oldest dropped first.
func (h *History) AddConsume(cluster, topic, params string) {
	entries := make([]ConsumeEntry, 0, len(h.Consume)+1)
	entries = append(entries, ConsumeEntry{
		Cluster: cluster,
		Topic:   topic,
		Params:  params,
		At:      time.Now().UTC(),
	})

	// The entry just added is this topic's first, so it counts against its cap.
	kept := 1
	for _, e := range h.Consume {
		if e.Cluster == cluster && e.Topic == topic {
			if e.Params == params || kept >= maxTopicConsumeHistory {
				continue
			}
			kept++
		}
		entries = append(entries, e)
	}

	if len(entries) > maxConsumeHistory {
		entries = entries[:maxConsumeHistory]
	}

	h.Consume = entries
}

// LastConsume returns the parameters last used on the given cluster and topic, or "" when
// the topic has not been consumed yet.
func (h *History) LastConsume(cluster, topic string) string {
	for _, e := range h.Consume {
		if e.Cluster == cluster && e.Topic == topic {
			return e.Params
		}
	}
	return ""
}

// ConsumeFor returns the entries remembered for the given cluster and topic, newest first.
// Parameters used on other topics are not offered: they name partitions, offsets and serdes
// that belong to a topic, and running them elsewhere is rarely what is meant.
func (h *History) ConsumeFor(cluster, topic string) []ConsumeEntry {
	var own []ConsumeEntry
	for _, e := range h.Consume {
		if e.Cluster == cluster && e.Topic == topic {
			own = append(own, e)
		}
	}
	return own
}
