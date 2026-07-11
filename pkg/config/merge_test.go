// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package config

import (
	"os"
	"path/filepath"
	"testing"
)

// The merge rule: a key the user set wins whatever its value, a key the user left out keeps
// its default.
func TestMergeYAMLInto(t *testing.T) {
	const defaults = `
root:
  text: default-text
  number: 30
  flag: true
  list:
    - a
    - b
  nested:
    kept: default-kept
    replaced: default-replaced
`

	type nested struct {
		Kept     string `yaml:"kept"`
		Replaced string `yaml:"replaced"`
	}
	type root struct {
		Root struct {
			Text   string   `yaml:"text"`
			Number int      `yaml:"number"`
			Flag   *bool    `yaml:"flag"`
			List   []string `yaml:"list"`
			Nested nested   `yaml:"nested"`
			Extra  string   `yaml:"extra"`
		} `yaml:"root"`
	}

	tests := []struct {
		name  string
		user  string
		check func(t *testing.T, got root)
	}{
		{
			name: "empty user document keeps every default",
			user: "",
			check: func(t *testing.T, got root) {
				if got.Root.Text != "default-text" || got.Root.Number != 30 {
					t.Errorf("defaults lost: %+v", got.Root)
				}
				if got.Root.Flag == nil || !*got.Root.Flag {
					t.Error("flag default should stay true")
				}
			},
		},
		{
			name: "false overrides a true default",
			user: "root:\n  flag: false\n",
			check: func(t *testing.T, got root) {
				if got.Root.Flag == nil || *got.Root.Flag {
					t.Errorf("flag = %v, want false", got.Root.Flag)
				}
				if got.Root.Text != "default-text" {
					t.Errorf("unrelated default lost: %q", got.Root.Text)
				}
			},
		},
		{
			name: "zero and empty string override",
			user: "root:\n  number: 0\n  text: \"\"\n",
			check: func(t *testing.T, got root) {
				if got.Root.Number != 0 {
					t.Errorf("number = %d, want 0", got.Root.Number)
				}
				if got.Root.Text != "" {
					t.Errorf("text = %q, want empty", got.Root.Text)
				}
			},
		},
		{
			name: "empty list overrides the default list",
			user: "root:\n  list: []\n",
			check: func(t *testing.T, got root) {
				if len(got.Root.List) != 0 {
					t.Errorf("list = %v, want empty", got.Root.List)
				}
			},
		},
		{
			name: "list is replaced wholesale, not appended",
			user: "root:\n  list:\n    - c\n",
			check: func(t *testing.T, got root) {
				if len(got.Root.List) != 1 || got.Root.List[0] != "c" {
					t.Errorf("list = %v, want [c]", got.Root.List)
				}
			},
		},
		{
			name: "explicit null clears a value",
			user: "root:\n  text: ~\n",
			check: func(t *testing.T, got root) {
				if got.Root.Text != "" {
					t.Errorf("text = %q, want empty", got.Root.Text)
				}
			},
		},
		{
			name: "nested override keeps sibling defaults",
			user: "root:\n  nested:\n    replaced: user-replaced\n",
			check: func(t *testing.T, got root) {
				if got.Root.Nested.Replaced != "user-replaced" {
					t.Errorf("replaced = %q", got.Root.Nested.Replaced)
				}
				if got.Root.Nested.Kept != "default-kept" {
					t.Errorf("kept = %q, want the default", got.Root.Nested.Kept)
				}
			},
		},
		{
			name: "keys absent from the defaults are added",
			user: "root:\n  extra: user-extra\n",
			check: func(t *testing.T, got root) {
				if got.Root.Extra != "user-extra" {
					t.Errorf("extra = %q", got.Root.Extra)
				}
				if got.Root.Text != "default-text" {
					t.Errorf("unrelated default lost: %q", got.Root.Text)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got root
			if err := mergeYAMLInto([]byte(defaults), []byte(tt.user), &got); err != nil {
				t.Fatalf("mergeYAMLInto() error = %v", err)
			}
			tt.check(t, got)
		})
	}
}

func TestMergeYAMLIntoInvalidUserDocument(t *testing.T) {
	var got struct{}
	if err := mergeYAMLInto([]byte("a: 1\n"), []byte("\tnot yaml"), &got); err == nil {
		t.Error("expected an error for a malformed user document")
	}
}

// The app config follows the same rule, including for lists: an explicitly empty list wins
// over the defaults.
func TestAppConfigListOverrides(t *testing.T) {
	tests := []struct {
		name string
		user string
		want []string
	}{
		{
			name: "unset keeps defaults",
			user: "karat:\n  api:\n    timeout: 10\n",
			want: []string{"^__.*", ".*-changelog$", ".*-repartition$"},
		},
		{
			name: "custom list replaces defaults",
			user: "karat:\n  ui:\n    internal_topic_patterns:\n      - \"^_schemas$\"\n",
			want: []string{"^_schemas$"},
		},
		{
			name: "empty list is honoured",
			user: "karat:\n  ui:\n    internal_topic_patterns: []\n",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := mergeAppConfig([]byte(tt.user))
			if err != nil {
				t.Fatalf("mergeAppConfig() error = %v", err)
			}

			got := cfg.Karat.UI.InternalTopicPatterns
			if len(got) != len(tt.want) {
				t.Fatalf("internal_topic_patterns = %v, want %v", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("internal_topic_patterns[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// The style config uses the same merge, so a partial style file only changes what it names.
func TestLoadColorConfigMergesOnTopOfDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "style.yaml")
	style := "karat:\n  label:\n    fgColor: \"red\"\n  title: \"blue\"\n"
	if err := os.WriteFile(path, []byte(style), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	colors, err := LoadColorConfig(path)
	if err != nil {
		t.Fatalf("LoadColorConfig() error = %v", err)
	}

	if got := colors.Karat.Label.FgColor; got != "red" {
		t.Errorf("label.fgColor = %q, want %q", got, "red")
	}
	if got := colors.Karat.Title; got != "blue" {
		t.Errorf("title = %q, want %q", got, "blue")
	}
	// Sibling of an overridden key, and a key the style file never mentions.
	if got := colors.Karat.Label.BgColor; got != "default" {
		t.Errorf("label.bgColor = %q, want the default %q", got, "default")
	}
	if got := colors.Karat.Selection.BgColor; got != "white" {
		t.Errorf("selection.bgColor = %q, want the default %q", got, "white")
	}
}

func TestLoadColorConfigDefaultsWhenNoStylePath(t *testing.T) {
	colors, err := LoadColorConfig("")
	if err != nil {
		t.Fatalf("LoadColorConfig() error = %v", err)
	}
	if got := colors.Karat.Label.FgColor; got != "orange" {
		t.Errorf("label.fgColor = %q, want the default %q", got, "orange")
	}
}

func TestLoadColorConfigMissingFile(t *testing.T) {
	if _, err := LoadColorConfig(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Error("expected an error for a missing style file")
	}
}
