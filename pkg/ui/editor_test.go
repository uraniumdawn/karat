// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"errors"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestEditorCommand(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		want       []string
	}{
		{
			name:       "the configured editor is used as is",
			configured: "nano",
			want:       []string{"nano"},
		},
		{
			// karat.editor is the only place an editor comes from, and default_config.yaml
			// gives it a value, so this is the cleared-by-hand case.
			name: "vim when karat.editor states nothing",
			want: []string{"vim"},
		},
		{
			// A configured editor that is only whitespace states nothing, so it must not
			// produce an empty argv.
			name:       "whitespace-only config falls back to vim",
			configured: "\t",
			want:       []string{"vim"},
		},
		{
			name:       "flags travel with the editor",
			configured: "code --wait",
			want:       []string{"code", "--wait"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := editorCommand(tt.configured)
			if !slices.Equal(got, tt.want) {
				t.Errorf("editorCommand(%q) = %v, want %v", tt.configured, got, tt.want)
			}
		})
	}
}

func TestMissingWaitFlagHint(t *testing.T) {
	tests := []struct {
		name     string
		argv     []string
		wantHint bool
		contains string
	}{
		{
			name:     "vs code without a wait flag",
			argv:     []string{"code"},
			wantHint: true,
			contains: `set karat.editor: "code --wait"`,
		},
		{
			name: "vs code with --wait",
			argv: []string{"code", "--wait"},
		},
		{
			name: "vs code with the short form",
			argv: []string{"code", "-w"},
		},
		{
			name:     "sublime is told the flag it actually takes",
			argv:     []string{"subl"},
			wantHint: true,
			contains: `set karat.editor: "subl -w"`,
		},
		{
			name: "sublime with -w",
			argv: []string{"subl", "-w"},
		},
		{
			// The editor is resolved by base name, so a full path still matches.
			name:     "absolute path",
			argv:     []string{"/usr/local/bin/code"},
			wantHint: true,
			contains: `set karat.editor: "code --wait"`,
		},
		{
			// A terminal editor returning unchanged content just means "quit without
			// saving" and must never produce a hint.
			name: "vim",
			argv: []string{"vim"},
		},
		{
			name: "nano",
			argv: []string{"nano"},
		},
		{
			name: "vim with arguments",
			argv: []string{"vim", "-u", "NONE"},
		},
		{
			name: "empty argv",
			argv: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := missingWaitFlagHint(tt.argv)

			if !tt.wantHint {
				if got != "" {
					t.Errorf("missingWaitFlagHint(%v) = %q, want no hint", tt.argv, got)
				}
				return
			}
			if got == "" {
				t.Fatalf("missingWaitFlagHint(%v) = \"\", want a hint", tt.argv)
			}
			if !strings.Contains(got, tt.contains) {
				t.Errorf("missingWaitFlagHint(%v) = %q, want it to contain %q",
					tt.argv, got, tt.contains)
			}
			// The status line has two colours: the theme's own, and red for a failure. A hint
			// is neither an error nor a colour of its own.
			if strings.Contains(got, "[") {
				t.Errorf("missingWaitFlagHint(%v) = %q, want no colour tag", tt.argv, got)
			}
		})
	}
}

func TestEditorErrorMessage(t *testing.T) {
	// A real *exec.Error, as returned when the binary is not on PATH.
	notFound := exec.Command("karat-no-such-editor-4a1f").Run()
	if notFound == nil {
		t.Fatal("expected running a non-existent binary to fail")
	}

	// A real *exec.ExitError, as returned when the editor runs and aborts.
	aborted := exec.Command("sh", "-c", "exit 3").Run()
	if aborted == nil {
		t.Fatal("expected a non-zero exit to fail")
	}

	tests := []struct {
		name     string
		editor   string
		err      error
		want     []string
		wantNotA string
	}{
		{
			name:   "editor not installed names the binary and points at karat.editor",
			editor: "vim",
			err:    notFound,
			want:   []string{"[red]", "editor 'vim'", "set karat.editor"},
		},
		{
			name:   "non-zero exit reads as a deliberate abort",
			editor: "vim",
			err:    aborted,
			// ":cq" is a cancel, not a failure, so it must not be red.
			want:     []string{"vim", "status 3", "nothing applied"},
			wantNotA: "[red]",
		},
		{
			name:   "anything else still reports the editor and the cause",
			editor: "nano",
			err:    errors.New("signal: killed"),
			want:   []string{"[red]", "editor 'nano'", "signal: killed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := editorErrorMessage(tt.editor, tt.err)

			for _, want := range tt.want {
				if !strings.Contains(got, want) {
					t.Errorf("editorErrorMessage() = %q, want it to contain %q", got, want)
				}
			}
			if tt.wantNotA != "" && strings.Contains(got, tt.wantNotA) {
				t.Errorf("editorErrorMessage() = %q, want it not to contain %q", got, tt.wantNotA)
			}
			if strings.Contains(got, "\n") {
				t.Errorf("editorErrorMessage() = %q, want a single status line", got)
			}
		})
	}
}
