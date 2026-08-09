// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"os"
	"os/exec"
	"strings"

	"github.com/rs/zerolog/log"
)

// OpenInEditor writes content to a temp file named after pattern (an os.CreateTemp
// pattern, e.g. "consume-params-*.txt"), suspends the TUI while $EDITOR (vim when
// unset) owns the terminal, and returns the edited content. The second return value
// is false when the file could not be created, written or read back; a status message
// is emitted in that case. Must be called from the UI goroutine.
func (app *App) OpenInEditor(pattern string, content []byte) ([]byte, bool) {
	tmpFile, err := os.CreateTemp("", pattern)
	if err != nil {
		log.Error().Err(err).Msg("failed to create temp file")
		SendStatusWithDefaultTTL("[red]failed to create temp file")
		return nil, false
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if _, err := tmpFile.Write(content); err != nil {
		_ = tmpFile.Close()
		log.Error().Err(err).Msg("failed to write temp file")
		SendStatusWithDefaultTTL("[red]failed to write temp file")
		return nil, false
	}
	if err := tmpFile.Close(); err != nil {
		log.Error().Err(err).Msg("failed to close temp file")
		SendStatusWithDefaultTTL("[red]failed to close temp file")
		return nil, false
	}

	editor := strings.Fields(os.Getenv("EDITOR"))
	if len(editor) == 0 {
		editor = []string{"vim"}
	}
	cmd := exec.Command(editor[0], append(editor[1:], tmpPath)...)

	// os.Stdin/Stdout/Stderr are redirected to the log file by InitLogger.
	// Open /dev/tty directly so the editor gets the actual terminal.
	tty, ttyErr := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if ttyErr == nil {
		cmd.Stdin = tty
		cmd.Stdout = tty
		cmd.Stderr = tty
	} else {
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	// Temporarily stop the TUI so the editor can take over the terminal.
	app.Suspend(func() {
		_ = cmd.Run()
		if ttyErr == nil {
			_ = tty.Close()
		}
	})

	edited, err := os.ReadFile(tmpPath)
	if err != nil {
		log.Error().Err(err).Msg("failed to read edited file")
		SendStatusWithDefaultTTL("[red]failed to read edited file")
		return nil, false
	}

	return edited, true
}
