// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/rs/zerolog/log"
)

// editorCommand resolves the editor command line from karat.editor, falling back to vim
// when it states nothing. The value is split on whitespace, so an editor and its flags
// travel together ("code --wait").
func editorCommand(configured string) []string {
	if fields := strings.Fields(configured); len(fields) > 0 {
		return fields
	}

	return []string{"vim"}
}

// OpenInEditor writes content to a temp file named after pattern (an os.CreateTemp
// pattern, e.g. "consume-params-*.txt"), suspends the TUI while the editor (see
// editorCommand) owns the terminal, and returns the edited content. The second return value
// is false when the file could not be created, written or read back; a status message
// is emitted in that case. Must be called from the UI goroutine.
func (app *App) OpenInEditor(pattern string, content []byte) ([]byte, bool) {
	tmpFile, err := os.CreateTemp("", pattern)
	if err != nil {
		log.Error().Err(err).Msg("failed to create temp file")
		SendStatusError("[red]failed to create temp file")
		return nil, false
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if _, err := tmpFile.Write(content); err != nil {
		_ = tmpFile.Close()
		log.Error().Err(err).Msg("failed to write temp file")
		SendStatusError("[red]failed to write temp file")
		return nil, false
	}
	if err := tmpFile.Close(); err != nil {
		log.Error().Err(err).Msg("failed to close temp file")
		SendStatusError("[red]failed to close temp file")
		return nil, false
	}

	editor := editorCommand(app.Config.Editor())
	cmd := exec.Command(editor[0], append(editor[1:], tmpPath)...)

	// os.Stdin/Stdout/Stderr are redirected to the log file by InitLogger.
	// Open /dev/tty directly so the editor gets the actual terminal.
	tty, ttyErr := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if ttyErr == nil {
		defer func() { _ = tty.Close() }()
		cmd.Stdin = tty
		cmd.Stdout = tty
		cmd.Stderr = tty
	} else {
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	// Temporarily stop the TUI so the editor can take over the terminal. Suspend runs the
	// callback synchronously and reports false when the application is already suspended,
	// in which case the editor never ran and the file is untouched.
	var runErr error
	if !app.Suspend(func() { runErr = cmd.Run() }) {
		log.Error().Msg("failed to suspend the application for the editor")
		SendStatusError("[red]cannot open the editor right now")
		return nil, false
	}
	if runErr != nil {
		log.Error().Err(runErr).Str("editor", editor[0]).Msg("editor did not complete")
		SendStatusNote(editorErrorMessage(editor[0], runErr))
		return nil, false
	}

	edited, err := os.ReadFile(tmpPath)
	if err != nil {
		log.Error().Err(err).Msg("failed to read edited file")
		SendStatusError("[red]failed to read edited file")
		return nil, false
	}

	// Content coming back untouched is normal — quitting without saving does it — but from
	// a GUI editor invoked without a wait flag it means the editor never blocked, and the
	// file is still open on screen. Say so instead of reporting "no changes".
	if bytes.Equal(edited, content) {
		if hint := missingWaitFlagHint(editor); hint != "" {
			log.Warn().Strs("editor", editor).Msg("editor returned before the file was edited")
			SendStatusNote(hint)
			return nil, false
		}
	}

	return edited, true
}

// guiEditorWaitFlag maps the editors that hand the file to an already-running instance and
// exit immediately to the flag that makes them block until it is closed. Without it karat
// reads the file back before it has been touched.
var guiEditorWaitFlag = map[string]string{
	"code":          "--wait",
	"code-insiders": "--wait",
	"codium":        "--wait",
	"subl":          "-w",
	"sublime_text":  "-w",
	"mate":          "-w",
}

// missingWaitFlagHint returns a status line when argv names one of those editors and
// carries no wait flag, and "" for every other editor — a terminal editor that returns
// unchanged content simply means the user quit without saving.
func missingWaitFlagHint(argv []string) string {
	if len(argv) == 0 {
		return ""
	}

	name := filepath.Base(argv[0])
	flag, needsWait := guiEditorWaitFlag[name]
	if !needsWait {
		return ""
	}
	// Both spellings are accepted by all of these editors, so either one counts.
	if slices.Contains(argv[1:], "--wait") || slices.Contains(argv[1:], "-w") {
		return ""
	}

	return fmt.Sprintf(
		`%s returned before the file was edited — set karat.editor: "%s %s"`,
		name,
		name,
		flag,
	)
}

// editorErrorMessage turns a failed editor run into a status line. A non-zero exit is an
// abort — ":cq" in vim, the convention git uses — so it reads as a deliberate cancel
// rather than a failure. Anything else means the editor never started, which is worth
// naming precisely: karat.editor defaults to vim, and on a host without it the only
// symptom used to be an empty document further down.
func editorErrorMessage(name string, err error) string {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return fmt.Sprintf(
			"%s exited with status %d, nothing applied",
			name,
			exitErr.ExitCode(),
		)
	}

	var execErr *exec.Error
	if errors.As(err, &execErr) {
		return fmt.Sprintf("[red]editor '%s': %s — set karat.editor", name, execErr.Err)
	}

	return fmt.Sprintf("[red]editor '%s' failed: %s", name, err)
}
