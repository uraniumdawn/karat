// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"fmt"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/atotto/clipboard"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"
	"github.com/uraniumdawn/karat/pkg/shell"

	"github.com/uraniumdawn/karat/pkg/util"
)

// CliTemplates displays CLI command templates for a specific topic.
func (app *App) CliTemplates(topicName string) {
	table := tview.NewTable()
	table.SetSelectable(true, false).
		SetBorder(true).
		SetBorderPadding(0, 0, 1, 0).
		SetTitle(" CLI commands ")

	table.SetSelectedStyle(
		tcell.StyleDefault.Foreground(
			tcell.GetColor(app.Colors.Karat.Selection.FgColor),
		).Background(
			tcell.GetColor(app.Colors.Karat.Selection.BgColor),
		),
	)

	bootstrap := app.Selected.Cluster.GetBootstrapServers()
	if bootstrap == "" {
		SendStatusError("[red]bootstrap.servers not found in cluster config")
		return
	}

	srURL := app.schemaRegistryURL()

	for i, templateCmd := range app.Config.Karat.CliTemplates {
		if strings.Contains(templateCmd, "{{srURL}}") && srURL == "" {
			SendStatusError(
				"[red]template uses {{srURL}} but no Schema Registry is selected",
			)
			return
		}
		command := util.BuildCliCommand(templateCmd, bootstrap, topicName, srURL)
		table.SetCell(i, 0, tview.NewTableCell(command))
	}

	table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			app.HideModalPage(CliTemplates)
			return nil
		}

		// <y> yanks, leaving <c> to mean "consume" wherever it appears.
		if IsKey(event, 'y') {
			row, _ := table.GetSelection()
			if row >= 0 && row < len(app.Config.Karat.CliTemplates) {
				templateCmd := app.Config.Karat.CliTemplates[row]
				command := util.BuildCliCommand(templateCmd, bootstrap, topicName, srURL)
				err := clipboard.WriteAll(command)
				if err != nil {
					log.Error().Err(err).Send()
					SendStatusError(
						fmt.Sprintf("[red]failed to copy to clipboard: %s", err.Error()),
					)
				}
			}
			return nil
		}

		// <Enter> runs the selected entry, as it does in every other list of actions. <e> means
		// "edit" everywhere else, which is the wrong promise for a key that runs a shell command.
		if event.Key() == tcell.KeyEnter {
			row, _ := table.GetSelection()
			if row >= 0 && row < len(app.Config.Karat.CliTemplates) {
				templateCmd := app.Config.Karat.CliTemplates[row]
				app.ExecuteCliCommand(topicName, templateCmd)
				app.HideModalPage(CliTemplates)
			}
			return nil
		}

		return event
	})

	modal := util.NewModal(table)

	app.Layout.PagesRegistry.UI.Pages.AddPage(CliTemplates, modal, true, false)
	app.ShowModalPage(CliTemplates)
}

func (app *App) schemaRegistryURL() string {
	if app.Selected.SchemaRegistry != nil {
		return app.Selected.SchemaRegistry.SchemaRegistryURL
	}
	return ""
}

// requestSignal asks the running process for a signal, and reports whether the request was
// taken. It never blocks: it runs on the UI goroutine, and the goroutine reading sig is gone
// once the process has exited — a blocking send there freezes the application with no way out.
//
// A refused request means one is already queued. shell.Execute acts on the first signal it
// receives and ignores the rest, so nothing is lost by dropping it.
func requestSignal(sig chan syscall.Signal, s syscall.Signal) bool {
	select {
	case sig <- s:
		return true
	default:
		SendStatusDone("a stop has already been requested")
		return false
	}
}

func (app *App) ExecuteCliCommand(topicName, commandTemplate string) {
	bootstrap := app.Selected.Cluster.GetBootstrapServers()
	if bootstrap == "" {
		SendStatusError("[red]bootstrap servers not configured")
		log.Error().Msg("bootstrap servers not configured")
		return
	}

	srURL := app.schemaRegistryURL()
	if strings.Contains(commandTemplate, "{{srURL}}") && srURL == "" {
		SendStatusError("[red]template uses {{srURL}} but no Schema Registry is selected")
		return
	}

	command := util.BuildCliCommand(commandTemplate, bootstrap, topicName, srURL)

	rc := make(chan string, 100)
	errCh := make(chan string, 10)
	sig := make(chan syscall.Signal, 1)
	processDone := make(chan int, 1)

	// No SetChangedFunc: every write to this view already happens inside a QueueUpdateDraw,
	// which redraws. A changed handler calling Draw would queue a second, redundant update —
	// tview runs the handler on a goroutine of its own, so each one is a goroutine parked in
	// QueueUpdate for as long as the UI goroutine is busy.
	view := tview.NewTextView().
		SetTextAlign(tview.AlignLeft).
		SetDynamicColors(true).
		SetWrap(true).
		SetWordWrap(true).
		SetMaxLines(1000).
		SetScrollable(true)

	view.SetBorder(true).
		SetBorderPadding(0, 0, 1, 0)

	// Prepare base title (truncate if needed)
	baseTitle := command
	_, _, screenWidth, _ := app.Layout.Content.GetRect()
	if screenWidth == 0 {
		screenWidth = 120 // default fallback
	}
	maxTitleLen := int(float64(screenWidth)*0.8) - 6 // Reserve space for spinner
	if len(baseTitle) > maxTitleLen && maxTitleLen > 4 {
		baseTitle = baseTitle[:maxTitleLen-3] + "..."
	}

	// Create a shorter page name for display in Opened Pages window
	// Calculate based on 80% of OpenedPages modal width
	// Modal is ~71% of screen width (5/7 flex ratio), minus borders and padding
	pageName := command
	modalWidth := int(float64(screenWidth) * 0.71)
	maxPageNameLen := int(float64(modalWidth) * 0.8)
	if maxPageNameLen < 30 {
		maxPageNameLen = 30 // minimum reasonable length
	}
	if len(pageName) > maxPageNameLen {
		pageName = pageName[:maxPageNameLen-3] + "..."
	}
	pageName = util.BuildPageKey(pageName)

	app.AddToPagesRegistry(pageName, view, CliExecutePageMenu, false)

	// The command is an operation in flight, so it takes the spinner and stands until it is
	// over: the exit-code message sent when the process terminates replaces it.
	SendStatusProgress("command executing")

	spinnerIndex := 0
	var isProcessActive int32 = 1 // 1 = active, 0 = inactive

	// Spinner goroutine
	go func() {
		ticker := time.NewTicker(80 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			if atomic.LoadInt32(&isProcessActive) == 0 {
				return
			}
			app.QueueUpdateDraw(func() {
				if atomic.LoadInt32(&isProcessActive) == 1 {
					view.SetTitle(fmt.Sprintf(" %s %s ", SpinnerFrames[spinnerIndex], baseTitle))
				}
			})
			spinnerIndex = (spinnerIndex + 1) % len(SpinnerFrames)
		}
	}()

	view.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsKey(event, 't') {
			if atomic.LoadInt32(&isProcessActive) == 0 {
				SendStatusDone("process already finished")
				return nil
			}
			if !requestSignal(sig, syscall.SIGTERM) {
				return nil
			}
			SendStatusProgress("stopping execution")
			return nil
		}
		if event.Key() == tcell.KeyCtrlK {
			if atomic.LoadInt32(&isProcessActive) == 0 {
				SendStatusDone("process already finished")
				return nil
			}
			if !requestSignal(sig, syscall.SIGKILL) {
				return nil
			}
			SendStatusProgress("killing process")
			return nil
		}
		// <x> closes the page, the same key the opened-pages modal uses. <C-d> is reserved for
		// deleting things in Kafka and must not also mean "close this".
		if IsKey(event, 'x') {
			if atomic.LoadInt32(&isProcessActive) == 1 {
				SendStatusDone("process in not finished yet")
				return nil
			}
			app.RemoveFromPagesRegistry(pageName)
			return nil
		}

		return event
	})

	// Execute command through shell to support pipes, redirects, etc.
	args := []string{"sh", "-c", command}
	go shell.Execute(args, rc, errCh, sig, processDone)

	// Single goroutine to handle all output and process termination
	// Exit codes follow Unix convention: 0=success, 1-127=error, 128+N=killed by signal N
	go func() {
		var exitCode int
		rcClosed := false
		errChClosed := false
		processDoneReceived := false

		shouldExit := func() bool {
			return rcClosed && errChClosed && processDoneReceived
		}

		// Process messages until both channels are closed AND processDone is received
		for !shouldExit() {
			select {
			case record, ok := <-rc:
				if !ok {
					rcClosed = true
					continue
				}
				app.QueueUpdateDraw(func() {
					_, _ = fmt.Fprintf(view, "%s\n", record)
					view.ScrollToEnd()
				})

			case errMsg, ok := <-errCh:
				if !ok {
					errChClosed = true
					continue
				}
				SendStatusProgress(errMsg)

			case exitCode = <-processDone:
				processDoneReceived = true

				// Stop spinner and update title (thread-safe)
				atomic.StoreInt32(&isProcessActive, 0)
				app.QueueUpdateDraw(func() {
					view.SetTitle(fmt.Sprintf(" %s ", baseTitle))
				})
			}
		}

		// Show final status message based on exit code
		switch {
		case exitCode == 0:
			SendStatusDone("process completed successfully (exit code 0)")
		case exitCode == 143: // 128 + 15 (SIGTERM)
			SendStatusDone("process stopped gracefully (SIGTERM)")
		case exitCode == 137: // 128 + 9 (SIGKILL)
			SendStatusDone("process killed (SIGKILL)")
		case exitCode >= 128:
			// Killed by other signal
			signal := exitCode - 128
			SendStatusDone(fmt.Sprintf("process killed by signal %d", signal))
		default:
			// Process error (exit code 1-127)
			SendStatusDone(fmt.Sprintf("process failed with exit code %d", exitCode))
		}
	}()
}
