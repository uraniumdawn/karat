// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"fmt"

	"github.com/gdamore/tcell/v2"

	"github.com/uraniumdawn/karat/pkg/config"
)

// modeColor is the colour the mode badge is painted in. Only yolo has one, and it is not
// configurable: a session that runs every deletion unasked has to look like one, whatever style
// file is loaded. The other two are as ordinary as the border they sit in.
func modeColor(mode config.Mode) tcell.Color {
	if mode == config.Yolo {
		return tcell.ColorRed
	}
	return tcell.ColorDefault
}

// cycleMode advances the mode karat is running in on <Tab> and saves it, so the choice
// outlives the session.
//
// Nothing is asked and nothing is said: the badge in the content border shows the new mode and
// this keypress ends in a redraw. Only a failed save is worth a message. It does not go through
// Modify either — switching out of read-only must not be refused by the very mode it is
// leaving.
//
// The mode karat runs on is this in-memory value; config.yaml only outlives the session. Every
// mutation reads it at the moment it runs, so a switch between opening a confirmation and
// applying it would decide the outcome. That window is closed on the other side too — <Tab>
// lives on the Clusters page, which a confirmation page cannot be left for — and refusing here
// says so in one place instead of leaving it to be inferred from three.
func (app *App) cycleMode() {
	if app.confirmationInFront() {
		SendStatusNote("finish the open confirmation first")
		return
	}

	app.Config.SetMode(config.NextMode(app.Config.Mode()))
	if err := app.Config.Save(); err != nil {
		SendStatusError(fmt.Sprintf("[red]failed to save config: %s", err.Error()))
	}
}
