// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package util

import (
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// drawIn lays modal out on a screen of the given size and returns the rect its content
// primitive ended up with.
func drawIn(t *testing.T, modal, content tview.Primitive, width, height int) (x, y, w, h int) {
	t.Helper()

	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatalf("screen init: %v", err)
	}
	defer screen.Fini()
	screen.SetSize(width, height)

	modal.SetRect(0, 0, width, height)
	modal.Draw(screen)

	return content.GetRect()
}

// The history picker must occupy the bottom third whatever it holds, so its geometry may
// not depend on the content at all. It carries the same margins as a confirmation dialog:
// one column left and right, and one row between it and the edge it is anchored to.
func TestNewBottomModalGeometry(t *testing.T) {
	sizes := []struct{ width, height int }{
		{100, 30},
		{80, 24},
		{200, 51},
	}

	for _, size := range sizes {
		content := tview.NewBox()
		x, y, w, h := drawIn(t, NewBottomModal(content), content, size.width, size.height)

		// A third of what is left once the bottom margin is taken off.
		available := size.height - 1
		if wantH := available - available*2/3; h != wantH {
			t.Errorf("%dx%d: height = %d, want %d", size.width, size.height, h, wantH)
		}
		if bottom := y + h; bottom != size.height-1 {
			t.Errorf("%dx%d: bottom edge = %d, want %d", size.width, size.height, bottom, size.height-1)
		}
		if x != 1 {
			t.Errorf("%dx%d: x = %d, want 1", size.width, size.height, x)
		}
		if wantW := size.width - 2; w != wantW {
			t.Errorf("%dx%d: width = %d, want %d", size.width, size.height, w, wantW)
		}
	}
}

// Both helpers must agree on the width, so a picker anchored at the bottom lines up with a
// dialog opened above it.
func TestModalWidthsMatch(t *testing.T) {
	const width, height = 100, 30

	bottomContent := tview.NewBox()
	bottomX, _, bottomW, _ := drawIn(t, NewBottomModal(bottomContent), bottomContent, width, height)

	wideContent := tview.NewBox()
	wideX, _, wideW, _ := drawIn(t, NewWideModal(wideContent, 3), wideContent, width, height)

	if bottomX != wideX || bottomW != wideW {
		t.Errorf("bottom modal x=%d w=%d, wide modal x=%d w=%d", bottomX, bottomW, wideX, wideW)
	}
}
