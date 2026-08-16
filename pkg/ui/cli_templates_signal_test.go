// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"runtime"
	"syscall"
	"testing"
	"time"
)

// TestRequestSignalNeverBlocksWithoutAReader pins the freeze behind <t> and <C-k> on a CLI
// execution page: both run on the UI goroutine, and shell.Execute stops reading the signal
// channel before the page learns the process is over. A blocking send onto the one-slot
// channel then has nobody to take it, ever.
func TestRequestSignalNeverBlocksWithoutAReader(t *testing.T) {
	sig := make(chan syscall.Signal, 1) // as ExecuteCliCommand declares it, with no reader

	type outcome struct{ first, second bool }
	done := make(chan outcome, 1)
	go func() {
		first := requestSignal(sig, syscall.SIGTERM)
		second := requestSignal(sig, syscall.SIGKILL)
		done <- outcome{first, second}
	}()

	select {
	case got := <-done:
		if !got.first {
			t.Errorf("first requestSignal = false, want true: the slot was free")
		}
		if got.second {
			t.Errorf("second requestSignal = true, want false: the slot was taken")
		}
	case <-time.After(5 * time.Second):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("requestSignal blocked: the application is frozen\n\n%s", buf[:n])
	}
}

// TestRequestSignalDeliversToAReader checks the ordinary path still delivers.
func TestRequestSignalDeliversToAReader(t *testing.T) {
	sig := make(chan syscall.Signal, 1)

	if !requestSignal(sig, syscall.SIGTERM) {
		t.Fatal("requestSignal = false, want true")
	}
	select {
	case got := <-sig:
		if got != syscall.SIGTERM {
			t.Errorf("signal = %v, want SIGTERM", got)
		}
	case <-time.After(time.Second):
		t.Fatal("the signal never arrived")
	}
}
