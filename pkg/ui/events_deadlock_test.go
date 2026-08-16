// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// parkedHandler starts a handler that receives one event and then parks, the way every event
// handler parks in QueueUpdate while the UI goroutine runs its update. It returns once the
// handler is parked.
func parkedHandler(ctx context.Context, t *testing.T, ch *EventChannel) (release func()) {
	t.Helper()

	ch.Run(ctx)

	parked := make(chan struct{})
	unpark := make(chan struct{})
	go func() {
		<-ch.C
		close(parked)
		<-unpark
		for {
			select {
			case <-ch.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	Publish(ch, "park", Payload{})
	<-parked

	var once bool
	return func() {
		if !once {
			once = true
			close(unpark)
		}
	}
}

// mustNotBlock fails with every goroutine stack if fn has not returned within the timeout.
func mustNotBlock(t *testing.T, what string, fn func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		buf := make([]byte, 1<<21)
		n := runtime.Stack(buf, true)
		t.Fatalf("%s blocked: the application is frozen\n\n%s", what, buf[:n])
	}
}

// TestPublishNeverBlocksOnAParkedHandler pins the freeze reported on every list page: a key
// handler runs on the UI goroutine and publishes, while the handler reading that channel is
// parked in QueueUpdate waiting for that same UI goroutine. With a direct send this is a cycle
// the application never leaves.
func TestPublishNeverBlocksOnAParkedHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch := NewEventChannel()
	release := parkedHandler(ctx, t, ch)
	defer release()

	mustNotBlock(t, "Publish from the UI goroutine", func() {
		for range 100 {
			Publish(ch, "get", Payload{})
		}
	})
}

// TestPublishNeverBlocksAcrossHandlers pins the same cycle one hop longer: the resources
// handler republishes onto the other channels, so a parked topics handler would otherwise
// block the resources handler, and through it the UI goroutine that published to it.
func TestPublishNeverBlocksAcrossHandlers(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	downstream := NewEventChannel()
	release := parkedHandler(ctx, t, downstream)
	defer release()

	upstream := NewEventChannel()
	upstream.Run(ctx)
	forwarded := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-upstream.C:
				Publish(downstream, "get", Payload{})
				select {
				case forwarded <- struct{}{}:
				default:
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	mustNotBlock(t, "Publish through a second handler", func() {
		Publish(upstream, "resource", Payload{})
		<-forwarded
		// The upstream handler is free again, so the UI goroutine's next publish lands.
		Publish(upstream, "resource", Payload{})
		<-forwarded
	})
}

// TestPublishDeliversInOrderOnceTheHandlerIsFree checks that the inbox defers events rather
// than dropping them: a handler that was busy must still see every event, oldest first.
func TestPublishDeliversInOrderOnceTheHandlerIsFree(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch := NewEventChannel()
	ch.Run(ctx)

	const count = 50
	received := make(chan EventType, count)
	started := make(chan struct{})
	unpark := make(chan struct{})
	go func() {
		<-ch.C // the event that parks the handler
		close(started)
		<-unpark
		for {
			select {
			case event := <-ch.C:
				received <- event.Type
			case <-ctx.Done():
				return
			}
		}
	}()

	Publish(ch, "park", Payload{})
	<-started

	mustNotBlock(t, "publishing to a parked handler", func() {
		for i := range count {
			Publish(ch, EventType(strconv.Itoa(i)), Payload{})
		}
	})
	close(unpark)

	for i := range count {
		want := EventType(strconv.Itoa(i))
		select {
		case got := <-received:
			if got != want {
				t.Fatalf("event %d = %q, want %q", i, got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("event %d (%q) never arrived", i, want)
		}
	}
}
