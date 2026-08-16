// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"context"
	"sync"
)

// EventType represents the type of event being published.
type EventType string

// Event represents an event with a type and payload.
type Event struct {
	Type    EventType
	Payload Payload
}

// Payload contains the data and force flag for an event.
type Payload struct {
	Data  any
	Force bool
}

// EventChannel carries events to the single goroutine handling one resource type.
//
// Publishing must never block the publisher. Every key handler publishes, and key handlers run
// on the UI goroutine; the handler goroutine reading C parks in QueueUpdate, which only the UI
// goroutine can release. A direct send from a key handler onto a channel whose handler is
// parked that way is a cycle the application never comes out of: tview's event loop is one
// select over its event queue and its update queue, so a keypress arriving while an update is
// pending is taken first about half the time. The same cycle closes through a second handler —
// the resources handler publishes onto every other channel, and blocks the UI goroutine that
// published to it in turn.
//
// Publish therefore only appends to an inbox, and the pump goroutine started by Run does the
// send. Nothing waits on the pump, so a handler parked in QueueUpdate stalls the pump and
// nothing else.
type EventChannel struct {
	// C is what the resource's event handler receives on. It is receive-only so that the
	// direct send this type exists to prevent cannot be written by accident.
	C <-chan Event

	// events is C, sendable. The pump is its only sender.
	events  chan Event
	mu      sync.Mutex
	pending []Event
	// wake carries at most one token: a signal that the inbox is worth taking from.
	wake chan struct{}
}

// NewEventChannel returns an event channel whose pump is not running yet. Run starts it.
func NewEventChannel() *EventChannel {
	events := make(chan Event)
	return &EventChannel{
		C:      events,
		events: events,
		wake:   make(chan struct{}, 1),
	}
}

// Publish queues an event for the channel's handler. It never blocks on anything but the inbox
// lock, and never drops an event: a handler that is busy grows the inbox instead.
func Publish(ch *EventChannel, eventType EventType, p Payload) {
	ch.mu.Lock()
	ch.pending = append(ch.pending, Event{Type: eventType, Payload: p})
	ch.mu.Unlock()

	select {
	case ch.wake <- struct{}{}:
	default:
		// A token is already waiting, and the take it triggers happens after the append
		// above, so it picks this event up too.
	}
}

// take returns the events published since the last call, oldest first.
func (ch *EventChannel) take() []Event {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	taken := ch.pending
	ch.pending = nil
	return taken
}

// Run starts the pump forwarding published events to C, in publication order, until ctx is
// done. It is called by the resource's event handler, so a channel with a handler always has
// a pump.
func (ch *EventChannel) Run(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch.wake:
				for _, event := range ch.take() {
					select {
					case ch.events <- event:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
}
