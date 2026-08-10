// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package config

// Mode is how much karat may change what it points at. It is a property of the session, not of
// any one connection: a session is careful or it is not.
type Mode string

const (
	// ReadOnly refuses every modifying action.
	ReadOnly Mode = "read-only"
	// Confirm allows a modifying action, each one confirmed first.
	Confirm Mode = "confirm"
	// Yolo allows a modifying action with no confirmation.
	Yolo Mode = "yolo"
)

// modeCycle is the order <Tab> walks the modes in, starting from the safest.
var modeCycle = []Mode{ReadOnly, Confirm, Yolo}

// NextMode returns the mode after the given one, wrapping around. An unknown mode is treated
// as Confirm, so <Tab> always leads somewhere valid.
func NextMode(mode Mode) Mode {
	for i, m := range modeCycle {
		if m == mode {
			return modeCycle[(i+1)%len(modeCycle)]
		}
	}
	return NextMode(Confirm)
}

// valid reports whether the mode is one karat knows.
func (m Mode) valid() bool {
	switch m {
	case ReadOnly, Confirm, Yolo:
		return true
	default:
		return false
	}
}

// Mode returns the mode karat is running in. A config that came through the loader always
// carries one of the three: default_config.yaml supplies it when the user's own config says
// nothing, and validateMode replaces anything unrecognised.
func (c *Config) Mode() Mode {
	return c.Karat.Mode
}

// SetMode records the mode karat is running in, ready for Save.
func (c *Config) SetMode(mode Mode) {
	c.Karat.Mode = mode
}
