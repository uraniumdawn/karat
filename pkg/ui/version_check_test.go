// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import "testing"

func TestIsNewerVersion(t *testing.T) {
	tests := []struct {
		b, a string
		want bool
	}{
		{"0.2.3", "0.2.2", true},
		{"0.3.0", "0.2.9", true},
		{"1.0.0", "0.9.9", true},
		{"0.2.2", "0.2.2", false},
		{"0.2.1", "0.2.2", false},
		{"0.1.0", "0.2.0", false},
		{"0.0.999", "0.2.2", false}, // dev build is not "newer"
		{"0.2.3", "0.0.999", true},  // release is newer than dev build
	}

	for _, tc := range tests {
		got := isNewerVersion(tc.b, tc.a)
		if got != tc.want {
			t.Errorf("isNewerVersion(%q, %q) = %v, want %v", tc.b, tc.a, got, tc.want)
		}
	}
}
