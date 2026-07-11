// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package connect

import "testing"

func task(state string) TaskStateInfo { return TaskStateInfo{State: state} }

func TestHealth(t *testing.T) {
	tests := []struct {
		name  string
		conn  string
		tasks []TaskStateInfo
		want  ConnectorHealth
	}{
		{
			"running all tasks running",
			"RUNNING",
			[]TaskStateInfo{task("RUNNING"), task("RUNNING")},
			HealthHealthy,
		},
		{
			"running some tasks failed",
			"RUNNING",
			[]TaskStateInfo{task("RUNNING"), task("FAILED")},
			HealthDegraded,
		},
		{
			"running all tasks failed",
			"RUNNING",
			[]TaskStateInfo{task("FAILED"), task("FAILED")},
			HealthUnhealthy,
		},
		{"running no tasks", "RUNNING", nil, HealthUnhealthy},
		{"connector failed", "FAILED", []TaskStateInfo{task("RUNNING")}, HealthUnhealthy},
		{"paused", "PAUSED", []TaskStateInfo{task("PAUSED")}, HealthPaused},
		{"stopped no tasks", "STOPPED", nil, HealthStopped},
		{"connector restarting", "RESTARTING", []TaskStateInfo{task("RUNNING")}, HealthRestarting},
		{
			"running task restarting",
			"RUNNING",
			[]TaskStateInfo{task("RUNNING"), task("RESTARTING")},
			HealthRestarting,
		},
		{
			"running task unassigned",
			"RUNNING",
			[]TaskStateInfo{task("RUNNING"), task("UNASSIGNED")},
			HealthUnassigned,
		},
		{"lowercase running", "running", []TaskStateInfo{task("running")}, HealthHealthy},
		{"unknown state", "WEIRD", []TaskStateInfo{task("RUNNING")}, HealthUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := &ConnectorStatus{
				Connector: ConnectorStateInfo{State: tt.conn},
				Tasks:     tt.tasks,
			}
			if got := Health(status); got != tt.want {
				t.Errorf("Health(conn=%q, tasks=%v) = %q, want %q", tt.conn, tt.tasks, got, tt.want)
			}
		})
	}
}

func TestHealthNilStatus(t *testing.T) {
	if got := Health(nil); got != HealthUnknown {
		t.Errorf("Health(nil) = %q, want %q", got, HealthUnknown)
	}
}
