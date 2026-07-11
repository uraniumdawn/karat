// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package connect

import "strings"

// ConnectorHealth is a connector's holistic health, derived from its own state and
// the states of its tasks.
type ConnectorHealth string

// Connector health values.
const (
	HealthHealthy    ConnectorHealth = "HEALTHY"
	HealthDegraded   ConnectorHealth = "DEGRADED"
	HealthUnhealthy  ConnectorHealth = "UNHEALTHY"
	HealthPaused     ConnectorHealth = "PAUSED"
	HealthStopped    ConnectorHealth = "STOPPED"
	HealthRestarting ConnectorHealth = "RESTARTING"
	HealthUnassigned ConnectorHealth = "UNASSIGNED"
	HealthUnknown    ConnectorHealth = "UNKNOWN"
)

// Connector/task state strings as reported by the Kafka Connect REST API.
const (
	stateRunning    = "RUNNING"
	stateFailed     = "FAILED"
	statePaused     = "PAUSED"
	stateStopped    = "STOPPED"
	stateRestarting = "RESTARTING"
	stateUnassigned = "UNASSIGNED"
)

// Health derives a connector's holistic health from its connector-level state and the
// states of its tasks:
//   - HEALTHY:    connector RUNNING and every task RUNNING
//   - DEGRADED:   connector RUNNING but some (not all) tasks FAILED
//   - UNHEALTHY:  connector FAILED, or RUNNING with no tasks / all tasks FAILED
//   - PAUSED / STOPPED / RESTARTING / UNASSIGNED: reflect the connector (or task) state
//   - UNKNOWN:    anything else, or a missing status
//
// The branch order matters: HEALTHY/UNHEALTHY/DEGRADED are evaluated before the
// lifecycle states so that, e.g., a RUNNING connector with a single failed task
// surfaces as DEGRADED rather than being masked by its RUNNING state.
func Health(status *ConnectorStatus) ConnectorHealth {
	if status == nil {
		return HealthUnknown
	}

	connState := strings.ToUpper(status.Connector.State)

	total := len(status.Tasks)
	var running, failed, restarting, unassigned int
	for _, t := range status.Tasks {
		switch strings.ToUpper(t.State) {
		case stateRunning:
			running++
		case stateFailed:
			failed++
		case stateRestarting:
			restarting++
		case stateUnassigned:
			unassigned++
		}
	}

	switch {
	case connState == stateRunning && total > 0 && running == total:
		return HealthHealthy
	case connState == stateFailed || (connState == stateRunning && (total == 0 || failed == total)):
		return HealthUnhealthy
	case connState == stateRunning && failed > 0 && failed < total:
		return HealthDegraded
	case connState == statePaused:
		return HealthPaused
	case connState == stateStopped:
		return HealthStopped
	case connState == stateRestarting || restarting > 0:
		return HealthRestarting
	case connState == stateUnassigned || (connState == stateRunning && unassigned > 0):
		return HealthUnassigned
	default:
		return HealthUnknown
	}
}
