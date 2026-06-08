// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

// Package connect provides a client wrapper for Kafka Connect REST API operations.
package connect

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/uraniumdawn/karat/pkg/config"
)

// Client wraps the Kafka Connect REST API client with cluster name context.
type Client struct {
	Name       string
	BaseURL    string
	HTTPClient *http.Client
}

// NewClient creates a new Kafka Connect client with the given configuration.
func NewClient(cfg *config.ConnectConfig, timeout time.Duration) (*Client, error) {
	client := &http.Client{
		Timeout: timeout,
	}

	if cfg.Username != "" && cfg.Password != "" {
		client.Transport = &basicAuthTransport{
			Username:  cfg.Username,
			Password:  cfg.Password,
			Transport: http.DefaultTransport,
		}
	}

	return &Client{
		Name:       cfg.Name,
		BaseURL:    cfg.URL,
		HTTPClient: client,
	}, nil
}

// basicAuthTransport adds basic authentication to HTTP requests.
type basicAuthTransport struct {
	Username  string
	Password  string
	Transport http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t *basicAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.SetBasicAuth(t.Username, t.Password)
	return t.transport().RoundTrip(req)
}

func (t *basicAuthTransport) transport() http.RoundTripper {
	if t.Transport != nil {
		return t.Transport
	}
	return http.DefaultTransport
}

// ListConnectors retrieves all connector names from the Kafka Connect cluster.
func (c *Client) ListConnectors(resultChan chan<- []string, errorChan chan<- error) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.HTTPClient.Timeout)
		defer cancel()

		connectors, err := c.listConnectors(ctx)
		if err != nil {
			errorChan <- fmt.Errorf("failed to list connectors: %w", err)
			return
		}

		resultChan <- connectors
	}()
}

// GetConnectorStatus retrieves the status of a specific connector.
func (c *Client) GetConnectorStatus(name string, resultChan chan<- *ConnectorStatus, errorChan chan<- error) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.HTTPClient.Timeout)
		defer cancel()

		status, err := c.getConnectorStatus(ctx, name)
		if err != nil {
			errorChan <- fmt.Errorf("failed to get connector status: %w", err)
			return
		}

		resultChan <- status
	}()
}

// GetConnectorConfig retrieves the configuration of a specific connector.
func (c *Client) GetConnectorConfig(name string, resultChan chan<- map[string]interface{}, errorChan chan<- error) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.HTTPClient.Timeout)
		defer cancel()

		config, err := c.getConnectorConfig(ctx, name)
		if err != nil {
			errorChan <- fmt.Errorf("failed to get connector config: %w", err)
			return
		}

		resultChan <- config
	}()
}

// UpdateConnectorConfig updates the configuration of a specific connector.
func (c *Client) UpdateConnectorConfig(
	name string, config map[string]interface{},
	resultChan chan<- bool, errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.HTTPClient.Timeout)
		defer cancel()

		body, err := json.Marshal(config)
		if err != nil {
			errorChan <- fmt.Errorf("marshaling config: %w", err)
			return
		}

		req, err := http.NewRequestWithContext(
			ctx, http.MethodPut,
			c.BaseURL+"/connectors/"+name+"/config",
			bytes.NewReader(body),
		)
		if err != nil {
			errorChan <- fmt.Errorf("creating request: %w", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.HTTPClient.Do(req)
		if err != nil {
			errorChan <- fmt.Errorf("executing request: %w", err)
			return
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			respBody, _ := io.ReadAll(resp.Body)
			errorChan <- fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
			return
		}

		resultChan <- true
	}()
}

// DescribeConnector fetches both status and config for a connector in parallel.
// Results are sent via resultChan; any errors are sent via errorChan.
func (c *Client) DescribeConnector(name string, resultChan chan<- *ConnectorDetail, errorChan chan<- error) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.HTTPClient.Timeout)
		defer cancel()

		detail := &ConnectorDetail{Name: name}
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			status, err := c.getConnectorStatus(ctx, name)
			if err != nil {
				errorChan <- fmt.Errorf("failed to get connector status: %w", err)
				return
			}
			detail.Status = status
		}()

		go func() {
			defer wg.Done()
			config, err := c.getConnectorConfig(ctx, name)
			if err != nil {
				errorChan <- fmt.Errorf("failed to get connector config: %w", err)
				return
			}
			detail.Config = config
		}()

		wg.Wait()
		resultChan <- detail
	}()
}

// doConnectorAction performs a connector state-change request (pause, resume,
// restart, delete) asynchronously. The actionPath parameter is appended to the
// connector URL (e.g. "pause", "resume"); pass an empty string when no suffix
// is needed (e.g. delete).
func (c *Client) doConnectorAction(name, method, actionPath string, resultChan chan<- bool, errorChan chan<- error) {
	go func() {
		url := c.BaseURL + "/connectors/" + name
		if actionPath != "" {
			url += "/" + actionPath
		}

		req, err := http.NewRequestWithContext(context.Background(), method, url, nil)
		if err != nil {
			errorChan <- fmt.Errorf("creating request: %w", err)
			return
		}

		resp, err := c.HTTPClient.Do(req)
		if err != nil {
			errorChan <- fmt.Errorf("executing request: %w", err)
			return
		}
		defer func() { _ = resp.Body.Close() }()

		switch resp.StatusCode {
		case http.StatusOK, http.StatusNoContent, http.StatusAccepted:
			resultChan <- true
		default:
			body, _ := io.ReadAll(resp.Body)
			errorChan <- fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
		}
	}()
}

// PauseConnector pauses a running connector.
func (c *Client) PauseConnector(name string, resultChan chan<- bool, errorChan chan<- error) {
	c.doConnectorAction(name, http.MethodPut, "pause", resultChan, errorChan)
}

// ResumeConnector resumes a paused connector.
func (c *Client) ResumeConnector(name string, resultChan chan<- bool, errorChan chan<- error) {
	c.doConnectorAction(name, http.MethodPut, "resume", resultChan, errorChan)
}

// RestartConnector restarts a connector and all its tasks.
func (c *Client) RestartConnector(name string, resultChan chan<- bool, errorChan chan<- error) {
	c.doConnectorAction(name, http.MethodPost, "restart?includeTasks=true", resultChan, errorChan)
}

// StopConnector stops a running connector.
func (c *Client) StopConnector(name string, resultChan chan<- bool, errorChan chan<- error) {
	c.doConnectorAction(name, http.MethodPut, "stop", resultChan, errorChan)
}

// DeleteConnector deletes a connector from the Kafka Connect cluster.
func (c *Client) DeleteConnector(name string, resultChan chan<- bool, errorChan chan<- error) {
	c.doConnectorAction(name, http.MethodDelete, "", resultChan, errorChan)
}

// DeleteConnectorOffsets resets all offsets for a stopped connector.
func (c *Client) DeleteConnectorOffsets(name string, resultChan chan<- bool, errorChan chan<- error) {
	c.doConnectorAction(name, http.MethodDelete, "offsets", resultChan, errorChan)
}

// SetConnectorOffsets applies the given partition offsets to a stopped connector
// via PATCH /connectors/{name}/offsets.
func (c *Client) SetConnectorOffsets(
	name string,
	offsets []ConnectorOffset,
	resultChan chan<- bool,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.HTTPClient.Timeout)
		defer cancel()

		entries := make([]connectorOffsetEntry, 0, len(offsets))
		for _, o := range offsets {
			entries = append(entries, connectorOffsetEntry{Partition: o.Partition, Offset: o.RawOffset})
		}

		body, err := json.Marshal(connectorOffsetsRaw{Offsets: entries})
		if err != nil {
			errorChan <- fmt.Errorf("marshaling offsets: %w", err)
			return
		}

		req, err := http.NewRequestWithContext(
			ctx, http.MethodPatch,
			c.BaseURL+"/connectors/"+name+"/offsets",
			bytes.NewReader(body),
		)
		if err != nil {
			errorChan <- fmt.Errorf("creating request: %w", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.HTTPClient.Do(req)
		if err != nil {
			errorChan <- fmt.Errorf("executing request: %w", err)
			return
		}
		defer func() { _ = resp.Body.Close() }()

		switch resp.StatusCode {
		case http.StatusOK, http.StatusCreated, http.StatusNoContent:
			resultChan <- true
		default:
			respBody, _ := io.ReadAll(resp.Body)
			errorChan <- fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
		}
	}()
}

// RestartTask restarts a specific task of a connector.
func (c *Client) RestartTask(name string, taskID int, resultChan chan<- bool, errorChan chan<- error) {
	c.doConnectorAction(name, http.MethodPost, fmt.Sprintf("tasks/%d/restart", taskID), resultChan, errorChan)
}

// GetConnectorOffsets fetches the current partition offsets for a connector.
func (c *Client) GetConnectorOffsets(name string, resultChan chan<- []ConnectorOffset, errorChan chan<- error) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.HTTPClient.Timeout)
		defer cancel()

		offsets, err := c.getConnectorOffsets(ctx, name)
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- offsets
	}()
}

func (c *Client) listConnectors(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/connectors", nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var connectors []string
	if err := json.NewDecoder(resp.Body).Decode(&connectors); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return connectors, nil
}

func (c *Client) getConnectorStatus(ctx context.Context, name string) (*ConnectorStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/connectors/"+name+"/status", nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var rawStatus connectorStatusRaw
	if err := json.NewDecoder(resp.Body).Decode(&rawStatus); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	status := &ConnectorStatus{
		Name: name,
		Connector: ConnectorStateInfo{
			State:    rawStatus.Connector.State,
			WorkerID: rawStatus.Connector.WorkerID,
		},
		Type: rawStatus.Type,
	}

	for _, t := range rawStatus.Tasks {
		status.Tasks = append(status.Tasks, TaskStateInfo{
			ID:       t.ID,
			State:    t.State,
			WorkerID: t.WorkerID,
			Trace:    t.Trace,
		})
	}

	return status, nil
}

func (c *Client) getConnectorConfig(ctx context.Context, name string) (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/connectors/"+name, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		Name   string                 `json:"name"`
		Config map[string]interface{} `json:"config"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	log.Debug().Str("connector", name).Msg("fetched connector config")
	return response.Config, nil
}

func (c *Client) getConnectorOffsets(ctx context.Context, name string) ([]ConnectorOffset, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/connectors/"+name+"/offsets", nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var raw connectorOffsetsRaw
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber() // preserve large offsets in their original textual form, avoiding float64 scientific notation
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	offsets := make([]ConnectorOffset, 0, len(raw.Offsets))
	for _, entry := range raw.Offsets {
		offsets = append(offsets, ConnectorOffset{
			TopicPartition: formatOffsetKey(entry.Partition),
			Offset:         formatOffsetValue(entry.Offset),
			Partition:      entry.Partition,
			RawOffset:      entry.Offset,
		})
	}

	log.Debug().Str("connector", name).Int("count", len(offsets)).Msg("fetched connector offsets")
	return offsets, nil
}

// formatOffsetKey renders a partition map as "topic:partition". Sink connectors
// expose "kafka_topic"/"kafka_partition" keys; for source connectors, which use
// connector-specific partition keys, it falls back to a sorted "key=value" join.
func formatOffsetKey(partition map[string]any) string {
	topic, hasTopic := partition["kafka_topic"]
	part, hasPartition := partition["kafka_partition"]
	if hasTopic && hasPartition {
		return fmt.Sprintf("%v:%v", topic, part)
	}
	return joinSorted(partition)
}

// formatOffsetValue renders an offset map as a plain value. Sink connectors expose
// a single "kafka_offset" key; source connectors may use custom offset keys, so it
// falls back to a sorted "key=value" join.
func formatOffsetValue(offset map[string]any) string {
	if value, ok := offset["kafka_offset"]; ok && len(offset) == 1 {
		return fmt.Sprintf("%v", value)
	}
	return joinSorted(offset)
}

func joinSorted(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, m[k]))
	}
	return strings.Join(parts, ",")
}

// connectorStatusRaw matches the raw JSON response from /connectors/{name}/status.
type connectorStatusRaw struct {
	Name      string `json:"name"`
	Connector struct {
		State    string `json:"state"`
		WorkerID string `json:"worker_id"`
	} `json:"connector"`
	Tasks []struct {
		ID       int    `json:"id"`
		State    string `json:"state"`
		WorkerID string `json:"worker_id"`
		Trace    string `json:"trace"`
	} `json:"tasks"`
	Type string `json:"type"`
}

// connectorOffsetEntry represents a single partition/offset pair as exchanged with
// the /connectors/{name}/offsets endpoint (both GET responses and PATCH requests).
type connectorOffsetEntry struct {
	Partition map[string]any `json:"partition"`
	Offset    map[string]any `json:"offset"`
}

// connectorOffsetsRaw matches the raw JSON response from /connectors/{name}/offsets.
type connectorOffsetsRaw struct {
	Offsets []connectorOffsetEntry `json:"offsets"`
}
