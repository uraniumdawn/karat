// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"maps"
	"slices"
	"strings"
	"testing"
)

func TestRenderTopicDocumentRoundTrip(t *testing.T) {
	// Values that a naive renderer would corrupt: '*' is a YAML alias, ',' and ':' need
	// care in plain scalars, and "1.0"/"1e9" must not be reformatted as numbers.
	configs := map[string]string{
		"cleanup.policy":     "compact,delete",
		"max.message.bytes":  "10485760",
		"leader.replication": "*",
		"dirty.ratio":        "0.5",
		"preallocate":        "true",
		"empty.value":        "",
		"trailing.zero":      "1.0",
		"exponent":           "1e9",
		"with.colon":         "a: b",
	}

	rendered, err := renderTopicDocument(editTopicDocumentHeader, "comments", 3, 12, configs, nil)
	if err != nil {
		t.Fatalf("renderTopicDocument() error = %v", err)
	}

	doc, got, removed, err := parseTopicDocument(rendered)
	if err != nil {
		t.Fatalf("parseTopicDocument() error = %v\ndocument:\n%s", err, rendered)
	}

	if doc.Name != "comments" || doc.ReplicationFactor != 3 || doc.Partitions != 12 {
		t.Errorf("got %+v, want name=comments replication_factor=3 partitions=12", doc)
	}
	if len(removed) != 0 {
		t.Errorf("removed = %v, want none", removed)
	}
	if !maps.Equal(got, configs) {
		t.Errorf("configs round trip = %v, want %v\ndocument:\n%s", got, configs, rendered)
	}
}

func TestRenderTopicDocumentNumbersAreUnquoted(t *testing.T) {
	rendered, err := renderTopicDocument(
		createTopicDocumentHeader,
		"comments",
		1,
		12,
		map[string]string{"max.message.bytes": "10485760"},
		nil,
	)
	if err != nil {
		t.Fatalf("renderTopicDocument() error = %v", err)
	}

	if !strings.Contains(string(rendered), "max.message.bytes: 10485760\n") {
		t.Errorf("want an unquoted number in:\n%s", rendered)
	}
}

func TestRenderTopicDocumentDefaultsAppendix(t *testing.T) {
	defaults := map[string]string{"retention.ms": "604800000", "segment.bytes": "1073741824"}

	tests := []struct {
		name    string
		configs map[string]string
	}{
		{
			name:    "topic with overrides",
			configs: map[string]string{"cleanup.policy": "compact"},
		},
		{
			// The common case: with no overrides the configs block must still be left open,
			// or an uncommented default lands under a "configs: {}" that already has a value.
			name:    "topic with no overrides",
			configs: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rendered, err := renderTopicDocument(
				editTopicDocumentHeader, "comments", 1, 12, tt.configs, defaults,
			)
			if err != nil {
				t.Fatalf("renderTopicDocument() error = %v", err)
			}

			// The appendix must be inert: parsing the document as rendered yields the
			// overrides only.
			_, got, _, err := parseTopicDocument(rendered)
			if err != nil {
				t.Fatalf("parseTopicDocument() error = %v", err)
			}
			if !maps.Equal(got, tt.configs) {
				t.Errorf("configs = %v, want %v", got, tt.configs)
			}

			// Uncommenting an appendix line must nest it under "configs:" — this is what
			// pins the appendix indentation to the document's own.
			uncommented := uncommentDefaults(t, string(rendered))
			_, got, _, err = parseTopicDocument([]byte(uncommented))
			if err != nil {
				t.Fatalf("parseTopicDocument() after uncommenting error = %v\ndocument:\n%s",
					err, uncommented)
			}

			want := maps.Clone(defaults)
			maps.Copy(want, tt.configs)
			if !maps.Equal(got, want) {
				t.Errorf("configs = %v, want %v\ndocument:\n%s", got, want, uncommented)
			}
		})
	}
}

// A document rendered with no overrides must leave "configs:" open rather than closing it
// with the "{}" yaml.v3 writes for an empty map, so overrides can be typed under it.
func TestRenderTopicDocumentLeavesEmptyConfigsOpen(t *testing.T) {
	rendered, err := renderTopicDocument(createTopicDocumentHeader, "new", 3, 6, nil, nil)
	if err != nil {
		t.Fatalf("renderTopicDocument() error = %v", err)
	}
	if strings.Contains(string(rendered), "configs: {}") {
		t.Fatalf("want an open configs block in:\n%s", rendered)
	}

	typed := string(rendered) + "  retention.ms: 604800000\n"
	_, got, _, err := parseTopicDocument([]byte(typed))
	if err != nil {
		t.Fatalf("parseTopicDocument() error = %v\ndocument:\n%s", err, typed)
	}
	if want := map[string]string{"retention.ms": "604800000"}; !maps.Equal(got, want) {
		t.Errorf("configs = %v, want %v", got, want)
	}
}

// uncommentDefaults strips the "# " prefix from the commented defaults block, the way a
// user would in the editor, and drops every other comment line.
func uncommentDefaults(t *testing.T, document string) string {
	t.Helper()

	var out []string
	inAppendix := false
	for _, line := range strings.Split(document, "\n") {
		if strings.Contains(line, "cluster default") {
			inAppendix = true
			continue
		}
		if inAppendix && strings.HasPrefix(line, "# ") {
			out = append(out, strings.TrimPrefix(line, "# "))
			continue
		}
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		out = append(out, line)
	}

	if !inAppendix {
		t.Fatalf("no defaults appendix found in:\n%s", document)
	}

	return strings.Join(out, "\n")
}

// TestEditSessionRoundTrip walks the whole edit path minus tview and Kafka: render the
// topic, apply the four edits a user actually makes in the editor, then parse, validate
// and derive the resets exactly as Edit Topic does on submit.
func TestEditSessionRoundTrip(t *testing.T) {
	current := map[string]string{
		"cleanup.policy":    "compact",
		"max.message.bytes": "10485760",
		"retention.ms":      "604800000",
	}
	defaults := map[string]string{"segment.bytes": "1073741824"}

	rendered, err := renderTopicDocument(editTopicDocumentHeader, "comments", 3, 12, current, defaults)
	if err != nil {
		t.Fatalf("renderTopicDocument() error = %v", err)
	}

	var edited []string
	for _, line := range strings.Split(string(rendered), "\n") {
		switch {
		case line == "partitions: 12":
			edited = append(edited, "partitions: 16") // grow the topic
		case strings.TrimSpace(line) == "cleanup.policy: compact":
			edited = append(edited, "  cleanup.policy: delete") // change a value
		case strings.Contains(line, "max.message.bytes"):
			continue // drop an override
		case strings.Contains(line, "segment.bytes"):
			edited = append(edited, strings.TrimPrefix(line, "# ")) // adopt a default
		default:
			edited = append(edited, line)
		}
	}

	document := strings.Join(edited, "\n")
	doc, configs, _, err := parseTopicDocument([]byte(document))
	if err != nil {
		t.Fatalf("parseTopicDocument() error = %v\ndocument:\n%s", err, document)
	}
	if err := validateTopicDocumentEdit(doc, "comments", 3, 12); err != nil {
		t.Fatalf("validateTopicDocumentEdit() error = %v", err)
	}

	if doc.Partitions != 16 {
		t.Errorf("partitions = %d, want 16", doc.Partitions)
	}

	wantConfigs := map[string]string{
		"cleanup.policy": "delete",
		"retention.ms":   "604800000",
		"segment.bytes":  "1073741824",
	}
	if !maps.Equal(configs, wantConfigs) {
		t.Errorf("configs = %v, want %v\ndocument:\n%s", configs, wantConfigs, document)
	}

	// The dropped override is what gets reset; adopting a default is a plain set.
	if got := removedConfigKeys(current, configs); !slices.Equal(got, []string{"max.message.bytes"}) {
		t.Errorf("removedConfigKeys() = %v, want [max.message.bytes]", got)
	}
}

// A clone starts from the source topic's document: renaming it is the only edit needed,
// and the shape and overrides must come back untouched.
func TestCloneSessionRoundTrip(t *testing.T) {
	source := map[string]string{
		"cleanup.policy": "compact",
		"retention.ms":   "604800000",
		"follower.replication.throttled.replicas": "",
	}

	rendered, err := renderTopicDocument(
		cloneTopicDocumentHeader("comments"),
		"comments",
		3,
		12,
		source,
		map[string]string{"segment.bytes": "1073741824"},
	)
	if err != nil {
		t.Fatalf("renderTopicDocument() error = %v", err)
	}

	document := strings.Replace(string(rendered), "name: comments", "name: comments-copy", 1)
	doc, configs, removed, err := parseTopicDocument([]byte(document))
	if err != nil {
		t.Fatalf("parseTopicDocument() error = %v\ndocument:\n%s", err, document)
	}
	if len(removed) > 0 {
		t.Errorf("removed = %v, want nothing removed by an untouched clone", removed)
	}
	if doc.Name != "comments-copy" || doc.ReplicationFactor != 3 || doc.Partitions != 12 {
		t.Errorf("doc = %+v, want comments-copy/3/12", doc)
	}
	if !maps.Equal(configs, source) {
		t.Errorf("configs = %v, want %v\ndocument:\n%s", configs, source, document)
	}

	if err := validateCloneName(doc.Name, "comments"); err != nil {
		t.Errorf("validateCloneName() error = %v", err)
	}
	if err := validateCloneName(" comments ", "comments"); err == nil {
		t.Error("validateCloneName() accepted the source topic's name")
	}
}

func TestParseTopicDocument(t *testing.T) {
	tests := []struct {
		name        string
		document    string
		wantConfigs map[string]string
		wantRemoved []string
		wantErr     string
	}{
		{
			name:        "empty document",
			document:    "  \n\n",
			wantErr:     "document is empty",
			wantConfigs: nil,
		},
		{
			name:     "no configs key",
			document: "name: a\nreplication_factor: 1\npartitions: 3\n",
			// An absent configs block means no overrides; the reset is derived by diffing.
			wantConfigs: map[string]string{},
		},
		{
			name:        "empty configs block",
			document:    "name: a\nreplication_factor: 1\npartitions: 3\nconfigs: {}\n",
			wantConfigs: map[string]string{},
		},
		{
			name: "null value is a reset",
			document: "name: a\nreplication_factor: 1\npartitions: 3\n" +
				"configs:\n  retention.ms:\n  cleanup.policy: compact\n",
			wantConfigs: map[string]string{"cleanup.policy": "compact"},
			wantRemoved: []string{"retention.ms"},
		},
		{
			name: "explicit empty string is a value",
			document: "name: a\nreplication_factor: 1\npartitions: 3\n" +
				"configs:\n  leader.replication: \"\"\n",
			wantConfigs: map[string]string{"leader.replication": ""},
		},
		{
			name: "scalars keep their exact text",
			document: "name: a\nreplication_factor: 1\npartitions: 3\n" +
				"configs:\n  a: 10485760\n  b: 0.5\n  c: true\n  d: '*'\n",
			wantConfigs: map[string]string{"a": "10485760", "b": "0.5", "c": "true", "d": "*"},
		},
		{
			name:     "unknown key",
			document: "name: a\nreplication_factor: 1\npartition: 3\n",
			wantErr:  "is not a valid key",
		},
		{
			name: "non-scalar config value",
			document: "name: a\nreplication_factor: 1\npartitions: 3\n" +
				"configs:\n  nested:\n    a: b\n",
			wantErr: "must be a single value",
		},
		{
			name:     "malformed yaml",
			document: "name: a\n  partitions: 3\n",
			wantErr:  "line",
		},
		{
			name:     "wrong type",
			document: "name: a\nreplication_factor: one\npartitions: 3\n",
			wantErr:  "line",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, configs, removed, err := parseTopicDocument([]byte(tt.document))

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseTopicDocument() error = nil, want one containing %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf(
						"parseTopicDocument() error = %q, want it to contain %q",
						err,
						tt.wantErr,
					)
				}
				if strings.Contains(err.Error(), "ui.topicDocument") {
					t.Errorf("parseTopicDocument() error leaks the Go type name: %q", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("parseTopicDocument() error = %v", err)
			}
			if !maps.Equal(configs, tt.wantConfigs) {
				t.Errorf("configs = %v, want %v", configs, tt.wantConfigs)
			}
			if !slices.Equal(removed, tt.wantRemoved) {
				t.Errorf("removed = %v, want %v", removed, tt.wantRemoved)
			}
		})
	}
}

func TestValidateTopicDocumentEdit(t *testing.T) {
	tests := []struct {
		name    string
		doc     topicDocument
		wantErr string
	}{
		{
			name: "unchanged",
			doc:  topicDocument{Name: "comments", ReplicationFactor: 3, Partitions: 12},
		},
		{
			name: "partitions increased",
			doc:  topicDocument{Name: "comments", ReplicationFactor: 3, Partitions: 16},
		},
		{
			name:    "name changed",
			doc:     topicDocument{Name: "orders", ReplicationFactor: 3, Partitions: 12},
			wantErr: "topic name cannot be changed ('comments' -> 'orders')",
		},
		{
			name:    "replication factor changed",
			doc:     topicDocument{Name: "comments", ReplicationFactor: 1, Partitions: 12},
			wantErr: "replication factor cannot be changed (3 -> 1)",
		},
		{
			name:    "partitions decreased",
			doc:     topicDocument{Name: "comments", ReplicationFactor: 3, Partitions: 8},
			wantErr: "partition count cannot be decreased (12 -> 8)",
		},
		{
			name: "name checked before replication factor",
			doc:  topicDocument{Name: "orders", ReplicationFactor: 1, Partitions: 8},
			// The first thing the user changed is the first thing they hear about.
			wantErr: "topic name cannot be changed ('comments' -> 'orders')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTopicDocumentEdit(tt.doc, "comments", 3, 12)

			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateTopicDocumentEdit() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateTopicDocumentEdit() error = nil, want %q", tt.wantErr)
			}
			if err.Error() != tt.wantErr {
				t.Errorf("validateTopicDocumentEdit() error = %q, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestRemovedConfigKeys(t *testing.T) {
	tests := []struct {
		name   string
		old    map[string]string
		edited map[string]string
		want   []string
	}{
		{
			name:   "removed keys are sorted",
			old:    map[string]string{"a": "1", "b": "2", "c": "3"},
			edited: map[string]string{"b": "2"},
			want:   []string{"a", "c"},
		},
		{
			name:   "changed value is not a removal",
			old:    map[string]string{"a": "1"},
			edited: map[string]string{"a": "2"},
		},
		{
			name:   "added keys are ignored",
			old:    map[string]string{"a": "1"},
			edited: map[string]string{"a": "1", "b": "2"},
		},
		{
			name:   "cleared config removes everything",
			old:    map[string]string{"a": "1", "b": "2"},
			edited: map[string]string{},
			want:   []string{"a", "b"},
		},
		{
			name:   "no previous overrides",
			old:    map[string]string{},
			edited: map[string]string{"a": "1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := removedConfigKeys(tt.old, tt.edited); !slices.Equal(got, tt.want) {
				t.Errorf("removedConfigKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScalarConfigValue(t *testing.T) {
	tests := []struct {
		value string
		want  any
	}{
		{"10485760", int64(10485760)},
		{"-1", int64(-1)},
		{"0.5", 0.5},
		{"true", true},
		{"false", false},
		{"compact", "compact"},
		{"", ""},
		{"*", "*"},
		// Reformatting these would change the value Kafka receives, so they stay strings.
		{"1.0", "1.0"},
		{"1e9", "1e9"},
		{"007", "007"},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if got := scalarConfigValue(tt.value); got != tt.want {
				t.Errorf("scalarConfigValue(%q) = %#v, want %#v", tt.value, got, tt.want)
			}
		})
	}
}

func TestUpdatedTopicConfigMessage(t *testing.T) {
	if got := updatedTopicConfigMessage("comments", 0); got != "topic 'comments' config has been updated" {
		t.Errorf("updatedTopicConfigMessage() = %q", got)
	}
	want := "topic 'comments' config has been updated (2 reset to default)"
	if got := updatedTopicConfigMessage("comments", 2); got != want {
		t.Errorf("updatedTopicConfigMessage() = %q, want %q", got, want)
	}
}

func TestTopicParamsValidate(t *testing.T) {
	tests := []struct {
		name    string
		params  TopicParams
		wantErr string
	}{
		{
			name:   "valid",
			params: TopicParams{TopicName: "comments", ReplicationFactor: 3, Partitions: 12},
		},
		{
			name:    "blank name",
			params:  TopicParams{TopicName: "  ", ReplicationFactor: 3, Partitions: 12},
			wantErr: "topic name cannot be empty",
		},
		{
			name:    "zero replication factor",
			params:  TopicParams{TopicName: "comments", Partitions: 12},
			wantErr: "replication factor must be greater than 0",
		},
		{
			name:    "zero partitions",
			params:  TopicParams{TopicName: "comments", ReplicationFactor: 3},
			wantErr: "partitions must be greater than 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.params.validate()

			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() error = %v, want nil", err)
				}
				return
			}
			if err == nil || err.Error() != tt.wantErr {
				t.Errorf("validate() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}
