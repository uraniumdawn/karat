// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// documentIndent is the block indentation shared by the documents karat hands to
// the editor. The topic document's commented defaults appendix is indented to match, so
// uncommenting one of its lines by dropping the "# " prefix leaves it correctly nested
// under "configs:".
const documentIndent = 2

const createTopicDocumentHeader = `# karat topic definition — save and quit to review the topic, or quit without
# saving to create nothing. Nothing is created until the review is confirmed.
#
# Overrides go under "configs:", one per line and indented by two spaces:
#   configs:
#     cleanup.policy: compact
#     retention.ms: 604800000

`

const editTopicDocumentHeader = `# karat topic definition — save and quit to review the changes, or quit without
# saving to leave the topic alone. Nothing is applied until the review is confirmed.
#
# Only "partitions" (increase only) and "configs" may change; "name" and
# "replication_factor" are fixed. Deleting a config line — or clearing its
# value, as in "retention.ms:" — resets that setting to the cluster default.

`

// cloneTopicDocumentHeader is the header of the document a clone starts from: the source
// topic's definition, which becomes a new topic as soon as "name" is changed.
func cloneTopicDocumentHeader(source string) string {
	return fmt.Sprintf(`# karat topic definition — a copy of '%s'. Change "name" to the topic to create,
# then save and quit to review it, or quit without saving to create nothing.
# Nothing is created until the review is confirmed.
#
# Overrides go under "configs:", one per line and indented by two spaces:
#   configs:
#     cleanup.policy: compact
#     retention.ms: 604800000

`, source)
}

// topicDocument is the parsed form of the YAML buffer handed to the editor. Config values
// are kept as yaml.Node rather than decoded Go values so the scalar reaches Kafka
// exactly as it was typed, and so a malformed entry can be reported with its line.
type topicDocument struct {
	Name              string               `yaml:"name"`
	ReplicationFactor int                  `yaml:"replication_factor"`
	Partitions        int                  `yaml:"partitions"`
	Configs           map[string]yaml.Node `yaml:"configs"`
}

// topicDocumentValues is the rendered form of the same document, holding config values
// as the YAML scalars they are written out as.
type topicDocumentValues struct {
	Name              string         `yaml:"name"`
	ReplicationFactor int            `yaml:"replication_factor"`
	Partitions        int            `yaml:"partitions"`
	Configs           map[string]any `yaml:"configs"`
}

// renderTopicDocument builds the editor buffer for a topic: the header comment, the
// topic itself as YAML, and — when defaults is non-empty — the settings still at their
// cluster default appended as commented-out lines ready to be uncommented.
func renderTopicDocument(
	header string,
	name string,
	replicationFactor int,
	partitions int,
	configs map[string]string,
	defaults map[string]string,
) ([]byte, error) {
	body, err := marshalIndentedYAML(topicDocumentValues{
		Name:              name,
		ReplicationFactor: replicationFactor,
		Partitions:        partitions,
		Configs:           scalarConfigValues(configs),
	})
	if err != nil {
		return nil, err
	}

	// yaml.v3 writes an empty map as "configs: {}", which closes the block: an uncommented
	// line from the defaults appendix — or one typed by hand — would then sit under a
	// mapping that already has a value and the document would not parse. A key with no
	// value leaves the block open and still reads back as no overrides. Configs is the
	// last field of the document, so the empty rendering occurs at most once.
	if len(configs) == 0 {
		body = bytes.Replace(body, []byte("configs: {}\n"), []byte("configs:\n"), 1)
	}

	var buf bytes.Buffer
	buf.WriteString(header)
	buf.Write(body)

	if len(defaults) > 0 {
		block, err := marshalIndentedYAML(scalarConfigValues(defaults))
		if err != nil {
			return nil, err
		}
		indented := indentLines(strings.TrimRight(string(block), "\n"), documentIndent)

		buf.WriteString("\n")
		buf.WriteString(commentOut("settings at their cluster default — uncomment to override:"))
		buf.WriteString("\n")
		buf.WriteString(commentOut(indented))
		buf.WriteString("\n")
	}

	return buf.Bytes(), nil
}

// parseTopicDocument decodes an edited buffer. Unknown top-level keys and non-scalar
// config values are rejected; a config entry with no value is a request to reset that
// setting to the cluster default and is reported in removed rather than in configs.
func parseTopicDocument(
	b []byte,
) (doc topicDocument, configs map[string]string, removed []string, err error) {
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)

	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return doc, nil, nil, errors.New("document is empty")
		}
		return doc, nil, nil, cleanYAMLError(err)
	}

	configs = make(map[string]string, len(doc.Configs))
	for key, node := range doc.Configs {
		if node.Kind != yaml.ScalarNode {
			return doc, nil, nil, fmt.Errorf(
				"line %d: config '%s' must be a single value",
				node.Line,
				key,
			)
		}
		if node.Tag == nullTag {
			removed = append(removed, key)
			continue
		}
		configs[key] = node.Value
	}
	sort.Strings(removed)

	return doc, configs, removed, nil
}

// nullTag is the YAML tag resolved for an entry written without a value.
const nullTag = "!!null"

// validateTopicDocumentEdit rejects the changes an edited document may not carry: the
// topic name and replication factor are fixed, and Kafka can only grow a partition count.
func validateTopicDocumentEdit(
	doc topicDocument,
	name string,
	replicationFactor int,
	partitions int,
) error {
	if doc.Name != name {
		return fmt.Errorf("topic name cannot be changed ('%s' -> '%s')", name, doc.Name)
	}
	if doc.ReplicationFactor != replicationFactor {
		return fmt.Errorf(
			"replication factor cannot be changed (%d -> %d)",
			replicationFactor,
			doc.ReplicationFactor,
		)
	}
	if doc.Partitions < partitions {
		return fmt.Errorf(
			"partition count cannot be decreased (%d -> %d)",
			partitions,
			doc.Partitions,
		)
	}
	return nil
}

// validateCloneName rejects a clone document that still carries the source topic's name.
func validateCloneName(name, source string) error {
	if strings.TrimSpace(name) == source {
		return fmt.Errorf("clone name must differ from the source topic '%s'", source)
	}
	return nil
}

// removedConfigKeys returns the keys present in old but gone from edited, sorted — the
// overrides to reset to the cluster default.
func removedConfigKeys(old, edited map[string]string) []string {
	var removed []string
	for key := range old {
		if _, ok := edited[key]; !ok {
			removed = append(removed, key)
		}
	}
	sort.Strings(removed)
	return removed
}

// scalarConfigValues maps every config value through scalarConfigValue.
func scalarConfigValues(configs map[string]string) map[string]any {
	values := make(map[string]any, len(configs))
	for key, value := range configs {
		values[key] = scalarConfigValue(value)
	}
	return values
}

// scalarConfigValue renders a config value as the YAML scalar that parses back to the
// exact same string: a number or bool when that round trip is byte-identical, and the
// string itself otherwise. Kafka config values are strings on the wire — the only point
// here is that "10485760" reads as a number instead of as a quoted string.
func scalarConfigValue(v string) any {
	if i, err := strconv.ParseInt(v, 10, 64); err == nil && strconv.FormatInt(i, 10) == v {
		return i
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil &&
		strconv.FormatFloat(f, 'g', -1, 64) == v {
		return f
	}
	if v == "true" || v == "false" {
		return v == "true"
	}
	return v
}

// marshalIndentedYAML encodes v with the document's block indentation.
func marshalIndentedYAML(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(documentIndent)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// indentLines prefixes every non-empty line of s with n spaces.
func indentLines(s string, n int) string {
	pad := strings.Repeat(" ", n)
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		if line != "" {
			lines[i] = pad + line
		}
	}
	return strings.Join(lines, "\n")
}

// cleanYAMLError flattens a yaml.v3 error into a single status-line-friendly sentence
// and strips the Go type name it leaks when reporting an unknown key.
func cleanYAMLError(err error) error {
	msg := strings.TrimPrefix(err.Error(), "yaml: unmarshal errors:\n")
	msg = strings.TrimPrefix(msg, "yaml: ")

	var parts []string
	for _, line := range strings.Split(msg, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if i := strings.Index(line, " not found in type "); i >= 0 {
			line = line[:i] + " is not a valid key"
		}
		parts = append(parts, line)
	}

	return errors.New(strings.Join(parts, "; "))
}
