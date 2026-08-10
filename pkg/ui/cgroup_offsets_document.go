// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/uraniumdawn/karat/pkg/client"
)

// offsetValueLegend is the part both offsets headers share: what a value may be. Kept
// separate so the two documents cannot drift apart on the syntax they accept.
const offsetValueLegend = `# A value is an absolute offset, or one of:
#   earliest | latest | @<timestamp> | none
# where <timestamp> is 2006-01-02T15:04:05.000, RFC3339, or unix milliseconds
# (no zone means UTC). "to-earliest" and "to-latest" are accepted too, matching
# the kafka-consumer-groups spelling.
`

// offsetsSeedNote explains the one thing neither document lists up front: a topic the
// group has never consumed.
const offsetsSeedNote = `# A topic the group has never consumed can be added by naming it under "topics:";
# its partitions come from cluster metadata and start with no committed offset.
`

const consumerGroupOffsetsHeader = `# karat consumer group offsets, one line per partition — save and quit to review
# the changes, or quit without saving to leave the group alone.
#
` + offsetValueLegend + `# "none" leaves the partition untouched.
#
# One value on a topic applies to all of its partitions. Add
#   all: <value>
# above "topics:" to set every topic at once; entries under "topics:" override it.
#
# A partition you delete is left untouched — unlike a topic config there is no
# default to fall back to, so absence means "do not touch".
#
` + offsetsSeedNote + `
`

const consumerGroupTopicOffsetsHeader = `# karat consumer group offsets, one line per topic — save and quit to review the
# changes, or quit without saving to leave the group alone.
#
# A topic's value applies to every partition of it, so each topic starts as "none",
# which leaves it untouched. Replace the ones you mean to move.
#
` + offsetValueLegend + `#
# Add
#   all: <value>
# above "topics:" to set every topic at once; entries under "topics:" override it.
#
` + offsetsSeedNote + `
`

// consumerGroupOffsetsDocument is the parsed form of the offsets buffer. All and Topics
// are kept as yaml.Node so both shapes a topic may take — a single value or a
// per-partition mapping — can be told apart, and so every entry can be reported with its
// own line.
type consumerGroupOffsetsDocument struct {
	Group  string    `yaml:"group"`
	All    yaml.Node `yaml:"all"`
	Topics yaml.Node `yaml:"topics"`
}

// renderConsumerGroupOffsetsDocument builds the editor buffer: the header, the group
// name, and every committed partition with its offset. Partitions the group has never
// committed on are not listed — there is nothing to change on them.
func renderConsumerGroupOffsetsDocument(
	group string,
	offsets []client.CommittedOffset,
) ([]byte, error) {
	topics := &yaml.Node{Kind: yaml.MappingNode}

	var partitions *yaml.Node
	currentTopic := ""
	for i, offset := range offsets {
		if i == 0 || offset.TopicPartition.Topic != currentTopic {
			currentTopic = offset.TopicPartition.Topic
			partitions = &yaml.Node{Kind: yaml.MappingNode}
			topics.Content = append(
				topics.Content,
				stringNode(currentTopic),
				partitions,
			)
		}

		partitions.Content = append(
			partitions.Content,
			intNode(int64(offset.TopicPartition.Partition)),
			intNode(offset.Committed),
		)
	}

	// A group with nothing committed renders as "topics: {}", which reads as "there is
	// nothing here" when it is in fact the place to seed one.
	if len(topics.Content) == 0 {
		topics.LineComment = "no committed offsets — list topics here to seed them"
	}

	body, err := marshalIndentedYAML(&yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			stringNode("group"), stringNode(group),
			stringNode("topics"), topics,
		},
	})
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.WriteString(consumerGroupOffsetsHeader)
	buf.Write(body)

	return buf.Bytes(), nil
}

// renderConsumerGroupTopicOffsetsDocument builds the editor buffer for the per-topic
// document: the header, the group name, and every topic the group has committed on as a
// single "none" waiting to be replaced.
func renderConsumerGroupTopicOffsetsDocument(
	group string,
	offsets []client.CommittedOffset,
) ([]byte, error) {
	topics := &yaml.Node{Kind: yaml.MappingNode}
	for _, topic := range newOffsetScope(offsets, nil).committedTopics() {
		topics.Content = append(topics.Content, stringNode(topic), stringNode("none"))
	}

	if len(topics.Content) == 0 {
		topics.LineComment = "no committed offsets — list topics here to seed them"
	}

	body, err := marshalIndentedYAML(&yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			stringNode("group"), stringNode(group),
			stringNode("topics"), topics,
		},
	})
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.WriteString(consumerGroupTopicOffsetsHeader)
	buf.Write(body)

	return buf.Bytes(), nil
}

// stringNode builds a scalar that stays a string, so a group or topic named "123" is
// quoted rather than emitted as a number.
func stringNode(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}
}

// intNode builds a plain integer scalar.
func intNode(value int64) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.FormatInt(value, 10)}
}

// parseConsumerGroupOffsetsDocument decodes an edited buffer into one target per
// partition, checked against the partitions the group has actually committed on. The
// group name is fixed; topics and partitions the group does not have are rejected rather
// than silently dropped, since either is a typo the user wants to hear about.
func parseConsumerGroupOffsetsDocument(
	b []byte,
	group string,
	committed []client.CommittedOffset,
	partitionCounts map[string]int,
) (map[client.TopicPartition]client.OffsetTarget, error) {
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)

	var doc consumerGroupOffsetsDocument
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("document is empty")
		}
		return nil, cleanYAMLError(err)
	}

	if doc.Group != group {
		return nil, fmt.Errorf("group cannot be changed ('%s' -> '%s')", group, doc.Group)
	}

	scope := newOffsetScope(committed, partitionCounts)
	targets := make(map[client.TopicPartition]client.OffsetTarget)

	// "all" is the base every topic starts from; entries under "topics" then override it.
	if err := applyAllTarget(&doc.All, scope, targets); err != nil {
		return nil, err
	}

	if doc.Topics.Kind == 0 {
		return targets, nil
	}
	if doc.Topics.Kind != yaml.MappingNode {
		return nil, fmt.Errorf(
			"line %d: topics must be a mapping of topic to offsets",
			doc.Topics.Line,
		)
	}

	for i := 0; i+1 < len(doc.Topics.Content); i += 2 {
		key, value := doc.Topics.Content[i], doc.Topics.Content[i+1]
		topic := key.Value

		partitions, ok := scope.partitions(topic)
		if !ok {
			return nil, fmt.Errorf(
				"line %d: topic '%s' does not exist on the cluster",
				key.Line,
				topic,
			)
		}

		switch value.Kind {
		case yaml.ScalarNode:
			target, keep, err := parseOffsetTarget(value)
			if err != nil {
				return nil, err
			}
			for _, partition := range partitions {
				tp := client.TopicPartition{Topic: topic, Partition: partition}
				if keep {
					targets[tp] = target
				} else {
					delete(targets, tp)
				}
			}

		case yaml.MappingNode:
			if err := parsePartitionTargets(value, topic, partitions, targets); err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf(
				"line %d: topic '%s' must be one value or a partition mapping",
				value.Line,
				topic,
			)
		}
	}

	return targets, nil
}

// offsetScope answers which partitions a document may address for a topic. Cluster
// metadata wins when it has the topic, so naming a whole topic covers partitions the
// group has not committed on yet — including every partition of a topic it has never
// consumed. The group's own partitions are the fallback for a topic that has since been
// deleted from the cluster but still has offsets committed.
type offsetScope struct {
	committed       map[string][]int32
	partitionCounts map[string]int
}

func newOffsetScope(
	committed []client.CommittedOffset,
	partitionCounts map[string]int,
) *offsetScope {
	byTopic := make(map[string][]int32)
	for _, offset := range committed {
		topic := offset.TopicPartition.Topic
		byTopic[topic] = append(byTopic[topic], offset.TopicPartition.Partition)
	}

	return &offsetScope{committed: byTopic, partitionCounts: partitionCounts}
}

// partitions returns the addressable partitions of a topic, sorted, and whether the topic
// is known at all.
func (s *offsetScope) partitions(topic string) ([]int32, bool) {
	if count, ok := s.partitionCounts[topic]; ok && count > 0 {
		partitions := make([]int32, count)
		for i := range partitions {
			partitions[i] = int32(i)
		}
		return partitions, true
	}

	if partitions, ok := s.committed[topic]; ok {
		sorted := append([]int32(nil), partitions...)
		slices.Sort(sorted)
		return sorted, true
	}

	return nil, false
}

// committedTopics returns the topics the group has committed offsets for, sorted. These
// are the topics "all" covers — seeding a brand new topic has to be asked for by name.
func (s *offsetScope) committedTopics() []string {
	topics := make([]string, 0, len(s.committed))
	for topic := range s.committed {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

// applyAllTarget seeds targets from the document's "all" value, if it has one.
func applyAllTarget(
	node *yaml.Node,
	scope *offsetScope,
	targets map[client.TopicPartition]client.OffsetTarget,
) error {
	if node.Kind == 0 || node.Tag == nullTag {
		return nil
	}

	target, keep, err := parseOffsetTarget(node)
	if err != nil {
		return err
	}
	if !keep {
		return nil
	}

	for _, topic := range scope.committedTopics() {
		partitions, ok := scope.partitions(topic)
		if !ok {
			continue
		}
		for _, partition := range partitions {
			targets[client.TopicPartition{Topic: topic, Partition: partition}] = target
		}
	}

	return nil
}

// parsePartitionTargets reads a topic's per-partition mapping into targets.
func parsePartitionTargets(
	node *yaml.Node,
	topic string,
	known []int32,
	targets map[client.TopicPartition]client.OffsetTarget,
) error {
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]

		partition, err := strconv.ParseInt(key.Value, 10, 32)
		if err != nil {
			return fmt.Errorf("line %d: '%s' is not a partition number", key.Line, key.Value)
		}
		if !slices.Contains(known, int32(partition)) {
			return fmt.Errorf(
				"line %d: %s has no partition %d",
				key.Line,
				topic,
				partition,
			)
		}

		target, keep, err := parseOffsetTarget(value)
		if err != nil {
			return err
		}

		tp := client.TopicPartition{Topic: topic, Partition: int32(partition)}
		if keep {
			targets[tp] = target
		} else {
			delete(targets, tp)
		}
	}

	return nil
}

// parseOffsetTarget reads one requested offset: an absolute value, a watermark keyword,
// an "@"-prefixed timestamp, or "none". keep is false for "none", which leaves the
// partitions it covers untouched — what makes an "all" value narrowable.
func parseOffsetTarget(node *yaml.Node) (target client.OffsetTarget, keep bool, err error) {
	if node.Kind != yaml.ScalarNode {
		return target, false, fmt.Errorf("line %d: expected a single value", node.Line)
	}

	value := strings.TrimSpace(node.Value)
	switch value {
	case "none":
		return target, false, nil
	// The "to-" spellings are the kafka-consumer-groups strategy names, accepted so a
	// reset expressed the CLI way needs no translation.
	case "earliest", "to-earliest":
		return client.OffsetTarget{Kind: client.OffsetTargetEarliest}, true, nil
	case "latest", "to-latest":
		return client.OffsetTarget{Kind: client.OffsetTargetLatest}, true, nil
	case "to-offset":
		return target, false, fmt.Errorf(
			"line %d: write the offset itself, as in '900'",
			node.Line,
		)
	case "to-timestamp":
		return target, false, fmt.Errorf(
			"line %d: write the timestamp itself, as in '@2026-08-01T00:00:00.000'",
			node.Line,
		)
	}

	if after, found := strings.CutPrefix(value, "@"); found {
		ms, err := parseTimestampMs(after)
		if err != nil {
			return target, false, fmt.Errorf("line %d: %s", node.Line, err.Error())
		}
		return client.OffsetTarget{Kind: client.OffsetTargetTimestamp, TimestampMs: ms}, true, nil
	}

	offset, err := strconv.ParseInt(value, 10, 64)
	if err != nil || offset < 0 {
		return target, false, fmt.Errorf(
			"line %d: '%s' is not an offset, 'earliest', 'latest', '@<timestamp>' or 'none'",
			node.Line,
			node.Value,
		)
	}

	return client.OffsetTarget{Kind: client.OffsetTargetAbsolute, Offset: offset}, true, nil
}

// parseTimestampMs reads unix milliseconds, or one of the accepted timestamp layouts.
func parseTimestampMs(value string) (int64, error) {
	if ms, err := strconv.ParseInt(value, 10, 64); err == nil {
		if ms < 0 {
			return 0, fmt.Errorf("'%s' is not a timestamp", value)
		}
		return ms, nil
	}

	t, err := parseTimestamp(value)
	if err != nil {
		return 0, fmt.Errorf("'%s' is not a timestamp", value)
	}

	return t.UnixMilli(), nil
}

// offsetChange is one partition's pending change, as shown on the confirmation page.
type offsetChange struct {
	TopicPartition client.TopicPartition
	From           int64
	// HasFrom is false for a partition the group has never committed on — a topic being
	// seeded, or one that gained partitions. Its From is meaningless and must not be
	// shown as offset 0, which would read as a rewind rather than a first commit.
	HasFrom bool
	To      int64
	// Note names the watermark or timestamp a non-literal value resolved through, so the
	// confirmation shows both what was asked for and what it came out as.
	Note    string
	InRange bool
}

// fromText renders the change's starting point for the confirmation page.
func (c offsetChange) fromText() string {
	if !c.HasFrom {
		return "(none)"
	}
	return strconv.FormatInt(c.From, 10)
}

// offsetChanges turns resolved targets into the changes to apply, dropping the partitions
// whose offset is already where it was asked to be. The result is ordered by topic then
// partition, matching the document.
func offsetChanges(
	committed []client.CommittedOffset,
	targets map[client.TopicPartition]client.OffsetTarget,
	resolved map[client.TopicPartition]client.ResolvedOffset,
) []offsetChange {
	current := make(map[client.TopicPartition]int64, len(committed))
	for _, offset := range committed {
		current[offset.TopicPartition] = offset.Committed
	}

	changes := make([]offsetChange, 0, len(resolved))
	for tp, offset := range resolved {
		from, ok := current[tp]
		if ok && from == offset.Target {
			continue
		}
		changes = append(changes, offsetChange{
			TopicPartition: tp,
			From:           from,
			HasFrom:        ok,
			To:             offset.Target,
			Note:           offsetTargetNote(targets[tp]),
			InRange:        offset.InRange(),
		})
	}

	sort.Slice(changes, func(i, j int) bool {
		if changes[i].TopicPartition.Topic != changes[j].TopicPartition.Topic {
			return changes[i].TopicPartition.Topic < changes[j].TopicPartition.Topic
		}
		return changes[i].TopicPartition.Partition < changes[j].TopicPartition.Partition
	})

	return changes
}

// offsetTargetNote describes a target that was not written as a literal offset.
func offsetTargetNote(target client.OffsetTarget) string {
	switch target.Kind {
	case client.OffsetTargetEarliest:
		return "earliest"
	case client.OffsetTargetLatest:
		return "latest"
	case client.OffsetTargetTimestamp:
		return "@" + strconv.FormatInt(target.TimestampMs, 10)
	default:
		return ""
	}
}

// outOfRangeChanges returns the changes that fall outside their partition's watermarks.
func outOfRangeChanges(changes []offsetChange) []offsetChange {
	var invalid []offsetChange
	for _, change := range changes {
		if !change.InRange {
			invalid = append(invalid, change)
		}
	}
	return invalid
}

// outOfRangeListLimit is how many offending partitions the out-of-range message names
// before summarising the rest — the status line is one line.
const outOfRangeListLimit = 3

// outOfRangeMessage names the partitions whose target sits outside their partition's log,
// with the bounds it had to fall between.
func outOfRangeMessage(
	invalid []offsetChange,
	resolved map[client.TopicPartition]client.ResolvedOffset,
) string {
	listed := invalid
	if len(listed) > outOfRangeListLimit {
		listed = listed[:outOfRangeListLimit]
	}

	parts := make([]string, 0, len(listed))
	for _, change := range listed {
		bounds := resolved[change.TopicPartition]
		parts = append(parts, fmt.Sprintf(
			"%s[%d] %d not in [%d, %d]",
			change.TopicPartition.Topic,
			change.TopicPartition.Partition,
			change.To,
			bounds.Earliest,
			bounds.Latest,
		))
	}

	message := "offset out of range: " + strings.Join(parts, ", ")
	if remaining := len(invalid) - len(listed); remaining > 0 {
		message += fmt.Sprintf(" (and %d more)", remaining)
	}

	return message
}

// renderOffsetChanges formats the pending changes for the confirmation page.
func renderOffsetChanges(group string, changes []offsetChange) string {
	var sb strings.Builder

	fmt.Fprintf(&sb, "Consumer group: %s\n\n", group)
	for _, change := range changes {
		fmt.Fprintf(&sb, "  %s[%d]  %s -> %d",
			change.TopicPartition.Topic,
			change.TopicPartition.Partition,
			change.fromText(),
			change.To,
		)
		if change.Note != "" {
			fmt.Fprintf(&sb, " (%s)", change.Note)
		}
		sb.WriteString("\n")
	}

	fmt.Fprintf(&sb, "\n%d partition%s will be committed.", len(changes), pluralSuffix(len(changes)))

	return sb.String()
}

// pluralSuffix returns the "s" that makes a count read correctly.
func pluralSuffix(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
